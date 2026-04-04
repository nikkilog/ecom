import base64
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from gspread_dataframe import get_as_dataframe


JOB_NAME = "edit_refs"

ALLOWED_OPS = {"LINK", "UNLINK", "REPLACE_ALL"}
SUPPORTED_ENTITY_TYPES = {"PRODUCT", "COLLECTION", "PAGE"}
OPTIONAL_ENTITY_TYPES = {"ORDER"}  # 预留，不默认启用

RUNLOG_HEADERS = [
    "run_id",
    "ts_cn",
    "job_name",
    "phase",
    "log_type",
    "status",
    "site_code",
    "entity_type",
    "gid",
    "field_key",
    "rows_loaded",
    "rows_pending",
    "rows_recognized",
    "rows_planned",
    "rows_written",
    "rows_skipped",
    "message",
    "error_reason",
]


# =========================================================
# 基础工具
# =========================================================
def now_cn_str() -> str:
    tz_cn = timezone(timedelta(hours=8))
    return datetime.now(tz_cn).strftime("%Y-%m-%d %H:%M:%S")


def make_run_id(prefix: str = "refs") -> str:
    return time.strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _norm_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _is_gid(s: str) -> bool:
    return _norm_str(s).startswith("gid://shopify/")


def _is_numeric(s: str) -> bool:
    s = _norm_str(s)
    return bool(s) and s.isdigit()


def _truncate(s: Any, n: int = 500) -> str:
    x = _norm_str(s)
    return x if len(x) <= n else x[: n - 3] + "..."


def _safe_json_dumps(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False)


def _dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items:
        v = _norm_str(x)
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _normalize_bool(x: Any) -> bool:
    s = _norm_str(x).lower()
    return s in {"1", "true", "yes", "y", "on"}


# =========================================================
# Google / Shopify clients
# =========================================================
def build_gspread_client_from_b64(sa_b64: str):
    sa_info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    return gspread.service_account_from_dict(sa_info)


def shopify_gql(
    shop_domain: str,
    api_version: str,
    access_token: str,
    query: str,
    variables: Optional[dict] = None,
    timeout: int = 60,
) -> dict:
    url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables or {}}
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"GraphQL HTTP {resp.status_code}: {resp.text[:800]}")
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError("GraphQL errors: " + _safe_json_dumps(data["errors"][:3]))
    return data.get("data") or {}


# =========================================================
# 表 / 配置
# =========================================================
def get_or_create_ws(spreadsheet, title: str, headers: List[str], rows: int = 5000, cols: int = 24):
    try:
        ws = spreadsheet.worksheet(title)
    except Exception:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=max(cols, len(headers) + 2))

    try:
        a1 = ws.acell("A1").value
    except Exception:
        a1 = None

    if not a1:
        ws.update(values=[headers], range_name="A1")
    return ws


def append_rows_safe(ws, rows: List[List[Any]]):
    if rows:
        ws.append_rows(rows, value_input_option="RAW")


def read_ws_df(spreadsheet, worksheet_title: str) -> pd.DataFrame:
    ws = spreadsheet.worksheet(worksheet_title)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str).fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def find_sheet_url_by_label(gc, console_core_url: str, site_code: str, label: str, cfg_tab_sites: str = "Cfg__Sites") -> str:
    sh = gc.open_by_url(console_core_url)
    df = read_ws_df(sh, cfg_tab_sites)

    need = {"site_code", "sheet_url", "label"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"Cfg__Sites missing columns: {missing}")

    site_code = _norm_str(site_code).upper()
    label = _norm_str(label)

    x = df.copy()
    x["site_code"] = x["site_code"].astype(str).str.strip().str.upper()
    x["label"] = x["label"].astype(str).str.strip()

    hit = x[(x["site_code"] == site_code) & (x["label"] == label)].copy()
    if hit.empty:
        raise RuntimeError(f"Cfg__Sites not found for site_code={site_code}, label={label}")

    url = _norm_str(hit.iloc[0]["sheet_url"])
    if not url:
        raise RuntimeError(f"Cfg__Sites sheet_url empty for site_code={site_code}, label={label}")
    return url


def build_type_map_from_cfg(cfg_df: pd.DataFrame) -> Dict[str, str]:
    def normalize_mf_type(data_type: str) -> str:
        t = _norm_str(data_type).lower()
        if t in {"reference", "metaobject_reference"}:
            return "metaobject_reference"
        if t in {"list.reference", "list.metaobject_reference"}:
            return "list.metaobject_reference"

        t2 = t.replace("_", ".").replace("-", ".")
        if t2 == "reference":
            return "metaobject_reference"
        if t2.startswith("list.") and "reference" in t2:
            return "list.metaobject_reference"
        return ""

    if "data_type" not in cfg_df.columns:
        raise RuntimeError("Cfg__Fields missing column: data_type")

    has_field_id = "field_id" in cfg_df.columns
    has_field_key = "field_key" in cfg_df.columns
    if not (has_field_id or has_field_key):
        raise RuntimeError("Cfg__Fields missing column: field_id or field_key")

    out: Dict[str, str] = {}
    for _, r in cfg_df.iterrows():
        mf_type = normalize_mf_type(r.get("data_type", ""))
        if not mf_type:
            continue

        if has_field_id:
            fid = _norm_str(r.get("field_id", ""))
            if fid:
                out[fid] = mf_type
                if "|" in fid:
                    try:
                        _, fk = parse_field_ref(fid, None)
                        if fk:
                            out[fk] = mf_type
                    except Exception:
                        pass

        if has_field_key:
            fk = _norm_str(r.get("field_key", ""))
            if fk:
                out[fk] = mf_type

    return out


# =========================================================
# 字段 / 值解析
# =========================================================
def parse_field_ref(field_ref: str, from_entity_type: Optional[str]) -> Tuple[str, str]:
    """
    支持：
      mf.custom.xxx
      PRODUCT|mf.custom.xxx
      COLLECTION|mf.custom.xxx
      PAGE|mf.custom.xxx
      ORDER|mf.custom.xxx
    返回：
      (entity_type_from_ref_or_empty, field_key)
    """
    s = _norm_str(field_ref)
    if not s:
        raise ValueError("empty field_id/field_key")

    et = ""
    fk = s

    if "|" in s:
        left, right = s.split("|", 1)
        et = _norm_str(left).upper()
        fk = _norm_str(right)
        allowed = SUPPORTED_ENTITY_TYPES | OPTIONAL_ENTITY_TYPES
        if et and et not in allowed:
            raise ValueError(f"invalid field_ref entity_type: {et}")

    if from_entity_type:
        fet = _norm_str(from_entity_type).upper()
        if fet and et and fet != et:
            raise ValueError(f"entity_type mismatch: row={fet} vs field_ref={et}")

    return et, fk


def ns_key_from_field_key(field_key: str) -> Tuple[str, str]:
    s = _norm_str(field_key)
    if not s:
        raise ValueError("empty field_key")

    parts = s.split(".")
    if len(parts) < 3:
        raise ValueError(f"invalid field_key: {s}")

    prefix = parts[0].lower()
    if prefix not in {"mf", "v_mf"}:
        raise ValueError(f"field_key must start with mf. or v_mf.: {s}")

    ns = _norm_str(parts[1])
    key = _norm_str(".".join(parts[2:]))

    if not ns or not key:
        raise ValueError(f"invalid field_key (missing namespace/key): {s}")

    return ns, key


def infer_missing_metafield_type(
    field_ref_or_key: str,
    type_map: Dict[str, str],
    default_type_for_missing: Dict[str, str],
) -> str:
    s = _norm_str(field_ref_or_key)
    if not s:
        return ""

    t = _norm_str(type_map.get(s, ""))
    if t:
        return t

    if "|" in s:
        try:
            _, fk = parse_field_ref(s, None)
            t2 = _norm_str(type_map.get(fk, ""))
            if t2:
                return t2
        except Exception:
            pass

    fb = _norm_str(default_type_for_missing.get(s, ""))
    if not fb and "|" in s:
        try:
            _, fk = parse_field_ref(s, None)
            fb = _norm_str(default_type_for_missing.get(fk, ""))
        except Exception:
            fb = ""

    if not fb:
        return ""

    if fb in {"metaobject_reference", "list.metaobject_reference"}:
        return fb

    return _norm_str(type_map.get(fb, ""))


def parse_gid_list(x: Any) -> List[str]:
    s = _norm_str(x)
    if not s:
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return _dedupe_keep_order([str(v) for v in arr])
        except Exception:
            pass

    items = re.split(r"[\n,\s]+", s)
    return _dedupe_keep_order(items)


def validate_metaobject_gid_list(gids: List[str]) -> List[str]:
    bad = [g for g in gids if not _norm_str(g).startswith("gid://shopify/Metaobject/")]
    return bad


def decode_value(mf_type: str, value_str: Any):
    t = _norm_str(mf_type)

    if t == "metaobject_reference":
        return _norm_str(value_str)

    if t == "list.metaobject_reference":
        s = _norm_str(value_str)
        if not s:
            return []
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return _dedupe_keep_order([str(v) for v in arr])
        except Exception:
            pass
        return parse_gid_list(s)

    return _norm_str(value_str)


def encode_value(mf_type: str, py_val: Any) -> str:
    t = _norm_str(mf_type)

    if t == "metaobject_reference":
        return _norm_str(py_val)

    if t == "list.metaobject_reference":
        arr = py_val if isinstance(py_val, list) else ([] if py_val in (None, "") else [str(py_val)])
        arr = _dedupe_keep_order([str(v) for v in arr])
        return json.dumps(arr, ensure_ascii=False)

    return _norm_str(py_val)


# =========================================================
# Shopify owner resolver / metafield read-write
# =========================================================
def resolve_owner_gid(
    shop_domain: str,
    api_version: str,
    access_token: str,
    entity_type: str,
    gid_or_handle: str,
    enable_order: bool = False,
) -> str:
    et = _norm_str(entity_type).upper()
    raw = _norm_str(gid_or_handle)

    if not raw:
        raise RuntimeError("from_gid_or_handle is empty")

    if _is_gid(raw):
        return raw

    if _is_numeric(raw):
        if et == "PRODUCT":
            return f"gid://shopify/Product/{raw}"
        if et == "COLLECTION":
            return f"gid://shopify/Collection/{raw}"
        if et == "PAGE":
            return f"gid://shopify/OnlineStorePage/{raw}"
        if et == "ORDER":
            if not enable_order:
                raise RuntimeError("ORDER is disabled in this job")
            return f"gid://shopify/Order/{raw}"
        raise RuntimeError(f"numeric id unsupported for entity_type={et}")

    if et == "PRODUCT":
        q = """query($handle:String!){ productByHandle(handle:$handle){ id } }"""
        d = shopify_gql(shop_domain, api_version, access_token, q, {"handle": raw})
        gid = _norm_str(((d.get("productByHandle") or {}).get("id")))
        if not gid:
            raise RuntimeError(f"PRODUCT handle not found: {raw}")
        return gid

    if et == "COLLECTION":
        q = """query($handle:String!){ collectionByHandle(handle:$handle){ id } }"""
        d = shopify_gql(shop_domain, api_version, access_token, q, {"handle": raw})
        gid = _norm_str(((d.get("collectionByHandle") or {}).get("id")))
        if not gid:
            raise RuntimeError(f"COLLECTION handle not found: {raw}")
        return gid

    if et == "PAGE":
        q = """
        query($first:Int!, $query:String!){
          pages(first:$first, query:$query){
            nodes{ id handle title }
          }
        }
        """
        d = shopify_gql(shop_domain, api_version, access_token, q, {"first": 10, "query": f"handle:{raw}"})
        nodes = (((d.get("pages") or {}).get("nodes")) or [])
        hit = [x for x in nodes if _norm_str(x.get("handle")) == raw]
        if not hit:
            raise RuntimeError(f"PAGE handle not found: {raw}")
        return _norm_str(hit[0].get("id"))

    if et == "ORDER":
        if not enable_order:
            raise RuntimeError("ORDER is disabled in this job")
        raise RuntimeError("ORDER currently supports gid/numeric only in this version")

    raise RuntimeError(f"unsupported entity_type={et}")


def get_owner_metafield(
    shop_domain: str,
    api_version: str,
    access_token: str,
    owner_gid: str,
    namespace: str,
    key: str,
) -> Optional[dict]:
    q = """
    query($id:ID!, $ns:String!, $key:String!){
      node(id:$id){
        id
        ... on HasMetafields {
          metafield(namespace:$ns, key:$key){
            id
            type
            value
          }
        }
      }
    }
    """
    d = shopify_gql(shop_domain, api_version, access_token, q, {"id": owner_gid, "ns": namespace, "key": key})
    node = d.get("node") or {}
    return node.get("metafield") or None


def set_metafield(
    shop_domain: str,
    api_version: str,
    access_token: str,
    owner_gid: str,
    namespace: str,
    key: str,
    mf_type: str,
    value_str: str,
):
    if _norm_str(mf_type) not in {"metaobject_reference", "list.metaobject_reference"}:
        raise RuntimeError(f"unsupported metafield type for write: {mf_type}")

    m = """
    mutation($metafields:[MetafieldsSetInput!]!){
      metafieldsSet(metafields:$metafields){
        metafields { id namespace key type value }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "metafields": [{
            "ownerId": owner_gid,
            "namespace": namespace,
            "key": key,
            "type": mf_type,
            "value": "" if value_str is None else str(value_str),
        }]
    }
    d = shopify_gql(shop_domain, api_version, access_token, m, variables)
    out = d.get("metafieldsSet") or {}
    errs = out.get("userErrors") or []
    if errs:
        e0 = errs[0]
        raise RuntimeError(
            f"metafieldsSet userError: code={e0.get('code')} | message={e0.get('message')} | field={e0.get('field')}"
        )
    return out.get("metafields") or []


# =========================================================
# 业务逻辑
# =========================================================
def apply_op(mf_type: str, cur_val: Any, op: str, targets: List[str]):
    t = _norm_str(mf_type)
    op = _norm_str(op).upper()
    targets = _dedupe_keep_order(targets)

    if t == "metaobject_reference":
        cur = _norm_str(cur_val)
        if op == "REPLACE_ALL":
            return targets[0] if targets else ""
        if op == "LINK":
            return targets[0] if targets else cur
        if op == "UNLINK":
            return "" if cur and cur in set(targets) else cur
        raise ValueError(f"unsupported op for {t}: {op}")

    if t == "list.metaobject_reference":
        cur_list = cur_val if isinstance(cur_val, list) else parse_gid_list(cur_val)
        cur_list = _dedupe_keep_order(cur_list)

        if op == "REPLACE_ALL":
            return targets

        if op == "LINK":
            cur_set = set(cur_list)
            out = list(cur_list)
            for x in targets:
                if x not in cur_set:
                    out.append(x)
                    cur_set.add(x)
            return out

        if op == "UNLINK":
            rm = set(targets)
            return [x for x in cur_list if x not in rm]

        raise ValueError(f"unsupported op for {t}: {op}")

    raise ValueError(f"unsupported metafield type: {t}")


def calc_change_type(mf_type: str, cur_val: Any, new_val: Any) -> str:
    t = _norm_str(mf_type)

    if t == "metaobject_reference":
        cur = _norm_str(cur_val)
        new = _norm_str(new_val)
        if cur == new:
            return "NO_CHANGE"
        if not cur and new:
            return "SET"
        if cur and not new:
            return "CLEAR"
        return "REPLACE"

    if t == "list.metaobject_reference":
        cur = _dedupe_keep_order(cur_val if isinstance(cur_val, list) else parse_gid_list(cur_val))
        new = _dedupe_keep_order(new_val if isinstance(new_val, list) else parse_gid_list(new_val))
        if cur == new:
            return "NO_CHANGE"
        if not cur and new:
            return "SET_LIST"
        if cur and not new:
            return "CLEAR_LIST"

        cur_set = set(cur)
        new_set = set(new)
        added = len(new_set - cur_set)
        removed = len(cur_set - new_set)

        if added and not removed:
            return f"ADD_{added}"
        if removed and not added:
            return f"REMOVE_{removed}"
        return f"MIXED_{added}ADD_{removed}DEL"

    return "CHANGED"


def validate_row_logic(
    entity_type: str,
    field_key: str,
    mf_type: str,
    op: str,
    targets: List[str],
    enable_order: bool,
) -> Optional[str]:
    et = _norm_str(entity_type).upper()
    fk = _norm_str(field_key)
    op = _norm_str(op).upper()

    if et not in SUPPORTED_ENTITY_TYPES and not (enable_order and et == "ORDER"):
        return f"unsupported_entity_type={et}"

    if fk.startswith("v_mf."):
        return "v_mf_not_supported_in_edit_refs"

    if not fk.startswith("mf."):
        return "field_key_must_start_with_mf"

    if op not in ALLOWED_OPS:
        return f"invalid_op={op}"

    bad_gids = validate_metaobject_gid_list(targets)
    if bad_gids:
        return f"invalid_metaobject_gid_count={len(bad_gids)}"

    if mf_type == "metaobject_reference":
        if op in {"LINK", "REPLACE_ALL"} and len(targets) > 1:
            return "single_reference_cannot_accept_multiple_targets"

    return None


# =========================================================
# Runlog
# =========================================================
def build_runlog_row(
    run_id: str,
    phase: str,
    log_type: str,
    status: str,
    site_code: str,
    entity_type: str = "",
    gid: str = "",
    field_key: str = "",
    rows_loaded: Any = "",
    rows_pending: Any = "",
    rows_recognized: Any = "",
    rows_planned: Any = "",
    rows_written: Any = "",
    rows_skipped: Any = "",
    message: str = "",
    error_reason: str = "",
) -> List[Any]:
    return [
        run_id,
        now_cn_str(),
        JOB_NAME,
        phase,
        log_type,
        status,
        site_code,
        entity_type,
        gid,
        field_key,
        rows_loaded,
        rows_pending,
        rows_recognized,
        rows_planned,
        rows_written,
        rows_skipped,
        _truncate(message, 4500),
        _truncate(error_reason, 800),
    ]


# =========================================================
# 主入口
# =========================================================
def run(
    *,
    site_code: str,
    shop_domain: str,
    api_version: str,
    access_token: str,
    gc,
    console_core_url: str,
    input_sheet_label: str = "edit",
    worksheet_title: str = "Edit__Refs",
    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = "Cfg__Fields",
    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",
    dry_run: bool = True,
    confirmed: bool = False,
    enable_order: bool = False,
    sleep_sec: float = 0.10,
    default_type_for_missing: Optional[Dict[str, str]] = None,
    preview_rows_limit: int = 25,
    detail_error_reason_limit: int = 2,
    print_progress_every: int = 20,
) -> Dict[str, Any]:
    """
    Preview:
      dry_run=True, confirmed=False
    Apply:
      dry_run=False, confirmed=True
    """
    if default_type_for_missing is None:
        default_type_for_missing = {}

    phase = "preview" if dry_run else "apply"
    if not dry_run and not confirmed:
        raise RuntimeError("Apply mode requires confirmed=True")

    run_id = make_run_id("refs")
    site_code = _norm_str(site_code).upper()

    # ---------- open sheets ----------
    edit_sheet_url = find_sheet_url_by_label(gc, console_core_url, site_code, input_sheet_label)
    cfg_sheet_url = find_sheet_url_by_label(gc, console_core_url, site_code, cfg_sheet_label)
    runlog_sheet_url = find_sheet_url_by_label(gc, console_core_url, site_code, runlog_sheet_label)

    edit_sh = gc.open_by_url(edit_sheet_url)
    cfg_sh = gc.open_by_url(cfg_sheet_url)
    runlog_sh = gc.open_by_url(runlog_sheet_url)

    df = read_ws_df(edit_sh, worksheet_title)
    cfg_df = read_ws_df(cfg_sh, cfg_tab_fields)
    ws_runlog = get_or_create_ws(runlog_sh, runlog_tab_name, RUNLOG_HEADERS, rows=30000, cols=24)

    field_col = "field_id" if "field_id" in df.columns else ("field_key" if "field_key" in df.columns else "")
    if not field_col:
        raise RuntimeError("Edit__Refs missing field_id or field_key")

    required = ["from_entity_type", "from_gid_or_handle", field_col, "op", "to_entry_gid_list"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Edit__Refs missing columns: {missing}")

    type_map = build_type_map_from_cfg(cfg_df)

    raw_rows_loaded = len(df)

    work = df.copy()
    work["_row_idx"] = range(len(work))
    work["from_entity_type"] = work["from_entity_type"].astype(str).str.strip().str.upper()
    work["from_gid_or_handle"] = work["from_gid_or_handle"].astype(str).str.strip()
    work[field_col] = work[field_col].astype(str).str.strip()
    work["op"] = work["op"].astype(str).str.strip().str.upper()
    work["to_entry_gid_list"] = work["to_entry_gid_list"].astype(str).str.strip()

    work = work[
        (work["from_entity_type"] != "") &
        (work["from_gid_or_handle"] != "") &
        (work[field_col] != "") &
        (work["op"].isin(ALLOWED_OPS))
    ].copy()

    rows_pending = len(work)

    planned_rows: List[dict] = []
    warnings: List[str] = []
    detail_log_rows: List[List[Any]] = []

    error_reason_counter: Dict[str, int] = {}
    rows_recognized = 0
    rows_planned = 0
    rows_written = 0
    rows_skipped = 0

    t0 = time.time()

    print(f"RUN_ID = {run_id}")
    print(f"phase  = {phase}")
    print(f"rows_loaded={raw_rows_loaded} | rows_pending={rows_pending}")

    # ---------- planning ----------
    for i, (_, r) in enumerate(work.iterrows(), start=1):
        entity_type = _norm_str(r["from_entity_type"]).upper()
        owner_input = _norm_str(r["from_gid_or_handle"])
        field_ref = _norm_str(r[field_col])
        op = _norm_str(r["op"]).upper()
        to_entry_gid_list = _norm_str(r["to_entry_gid_list"])

        owner_gid = ""
        field_key = ""
        error_reason = ""
        message = ""

        try:
            _, field_key = parse_field_ref(field_ref, entity_type)
            ns, key = ns_key_from_field_key(field_key)

            owner_gid = resolve_owner_gid(
                shop_domain=shop_domain,
                api_version=api_version,
                access_token=access_token,
                entity_type=entity_type,
                gid_or_handle=owner_input,
                enable_order=enable_order,
            )

            mf = get_owner_metafield(
                shop_domain=shop_domain,
                api_version=api_version,
                access_token=access_token,
                owner_gid=owner_gid,
                namespace=ns,
                key=key,
            )

            if mf:
                mf_type = _norm_str(mf.get("type"))
                cur_val = decode_value(mf_type, mf.get("value"))
            else:
                mf_type = infer_missing_metafield_type(field_ref, type_map, default_type_for_missing) or \
                          infer_missing_metafield_type(field_key, type_map, default_type_for_missing)
                if not mf_type:
                    raise RuntimeError(
                        f"metafield_missing_and_type_not_inferred: {field_key}"
                    )
                cur_val = decode_value(mf_type, None)

            targets = parse_gid_list(to_entry_gid_list)

            row_logic_err = validate_row_logic(
                entity_type=entity_type,
                field_key=field_key,
                mf_type=mf_type,
                op=op,
                targets=targets,
                enable_order=enable_order,
            )
            if row_logic_err:
                raise RuntimeError(row_logic_err)

            new_py = apply_op(mf_type, cur_val, op, targets)
            new_str = encode_value(mf_type, new_py)
            change_type = calc_change_type(mf_type, cur_val, new_py)

            planned_rows.append({
                "entity_type": entity_type,
                "owner_input": owner_input,
                "owner_gid": owner_gid,
                "field_ref": field_ref,
                "field_key": field_key,
                "namespace": ns,
                "key": key,
                "mf_type": mf_type,
                "op": op,
                "targets": targets,
                "current_value": cur_val,
                "planned_value": new_py,
                "planned_value_str": new_str,
                "change_type": change_type,
                "status": "PLANNED" if change_type != "NO_CHANGE" else "NO_CHANGE",
            })
            rows_recognized += 1
            rows_planned += 1

        except Exception as e:
            rows_skipped += 1
            error_reason = _truncate(str(e), 500)
            message = f"{entity_type}:{owner_input} | {field_ref} | op={op}"
            warnings.append(f"{message} | {error_reason}")

            cnt = error_reason_counter.get(error_reason, 0)
            if cnt < detail_error_reason_limit:
                detail_log_rows.append(
                    build_runlog_row(
                        run_id=run_id,
                        phase=phase,
                        log_type="detail",
                        status="FAIL",
                        site_code=site_code,
                        entity_type=entity_type,
                        gid=owner_gid or owner_input,
                        field_key=field_key or field_ref,
                        rows_loaded=raw_rows_loaded,
                        rows_pending=rows_pending,
                        rows_recognized=rows_recognized,
                        rows_planned=rows_planned,
                        rows_written=rows_written,
                        rows_skipped=rows_skipped,
                        message=message,
                        error_reason=error_reason,
                    )
                )
            error_reason_counter[error_reason] = cnt + 1

        if i % max(print_progress_every, 1) == 0:
            print(f"[plan] {i}/{rows_pending} | recognized={rows_recognized} | planned={rows_planned} | skipped={rows_skipped}")

    # ---------- preview print ----------
    print("\n=== SUMMARY ===")
    print(f"rows_loaded     = {raw_rows_loaded}")
    print(f"rows_pending    = {rows_pending}")
    print(f"rows_recognized = {rows_recognized}")
    print(f"rows_planned    = {rows_planned}")
    print(f"rows_written    = {rows_written}")
    print(f"rows_skipped    = {rows_skipped}")

    print("\n=== PREVIEW ===")
    if not planned_rows:
        print("No planned rows.")
    else:
        pv = pd.DataFrame([{
            "entity_type": x["entity_type"],
            "owner_input": x["owner_input"],
            "field_key": x["field_key"],
            "mf_type": x["mf_type"],
            "op": x["op"],
            "change_type": x["change_type"],
            "current_value": _truncate(_safe_json_dumps(x["current_value"]), 180),
            "planned_value": _truncate(_safe_json_dumps(x["planned_value"]), 180),
        } for x in planned_rows[:preview_rows_limit]])
        print(pv.to_string(index=False))

    print("\n=== WARNINGS ===")
    if warnings:
        for x in warnings[: min(20, len(warnings))]:
            print("-", _truncate(x, 350))
        if len(warnings) > 20:
            print(f"... ({len(warnings) - 20} more)")
    else:
        print("No warnings.")

    # ---------- apply ----------
    if not dry_run:
        print("\n=== APPLY ===")
        for i, row in enumerate(planned_rows, start=1):
            if row["change_type"] == "NO_CHANGE":
                continue

            set_metafield(
                shop_domain=shop_domain,
                api_version=api_version,
                access_token=access_token,
                owner_gid=row["owner_gid"],
                namespace=row["namespace"],
                key=row["key"],
                mf_type=row["mf_type"],
                value_str=row["planned_value_str"],
            )
            rows_written += 1

            if sleep_sec > 0:
                time.sleep(sleep_sec)

            if i % max(print_progress_every, 1) == 0:
                print(f"[apply] {i}/{len(planned_rows)} | rows_written={rows_written}")

    elapsed_sec = int(time.time() - t0)

    # ---------- runlog ----------
    summary_status = "OK" if rows_skipped == 0 else ("PARTIAL_OK" if rows_planned > 0 else "FAIL")
    summary_message = (
        f"elapsed_sec={elapsed_sec}; "
        f"worksheet={worksheet_title}; "
        f"preview_rows_limit={preview_rows_limit}; "
        f"dry_run={dry_run}; "
        f"confirmed={confirmed}; "
        f"enable_order={enable_order}"
    )

    summary_row = build_runlog_row(
        run_id=run_id,
        phase=phase,
        log_type="summary",
        status=summary_status,
        site_code=site_code,
        entity_type="MIXED",
        gid="",
        field_key="",
        rows_loaded=raw_rows_loaded,
        rows_pending=rows_pending,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message=summary_message,
        error_reason="",
    )

    append_rows_safe(ws_runlog, [summary_row] + detail_log_rows)

    return {
        "run_id": run_id,
        "phase": phase,
        "site_code": site_code,
        "rows_loaded": raw_rows_loaded,
        "rows_pending": rows_pending,
        "rows_recognized": rows_recognized,
        "rows_planned": rows_planned,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "warnings": warnings,
        "preview": planned_rows[:preview_rows_limit],
        "all_planned_count": len(planned_rows),
        "elapsed_sec": elapsed_sec,
        "edit_sheet_url": edit_sheet_url,
        "cfg_sheet_url": cfg_sheet_url,
        "runlog_sheet_url": runlog_sheet_url,
    }
