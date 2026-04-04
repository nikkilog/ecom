# shopify_sync/edit_entries_update.py

from __future__ import annotations

import base64
import json
import random
import re
import time
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials


RUNLOG_HEADER_18 = [
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


M_METAOBJECT_UPDATE = """
mutation($id: ID!, $metaobject: MetaobjectUpdateInput!) {
  metaobjectUpdate(id: $id, metaobject: $metaobject) {
    metaobject { id handle type }
    userErrors { field message code }
  }
}
"""

Q_METAOBJECT_BY_ID = """
query($id: ID!) {
  node(id: $id) {
    ... on Metaobject {
      id
      handle
      type
    }
  }
}
"""

Q_METAOBJECT_BY_HANDLE_EXACT = """
query($handle: MetaobjectHandleInput!) {
  metaobjectByHandle(handle: $handle) {
    id
    handle
    type
  }
}
"""

Q_COLLECTION_BY_HANDLE = """
query($h: String!) {
  collectionByHandle(handle: $h) {
    id
    handle
  }
}
"""


@dataclass
class Context:
    site_code: str
    job_name: str
    tz_name: str
    dry_run: bool
    confirmed: bool
    default_mode: str
    empty_means_clear: bool
    job_chunk_size: int
    preview_limit: int
    detail_limit_per_error: int
    cfg_sites_url: str
    cfg_sites_tab: str
    label_edit: str
    label_config: str
    label_runlog: str
    tab_edit_update: str
    tab_cfg_fields: str
    tab_runlog: str
    shop_domain: str
    api_version: str
    gsheet_sa_b64: str
    shopify_token: str
    run_id: Optional[str] = None


class RunLogger:
    def __init__(self, ws_log, ctx: Context):
        self.ws_log = ws_log
        self.ctx = ctx
        self.buf: List[List[Any]] = []
        self.flush_every = 200
        self.detail_seen_count: Dict[Tuple[str, str], int] = {}

    def ensure_header(self) -> None:
        self.ws_log.update("A1:R1", [RUNLOG_HEADER_18])

    def add(
        self,
        *,
        phase: str,
        log_type: str,
        status: str,
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
    ) -> None:
        self.buf.append([
            self.ctx.run_id,
            now_cn_str(self.ctx.tz_name),
            self.ctx.job_name,
            phase,
            log_type,
            status,
            self.ctx.site_code,
            entity_type,
            gid,
            field_key,
            rows_loaded,
            rows_pending,
            rows_recognized,
            rows_planned,
            rows_written,
            rows_skipped,
            str(message)[:1000],
            str(error_reason)[:250],
        ])
        if len(self.buf) >= self.flush_every:
            self.flush()

    def add_detail_limited(
        self,
        *,
        phase: str,
        status: str,
        entity_type: str,
        gid: str,
        field_key: str,
        message: str,
        error_reason: str,
    ) -> None:
        k = (phase, error_reason or "UNKNOWN")
        n = self.detail_seen_count.get(k, 0)
        if n >= self.ctx.detail_limit_per_error:
            return
        self.detail_seen_count[k] = n + 1
        self.add(
            phase=phase,
            log_type="detail",
            status=status,
            entity_type=entity_type,
            gid=gid,
            field_key=field_key,
            message=message,
            error_reason=error_reason,
        )

    def flush(self) -> None:
        if not self.buf:
            return
        for i in range(8):
            try:
                self.ws_log.append_rows(self.buf, value_input_option="RAW", table_range="A:R")
                self.buf = []
                return
            except Exception:
                time.sleep(min(2 ** i, 20) + random.random())
        raise RuntimeError("Failed writing Ops__RunLog after retries")


def now_cn_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def gen_run_id(job_name: str, tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime(f"{job_name}_%Y%m%d_%H%M%S")


def _norm(x: Any) -> str:
    return str(x).strip()


def _norm_lower(x: Any) -> str:
    return _norm(x).lower()


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for x in candidates:
        if x.lower() in cols:
            return cols[x.lower()]
    return None


def split_multi(raw: str) -> List[str]:
    return [x.strip() for x in re.split(r"[,\n]+", str(raw)) if x.strip()]


def slot_to_int(x: Any) -> Optional[int]:
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def chunk(lst: List[Any], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def err_signature(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"sheet_row=\d+", "sheet_row=?", s)
    s = re.sub(r"gid://shopify/[A-Za-z]+/\d+", "gid://shopify/X/?", s)
    s = re.sub(r"handle=[A-Za-z0-9\-_:/]+", "handle=?", s)
    return s[:240]


def reason_from_error(s: str) -> str:
    t = str(s).lower()
    if "field_id not found" in t:
        return "FIELD_ID_NOT_FOUND"
    if "missing key in cfg__fields" in t:
        return "CFG_KEY_MISSING"
    if "entry not found by gid" in t:
        return "ENTRY_GID_NOT_FOUND"
    if "type mismatch" in t or "actual metaobject type" in t:
        return "TYPE_MISMATCH"
    if "collection_reference parse failed" in t:
        return "COLLECTION_REF_PARSE_FAILED"
    if "metaobject handle not found" in t:
        return "METAOBJECT_REF_NOT_FOUND"
    if "graphql" in t or "usererrors" in t:
        return "SHOPIFY_API_ERROR"
    if "multiple new_handle" in t:
        return "MULTIPLE_NEW_HANDLE"
    if "duplicate handle target" in t:
        return "DUPLICATE_TARGET_HANDLE"
    return "OTHER"


def format_user_errors(ues: List[Dict[str, Any]]) -> str:
    parts = []
    for u in (ues or []):
        code = u.get("code") or ""
        msg = u.get("message") or ""
        fld = u.get("field") or []
        parts.append(f"{code} | {msg} | field={fld}")
    return " ; ".join(parts)[:1000]


def build_gc(sa_b64: str):
    sa_info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)


class ShopifyClient:
    def __init__(self, shop_domain: str, api_version: str, token: str):
        self.graphql_url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        }

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None, retry: int = 6) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        for i in range(retry):
            r = requests.post(self.graphql_url, headers=self.headers, json=payload, timeout=90)
            if r.status_code in (429, 502, 503, 504):
                time.sleep(min(2 ** i, 20) + random.random())
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("errors"):
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        raise RuntimeError("GraphQL failed after retries")


def resolve_sheet_url_from_cfg_sites(gc, cfg_sites_url: str, cfg_sites_tab: str, site_code: str, label: str) -> str:
    sh = gc.open_by_url(cfg_sites_url)
    ws = sh.worksheet(cfg_sites_tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows).fillna("")
    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    c_site = pick_col(df, ["site_code"])
    c_label = pick_col(df, ["label"])
    c_url = pick_col(df, ["sheet_url"])
    if not c_site or not c_label or not c_url:
        raise ValueError(f"Cfg__Sites must contain site_code/label/sheet_url. Got={df.columns.tolist()}")

    hit = df[
        (df[c_site].astype(str).str.strip().str.lower() == _norm_lower(site_code)) &
        (df[c_label].astype(str).str.strip().str.lower() == _norm_lower(label))
    ].copy()

    if hit.empty:
        raise ValueError(f"Cfg__Sites no match for site_code={site_code}, label={label}")

    url = str(hit.iloc[0][c_url]).strip()
    if not url:
        raise ValueError(f"Cfg__Sites matched but sheet_url empty for site_code={site_code}, label={label}")
    return url


def load_cfg_fields(ws_fields) -> Dict[str, Dict[str, str]]:
    rows = ws_fields.get_all_records()
    df = pd.DataFrame(rows).fillna("")

    c_field_id = pick_col(df, ["field_id"])
    c_entity = pick_col(df, ["entity_type"])
    c_field_key = pick_col(df, ["field_key"])
    c_namespace = pick_col(df, ["namespace"])
    c_key = pick_col(df, ["key"])
    c_data_type = pick_col(df, ["data_type", "field_type"])

    need = [c_field_id, c_entity, c_field_key, c_namespace, c_key, c_data_type]
    if not all(need):
        raise ValueError(f"Cfg__Fields required columns missing. Got={df.columns.tolist()}")

    out: Dict[str, Dict[str, str]] = {}
    for _, r in df.iterrows():
        fid = str(r[c_field_id]).strip()
        if not fid:
            continue
        out[fid] = {
            "field_id": fid,
            "entity_type": str(r[c_entity]).strip(),
            "field_key": str(r[c_field_key]).strip(),
            "namespace": str(r[c_namespace]).strip(),
            "key": str(r[c_key]).strip(),
            "data_type": str(r[c_data_type]).strip(),
        }
    return out


def load_edit_rows(ws_edit, required_cols: List[str]) -> pd.DataFrame:
    vals = ws_edit.get_all_values()
    if len(vals) < 2:
        raise ValueError("Edit__Entries_Update is empty")

    hdr = [str(x).strip() for x in vals[0]]
    data = vals[1:]
    rows = []
    max_len = len(hdr)

    for i, r in enumerate(data, start=2):
        rr = [str(x).strip() for x in r]
        rr += [""] * (max_len - len(rr))
        rec = dict(zip(hdr, rr))
        rec["_sheet_row"] = i
        rows.append(rec)

    df = pd.DataFrame(rows).fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Edit__Entries_Update missing required columns: {missing}")

    for opt in ["slot", "note"]:
        if opt not in df.columns:
            df[opt] = ""

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    df["op"] = df["op"].astype(str).str.strip().str.upper()
    df["mode"] = df["mode"].astype(str).str.strip().str.upper()
    return df


def extract_type_from_field_id(field_id: str) -> str:
    s = str(field_id).strip()
    m = re.match(r"^METAOBJECT_ENTRY\|mo\.([^.]+)\..+$", s)
    if not m:
        return ""
    return m.group(1).strip()


class Resolver:
    def __init__(self, sc: ShopifyClient):
        self.sc = sc
        self.collection_cache: Dict[str, str] = {}
        self.mo_info_cache: Dict[str, Dict[str, str]] = {}
        self.mo_gid_by_handle_cache: Dict[Tuple[str, str], str] = {}

    def get_metaobject_info_by_gid(self, entry_gid: str) -> Dict[str, str]:
        gid = str(entry_gid).strip()
        if gid in self.mo_info_cache:
            return self.mo_info_cache[gid]
        data = self.sc.gql(Q_METAOBJECT_BY_ID, {"id": gid})
        node = data.get("node") or {}
        info = {
            "id": node.get("id", "") or "",
            "handle": node.get("handle", "") or "",
            "type": node.get("type", "") or "",
        }
        self.mo_info_cache[gid] = info
        return info

    def to_gid_collection(self, val: str) -> str:
        v = str(val).strip()
        if not v:
            return ""
        if v.startswith("gid://"):
            return v
        if v in self.collection_cache:
            return self.collection_cache[v]
        if re.fullmatch(r"\d+", v):
            gid = f"gid://shopify/Collection/{v}"
            self.collection_cache[v] = gid
            return gid

        data = self.sc.gql(Q_COLLECTION_BY_HANDLE, {"h": v})
        node = data.get("collectionByHandle") or {}
        gid = node.get("id", "") or ""
        self.collection_cache[v] = gid
        return gid

    def parse_type_and_handle(self, raw: str, fallback_type: str) -> Tuple[str, str]:
        s = str(raw).strip()
        if "/" in s:
            t, h = s.split("/", 1)
            return t.strip(), h.strip()
        if ":" in s:
            t, h = s.split(":", 1)
            return t.strip(), h.strip()
        return fallback_type, s

    def gid_from_metaobject_handle(self, mo_type: str, handle_str: str, strict: bool = True) -> str:
        k = (mo_type, handle_str)
        if k in self.mo_gid_by_handle_cache:
            gid = self.mo_gid_by_handle_cache[k]
            if strict and not gid:
                raise ValueError(f"metaobject handle not found (cached): type={mo_type} handle={handle_str}")
            return gid

        data = self.sc.gql(Q_METAOBJECT_BY_HANDLE_EXACT, {"handle": {"type": mo_type, "handle": handle_str}})
        node = data.get("metaobjectByHandle") or {}
        gid = node.get("id", "") or ""
        self.mo_gid_by_handle_cache[k] = gid

        if strict and not gid:
            raise ValueError(f"metaobject handle not found: type={mo_type} handle={handle_str}")
        return gid

    def to_gid_metaobject(self, raw: str, current_entry_type: str, strict: bool = True) -> str:
        v = str(raw).strip()
        if not v:
            return ""
        if v.startswith("gid://"):
            return v
        ref_type, ref_handle = self.parse_type_and_handle(v, fallback_type=current_entry_type)
        return self.gid_from_metaobject_handle(ref_type, ref_handle, strict=strict)


def preflight(
    df: pd.DataFrame,
    cfg_by_field_id: Dict[str, Dict[str, str]],
    resolver: Resolver,
    ctx: Context,
) -> Tuple[pd.DataFrame, List[str], Dict[str, int], List[Dict[str, Any]]]:
    warnings: List[str] = []

    df = df[~(
        (df["op"] == "") &
        (df["entry_gid"] == "") &
        (df["field_id"] == "") &
        (df["value"] == "") &
        (df["new_handle"] == "")
    )].copy()

    df["mode"] = df["mode"].replace("", ctx.default_mode)

    bad_op = df[df["op"] != "UPDATE"]
    if not bad_op.empty:
        raise ValueError(f"Only op=UPDATE allowed. Bad rows={bad_op[['op','_sheet_row']].head(20).to_dict('records')}")

    bad_mode = df[~df["mode"].isin(["STRICT", "LOOSE"])]
    if not bad_mode.empty:
        raise ValueError(f"Invalid mode. Bad rows={bad_mode[['mode','_sheet_row']].head(20).to_dict('records')}")

    bad_gid = df[df["entry_gid"] == ""]
    if not bad_gid.empty:
        raise ValueError(f"Update rows require entry_gid. Bad rows={bad_gid[['_sheet_row']].head(20).to_dict('records')}")

    bad_empty_action = df[(df["field_id"] == "") & (df["new_handle"] == "")]
    if not bad_empty_action.empty:
        raise ValueError(
            f"Each row must have field_id or new_handle. Bad rows={bad_empty_action[['_sheet_row']].head(20).to_dict('records')}"
        )

    df_with_field = df[df["field_id"] != ""].copy()
    bad_field = df_with_field[~df_with_field["field_id"].isin(cfg_by_field_id.keys())]
    if not bad_field.empty:
        raise ValueError(f"field_id not found in Cfg__Fields. Bad rows={bad_field[['field_id','_sheet_row']].head(20).to_dict('records')}")

    bad_entity = []
    for _, r in df_with_field.iterrows():
        meta = cfg_by_field_id.get(r["field_id"], {})
        if meta.get("entity_type") != "METAOBJECT_ENTRY":
            bad_entity.append({
                "field_id": r["field_id"],
                "_sheet_row": r["_sheet_row"],
                "entity_type": meta.get("entity_type", ""),
            })
    if bad_entity:
        raise ValueError(f"Only METAOBJECT_ENTRY field_id allowed. Bad rows={bad_entity[:20]}")

    bad_type_match = []
    gid_type_cache: Dict[str, str] = {}
    gids = list(dict.fromkeys(df["entry_gid"].astype(str).tolist()))

    for gid in gids:
        gid_type_cache[gid] = resolver.get_metaobject_info_by_gid(gid).get("type", "") or ""

    for _, r in df_with_field.iterrows():
        fid_type = extract_type_from_field_id(r["field_id"])
        actual_type = gid_type_cache.get(r["entry_gid"], "")
        if fid_type and actual_type and fid_type != actual_type:
            bad_type_match.append({
                "entry_gid": r["entry_gid"],
                "field_id": r["field_id"],
                "field_id_type": fid_type,
                "actual_type": actual_type,
                "_sheet_row": r["_sheet_row"],
            })
    if bad_type_match:
        raise ValueError(f"field_id type mismatch with entry_gid actual type. Bad rows={bad_type_match[:20]}")

    # 同一 entry_gid 多个不同 new_handle：直接报错
    handle_conflicts = []
    for entry_gid, g in df.groupby("entry_gid", dropna=False):
        hs = [str(x).strip() for x in g["new_handle"].tolist() if str(x).strip()]
        uniq = sorted(set(hs))
        if len(uniq) > 1:
            handle_conflicts.append({
                "entry_gid": entry_gid,
                "new_handles": uniq,
                "sheet_rows": g["_sheet_row"].tolist()[:20],
            })
    if handle_conflicts:
        raise ValueError(f"multiple new_handle found for same entry_gid. Bad groups={handle_conflicts[:20]}")

    # 多个 entry 目标 handle 重复：给 warning
    handle_target_groups = (
        df[df["new_handle"] != ""]
        .groupby("new_handle")["entry_gid"]
        .nunique()
        .reset_index(name="gid_cnt")
    )
    dup_targets = handle_target_groups[handle_target_groups["gid_cnt"] > 1]
    if not dup_targets.empty:
        warnings.append(
            "duplicate handle target detected: "
            + "; ".join([f"{r['new_handle']} -> {r['gid_cnt']} gids" for _, r in dup_targets.head(20).iterrows()])
        )

    counters = {
        "rows_loaded": int(len(df)),
        "rows_pending": int(len(df)),
        "rows_recognized": int(len(df_with_field)),
        "distinct_gid_count": int(len(gids)),
    }

    preview_rows: List[Dict[str, Any]] = []
    return df, warnings, counters, preview_rows


def build_fields_for_entry(
    rows_for_entry: List[Dict[str, Any]],
    entry_type: str,
    strict: bool,
    cfg_by_field_id: Dict[str, Dict[str, str]],
    resolver: Resolver,
    empty_means_clear: bool,
) -> Tuple[List[Dict[str, str]], List[str]]:
    by_field: Dict[str, List[Dict[str, Any]]] = {}
    warnings: List[str] = []

    for r in rows_for_entry:
        fid = str(r.get("field_id", "")).strip()
        if not fid:
            continue
        by_field.setdefault(fid, []).append(r)

    out: List[Dict[str, str]] = []

    for field_id, items in by_field.items():
        meta = cfg_by_field_id.get(field_id)
        if not meta:
            if strict:
                sr = items[0].get("_sheet_row")
                raise ValueError(f"sheet_row={sr} field_id not found: {field_id}")
            warnings.append(f"skip unknown field_id: {field_id}")
            continue

        data_type = str(meta.get("data_type", "")).strip().lower()
        key = str(meta.get("key", "")).strip()

        if not key:
            if strict:
                sr = items[0].get("_sheet_row")
                raise ValueError(f"sheet_row={sr} missing key in Cfg__Fields for field_id={field_id}")
            warnings.append(f"skip field with empty key: {field_id}")
            continue

        if data_type.startswith("list."):
            def _sort_key(r):
                si = slot_to_int(r.get("slot", ""))
                return (999999 if si is None else si, int(r.get("_sheet_row", 10 ** 9)))

            items_sorted = sorted(items, key=_sort_key)

            vals: List[str] = []
            for r in items_sorted:
                raw = str(r.get("value", "")).strip()
                if raw == "":
                    continue
                vals.extend(split_multi(raw))

            if not vals:
                if empty_means_clear:
                    out.append({"key": key, "value": "[]"})
                continue

            if "collection_reference" in data_type:
                gids = []
                for it in vals:
                    gid = resolver.to_gid_collection(it)
                    if not gid:
                        raise ValueError(f"collection_reference parse failed: {it}")
                    gids.append(gid)
                out.append({"key": key, "value": json.dumps(gids)})
                continue

            if "metaobject_reference" in data_type:
                gids = []
                for it in vals:
                    gids.append(resolver.to_gid_metaobject(it, current_entry_type=entry_type, strict=strict))
                out.append({"key": key, "value": json.dumps(gids)})
                continue

            out.append({"key": key, "value": json.dumps(vals, ensure_ascii=False)})
            continue

        # scalar
        last_nonempty = ""
        for r in sorted(items, key=lambda x: int(x.get("_sheet_row", 10 ** 9))):
            raw = str(r.get("value", "")).strip()
            if raw != "":
                last_nonempty = raw

        if last_nonempty == "":
            if not empty_means_clear:
                continue
            if "collection_reference" in data_type or "metaobject_reference" in data_type:
                # 单值 reference 清空行为先不做隐式清空，避免误写
                warnings.append(f"reference clear skipped: {field_id}")
                continue
            out.append({"key": key, "value": ""})
            continue

        if "collection_reference" in data_type:
            gid = resolver.to_gid_collection(last_nonempty)
            if not gid:
                raise ValueError(f"collection_reference parse failed: {last_nonempty}")
            out.append({"key": key, "value": gid})
            continue

        if "metaobject_reference" in data_type:
            gid = resolver.to_gid_metaobject(last_nonempty, current_entry_type=entry_type, strict=strict)
            out.append({"key": key, "value": gid})
            continue

        out.append({"key": key, "value": last_nonempty})

    return out, warnings


def build_jobs(
    plan_df: pd.DataFrame,
    cfg_by_field_id: Dict[str, Dict[str, str]],
    resolver: Resolver,
    ctx: Context,
) -> Tuple[List[Dict[str, Any]], List[str], Dict[str, int], List[Dict[str, Any]]]:
    records = plan_df.to_dict("records")
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        entry_gid = str(r.get("entry_gid", "")).strip()
        groups.setdefault(entry_gid, []).append(r)

    jobs: List[Dict[str, Any]] = []
    warnings: List[str] = []
    preview: List[Dict[str, Any]] = []
    rows_skipped = 0

    for entry_gid, items in groups.items():
        modes = [str(x.get("mode", "")).strip().upper() or ctx.default_mode for x in items]
        strict = ("STRICT" in modes)
        min_row = min(int(x.get("_sheet_row", 10 ** 9)) for x in items)

        info = resolver.get_metaobject_info_by_gid(entry_gid)
        entry_type = info.get("type", "") or ""
        current_handle = info.get("handle", "") or ""

        if not entry_type:
            if strict:
                raise ValueError(f"sheet_row={min_row} entry not found by gid: {entry_gid}")
            rows_skipped += len(items)
            warnings.append(f"skip missing entry_gid in Shopify: {entry_gid}")
            continue

        new_handles = []
        for r in sorted(items, key=lambda x: int(x.get("_sheet_row", 10 ** 9))):
            nh = str(r.get("new_handle", "")).strip()
            if nh:
                new_handles.append(nh)
        target_handle = new_handles[-1] if new_handles else ""

        fields_input, field_warnings = build_fields_for_entry(
            rows_for_entry=items,
            entry_type=entry_type,
            strict=strict,
            cfg_by_field_id=cfg_by_field_id,
            resolver=resolver,
            empty_means_clear=ctx.empty_means_clear,
        )
        warnings.extend([f"{entry_gid}: {w}" for w in field_warnings])

        metaobject_input: Dict[str, Any] = {}
        if target_handle:
            metaobject_input["handle"] = target_handle
        if fields_input:
            metaobject_input["fields"] = fields_input

        if not metaobject_input:
            rows_skipped += len(items)
            warnings.append(f"{entry_gid}: nothing to update")
            continue

        jobs.append({
            "_min_row": min_row,
            "entry_gid": entry_gid,
            "entry_type": entry_type,
            "strict": strict,
            "current_handle": current_handle,
            "target_handle": target_handle,
            "items": items,
            "metaobject_input": metaobject_input,
            "fields_count": len(fields_input),
        })

        if len(preview) < ctx.preview_limit:
            preview.append({
                "sheet_row": min_row,
                "entry_gid": entry_gid,
                "entry_type": entry_type,
                "current_handle": current_handle,
                "target_handle": target_handle,
                "fields_count": len(fields_input),
                "field_keys": [x["key"] for x in fields_input[:12]],
                "has_handle_change": bool(target_handle and target_handle != current_handle),
                "fields_preview": fields_input[:5],
            })

    jobs.sort(key=lambda x: int(x["_min_row"]))

    counters = {
        "rows_planned": int(sum(len(x["items"]) for x in jobs)),
        "jobs_planned": int(len(jobs)),
        "rows_skipped_build": int(rows_skipped),
    }
    return jobs, warnings, counters, preview


def execute_jobs(
    jobs: List[Dict[str, Any]],
    resolver: Resolver,
    sc: ShopifyClient,
    logger: RunLogger,
    ctx: Context,
) -> Dict[str, Any]:
    ok = fail = skip = written = 0
    seen_fail_signatures = set()

    for batch_idx, batch in enumerate(chunk(jobs, ctx.job_chunk_size), start=1):
        print(f"Batch {batch_idx}: {len(batch)} jobs")

        for job in batch:
            entry_gid = job["entry_gid"]
            entry_type = job["entry_type"]
            strict = job["strict"]
            payload = {
                "id": entry_gid,
                "metaobject": job["metaobject_input"],
            }

            try:
                if not job["metaobject_input"]:
                    skip += 1
                    continue

                if ctx.dry_run:
                    ok += 1
                    continue

                if not ctx.confirmed:
                    raise RuntimeError("Apply blocked: CONFIRMED must be True when DRY_RUN=False")

                data = sc.gql(M_METAOBJECT_UPDATE, payload)
                mu = data.get("metaobjectUpdate") or {}
                ues = mu.get("userErrors") or []
                if ues:
                    raise RuntimeError(format_user_errors(ues))

                ok += 1
                written += 1

            except Exception as e:
                if strict:
                    fail += 1
                else:
                    skip += 1

                msg = f"sheet_row={job['_min_row']} gid={entry_gid} | {str(e)}"
                reason = reason_from_error(str(e))
                sig = err_signature(msg)
                if sig not in seen_fail_signatures:
                    seen_fail_signatures.add(sig)
                    logger.add_detail_limited(
                        phase="apply" if not ctx.dry_run else "preview",
                        status="FAIL" if strict else "SKIP",
                        entity_type=entry_type or "METAOBJECT_ENTRY",
                        gid=entry_gid,
                        field_key="update",
                        message=msg,
                        error_reason=reason,
                    )

    return {
        "ok": ok,
        "fail": fail,
        "skip": skip,
        "written": written,
    }


def run(
    *,
    site_code: str,
    gsheet_sa_b64: str,
    shopify_token: str,
    shop_domain: str,
    api_version: str,
    cfg_sites_url: str,
    cfg_sites_tab: str = "Cfg__Sites",
    label_edit: str = "edit",
    label_config: str = "config",
    label_runlog: str = "runlog_sheet",
    tab_edit_update: str = "Edit__Entries_Update",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_runlog: str = "Ops__RunLog",
    dry_run: bool = True,
    confirmed: bool = False,
    default_mode: str = "LOOSE",
    empty_means_clear: bool = True,
    tz_name: str = "Asia/Shanghai",
    job_name: str = "edit_entries_update",
    run_id: Optional[str] = None,
    job_chunk_size: int = 25,
    preview_limit: int = 20,
    detail_limit_per_error: int = 2,
) -> Dict[str, Any]:
    ctx = Context(
        site_code=site_code.strip().upper(),
        job_name=job_name,
        tz_name=tz_name,
        dry_run=dry_run,
        confirmed=confirmed,
        default_mode=default_mode,
        empty_means_clear=empty_means_clear,
        job_chunk_size=job_chunk_size,
        preview_limit=preview_limit,
        detail_limit_per_error=detail_limit_per_error,
        cfg_sites_url=cfg_sites_url,
        cfg_sites_tab=cfg_sites_tab,
        label_edit=label_edit,
        label_config=label_config,
        label_runlog=label_runlog,
        tab_edit_update=tab_edit_update,
        tab_cfg_fields=tab_cfg_fields,
        tab_runlog=tab_runlog,
        shop_domain=shop_domain,
        api_version=api_version,
        gsheet_sa_b64=gsheet_sa_b64,
        shopify_token=shopify_token,
        run_id=run_id or gen_run_id(job_name, tz_name),
    )

    if (not ctx.dry_run) and (not ctx.confirmed):
        raise ValueError("Apply blocked: set DRY_RUN=False and CONFIRMED=True together.")

    gc = build_gc(ctx.gsheet_sa_b64)
    sc = ShopifyClient(ctx.shop_domain, ctx.api_version, ctx.shopify_token)
    resolver = Resolver(sc)

    url_edit = resolve_sheet_url_from_cfg_sites(gc, ctx.cfg_sites_url, ctx.cfg_sites_tab, ctx.site_code, ctx.label_edit)
    url_config = resolve_sheet_url_from_cfg_sites(gc, ctx.cfg_sites_url, ctx.cfg_sites_tab, ctx.site_code, ctx.label_config)
    url_runlog = resolve_sheet_url_from_cfg_sites(gc, ctx.cfg_sites_url, ctx.cfg_sites_tab, ctx.site_code, ctx.label_runlog)

    sh_edit = gc.open_by_url(url_edit)
    sh_cfg = gc.open_by_url(url_config)
    sh_runlog = gc.open_by_url(url_runlog)

    ws_edit = sh_edit.worksheet(ctx.tab_edit_update)
    ws_fields = sh_cfg.worksheet(ctx.tab_cfg_fields)
    ws_log = sh_runlog.worksheet(ctx.tab_runlog)

    logger = RunLogger(ws_log, ctx)
    logger.ensure_header()

    cfg_by_field_id = load_cfg_fields(ws_fields)
    df = load_edit_rows(
        ws_edit,
        required_cols=["op", "entry_gid", "mode", "field_id", "value", "new_handle"],
    )

    plan_df, warnings_preflight, c1, _ = preflight(df, cfg_by_field_id, resolver, ctx)
    jobs, warnings_build, c2, preview = build_jobs(plan_df, cfg_by_field_id, resolver, ctx)

    warnings_all = warnings_preflight + warnings_build

    summary = {
        "run_id": ctx.run_id,
        "site_code": ctx.site_code,
        "job_name": ctx.job_name,
        "phase": "preview" if ctx.dry_run else "apply",
        "dry_run": ctx.dry_run,
        "confirmed": ctx.confirmed,
        "rows_loaded": c1["rows_loaded"],
        "rows_pending": c1["rows_pending"],
        "rows_recognized": c1["rows_recognized"],
        "rows_planned": c2["rows_planned"],
        "jobs_planned": c2["jobs_planned"],
        "rows_written": 0,
        "rows_skipped": c2["rows_skipped_build"],
        "warning_count": len(warnings_all),
        "edit_url": sh_edit.url,
        "config_url": sh_cfg.url,
        "runlog_url": sh_runlog.url,
        "worksheet_edit": ctx.tab_edit_update,
        "worksheet_runlog": ctx.tab_runlog,
    }

    logger.add(
        phase=summary["phase"],
        log_type="summary",
        status="OK",
        entity_type="METAOBJECT_ENTRY",
        gid="",
        field_key="summary",
        rows_loaded=summary["rows_loaded"],
        rows_pending=summary["rows_pending"],
        rows_recognized=summary["rows_recognized"],
        rows_planned=summary["rows_planned"],
        rows_written=summary["rows_written"],
        rows_skipped=summary["rows_skipped"],
        message=f"pre-execute summary; jobs_planned={summary['jobs_planned']}; warnings={summary['warning_count']}",
        error_reason="",
    )

    if warnings_all:
        for w in warnings_all[:20]:
            logger.add_detail_limited(
                phase=summary["phase"],
                status="WARN",
                entity_type="METAOBJECT_ENTRY",
                gid="",
                field_key="warning",
                message=w,
                error_reason="WARNING",
            )

    exec_result = execute_jobs(jobs, resolver, sc, logger, ctx)
    summary["rows_written"] = exec_result["written"]
    summary["rows_skipped"] = int(summary["rows_skipped"]) + int(exec_result["skip"])
    summary["ok"] = exec_result["ok"]
    summary["fail"] = exec_result["fail"]
    summary["skip"] = exec_result["skip"]

    logger.add(
        phase=summary["phase"],
        log_type="summary",
        status="OK" if exec_result["fail"] == 0 else "PARTIAL_FAIL",
        entity_type="METAOBJECT_ENTRY",
        gid="",
        field_key="summary",
        rows_loaded=summary["rows_loaded"],
        rows_pending=summary["rows_pending"],
        rows_recognized=summary["rows_recognized"],
        rows_planned=summary["rows_planned"],
        rows_written=summary["rows_written"],
        rows_skipped=summary["rows_skipped"],
        message=f"done; ok={summary['ok']} fail={summary['fail']} skip={summary['skip']} jobs={summary['jobs_planned']}",
        error_reason="",
    )
    logger.flush()

    return {
        "summary": summary,
        "preview": preview,
        "warnings": warnings_all[:100],
        "jobs_planned": jobs[:ctx.preview_limit],
    }
