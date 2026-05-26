# shopify_sync/edit_metafieldblocks.py

from __future__ import annotations

import base64
import datetime as dt
import json
import random
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import gspread
import pandas as pd
import requests
from google.oauth2 import service_account

try:
    from google.colab import userdata
except Exception:
    userdata = None


# =========================================================
# Constants
# =========================================================

CFG_SITES_TAB_DEFAULT = "Cfg__Sites"
CFG_ACCOUNT_TAB_DEFAULT = "Cfg__account_id"
CFG_FIELDS_TAB_DEFAULT = "Cfg__Fields"

INPUT_HEADER = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "block_type",
    "block_seq",
    "title",
    "body",
    "value",
    "action",
    "mode",
    "note",
]

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "VARIANT", "COLLECTION", "PAGE"}
SUPPORTED_ACTIONS = {"SET", "CLEAR", "SKIP"}
SUPPORTED_BLOCK_TYPES = {"list_item", "bullet", "feature", "paragraph"}
SUPPORTED_RICH_BLOCK_TYPES = {"bullet", "feature", "paragraph"}

FORBIDDEN_SHOPIFY_PREFIXES = ("mf.shopify.", "v_mf.shopify.", "v.mf.shopify.")

RUNLOG_HEADER = [
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


Q_PRODUCT_BY_HANDLE = """
query($handle: String!) {
  productByHandle(handle: $handle) { id handle title }
}
"""

Q_COLLECTION_BY_HANDLE = """
query($handle: String!) {
  collectionByHandle(handle: $handle) { id handle title }
}
"""

Q_PAGES_BY_QUERY = """
query($q: String!, $first: Int!) {
  pages(first: $first, query: $q) { edges { node { id handle title } } }
}
"""

Q_VARIANTS_BY_QUERY = """
query($q: String!, $first: Int!) {
  productVariants(first: $first, query: $q) { edges { node { id sku } } }
}
"""

Q_NODES_EXIST = """
query($ids: [ID!]!) {
  nodes(ids: $ids) { id }
}
"""

M_SET = """
mutation setMf($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key type value }
    userErrors { field message code }
  }
}
"""


# =========================================================
# Data objects
# =========================================================

@dataclass
class ShopifyClient:
    graph_url: str
    headers: dict[str, str]
    timeout: int = 60


# =========================================================
# Generic utils
# =========================================================

def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _utc_run_id(prefix: str = "edit_metafieldblocks") -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    return s


def _norm_upper(x: Any) -> str:
    return _norm_str(x).upper()


def _norm_block_type(x: Any) -> str:
    return _norm_str(x).lower()


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _safe_int_or_none(x: Any) -> Optional[int]:
    s = _norm_str(x)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _chunk_list(items: list[Any], size: int):
    for i in range(0, len(items), size):
        yield i, items[i:i + size]


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        v = _norm_str(item)
        if not v:
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _is_json_array_string(s: str) -> bool:
    s = _norm_str(s)
    if not (s.startswith("[") and s.endswith("]")):
        return False
    try:
        return isinstance(json.loads(s), list)
    except Exception:
        return False


def _split_items(s: str) -> list[str]:
    s = _norm_str(s)
    if not s:
        return []
    # Compatibility only. Standard Wide_MFBs uses Display Name-1/-2/-3 columns.
    # Do not split comma by default because model/brand text may contain comma.
    parts = re.split(r"[;|\n]+", s)
    return [p.strip() for p in parts if p and p.strip()]


# =========================================================
# Secrets / clients
# =========================================================

def _get_secret(secret_name: str) -> str:
    if userdata is None:
        raise RuntimeError("google.colab.userdata is unavailable. This module is intended for Colab runner use.")
    v = userdata.get(secret_name)
    if not v:
        raise ValueError(f"Missing Colab Secret: {secret_name}")
    return v


def build_gsheet_client(gsheet_sa_b64_secret: str) -> gspread.Client:
    sa_b64 = _get_secret(gsheet_sa_b64_secret)
    sa_info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def build_shopify_client(
    shopify_token_secret: str,
    shop_domain: str,
    api_version: str,
    http_timeout: int = 60,
) -> ShopifyClient:
    token = _get_secret(shopify_token_secret)
    return ShopifyClient(
        graph_url=f"https://{shop_domain}/admin/api/{api_version}/graphql.json",
        headers={
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        },
        timeout=http_timeout,
    )


def gql(client: ShopifyClient, query: str, variables: Optional[dict] = None, retries: int = 6) -> dict:
    payload = {"query": query, "variables": variables or {}}
    last_err = None

    for i in range(retries):
        try:
            r = requests.post(
                client.graph_url,
                headers=client.headers,
                json=payload,
                timeout=client.timeout,
            )
            data = r.json()

            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}: {str(data)[:500]}")

            if data.get("errors"):
                raise RuntimeError(data["errors"])

            if data.get("data") is None:
                raise RuntimeError(f"No data returned: {data}")

            return data["data"]

        except Exception as e:
            last_err = e
            time.sleep(min(2**i, 12) + random.random())

    raise RuntimeError(f"GraphQL failed after retries: {last_err}")


# =========================================================
# Sheet routing / config
# =========================================================

def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    sh = gc.open_by_url(console_core_url)
    ws = sh.worksheet(cfg_sites_tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"{cfg_sites_tab} is empty")

    required = ["site_code", "label", "sheet_url"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{cfg_sites_tab} missing required columns: {missing}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    m = df[
        (df["site_code"] == site_code.strip().upper())
        & (df["label"] == label.strip())
        & (df["sheet_url"] != "")
    ].copy()

    if m.empty:
        raise ValueError(f"Cannot find sheet_url for site_code={site_code}, label={label} in {cfg_sites_tab}")

    return m.iloc[0]["sheet_url"]


def open_ws_by_label_and_title(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    worksheet_title: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
):
    sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=label,
        cfg_sites_tab=cfg_sites_tab,
    )
    sh = gc.open_by_url(sheet_url)
    ws = sh.worksheet(worksheet_title)
    return sh, ws, sheet_url




# =========================================================
# RunLog
# =========================================================

class RunLogger:
    def __init__(
        self,
        gc: gspread.Client,
        runlog_sheet_url: str,
        runlog_tab_name: str,
        run_id: str,
        job_name: str,
        site_code: str,
        flush_every: int = 200,
    ):
        self.runlog_sheet_url = runlog_sheet_url
        self.runlog_tab_name = runlog_tab_name
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.flush_every = flush_every
        self._buf: list[list[Any]] = []

        sh = gc.open_by_url(runlog_sheet_url)
        try:
            self.ws = sh.worksheet(runlog_tab_name)
        except gspread.WorksheetNotFound:
            self.ws = sh.add_worksheet(title=runlog_tab_name, rows=1000, cols=len(RUNLOG_HEADER))

        self.ws.update(range_name="A1:R1", values=[RUNLOG_HEADER])

    def log_row(
        self,
        *,
        phase: str,
        log_type: str,
        status: str,
        entity_type: str = "",
        gid: str = "",
        field_key: str = "",
        rows_loaded: int = 0,
        rows_pending: int = 0,
        rows_recognized: int = 0,
        rows_planned: int = 0,
        rows_written: int = 0,
        rows_skipped: int = 0,
        message: str = "",
        error_reason: str = "",
    ):
        self._buf.append([
            self.run_id,
            _now_cn_str(),
            self.job_name,
            phase,
            log_type,
            status,
            self.site_code,
            entity_type,
            gid,
            field_key,
            rows_loaded,
            rows_pending,
            rows_recognized,
            rows_planned,
            rows_written,
            rows_skipped,
            message,
            error_reason,
        ])
        if len(self._buf) >= self.flush_every:
            self.flush()

    def flush(self):
        if not self._buf:
            return
        for i in range(6):
            try:
                self.ws.append_rows(self._buf, value_input_option="RAW", table_range="A:R")
                self._buf = []
                return
            except Exception:
                time.sleep(min(2**i, 20) + random.random())
        raise RuntimeError("Failed to write RunLog after retries")


def log_grouped_details(
    logger: RunLogger,
    *,
    phase: str,
    status: str,
    rows_loaded: int,
    rows_pending: int,
    rows_recognized: int,
    rows_planned: int,
    rows_written: int,
    rows_skipped: int,
    detail_rows: list[dict[str, Any]],
    max_per_reason: int = 3,
):
    grouped = defaultdict(list)
    for r in detail_rows:
        reason = _norm_str(r.get("error_reason")) or "unknown"
        grouped[reason].append(r)

    for reason, items in grouped.items():
        for row in items[:max_per_reason]:
            logger.log_row(
                phase=phase,
                log_type="detail",
                status=status,
                entity_type=_norm_str(row.get("entity_type")),
                gid=_norm_str(row.get("owner_id") or row.get("gid") or row.get("gid_or_handle")),
                field_key=_norm_str(row.get("field_key")),
                rows_loaded=rows_loaded,
                rows_pending=rows_pending,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                message=_norm_str(row.get("message")),
                error_reason=reason,
            )

def load_account_config(
    gc_console: gspread.Client,
    console_core_url: str,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
) -> dict[str, str]:
    """
    Cfg__account_id belongs to Console Core itself.
    Do NOT route this tab through sheet_label=config.
    """
    sh_console = gc_console.open_by_url(console_core_url)
    ws = sh_console.worksheet(cfg_account_tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"{cfg_account_tab} is empty in Console Core")

    lower_cols = {str(c).lower().strip(): c for c in df.columns}

    if "key" in lower_cols and "value" in lower_cols:
        k_col = lower_cols["key"]
        v_col = lower_cols["value"]
    elif "config_key" in lower_cols and "config_value" in lower_cols:
        k_col = lower_cols["config_key"]
        v_col = lower_cols["config_value"]
    else:
        raise ValueError(
            f"{cfg_account_tab} must contain key/value or config_key/config_value columns. "
            f"Found columns: {list(df.columns)}"
        )

    out = {}
    for r in df.to_dict("records"):
        k = _norm_str(r.get(k_col))
        v = _norm_str(r.get(v_col))
        if k:
            out[k] = v
    return out


def _pick_account_value(account_cfg: dict[str, str], keys: list[str], required: bool = True) -> str:
    for k in keys:
        v = _norm_str(account_cfg.get(k))
        if v:
            return v
    if required:
        raise ValueError(f"Missing required account config. Tried keys: {keys}")
    return ""


# =========================================================
# Cfg__Fields / type resolving
# =========================================================

def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = ws_cfg_fields.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    for c in ["display_name", "entity_type", "field_key", "data_type", "source_type"]:
        if c not in df.columns:
            df[c] = ""

    df["display_name"] = df["display_name"].astype(str).str.strip()
    df["entity_type"] = df["entity_type"].astype(str).str.upper().str.strip()
    df["field_key"] = df["field_key"].astype(str).str.strip()
    df["data_type"] = df["data_type"].astype(str).str.strip().str.lower()
    df["source_type"] = df["source_type"].astype(str).str.upper().str.strip()

    df = df[
        (df["source_type"].eq("METAFIELD"))
        | (df["field_key"].str.startswith("mf."))
        | (df["field_key"].str.startswith("v_mf."))
    ].copy()

    df = df[(df["entity_type"] != "") & (df["field_key"] != "")].copy()
    return df


def build_cfg_field_map(cfg_fields: pd.DataFrame) -> dict[tuple[str, str], dict[str, str]]:
    key_cols = ["entity_type", "field_key"]
    dup = cfg_fields[cfg_fields.duplicated(key_cols, keep=False)].copy()
    if not dup.empty:
        examples = dup[key_cols + ["display_name", "data_type"]].head(50).to_dict("records")
        raise ValueError({
            "message": "Cfg__Fields has duplicate entity_type + field_key. It must be unique.",
            "examples": examples,
        })

    out = {}
    for r in cfg_fields.to_dict("records"):
        out[(_norm_upper(r.get("entity_type")), _norm_str(r.get("field_key")))] = {
            "display_name": _norm_str(r.get("display_name")),
            "data_type": _norm_str(r.get("data_type")).lower(),
            "source_type": _norm_upper(r.get("source_type")),
        }
    return out


def parse_field_key(field_key: str):
    field_key = _norm_str(field_key)
    if field_key.startswith("mf."):
        prefix = "mf."
    elif field_key.startswith("v_mf."):
        prefix = "v_mf."
    else:
        return None

    rest = field_key[len(prefix):]
    parts = rest.split(".")
    if len(parts) < 2:
        return None

    namespace = parts[0]
    key = ".".join(parts[1:])
    if not namespace or not key:
        return None

    return prefix, namespace, key


def _ref_scalar_default(reference_default_kind: str) -> str:
    k = _norm_str(reference_default_kind).lower() or "mixed"
    return "metaobject_reference" if k == "metaobject" else "mixed_reference"


def _ref_list_default(reference_default_kind: str) -> str:
    k = _norm_str(reference_default_kind).lower() or "mixed"
    return "list.metaobject_reference" if k == "metaobject" else "list.mixed_reference"


def map_cfg_dtype_to_shopify_type(cfg_dt: str, reference_default_kind: str = "mixed") -> str:
    dt_ = _norm_str(cfg_dt).lower()

    explicit_scalars = {
        "boolean", "json", "multi_line_text_field", "number_decimal", "number_integer", "rich_text_field", "single_line_text_field",
        "product_reference", "variant_reference", "collection_reference", "metaobject_reference", "mixed_reference",
    }
    if dt_ in explicit_scalars:
        return dt_

    if dt_.startswith("list."):
        inner = dt_[5:].strip()
        explicit_list_inner = {
            "boolean", "json", "multi_line_text_field", "number_decimal", "number_integer", "single_line_text_field",
            "product_reference", "variant_reference", "collection_reference", "metaobject_reference", "mixed_reference",
        }
        if inner in explicit_list_inner:
            return "list." + inner
        if inner in ("reference", "ref"):
            return _ref_list_default(reference_default_kind)
        if inner == "string":
            return "list.single_line_text_field"
        if inner == "text":
            return "list.multi_line_text_field"
        if inner in ("int", "integer"):
            return "list.number_integer"
        if inner in ("float", "decimal"):
            return "list.number_decimal"
        return "list.single_line_text_field"

    if dt_ in ("reference", "ref"):
        return _ref_scalar_default(reference_default_kind)
    if dt_ == "text":
        return "multi_line_text_field"
    if dt_ in ("number", "int", "integer"):
        return "number_integer"
    if dt_ in ("decimal", "float"):
        return "number_decimal"

    return "single_line_text_field"


def validate_block_type_for_data_type(block_type: str, data_type: str) -> Optional[str]:
    bt = _norm_block_type(block_type)
    dt_ = _norm_str(data_type).lower()

    if dt_.startswith("list."):
        if bt != "list_item":
            return f"data_type={dt_} only allows block_type=list_item"
        return None

    if dt_ == "rich_text_field":
        if bt not in SUPPORTED_RICH_BLOCK_TYPES:
            return f"data_type=rich_text_field only allows block_type=bullet/feature/paragraph"
        return None

    return f"data_type={dt_} is not supported by Edit__MetafieldBlocks. Use Edit__ValuesLong for single/simple fields."


# =========================================================
# Input loading / block planning
# =========================================================

def load_blocks_sheet(ws_blocks, mode_default: str = "STRICT", action_default: str = "SET") -> pd.DataFrame:
    rows = ws_blocks.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=INPUT_HEADER + ["_sheet_row", "_source_order"])

    missing = [c for c in INPUT_HEADER if c not in df.columns]
    if missing:
        raise ValueError(f"Edit__MetafieldBlocks missing required columns: {missing}")

    # run_id is optional. If present, process only blank run_id rows.
    optional_cols = ["run_id"] if "run_id" in df.columns else []
    df = df[INPUT_HEADER + optional_cols].copy()
    df["_sheet_row"] = range(2, 2 + len(df))
    df["_source_order"] = range(len(df))

    for c in INPUT_HEADER + optional_cols:
        df[c] = df[c].apply(_norm_str)

    # Drop fully blank rows.
    df = df[~df[INPUT_HEADER].apply(lambda r: all(_norm_str(x) == "" for x in r), axis=1)].copy()

    if "run_id" in df.columns:
        df = df[df["run_id"].eq("")].copy()

    df["entity_type"] = df["entity_type"].str.upper().str.strip()
    df["block_type"] = df["block_type"].str.lower().str.strip()
    df["action"] = df["action"].str.upper().str.strip().replace("", action_default.upper())
    df["mode"] = df["mode"].str.upper().str.strip().replace("", mode_default.upper())

    return df


def normalize_and_validate_blocks(
    df: pd.DataFrame,
    cfg_field_map: dict[tuple[str, str], dict[str, str]],
    only_entity_types: Optional[set[str]] = None,
    only_field_prefixes: Optional[set[str]] = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if df.empty:
        return df.copy(), []

    d = df.copy()
    errors = []

    if only_entity_types:
        allow = {x.upper() for x in only_entity_types}
        d = d[d["entity_type"].isin(allow)].copy()

    if only_field_prefixes:
        prefixes = tuple(only_field_prefixes)
        d = d[d["field_key"].astype(str).str.startswith(prefixes)].copy()

    d = d[d["action"] != "SKIP"].copy()

    for r in d.to_dict("records"):
        sheet_row = r.get("_sheet_row")
        et = _norm_upper(r.get("entity_type"))
        owner = _norm_str(r.get("gid_or_handle"))
        fk = _norm_str(r.get("field_key"))
        action = _norm_upper(r.get("action"))
        bt = _norm_block_type(r.get("block_type"))

        if et not in SUPPORTED_ENTITY_TYPES:
            errors.append({"sheet_row": sheet_row, "error_reason": "invalid_entity_type", "message": f"entity_type={et}"})
            continue

        if not owner:
            errors.append({"sheet_row": sheet_row, "error_reason": "missing_gid_or_handle", "message": "gid_or_handle is required"})
            continue

        if not fk:
            errors.append({"sheet_row": sheet_row, "error_reason": "missing_field_key", "message": "field_key is required"})
            continue

        if any(fk.lower().startswith(p) for p in FORBIDDEN_SHOPIFY_PREFIXES):
            errors.append({"sheet_row": sheet_row, "error_reason": "forbidden_shopify_field_key", "message": f"field_key={fk}"})
            continue

        parsed = parse_field_key(fk)
        if not parsed:
            errors.append({"sheet_row": sheet_row, "error_reason": "field_key_not_recognized", "message": f"field_key={fk}"})
            continue

        prefix, _, _ = parsed
        if prefix == "v_mf." and et != "VARIANT":
            errors.append({"sheet_row": sheet_row, "error_reason": "prefix_entity_mismatch", "message": f"entity_type={et}, field_key={fk}"})
            continue
        if prefix == "mf." and et == "VARIANT":
            errors.append({"sheet_row": sheet_row, "error_reason": "prefix_entity_mismatch", "message": f"entity_type={et}, field_key={fk}"})
            continue

        if action not in SUPPORTED_ACTIONS:
            errors.append({"sheet_row": sheet_row, "error_reason": "invalid_action", "message": f"action={action}"})
            continue

        cfg = cfg_field_map.get((et, fk))
        if not cfg:
            errors.append({"sheet_row": sheet_row, "error_reason": "field_key_not_in_cfg_fields", "message": f"entity_type={et}, field_key={fk}"})
            continue

        if action != "CLEAR":
            if bt not in SUPPORTED_BLOCK_TYPES:
                errors.append({"sheet_row": sheet_row, "error_reason": "invalid_block_type", "message": f"block_type={bt}"})
                continue

            msg = validate_block_type_for_data_type(bt, cfg.get("data_type", ""))
            if msg:
                errors.append({"sheet_row": sheet_row, "error_reason": "block_type_data_type_mismatch", "message": msg})
                continue

    if errors:
        return d, errors

    return d, []


# =========================================================
# Value builders
# =========================================================

def _sort_group_rows(g: pd.DataFrame) -> pd.DataFrame:
    d = g.copy()
    d["_seq_sort"] = d["block_seq"].apply(_safe_int_or_none)
    d["_seq_sort"] = d["_seq_sort"].apply(lambda x: 999999999 if x is None else x)
    return d.sort_values(["_seq_sort", "_source_order"], kind="stable").copy()


def build_rich_text_bullet(items: list[str]) -> str:
    clean_items = [x.strip() for x in items if x and x.strip()]
    root = {
        "type": "root",
        "children": [
            {
                "type": "list",
                "listType": "unordered",
                "children": [
                    {"type": "list-item", "children": [{"type": "text", "value": item}]}
                    for item in clean_items
                ],
            }
        ],
    }
    return _json_dumps(root)


def build_rich_text_feature(rows: list[dict[str, Any]]) -> str:
    children = []
    for r in rows:
        title = _norm_str(r.get("title"))
        body = _norm_str(r.get("body"))
        if not title and not body:
            continue
        paragraph_children = []
        if title:
            paragraph_children.append({"type": "text", "value": title, "bold": True})
        if body:
            paragraph_children.append({"type": "text", "value": f"\n{body}" if title else body})
        children.append({"type": "paragraph", "children": paragraph_children})
    return _json_dumps({"type": "root", "children": children})


def build_rich_text_paragraph(rows: list[dict[str, Any]]) -> str:
    children = []
    for r in rows:
        title = _norm_str(r.get("title"))
        body = _norm_str(r.get("body") or r.get("value"))
        if not title and not body:
            continue
        paragraph_children = []
        if title:
            paragraph_children.append({"type": "text", "value": title, "bold": True})
        if body:
            paragraph_children.append({"type": "text", "value": f"\n{body}" if title else body})
        children.append({"type": "paragraph", "children": paragraph_children})
    return _json_dumps({"type": "root", "children": children})


def to_product_gid(x: str) -> str:
    s = _norm_str(x)
    if s.startswith("gid://shopify/Product/"):
        return s
    if re.fullmatch(r"\d+", s):
        return f"gid://shopify/Product/{s}"
    raise ValueError(f"Invalid Product reference value: {s}")


def to_variant_gid(x: str) -> str:
    s = _norm_str(x)
    if s.startswith("gid://shopify/ProductVariant/"):
        return s
    if re.fullmatch(r"\d+", s):
        return f"gid://shopify/ProductVariant/{s}"
    raise ValueError(f"Invalid Variant reference value: {s}")


def to_collection_gid(x: str) -> str:
    s = _norm_str(x)
    if s.startswith("gid://shopify/Collection/"):
        return s
    if re.fullmatch(r"\d+", s):
        return f"gid://shopify/Collection/{s}"
    raise ValueError(f"Invalid Collection reference value: {s}")


def normalize_reference_items_by_type(mf_type: str, items: list[str]) -> list[str]:
    t = _norm_str(mf_type).lower()
    if t == "list.product_reference":
        return [to_product_gid(x) for x in items]
    if t == "product_reference":
        return [to_product_gid(items[0])] if items else []
    if t == "list.variant_reference":
        return [to_variant_gid(x) for x in items]
    if t == "variant_reference":
        return [to_variant_gid(items[0])] if items else []
    if t == "list.collection_reference":
        return [to_collection_gid(x) for x in items]
    if t == "collection_reference":
        return [to_collection_gid(items[0])] if items else []
    return items


def value_for_clear(mf_type: str) -> str:
    return "[]" if _norm_str(mf_type).startswith("list.") else ""


def build_grouped_metafield_inputs(
    df_blocks: pd.DataFrame,
    cfg_field_map: dict[tuple[str, str], dict[str, str]],
    reference_default_kind: str = "mixed",
    dedupe_list_values: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Returns:
      set_inputs: Shopify MetafieldsSetInput list without ownerId resolved yet.
      meta_rows: metadata corresponding to set_inputs.
      preview_rows: human-readable preview.
      errors: build errors.
    """
    set_inputs = []
    meta_rows = []
    preview_rows = []
    errors = []

    if df_blocks.empty:
        return set_inputs, meta_rows, preview_rows, errors

    group_cols = ["entity_type", "gid_or_handle", "field_key"]

    for group_key, g0 in df_blocks.groupby(group_cols, dropna=False, sort=False):
        entity_type, gid_or_handle, field_key = group_key
        et = _norm_upper(entity_type)
        owner_ref = _norm_str(gid_or_handle)
        fk = _norm_str(field_key)
        g = _sort_group_rows(g0)

        cfg = cfg_field_map.get((et, fk))
        if not cfg:
            errors.append({
                "entity_type": et,
                "gid_or_handle": owner_ref,
                "field_key": fk,
                "error_reason": "field_key_not_in_cfg_fields",
                "message": f"entity_type={et}, field_key={fk}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        parsed = parse_field_key(fk)
        if not parsed:
            errors.append({
                "entity_type": et,
                "gid_or_handle": owner_ref,
                "field_key": fk,
                "error_reason": "field_key_not_recognized",
                "message": f"field_key={fk}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        _, namespace, key = parsed
        mf_type = map_cfg_dtype_to_shopify_type(cfg.get("data_type", ""), reference_default_kind=reference_default_kind)

        actions = sorted(set(g["action"].astype(str).str.upper().str.strip().tolist()))
        actions = [a for a in actions if a]
        if len(actions) > 1:
            errors.append({
                "entity_type": et,
                "gid_or_handle": owner_ref,
                "field_key": fk,
                "error_reason": "mixed_actions_in_group",
                "message": f"actions={actions}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        action = actions[0] if actions else "SET"
        mode = _norm_str(g["mode"].dropna().iloc[0]) or "STRICT"
        notes = [n for n in g["note"].astype(str).tolist() if _norm_str(n)]
        note = " | ".join(notes[:3])

        if action == "CLEAR":
            value_to_write = value_for_clear(mf_type)
            block_type = "CLEAR"
        else:
            block_types = [x for x in g["block_type"].astype(str).str.lower().str.strip().tolist() if x]
            unique_block_types = sorted(set(block_types))
            if len(unique_block_types) != 1:
                errors.append({
                    "entity_type": et,
                    "gid_or_handle": owner_ref,
                    "field_key": fk,
                    "error_reason": "mixed_block_types_in_group",
                    "message": f"block_types={unique_block_types}",
                    "sheet_rows": g["_sheet_row"].tolist(),
                })
                continue
            block_type = unique_block_types[0]
            records = g.to_dict("records")

            if block_type == "list_item":
                values = []
                for r in records:
                    raw = _norm_str(r.get("value"))
                    if not raw:
                        continue
                    # Compatibility only: split legacy single-cell values containing ; | newline.
                    if ";" in raw or "|" in raw or "\n" in raw:
                        values.extend(_split_items(raw))
                    else:
                        values.append(raw)
                values = [v for v in values if _norm_str(v)]
                if dedupe_list_values:
                    values = _dedupe_keep_order(values)
                values = normalize_reference_items_by_type(mf_type, values)
                value_to_write = _json_dumps(values)

            elif block_type == "bullet":
                items = [_norm_str(r.get("body") or r.get("value")) for r in records]
                value_to_write = build_rich_text_bullet(items)

            elif block_type == "feature":
                value_to_write = build_rich_text_feature(records)

            elif block_type == "paragraph":
                value_to_write = build_rich_text_paragraph(records)

            else:
                errors.append({
                    "entity_type": et,
                    "gid_or_handle": owner_ref,
                    "field_key": fk,
                    "error_reason": "unsupported_block_type",
                    "message": f"block_type={block_type}",
                    "sheet_rows": g["_sheet_row"].tolist(),
                })
                continue

        set_inputs.append({
            "owner_ref": owner_ref,
            "ownerId": "",  # resolved later
            "namespace": namespace,
            "key": key,
            "type": mf_type,
            "value": str(value_to_write),
        })

        meta_rows.append({
            "entity_type": et,
            "gid_or_handle": owner_ref,
            "field_key": fk,
            "action": action,
            "mode": mode,
            "block_type": block_type,
            "mf_type": mf_type,
            "sheet_rows": g["_sheet_row"].tolist(),
            "note": note,
        })

        preview_rows.append({
            "entity_type": et,
            "gid_or_handle": owner_ref,
            "field_key": fk,
            "action": action,
            "mode": mode,
            "block_type": block_type,
            "mf_type": mf_type,
            "value_preview": str(value_to_write)[:500],
            "sheet_rows": ",".join(str(x) for x in g["_sheet_row"].tolist()[:10]),
        })

    return set_inputs, meta_rows, preview_rows, errors


# =========================================================
# Owner resolution
# =========================================================

def normalize_owner_ref(entity_type: str, gid_or_handle: str) -> str:
    s = _norm_str(gid_or_handle)
    if s.startswith("gid://"):
        return s
    if re.fullmatch(r"\d+", s):
        if entity_type == "PRODUCT":
            return f"gid://shopify/Product/{s}"
        if entity_type == "VARIANT":
            return f"gid://shopify/ProductVariant/{s}"
        if entity_type == "COLLECTION":
            return f"gid://shopify/Collection/{s}"
        if entity_type == "PAGE":
            return f"gid://shopify/Page/{s}"
    return s


def resolve_product_by_handle(client: ShopifyClient, handle: str) -> Optional[str]:
    data = gql(client, Q_PRODUCT_BY_HANDLE, {"handle": handle})
    node = data.get("productByHandle")
    return node["id"] if node else None


def resolve_collection_by_handle(client: ShopifyClient, handle: str) -> Optional[str]:
    data = gql(client, Q_COLLECTION_BY_HANDLE, {"handle": handle})
    node = data.get("collectionByHandle")
    return node["id"] if node else None


def resolve_page_by_handle(client: ShopifyClient, handle: str) -> Optional[str]:
    q = f'handle:"{handle}"'
    data = gql(client, Q_PAGES_BY_QUERY, {"q": q, "first": 5})
    edges = ((data.get("pages") or {}).get("edges") or [])
    return edges[0]["node"]["id"] if edges else None


def resolve_variant_by_sku(client: ShopifyClient, sku: str) -> Optional[str]:
    q = f'sku:"{sku}"'
    data = gql(client, Q_VARIANTS_BY_QUERY, {"q": q, "first": 5})
    edges = ((data.get("productVariants") or {}).get("edges") or [])
    return edges[0]["node"]["id"] if edges else None


def nodes_exist_map(client: ShopifyClient, ids: list[str], chunk_size: int = 80) -> dict[str, bool]:
    out = {}
    ids = [x for x in ids if isinstance(x, str) and x.strip()]
    for _, part in _chunk_list(ids, chunk_size):
        data = gql(client, Q_NODES_EXIST, {"ids": part})
        nodes = data.get("nodes") or []
        exist_set = {n["id"] for n in nodes if n and n.get("id")}
        for x in part:
            out[x] = x in exist_set
    return out


def resolve_owner_ids(
    client: ShopifyClient,
    set_inputs: list[dict[str, Any]],
    meta_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    resolved_inputs = []
    resolved_meta = []
    errors = []

    need_resolve = []
    for inp, meta in zip(set_inputs, meta_rows):
        et = _norm_upper(meta.get("entity_type"))
        raw = _norm_str(meta.get("gid_or_handle"))
        owner_ref = normalize_owner_ref(et, raw)
        inp["owner_ref"] = owner_ref

        if owner_ref.startswith("gid://"):
            inp["ownerId"] = owner_ref
        else:
            need_resolve.append((et, owner_ref))

    unique_need = sorted(set(need_resolve))
    cache = {}
    for et, ref in unique_need:
        if et == "PRODUCT":
            cache[(et, ref)] = resolve_product_by_handle(client, ref)
        elif et == "COLLECTION":
            cache[(et, ref)] = resolve_collection_by_handle(client, ref)
        elif et == "PAGE":
            cache[(et, ref)] = resolve_page_by_handle(client, ref)
        elif et == "VARIANT":
            cache[(et, ref)] = resolve_variant_by_sku(client, ref)
        else:
            cache[(et, ref)] = None

    for inp, meta in zip(set_inputs, meta_rows):
        if not inp.get("ownerId"):
            et = _norm_upper(meta.get("entity_type"))
            inp["ownerId"] = cache.get((et, inp.get("owner_ref")))

    owner_ids = [_norm_str(inp.get("ownerId")) for inp in set_inputs if _norm_str(inp.get("ownerId"))]
    exist_map = nodes_exist_map(client, sorted(set(owner_ids)), chunk_size=80)

    for inp, meta in zip(set_inputs, meta_rows):
        owner_id = _norm_str(inp.get("ownerId"))
        if not owner_id:
            errors.append({
                "entity_type": meta.get("entity_type", ""),
                "gid_or_handle": meta.get("gid_or_handle", ""),
                "field_key": meta.get("field_key", ""),
                "error_reason": "cannot_resolve_owner_id",
                "message": f"cannot resolve owner_ref={inp.get('owner_ref')}",
            })
            continue
        if not exist_map.get(owner_id, False):
            errors.append({
                "entity_type": meta.get("entity_type", ""),
                "gid_or_handle": meta.get("gid_or_handle", ""),
                "field_key": meta.get("field_key", ""),
                "error_reason": "owner_not_found_in_shop",
                "message": f"owner_id not found: {owner_id}",
            })
            continue

        clean_inp = dict(inp)
        clean_inp.pop("owner_ref", None)
        resolved_inputs.append(clean_inp)

        meta2 = dict(meta)
        meta2["owner_id"] = owner_id
        resolved_meta.append(meta2)

    return resolved_inputs, resolved_meta, errors


# =========================================================
# Apply Shopify writes
# =========================================================

def parse_error_index(field_path):
    try:
        if isinstance(field_path, list) and len(field_path) >= 2 and str(field_path[0]) == "metafields":
            return int(field_path[1])
    except Exception:
        return None
    return None


def apply_metafields_set(
    client: ShopifyClient,
    set_inputs: list[dict[str, Any]],
    meta_rows: list[dict[str, Any]],
    set_batch_size: int = 25,
) -> dict[str, Any]:
    total = len(set_inputs)
    if total == 0:
        print("=== Applying metafieldsSet === total=0")
        return {"ok_count": 0, "fail_count": 0, "total": 0, "detail_fail_rows": []}

    total_batches = (total + set_batch_size - 1) // set_batch_size
    print(f"=== Applying metafieldsSet === total={total}, batches={total_batches}, batch_size={set_batch_size}")

    ok_count = 0
    fail_count = 0
    detail_fail_rows = []

    for batch_no, (start_idx, batch) in enumerate(_chunk_list(set_inputs, set_batch_size), start=1):
        meta_batch = meta_rows[start_idx:start_idx + len(batch)]
        print(f"Batch {batch_no}/{total_batches}: {len(batch)} items ... ", end="", flush=True)

        try:
            data = gql(client, M_SET, {"metafields": batch})
            resp = data["metafieldsSet"]
            user_errors = resp.get("userErrors") or []

            if not user_errors:
                ok_count += len(batch)
                print("OK", flush=True)
                continue

            err_by_i = {}
            non_indexed_errors = []
            for e in user_errors:
                idx = parse_error_index(e.get("field"))
                if idx is None:
                    non_indexed_errors.append(e)
                else:
                    err_by_i.setdefault(idx, []).append(e)

            fail_items = 0
            for idx, errs in err_by_i.items():
                if not (0 <= idx < len(meta_batch)):
                    continue
                fail_items += 1
                r = meta_batch[idx]
                inp = batch[idx]
                detail_fail_rows.append({
                    "entity_type": r.get("entity_type", ""),
                    "gid_or_handle": r.get("gid_or_handle", ""),
                    "field_key": r.get("field_key", ""),
                    "error_reason": "shopify_user_error",
                    "message": (
                        f"code={errs[0].get('code', '')} | msg={errs[0].get('message', '')} | "
                        f"field={errs[0].get('field')} | ns={inp.get('namespace')} key={inp.get('key')} "
                        f"type={inp.get('type')} value={str(inp.get('value'))[:160]}"
                    ),
                })

            if fail_items == 0:
                fail_count += len(batch)
                print(f"FAILED (fail={len(batch)})", flush=True)
                detail_fail_rows.append({
                    "entity_type": "",
                    "gid_or_handle": "",
                    "field_key": "",
                    "error_reason": "shopify_batch_error",
                    "message": (
                        f"batch_error start={start_idx} size={len(batch)} | no per-item index returned | "
                        f"user_errors={json.dumps(non_indexed_errors, ensure_ascii=False)[:500]}"
                    ),
                })
            else:
                batch_ok = len(batch) - fail_items
                ok_count += batch_ok
                fail_count += fail_items
                print(f"PARTIAL_FAIL (ok={batch_ok}, fail={fail_items})", flush=True)

                for e in non_indexed_errors[:3]:
                    detail_fail_rows.append({
                        "entity_type": "",
                        "gid_or_handle": "",
                        "field_key": "",
                        "error_reason": "shopify_batch_error",
                        "message": f"batch_error start={start_idx} size={len(batch)} | code={e.get('code', '')} | msg={e.get('message', '')} | field={e.get('field')}",
                    })

        except Exception as e:
            fail_count += len(batch)
            print("FAILED", flush=True)
            print(f"  exception: {e}", flush=True)
            for r, inp in zip(meta_batch, batch):
                detail_fail_rows.append({
                    "entity_type": r.get("entity_type", ""),
                    "gid_or_handle": r.get("gid_or_handle", ""),
                    "field_key": r.get("field_key", ""),
                    "error_reason": "batch_exception",
                    "message": f"exception: {e} | ns={inp.get('namespace')} key={inp.get('key')} type={inp.get('type')}",
                })

    print(f"=== Apply done === total={total}, ok={ok_count}, fail={fail_count}", flush=True)
    return {"ok_count": ok_count, "fail_count": fail_count, "total": total, "detail_fail_rows": detail_fail_rows}


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    console_gsheet_sa_b64_secret: str,

    job_name: str = "edit_metafieldblocks",

    input_sheet_label: str = "edit",
    input_worksheet_title: str = "Edit__MetafieldBlocks",

    config_sheet_label: str = "config",
    runlog_sheet_label: str = "runlog_sheet",
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    runlog_tab_name: str = "Ops__RunLog",

    dry_run: bool = True,
    confirmed: bool = False,
    preview_limit: int = 50,

    mode_default: str = "STRICT",
    action_default: str = "SET",
    only_entity_types: Optional[set[str]] = None,
    only_field_prefixes: Optional[set[str]] = None,

    reference_default_kind: str = "mixed",
    dedupe_list_values: bool = True,

    set_batch_size: int = 25,
    http_timeout: int = 60,
    detail_max_per_reason: int = 3,
) -> dict[str, Any]:
    """
    Direct Shopify writer for structured metafield blocks.

    Responsibility:
      edit / Edit__MetafieldBlocks -> Shopify metafieldsSet

    It does NOT write Edit__ValuesLong.
    It writes RunLog to sheet_label=runlog_sheet / runlog_tab_name.
    """
    run_id = _utc_run_id("edit_metafieldblocks")

    gc_console = build_gsheet_client(console_gsheet_sa_b64_secret)
    account_cfg = load_account_config(
        gc_console=gc_console,
        console_core_url=console_core_url,
        cfg_account_tab=cfg_account_tab,
    )

    site_gsheet_secret = _pick_account_value(
        account_cfg,
        ["GSHEET_SA_B64_SECRET", f"{site_code.upper()}_GSHEET_SA_B64_SECRET", "gsheet_sa_b64_secret"],
        required=True,
    )
    shopify_token_secret = _pick_account_value(
        account_cfg,
        ["SHOPIFY_TOKEN_SECRET", f"{site_code.upper()}_SHOPIFY_TOKEN_SECRET", "shopify_token_secret"],
        required=True,
    )
    shop_domain = _pick_account_value(
        account_cfg,
        ["SHOP_DOMAIN", "SHOPIFY_SHOP_DOMAIN", "shop_domain"],
        required=True,
    )
    api_version = _pick_account_value(
        account_cfg,
        ["SHOPIFY_API_VERSION", "API_VERSION", "api_version"],
        required=False,
    ) or "2026-04"

    gc_site = build_gsheet_client(site_gsheet_secret)

    _, ws_blocks, input_sheet_url = open_ws_by_label_and_title(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_cfg_fields, cfg_sheet_url = open_ws_by_label_and_title(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        label=config_sheet_label,
        worksheet_title=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
    )

    runlog_sheet_url = get_sheet_url_by_label(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        label=runlog_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )
    logger = RunLogger(
        gc=gc_site,
        runlog_sheet_url=runlog_sheet_url,
        runlog_tab_name=runlog_tab_name,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
    )

    def _meta(extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        base = {
            "console_core_url": console_core_url,
            "input_sheet_url": input_sheet_url,
            "cfg_sheet_url": cfg_sheet_url,
            "runlog_sheet_url": runlog_sheet_url,
            "runlog_tab_name": runlog_tab_name,
            "input_worksheet_title": input_worksheet_title,
            "cfg_tab_fields": cfg_tab_fields,
            "site_gsheet_secret": site_gsheet_secret,
            "shop_domain": shop_domain,
            "api_version": api_version,
            "dry_run": dry_run,
            "confirmed": confirmed,
        }
        if extra:
            base.update(extra)
        return base

    def _result(
        *,
        status: str,
        summary: dict[str, Any],
        phase: str,
        log_status: str,
        message: str,
        error_reason: str = "",
        errors: Optional[list[dict[str, Any]]] = None,
        warnings: Optional[list[dict[str, Any]]] = None,
        preview: Optional[list[dict[str, Any]]] = None,
        meta_extra: Optional[dict[str, Any]] = None,
        detail_status: str = "FAIL",
    ) -> dict[str, Any]:
        rows_loaded = int(summary.get("rows_loaded", 0) or 0)
        rows_pending = int(summary.get("rows_pending", summary.get("rows_in_scope", 0)) or 0)
        rows_recognized = int(summary.get("rows_recognized", summary.get("rows_in_scope", 0)) or 0)
        rows_planned = int(summary.get("rows_planned", 0) or 0)
        rows_written = int(summary.get("rows_written", summary.get("written", 0)) or 0)
        rows_skipped = int(summary.get("rows_skipped", len(errors or [])) or 0)

        logger.log_row(
            phase=phase,
            log_type="summary",
            status=log_status,
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            message=message,
            error_reason=error_reason,
        )
        if errors:
            log_grouped_details(
                logger,
                phase=phase,
                status=detail_status,
                rows_loaded=rows_loaded,
                rows_pending=rows_pending,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                detail_rows=errors,
                max_per_reason=detail_max_per_reason,
            )
        logger.flush()

        return {
            "status": status,
            "site_code": site_code,
            "job_name": job_name,
            "run_id": run_id,
            "summary": summary,
            "errors": (errors or [])[:preview_limit],
            "warnings": (warnings or [])[:preview_limit],
            "preview": (preview or [])[:preview_limit],
            "meta": _meta(meta_extra),
        }

    cfg_fields = load_cfg_fields(ws_cfg_fields)
    cfg_field_map = build_cfg_field_map(cfg_fields)

    df_raw = load_blocks_sheet(
        ws_blocks,
        mode_default=mode_default,
        action_default=action_default,
    )
    rows_loaded = int(len(df_raw))

    df_blocks, validation_errors = normalize_and_validate_blocks(
        df=df_raw,
        cfg_field_map=cfg_field_map,
        only_entity_types=only_entity_types,
        only_field_prefixes=only_field_prefixes,
    )
    rows_in_scope = int(len(df_blocks))

    if validation_errors:
        summary = {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_in_scope,
            "rows_recognized": rows_in_scope,
            "rows_planned": 0,
            "rows_written": 0,
            "rows_skipped": int(len(validation_errors)),
            "errors": int(len(validation_errors)),
        }
        return _result(
            status="error",
            summary=summary,
            phase="preview",
            log_status="ERROR",
            message=f"Validation failed. errors={len(validation_errors)}",
            error_reason="validation_errors",
            errors=validation_errors,
            preview=[],
        )

    set_inputs, meta_rows, preview_rows, build_errors = build_grouped_metafield_inputs(
        df_blocks=df_blocks,
        cfg_field_map=cfg_field_map,
        reference_default_kind=reference_default_kind,
        dedupe_list_values=dedupe_list_values,
    )

    if build_errors:
        summary = {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_in_scope,
            "rows_recognized": rows_in_scope,
            "rows_planned": int(len(set_inputs)),
            "rows_written": 0,
            "rows_skipped": int(len(build_errors)),
            "errors": int(len(build_errors)),
        }
        return _result(
            status="error",
            summary=summary,
            phase="preview",
            log_status="ERROR",
            message=f"Build failed. errors={len(build_errors)}",
            error_reason="build_errors",
            errors=build_errors,
            preview=preview_rows,
        )

    shopify = build_shopify_client(
        shopify_token_secret=shopify_token_secret,
        shop_domain=shop_domain,
        api_version=api_version,
        http_timeout=http_timeout,
    )

    resolved_inputs, resolved_meta, resolve_errors = resolve_owner_ids(
        client=shopify,
        set_inputs=set_inputs,
        meta_rows=meta_rows,
    )

    if resolve_errors:
        summary = {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_in_scope,
            "rows_recognized": rows_in_scope,
            "rows_planned": int(len(set_inputs)),
            "rows_resolved": int(len(resolved_inputs)),
            "rows_written": 0,
            "rows_skipped": int(len(resolve_errors)),
            "errors": int(len(resolve_errors)),
        }
        return _result(
            status="error",
            summary=summary,
            phase="preview",
            log_status="ERROR",
            message=f"Owner resolution failed. errors={len(resolve_errors)}",
            error_reason="resolve_errors",
            errors=resolve_errors,
            preview=preview_rows,
        )

    if not confirmed:
        summary = {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_in_scope,
            "rows_recognized": rows_in_scope,
            "rows_planned": int(len(resolved_inputs)),
            "rows_written": 0,
            "rows_skipped": 0,
            "errors": 0,
        }
        return _result(
            status="needs_confirmation",
            summary=summary,
            phase="preview",
            log_status="NEEDS_CONFIRMATION",
            message="Preview generated. No Shopify write executed.",
            preview=preview_rows,
            meta_extra={"generated_at_cn": _now_cn_str()},
            detail_status="SKIP",
        )

    if dry_run:
        summary = {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_in_scope,
            "rows_recognized": rows_in_scope,
            "rows_planned": int(len(resolved_inputs)),
            "rows_written": 0,
            "rows_skipped": 0,
            "errors": 0,
        }
        return _result(
            status="dry_run_confirmed_no_apply",
            summary=summary,
            phase="apply",
            log_status="SUCCESS",
            message="Confirmed but DRY_RUN=True. No Shopify write executed.",
            preview=preview_rows,
            meta_extra={"generated_at_cn": _now_cn_str()},
            detail_status="SKIP",
        )

    apply_result = apply_metafields_set(
        client=shopify,
        set_inputs=resolved_inputs,
        meta_rows=resolved_meta,
        set_batch_size=set_batch_size,
    )

    rows_written = int(apply_result["ok_count"])
    apply_fail_count = int(apply_result["fail_count"])
    detail_fail_rows = apply_result.get("detail_fail_rows") or []

    if apply_fail_count > 0 and rows_written > 0:
        status = "partial_success"
        log_status = "PARTIAL_SUCCESS"
    elif apply_fail_count > 0 and rows_written == 0:
        status = "error"
        log_status = "ERROR"
    else:
        status = "applied"
        log_status = "SUCCESS"

    summary = {
        "rows_loaded": rows_loaded,
        "rows_pending": rows_in_scope,
        "rows_recognized": rows_in_scope,
        "rows_planned": int(len(resolved_inputs)),
        "rows_written": rows_written,
        "rows_skipped": int(apply_fail_count),
        "apply_fail_count": apply_fail_count,
        "errors": int(len(detail_fail_rows)),
    }
    return _result(
        status=status,
        summary=summary,
        phase="apply",
        log_status=log_status,
        message=(
            f"Apply completed | rows_planned={len(resolved_inputs)} | "
            f"rows_written={rows_written} | apply_fail_count={apply_fail_count}"
        ),
        error_reason="" if not detail_fail_rows else "shopify_apply_errors",
        errors=detail_fail_rows,
        preview=preview_rows,
        meta_extra={"applied_at_cn": _now_cn_str()},
    )
