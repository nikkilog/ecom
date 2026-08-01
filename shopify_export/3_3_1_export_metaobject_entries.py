# shopify_export/export_metaobject_entries.py

from __future__ import annotations

import base64
import datetime as dt
import json
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo


# =========================================================
# Defaults
# =========================================================
DEFAULT_TZ_NAME = "Asia/Shanghai"
DEFAULT_API_VERSION = "2026-01"

TAB_CFG_SITES = "Cfg__Sites"
TAB_METAOBJECT_DEFS = "Cfg__MetaobjectDefs"
TAB_EXPORT = "MetaobjectEntries"
TAB_RUNLOG = "Ops__RunLog"

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


# =========================================================
# GraphQL
# =========================================================
Q_METAOBJECTS_RICH = """
query ($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      updatedAt
      fields {
        key
        type
        value
        references(first: 50) {
          nodes {
            __typename
            ... on Metaobject { id handle }
            ... on Product { id handle }
            ... on ProductVariant { id sku }
            ... on Collection { id handle }
            ... on Page { id handle }
          }
        }
        reference {
          __typename
          ... on Metaobject { id handle }
          ... on Product { id handle }
          ... on ProductVariant { id sku }
          ... on Collection { id handle }
          ... on Page { id handle }
        }
      }
    }
  }
}
"""

Q_METAOBJECTS_NO_UPDATED = """
query ($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      fields {
        key
        type
        value
        references(first: 50) {
          nodes {
            __typename
            ... on Metaobject { id handle }
            ... on Product { id handle }
            ... on ProductVariant { id sku }
            ... on Collection { id handle }
            ... on Page { id handle }
          }
        }
        reference {
          __typename
          ... on Metaobject { id handle }
          ... on Product { id handle }
          ... on ProductVariant { id sku }
          ... on Collection { id handle }
          ... on Page { id handle }
        }
      }
    }
  }
}
"""

Q_METAOBJECTS_MIN = """
query ($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      fields {
        key
        type
        value
      }
    }
  }
}
"""


# =========================================================
# Models
# =========================================================
@dataclass
class ShopifyClient:
    shop_domain: str
    access_token: str
    api_version: str = DEFAULT_API_VERSION
    timeout: int = 60
    min_sleep: float = 0.10
    max_retries: int = 5

    def __post_init__(self) -> None:
        self.url = f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
            }
        )

    def gql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(
                    self.url,
                    json={"query": query, "variables": variables},
                    timeout=self.timeout,
                )
                text = resp.text
                if resp.status_code >= 500 or resp.status_code == 429:
                    raise RuntimeError(f"HTTP {resp.status_code}: {text[:500]}")

                data = resp.json()
                if data.get("errors"):
                    raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False)[:1000])

                # soft throttle sleep
                try:
                    throttle = (
                        data.get("extensions", {})
                        .get("cost", {})
                        .get("throttleStatus", {})
                    )
                    currently_available = throttle.get("currentlyAvailable")
                    restore_rate = throttle.get("restoreRate")
                    if currently_available is not None and restore_rate:
                        if currently_available < 100:
                            time.sleep(max(0.2, 60.0 / max(restore_rate, 1)))
                        else:
                            time.sleep(self.min_sleep)
                    else:
                        time.sleep(self.min_sleep)
                except Exception:
                    time.sleep(self.min_sleep)

                return data
            except Exception as e:
                last_err = e
                if attempt == self.max_retries:
                    raise
                time.sleep(min(8, 0.7 * attempt + random.random()))
        raise RuntimeError(f"GraphQL failed: {last_err}")


# =========================================================
# General helpers
# =========================================================
def _now_cn_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def _gen_run_id(job_name: str, tz_name: str) -> str:
    return f"{job_name}_{dt.datetime.now(ZoneInfo(tz_name)).strftime('%Y%m%d_%H%M%S')}"


def _build_gc_from_sa_b64(sa_b64: str) -> gspread.Client:
    info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _open_ws_by_url(gc: gspread.Client, url: str, title: str):
    sh = gc.open_by_url(url)
    return sh.worksheet(title)


def _ws_to_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    width = len(header)
    norm_rows = []
    for row in rows:
        row = list(row[:width]) + [""] * max(0, width - len(row))
        norm_rows.append(row)
    return pd.DataFrame(norm_rows, columns=header)


def _ensure_runlog_header(ws) -> None:
    cur = ws.get_all_values()
    if not cur or cur[0] != RUNLOG_HEADER_18:
        ws.clear()
        ws.update("A1:R1", [RUNLOG_HEADER_18])


def _append_rows_safe(ws, rows: List[List[Any]]) -> None:
    if rows:
        ws.append_rows(rows, value_input_option="RAW")


def _clean_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _is_true(x: Any) -> bool:
    return _clean_str(x).upper() in {"TRUE", "1", "YES", "Y"}


def _unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _is_list_field_type(field_type: str) -> bool:
    return _clean_str(field_type).startswith("list.")


def _safe_json_loads(s: Any) -> Optional[Any]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _extract_handle_or_sku(node: Dict[str, Any]) -> str:
    if not isinstance(node, dict):
        return ""
    return _clean_str(node.get("handle") or node.get("sku") or "")


# =========================================================
# Cfg__Sites routing
# =========================================================
def _resolve_site_urls(gc: gspread.Client, console_core_url: str, site_code: str) -> Dict[str, str]:
    ws = _open_ws_by_url(gc, console_core_url, TAB_CFG_SITES)
    df = _ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    df.columns = [str(c).strip() for c in df.columns]
    need = {"site_code", "sheet_url", "label"}
    if not need.issubset(set(df.columns)):
        raise ValueError(f"Cfg__Sites missing columns: {sorted(list(need - set(df.columns)))}")

    dfx = df[df["site_code"].astype(str).str.strip().str.upper() == site_code.strip().upper()].copy()
    if dfx.empty:
        raise ValueError(f"Cfg__Sites: site_code not found: {site_code}")

    out: Dict[str, str] = {}
    for _, r in dfx.iterrows():
        label = _clean_str(r.get("label"))
        sheet_url = _clean_str(r.get("sheet_url"))
        if label and sheet_url:
            out[label] = sheet_url

    required_labels = ["config", "export_other", "runlog_sheet"]
    missing = [x for x in required_labels if x not in out]
    if missing:
        raise ValueError(f"Cfg__Sites missing labels for {site_code}: {missing}")

    return out


# =========================================================
# Defs
# =========================================================
def load_defs_df(gc: gspread.Client, config_url: str) -> pd.DataFrame:
    ws = _open_ws_by_url(gc, config_url, TAB_METAOBJECT_DEFS)
    df = _ws_to_df(ws)
    if df.empty:
        return pd.DataFrame(columns=["type", "type_name", "field_key", "field_type"])

    df.columns = [str(c).strip() for c in df.columns]
    for c in ["type", "field_key", "field_type", "type_name"]:
        if c not in df.columns:
            df[c] = ""
    df["type"] = df["type"].astype(str).str.strip()
    df["field_key"] = df["field_key"].astype(str).str.strip()
    df["field_type"] = df["field_type"].astype(str).str.strip()
    df["type_name"] = df["type_name"].astype(str).str.strip()
    df = df[(df["type"] != "") & (df["field_key"] != "")].copy()
    return df


def resolve_types_to_export(defs_df: pd.DataFrame, only_types: Optional[List[str]] = None) -> List[str]:
    if only_types:
        return [str(x).strip() for x in only_types if str(x).strip()]
    if defs_df.empty:
        return []
    return _unique_keep_order(defs_df["type"].astype(str).tolist())


# =========================================================
# Fetch
# =========================================================
def _pick_query_variant(client: ShopifyClient, mo_type: str) -> Tuple[str, str]:
    probes = [
        ("RICH", Q_METAOBJECTS_RICH),
        ("NO_UPDATED", Q_METAOBJECTS_NO_UPDATED),
        ("MIN", Q_METAOBJECTS_MIN),
    ]
    last_err = None
    for name, q in probes:
        try:
            client.gql(q, {"type": mo_type, "first": 1, "after": None})
            return name, q
        except Exception as e:
            last_err = e
    raise RuntimeError(f"All query variants failed for type={mo_type}: {last_err}")


def fetch_metaobjects_for_type(
    client: ShopifyClient,
    mo_type: str,
) -> Tuple[List[Dict[str, Any]], str]:
    variant_name, query = _pick_query_variant(client, mo_type)

    rows: List[Dict[str, Any]] = []
    after = None
    while True:
        data = client.gql(query, {"type": mo_type, "first": 100, "after": after})
        root = data["data"]["metaobjects"]
        nodes = root.get("nodes", []) or []
        rows.extend(nodes)
        page = root.get("pageInfo", {}) or {}
        if not page.get("hasNextPage"):
            break
        after = page.get("endCursor")
    return rows, variant_name


# =========================================================
# Flatten
# =========================================================
def _flatten_field_record(field: Dict[str, Any]) -> Dict[str, Any]:
    """
    return:
      {
        "key": "...",
        "type": "...",
        "value": "...",
        "single_ref_handle_or_sku": "...",
        "list_handles_or_skus": [...],
      }
    """
    out = {
        "key": _clean_str(field.get("key")),
        "type": _clean_str(field.get("type")),
        "value": field.get("value"),
        "single_ref_handle_or_sku": "",
        "list_handles_or_skus": [],
    }

    ref = field.get("reference")
    if isinstance(ref, dict):
        out["single_ref_handle_or_sku"] = _extract_handle_or_sku(ref)

    refs = (((field.get("references") or {}).get("nodes")) if isinstance(field.get("references"), dict) else None)
    if isinstance(refs, list):
        out["list_handles_or_skus"] = [_extract_handle_or_sku(x) for x in refs if _extract_handle_or_sku(x)]

    return out


def _normalize_scalar_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def _parse_list_value(raw_value: Any) -> List[str]:
    """
    Shopify list values often come as JSON array strings.
    """
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(x) for x in raw_value]
    sv = str(raw_value).strip()
    if not sv:
        return []
    parsed = _safe_json_loads(sv)
    if isinstance(parsed, list):
        return ["" if x is None else str(x) for x in parsed]
    # fallback: treat as single item
    return [sv]


def build_export_rows(
    raw_by_type: Dict[str, List[Dict[str, Any]]],
    defs_df: pd.DataFrame,
    list_join_sep: str = " | ",
    split_hard_cap: int = 50,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    """
    returns:
      df_entries,
      split_widths: {"type.field_key": max_cols},
      extras: ["type.field_key", ...]
    """
    defs_order: Dict[str, List[Tuple[str, str]]] = {}
    defs_ftype_map: Dict[str, str] = {}

    if not defs_df.empty:
        for mo_type, g in defs_df.groupby("type", sort=False):
            items = []
            for _, r in g.iterrows():
                fk = _clean_str(r["field_key"])
                ft = _clean_str(r["field_type"])
                items.append((fk, ft))
                defs_ftype_map[f"{mo_type}.{fk}"] = ft
            defs_order[mo_type] = items

    system_cols = [
        "_sys.type",
        "_sys.entry_gid",
        "_sys.handle",
        "_sys.updated_at",
        "_sys.synced_at",
    ]

    rows_out: List[Dict[str, Any]] = []
    split_widths: Dict[str, int] = {}
    extras_seen: List[str] = []

    for mo_type, entries in raw_by_type.items():
        for node in entries:
            row: Dict[str, Any] = {
                "_sys.type": mo_type,
                "_sys.entry_gid": _clean_str(node.get("id")),
                "_sys.handle": _clean_str(node.get("handle")),
                "_sys.updated_at": _clean_str(node.get("updatedAt")),
                "_sys.synced_at": "",
            }

            fields = node.get("fields") or []
            if not isinstance(fields, list):
                fields = []

            actual_field_keys: List[str] = []

            for field in fields:
                ff = _flatten_field_record(field)
                fk = ff["key"]
                if not fk:
                    continue

                actual_field_keys.append(fk)
                full_key = f"{mo_type}.{fk}"
                field_type = defs_ftype_map.get(full_key, _clean_str(ff["type"]))

                # list.*
                if _is_list_field_type(field_type):
                    items = _parse_list_value(ff["value"])
                    handles = ff["list_handles_or_skus"]

                    row[full_key] = list_join_sep.join(items)

                    width = min(max(len(items), 1), split_hard_cap)
                    split_widths[full_key] = max(split_widths.get(full_key, 1), width)

                    for i in range(width):
                        col = full_key if i == 0 else f"{full_key}_{i}"
                        row[col] = items[i] if i < len(items) else ""

                    if handles:
                        hwidth = min(max(len(handles), 1), split_hard_cap)
                        split_widths[f"{full_key}__handle"] = max(
                            split_widths.get(f"{full_key}__handle", 1), hwidth
                        )
                        row[f"{full_key}__handle"] = handles[0] if len(handles) >= 1 else ""
                        for i in range(hwidth):
                            col = f"{full_key}__handle" if i == 0 else f"{full_key}__handle_{i}"
                            row[col] = handles[i] if i < len(handles) else ""
                else:
                    scalar = _normalize_scalar_value(ff["value"])
                    row[full_key] = scalar

                    # single reference extra readable col
                    ref_h = ff["single_ref_handle_or_sku"]
                    if ref_h:
                        row[f"{full_key}__handle"] = ref_h

            # extras from actual fields not covered by defs
            defs_keys_this_type = {fk for fk, _ in defs_order.get(mo_type, [])}
            for fk in actual_field_keys:
                if fk not in defs_keys_this_type:
                    extras_seen.append(f"{mo_type}.{fk}")

            rows_out.append(row)

    df = pd.DataFrame(rows_out) if rows_out else pd.DataFrame(columns=system_cols)
    if df.empty:
        return pd.DataFrame(columns=system_cols), split_widths, _unique_keep_order(extras_seen)

    # final ordered columns
    final_cols: List[str] = list(system_cols)

    # defs main order
    for mo_type, pairs in defs_order.items():
        for fk, ft in pairs:
            base = f"{mo_type}.{fk}"
            if _is_list_field_type(ft):
                width = max(split_widths.get(base, 1), 1)
                for i in range(width):
                    final_cols.append(base if i == 0 else f"{base}_{i}")

                hbase = f"{base}__handle"
                if hbase in df.columns or split_widths.get(hbase):
                    hwidth = max(split_widths.get(hbase, 1), 1)
                    for i in range(hwidth):
                        final_cols.append(hbase if i == 0 else f"{hbase}_{i}")
            else:
                final_cols.append(base)
                hbase = f"{base}__handle"
                if hbase in df.columns:
                    final_cols.append(hbase)

    # extras tail
    extras_unique = _unique_keep_order(extras_seen)
    for base in extras_unique:
        if base in final_cols:
            continue
        if base in df.columns:
            final_cols.append(base)
        # append split siblings if exist
        i = 1
        while f"{base}_{i}" in df.columns:
            final_cols.append(f"{base}_{i}")
            i += 1
        if f"{base}__handle" in df.columns:
            final_cols.append(f"{base}__handle")
            j = 1
            while f"{base}__handle_{j}" in df.columns:
                final_cols.append(f"{base}__handle_{j}")
                j += 1

    # any remaining columns not yet included
    for c in df.columns.tolist():
        if c not in final_cols:
            final_cols.append(c)

    for c in final_cols:
        if c not in df.columns:
            df[c] = ""

    df = df[final_cols].copy()
    df["_sys.synced_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df.fillna(""), split_widths, extras_unique


# =========================================================
# 2-row header
# =========================================================
def build_two_row_header(cols: List[str]) -> List[List[str]]:
    row1: List[str] = []
    row2: List[str] = []

    for c in cols:
        if c.startswith("_sys."):
            row1.append("_sys")
            row2.append(c.replace("_sys.", "", 1))
            continue

        base = c
        suffix = ""

        m = re.match(r"^(.*?)(__handle(?:_\d+)?|_\d+)$", c)
        if m:
            base = m.group(1)
            suffix = m.group(2)

        parts = base.split(".", 1)
        if len(parts) == 2:
            mo_type, field_key = parts
        else:
            mo_type, field_key = "", base

        row1.append(mo_type)
        row2.append(field_key + suffix)

    return [row1, row2]


# =========================================================
# Write export sheet
# =========================================================
def _read_existing_header_2rows(ws) -> List[List[str]]:
    vals = ws.get_all_values()
    if len(vals) >= 2:
        return [vals[0], vals[1]]
    return []


def write_export_sheet(
    ws,
    df_entries: pd.DataFrame,
    write_mode: str = "OVERWRITE",
) -> Dict[str, Any]:
    cols = df_entries.columns.tolist()
    header2 = build_two_row_header(cols)
    body = df_entries.astype(str).fillna("").values.tolist()
    values = header2 + body

    write_mode = _clean_str(write_mode).upper() or "OVERWRITE"

    if write_mode == "APPEND":
        existing = _read_existing_header_2rows(ws)
        if existing != header2:
            raise ValueError("APPEND aborted: target header != current generated header")
        ws.append_rows(body, value_input_option="RAW")
        return {
            "rows_written": len(body),
            "cols_written": len(cols),
            "write_mode": "APPEND",
        }

    # default overwrite
    ws.clear()
    end_col = max(len(cols), 1)
    # simple full update
    ws.update(values=values, range_name=f"A1:{_col_num_to_a1(end_col)}{len(values)}")
    return {
        "rows_written": len(body),
        "cols_written": len(cols),
        "write_mode": "OVERWRITE",
    }


def _col_num_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


# =========================================================
# Runlog
# =========================================================
def _make_log_row(
    *,
    run_id: str,
    ts_cn: str,
    job_name: str,
    phase: str,
    log_type: str,
    status: str,
    site_code: str,
    entity_type: str = "METAOBJECT_ENTRY",
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
        ts_cn,
        job_name,
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
        message,
        error_reason,
    ]


def write_runlog(
    ws_log,
    summary_row: List[Any],
    detail_rows: List[List[Any]],
) -> None:
    _ensure_runlog_header(ws_log)
    rows = [summary_row] + detail_rows
    _append_rows_safe(ws_log, rows)


# =========================================================
# Summary text
# =========================================================
def _format_split_summary(split_widths: Dict[str, int], limit: int = 20) -> str:
    if not split_widths:
        return "No dynamic split columns."
    items = sorted(split_widths.items(), key=lambda x: (-x[1], x[0]))
    lines = []
    for k, v in items[:limit]:
        lines.append(f"{k} -> {v} col(s)")
    if len(items) > limit:
        lines.append(f"... and {len(items) - limit} more")
    return "\n".join(lines)


# =========================================================
# Public run
# =========================================================
def run(
    *,
    site_code: str,
    console_core_url: str,
    shop_domain: str,
    shopify_access_token: str,
    gsheet_sa_b64: str,
    api_version: str = DEFAULT_API_VERSION,
    job_name: str = "export_metaobject_entries",
    tz_name: str = DEFAULT_TZ_NAME,
    only_types: Optional[List[str]] = None,
    write_mode: str = "OVERWRITE",   # OVERWRITE / APPEND
    dry_run: bool = False,
    export_tab_name: str = TAB_EXPORT,
    runlog_tab_name: str = TAB_RUNLOG,
    list_join_sep: str = " | ",
    split_hard_cap: int = 50,
    print_preview_rows: int = 5,
) -> Dict[str, Any]:
    """
    Pure export job:
    - route by Cfg__Sites
    - defs from config/Cfg__MetaobjectDefs
    - output to export_other/MetaobjectEntries
    - 2-row header
    - APPEND only if header exactly matches
    """
    ts_cn = _now_cn_str(tz_name)
    run_id = _gen_run_id(job_name, tz_name)

    gc = _build_gc_from_sa_b64(gsheet_sa_b64)
    urls = _resolve_site_urls(gc, console_core_url, site_code)

    config_url = urls["config"]
    export_url = urls["export_other"]
    runlog_url = urls["runlog_sheet"]

    defs_df = load_defs_df(gc, config_url)
    types_to_export = resolve_types_to_export(defs_df, only_types=only_types)
    if not types_to_export:
        raise ValueError("No metaobject types resolved from Cfg__MetaobjectDefs")

    client = ShopifyClient(
        shop_domain=shop_domain,
        access_token=shopify_access_token,
        api_version=api_version,
    )

    raw_by_type: Dict[str, List[Dict[str, Any]]] = {}
    query_variants: Dict[str, str] = {}
    detail_rows: List[List[Any]] = []
    detail_error_counter: Dict[str, int] = {}

    total_loaded = 0
    for mo_type in types_to_export:
        try:
            nodes, qv = fetch_metaobjects_for_type(client, mo_type)
            raw_by_type[mo_type] = nodes
            query_variants[mo_type] = qv
            total_loaded += len(nodes)
        except Exception as e:
            reason = "FETCH_TYPE_FAILED"
            if detail_error_counter.get(reason, 0) < 2:
                detail_rows.append(
                    _make_log_row(
                        run_id=run_id,
                        ts_cn=ts_cn,
                        job_name=job_name,
                        phase="preview" if dry_run else "apply",
                        log_type="detail",
                        status="ERROR",
                        site_code=site_code,
                        field_key=mo_type,
                        message=str(e)[:500],
                        error_reason=reason,
                    )
                )
                detail_error_counter[reason] = detail_error_counter.get(reason, 0) + 1

    df_entries, split_widths, extras = build_export_rows(
        raw_by_type=raw_by_type,
        defs_df=defs_df,
        list_join_sep=list_join_sep,
        split_hard_cap=split_hard_cap,
    )

    rows_planned = len(df_entries)
    rows_written = 0
    rows_skipped = 0
    status = "OK"
    write_result: Dict[str, Any] = {}

    split_text = _format_split_summary(split_widths)
    extras_text = ", ".join(extras[:30]) if extras else ""

    if not dry_run:
        try:
            ws_export = _open_ws_by_url(gc, export_url, export_tab_name)
            write_result = write_export_sheet(ws_export, df_entries, write_mode=write_mode)
            rows_written = int(write_result["rows_written"])
        except Exception as e:
            status = "ERROR"
            reason = "WRITE_EXPORT_FAILED"
            if detail_error_counter.get(reason, 0) < 2:
                detail_rows.append(
                    _make_log_row(
                        run_id=run_id,
                        ts_cn=ts_cn,
                        job_name=job_name,
                        phase="apply",
                        log_type="detail",
                        status="ERROR",
                        site_code=site_code,
                        message=str(e)[:500],
                        error_reason=reason,
                    )
                )
                detail_error_counter[reason] = detail_error_counter.get(reason, 0) + 1
    else:
        rows_skipped = rows_planned

    summary_msg_parts = [
        f"types={len(types_to_export)}",
        f"rows_loaded={total_loaded}",
        f"rows_planned={rows_planned}",
        f"rows_written={rows_written}",
        f"write_mode={_clean_str(write_mode).upper()}",
        f"query_variants={json.dumps(query_variants, ensure_ascii=False)}",
        f"dynamic_splits={len(split_widths)}",
    ]
    if extras:
        summary_msg_parts.append(f"defs_missing_fields={extras_text}")

    summary_row = _make_log_row(
        run_id=run_id,
        ts_cn=ts_cn,
        job_name=job_name,
        phase="preview" if dry_run else "apply",
        log_type="summary",
        status=status,
        site_code=site_code,
        rows_loaded=total_loaded,
        rows_pending=rows_planned if dry_run else "",
        rows_recognized=rows_planned,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message=" | ".join(summary_msg_parts),
        error_reason="",
    )

    try:
        ws_log = _open_ws_by_url(gc, runlog_url, runlog_tab_name)
        write_runlog(ws_log, summary_row, detail_rows)
    except Exception as e:
        print(f"[WARN] runlog write failed: {e}")

    preview_df = df_entries.head(print_preview_rows).copy()

    # console output
    print("=" * 80)
    print(f"run_id      : {run_id}")
    print(f"job_name    : {job_name}")
    print(f"site_code   : {site_code}")
    print(f"shop_domain : {shop_domain}")
    print(f"types       : {types_to_export}")
    print(f"rows_loaded : {total_loaded}")
    print(f"rows_planned: {rows_planned}")
    print(f"rows_written: {rows_written}")
    print(f"write_mode  : {_clean_str(write_mode).upper()}")
    print(f"dry_run     : {dry_run}")
    print("-" * 80)
    print("Query variants:")
    for k, v in query_variants.items():
        print(f"  - {k}: {v}")
    print("-" * 80)
    print("Dynamic split summary:")
    print(split_text)
    print("-" * 80)
    if extras:
        print("Defs missing fields found in live data:")
        for x in extras[:50]:
            print(f"  - {x}")
    else:
        print("Defs coverage: no extra live fields found.")
    print("=" * 80)

    return {
        "ok": status == "OK",
        "run_id": run_id,
        "job_name": job_name,
        "site_code": site_code,
        "types_to_export": types_to_export,
        "query_variants": query_variants,
        "rows_loaded": total_loaded,
        "rows_planned": rows_planned,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "write_mode": _clean_str(write_mode).upper(),
        "split_widths": split_widths,
        "defs_missing_fields": extras,
        "preview": preview_df,
        "df_entries": df_entries,
        "write_result": write_result,
    }
