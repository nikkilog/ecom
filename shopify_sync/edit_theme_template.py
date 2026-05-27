# -*- coding: utf-8 -*-
"""
shopify_sync.edit_theme_template

Batch update Shopify theme template suffix for PRODUCT / COLLECTION / PAGE from Google Sheets.

Root-fix version: Cfg__account_id is parsed from raw two-column values so both headerless
and headered key/value config tabs are supported safely.

Input worksheet:
  Edit__ThemeTemplate

Required columns:
  entity_type | gid_or_handle | field_key | desired_value | action | mode | note

Rules:
  - Cfg__Sites: find sheet route by SITE_CODE + label="edit" for the input spreadsheet.
  - Cfg__account_id: read SHOP_DOMAIN, SHOPIFY_API_VERSION, SHOPIFY_TOKEN_SECRET, GSHEET_SA_B64_SECRET.
  - field_key must be core.template_suffix.
  - entity_type supports PRODUCT, COLLECTION, PAGE.
  - action supports SET and CLEAR.
  - desired_value is the template suffix only, not a full template filename.
    Example: templates/product.rubber-track.json => rubber-track
  - DRY_RUN=True only validates and previews; no Shopify write.
  - CONFIRMED=True is required when DRY_RUN=False.
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception as exc:  # pragma: no cover
    raise ImportError(
        "Missing Google Sheets dependencies. Install with: pip install gspread google-auth pandas requests"
    ) from exc


REQUIRED_COLUMNS = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "desired_value",
    "action",
    "mode",
    "note",
]

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "COLLECTION", "PAGE"}
SUPPORTED_FIELD_KEYS = {"core.template_suffix"}
SUPPORTED_ACTIONS = {"SET", "CLEAR"}

ENTITY_TO_GID_TYPE = {
    "PRODUCT": "Product",
    "COLLECTION": "Collection",
    "PAGE": "Page",
}


# -----------------------------
# Basic helpers
# -----------------------------

def _now_cn() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def _run_id(job_name: str) -> str:
    return f"{job_name}_{datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d_%H%M%S')}"


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v).strip()


def _norm_key(v: Any) -> str:
    return _as_str(v).strip()


def _norm_entity(v: Any) -> str:
    return _as_str(v).upper().strip()


def _norm_action(v: Any) -> str:
    s = _as_str(v).upper().strip()
    return s or "SET"


def _is_truthy(v: Any) -> bool:
    s = _as_str(v).lower()
    return s in {"true", "yes", "y", "1", "ok", "ready", "set"}


def _get_secret(secret_name: str) -> str:
    """
    Read secret from:
      1. Google Colab userdata
      2. environment variable
    """
    secret_name = _as_str(secret_name)
    if not secret_name:
        raise ValueError("Secret name is blank.")

    try:
        from google.colab import userdata  # type: ignore
        val = userdata.get(secret_name)
        if val:
            return val
    except Exception:
        pass

    val = os.environ.get(secret_name)
    if val:
        return val

    raise KeyError(f"Secret not found: {secret_name}")


def _load_sa_info_from_b64_secret(secret_name: str) -> Dict[str, Any]:
    raw = _get_secret(secret_name).strip()
    # Supports either base64 JSON or raw JSON.
    if raw.startswith("{"):
        return json.loads(raw)
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)
    except Exception as exc:
        raise ValueError(
            f"Secret {secret_name} is neither raw service-account JSON nor base64-encoded JSON."
        ) from exc


def _gspread_client_from_sa_secret(secret_name: str) -> gspread.Client:
    info = _load_sa_info_from_b64_secret(secret_name)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _sheet_id_from_url_or_id(value: str) -> str:
    value = _as_str(value)
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    if m:
        return m.group(1)
    return value


def _open_spreadsheet(gc: gspread.Client, url_or_id: str) -> gspread.Spreadsheet:
    sid = _sheet_id_from_url_or_id(url_or_id)
    return gc.open_by_key(sid)


def _worksheet_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    headers = [_as_str(h) for h in values[0]]
    rows = values[1:]

    # Make duplicate/blank headers safe for pandas/gspread tables.
    safe_headers: List[str] = []
    seen: Dict[str, int] = {}
    for i, h in enumerate(headers):
        base = h or f"__blank_col_{i + 1}"
        if base in seen:
            seen[base] += 1
            safe_headers.append(f"{base}__dup{seen[base]}")
        else:
            seen[base] = 0
            safe_headers.append(base)

    width = len(safe_headers)
    normalized_rows = []
    for row in rows:
        row = list(row[:width]) + [""] * max(0, width - len(row))
        normalized_rows.append(row)

    df = pd.DataFrame(normalized_rows, columns=safe_headers)
    # Drop fully blank rows.
    if len(df):
        df = df.loc[~df.apply(lambda r: all(_as_str(x) == "" for x in r), axis=1)].copy()
    return df


def _norm_config_key(v: Any) -> str:
    """Normalize config keys so Cfg__account_id is not fragile."""
    s = _as_str(v)
    s = s.replace("﻿", "")
    s = s.replace("：", ":")
    s = re.sub(r"\s+", "", s)
    return s.upper()


def _looks_like_config_header(key: str, value: str) -> bool:
    k = _norm_config_key(key).lower()
    v = _norm_config_key(value).lower()
    header_keys = {
        "key", "configkey", "config_key", "settingkey", "setting_key",
        "name", "fieldkey", "field_key", "accountkey", "account_key",
        "配置项", "配置键", "字段", "字段名",
    }
    header_values = {
        "value", "configvalue", "config_value", "settingvalue", "setting_value",
        "val", "fieldvalue", "field_value", "accountvalue", "account_value",
        "配置值", "值",
    }
    return k in header_keys and v in header_values


def _worksheet_to_key_value_config(
    ws: gspread.Worksheet,
    *,
    tab_name: str = "Cfg__account_id",
    strict_duplicates: bool = True,
) -> Dict[str, str]:
    """
    Read a 2-column key/value config worksheet directly from raw values.

    This intentionally does NOT go through _worksheet_to_df(), because config tabs
    are often headerless. A headerless sheet like this is valid:

        SHOP_DOMAIN              | aeqjdw-r1.myshopify.com
        SHOPIFY_API_VERSION      | 2026-01
        SHOPIFY_TOKEN_SECRET     | NRP_SHOPIFY_ACCESS_TOKEN

    A headered version is also valid:

        config_key               | config_value
        SHOP_DOMAIN              | aeqjdw-r1.myshopify.com

    Rules:
      - first two columns are used; extra columns are ignored
      - fully blank rows are ignored
      - keys are normalized to uppercase, whitespace-free form
      - duplicate keys with different values are blocked
      - duplicate keys with the same value are harmless
    """
    values = ws.get_all_values()
    if not values:
        raise ValueError(f"{tab_name} is empty.")

    out: Dict[str, str] = {}
    raw_seen: Dict[str, str] = {}
    duplicate_conflicts: List[str] = []

    for row_idx, row in enumerate(values, start=1):
        row = list(row)
        key_raw = _as_str(row[0] if len(row) >= 1 else "")
        val_raw = _as_str(row[1] if len(row) >= 2 else "")

        if not key_raw and not val_raw:
            continue

        # Skip a real header row only when both columns look like headers.
        if row_idx == 1 and _looks_like_config_header(key_raw, val_raw):
            continue

        key_norm = _norm_config_key(key_raw)
        if not key_norm:
            continue

        if key_norm in out:
            if out[key_norm] != val_raw:
                duplicate_conflicts.append(
                    f"row {row_idx}: {key_raw} duplicates {raw_seen.get(key_norm, key_norm)} with different values"
                )
            continue

        out[key_norm] = val_raw
        raw_seen[key_norm] = key_raw

    if strict_duplicates and duplicate_conflicts:
        raise ValueError(
            f"{tab_name} has duplicate config keys with different values: "
            + "; ".join(duplicate_conflicts[:10])
        )

    if not out:
        raise ValueError(
            f"{tab_name} did not produce any config key/value pairs. "
            "Expected first two columns to be config key and config value."
        )

    return out


def _dict_from_two_col_df(df: pd.DataFrame) -> Dict[str, str]:
    """Backward-compatible parser for existing callers. Prefer _worksheet_to_key_value_config for config tabs."""
    if df.empty or df.shape[1] < 2:
        return {}

    key_col = df.columns[0]
    val_col = df.columns[1]
    out: Dict[str, str] = {}

    # If df came from a headerless two-column sheet, pandas consumed the first config row as headers.
    # Put it back unless those headers look like real key/value headers.
    if not _looks_like_config_header(key_col, val_col):
        k0 = _norm_config_key(key_col)
        if k0:
            out[k0] = _as_str(val_col)

    for _, row in df.iterrows():
        k = _norm_config_key(row.get(key_col))
        if not k:
            continue
        out[k] = _as_str(row.get(val_col))
    return out


def _find_site_route(cfg_sites: pd.DataFrame, site_code: str, label: str) -> Dict[str, str]:
    required = {"site_code", "label"}
    missing = required - set(cfg_sites.columns)
    if missing:
        raise ValueError(f"Cfg__Sites missing required columns: {sorted(missing)}")

    tmp = cfg_sites.copy()
    tmp["_site_code_norm"] = tmp["site_code"].map(lambda x: _as_str(x).upper())
    tmp["_label_norm"] = tmp["label"].map(lambda x: _as_str(x))
    matched = tmp[(tmp["_site_code_norm"] == site_code.upper()) & (tmp["_label_norm"] == label)]

    if matched.empty:
        raise ValueError(f"Cfg__Sites route not found for site_code={site_code}, label={label}")

    row = matched.iloc[0].to_dict()
    sheet_url = _as_str(row.get("sheet_url")) or _as_str(row.get("sheet_id"))
    if not sheet_url:
        raise ValueError(f"Cfg__Sites route found but sheet_url/sheet_id is blank for label={label}")
    return {k: _as_str(v) for k, v in row.items() if not k.startswith("_")}


def _pick_config(account_cfg: Dict[str, str], key: str, required: bool = True, default: str = "") -> str:
    key_norm = _norm_config_key(key)
    val = _as_str(account_cfg.get(key_norm, default))
    if required and not val:
        available = ", ".join(sorted(account_cfg.keys())) or "<none>"
        raise ValueError(
            f"Cfg__account_id missing required config: {key}. "
            f"Available keys read from Cfg__account_id: {available}"
        )
    return val


# -----------------------------
# Shopify API
# -----------------------------

@dataclass
class ShopifyClient:
    shop_domain: str
    api_version: str
    access_token: str
    timeout: int = 60

    @property
    def endpoint(self) -> str:
        return f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = requests.post(
            self.endpoint,
            headers={
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables or {}},
            timeout=self.timeout,
        )
        try:
            data = resp.json()
        except Exception:
            raise RuntimeError(f"Non-JSON Shopify response HTTP {resp.status_code}: {resp.text[:500]}")

        if resp.status_code >= 400:
            raise RuntimeError(f"Shopify HTTP {resp.status_code}: {json.dumps(data, ensure_ascii=False)[:1000]}")

        if data.get("errors"):
            raise RuntimeError(f"Shopify GraphQL errors: {json.dumps(data.get('errors'), ensure_ascii=False)[:1000]}")

        return data


def _gid_from_numeric(entity_type: str, numeric_id: str) -> str:
    gid_type = ENTITY_TO_GID_TYPE[entity_type]
    return f"gid://shopify/{gid_type}/{numeric_id}"


def _looks_numeric_id(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", s))


def _looks_gid(s: str) -> bool:
    return s.startswith("gid://shopify/")


def _resolve_gid(client: ShopifyClient, entity_type: str, gid_or_handle: str) -> str:
    raw = _as_str(gid_or_handle)
    if not raw:
        raise ValueError("gid_or_handle is blank.")

    if _looks_gid(raw):
        return raw

    if _looks_numeric_id(raw):
        return _gid_from_numeric(entity_type, raw)

    # Handle lookup. Numeric/GID is preferred for PAGE because page handles can collide in older stores.
    handle = raw.strip()

    if entity_type == "PRODUCT":
        q = """
        query ResolveProductByHandle($handle: String!) {
          productByHandle(handle: $handle) { id handle title templateSuffix }
        }
        """
        data = client.graphql(q, {"handle": handle})
        node = data.get("data", {}).get("productByHandle")
        if not node:
            raise ValueError(f"PRODUCT handle not found: {handle}")
        return node["id"]

    if entity_type == "COLLECTION":
        q = """
        query ResolveCollectionByHandle($handle: String!) {
          collectionByHandle(handle: $handle) { id handle title templateSuffix }
        }
        """
        data = client.graphql(q, {"handle": handle})
        node = data.get("data", {}).get("collectionByHandle")
        if not node:
            raise ValueError(f"COLLECTION handle not found: {handle}")
        return node["id"]

    if entity_type == "PAGE":
        q = """
        query ResolvePageByHandle($query: String!) {
          pages(first: 2, query: $query) {
            nodes { id handle title templateSuffix }
          }
        }
        """
        data = client.graphql(q, {"query": f"handle:{handle}"})
        nodes = data.get("data", {}).get("pages", {}).get("nodes", [])
        nodes = [n for n in nodes if _as_str(n.get("handle")) == handle]
        if not nodes:
            raise ValueError(f"PAGE handle not found: {handle}")
        if len(nodes) > 1:
            raise ValueError(f"PAGE handle matched multiple pages, use numeric ID/GID instead: {handle}")
        return nodes[0]["id"]

    raise ValueError(f"Unsupported entity_type: {entity_type}")


def _template_value_for_action(action: str, desired_value: str) -> Optional[str]:
    if action == "CLEAR":
        return None
    val = _as_str(desired_value)
    if val in {"", "CLEAR", "clear", "NULL", "null", "None", "none"}:
        return None
    # Defensive cleanup if someone pasted a full theme filename/path.
    val = re.sub(r"^templates/", "", val)
    val = re.sub(r"^(product|collection|page)\.", "", val)
    val = re.sub(r"\.(json|liquid)$", "", val)
    return val


def _mutation_for(entity_type: str) -> str:
    if entity_type == "PRODUCT":
        return """
        mutation UpdateProductTemplate($product: ProductUpdateInput!) {
          productUpdate(product: $product) {
            product { id handle title templateSuffix }
            userErrors { field message }
          }
        }
        """

    if entity_type == "COLLECTION":
        return """
        mutation UpdateCollectionTemplate($input: CollectionInput!) {
          collectionUpdate(input: $input) {
            collection { id handle title templateSuffix }
            userErrors { field message }
          }
        }
        """

    if entity_type == "PAGE":
        return """
        mutation UpdatePageTemplate($id: ID!, $page: PageUpdateInput!) {
          pageUpdate(id: $id, page: $page) {
            page { id handle title templateSuffix }
            userErrors { field message }
          }
        }
        """

    raise ValueError(f"Unsupported entity_type: {entity_type}")


def _variables_for(entity_type: str, gid: str, template_suffix: Optional[str]) -> Dict[str, Any]:
    if entity_type == "PRODUCT":
        return {"product": {"id": gid, "templateSuffix": template_suffix}}
    if entity_type == "COLLECTION":
        return {"input": {"id": gid, "templateSuffix": template_suffix}}
    if entity_type == "PAGE":
        return {"id": gid, "page": {"templateSuffix": template_suffix}}
    raise ValueError(f"Unsupported entity_type: {entity_type}")


def _extract_mutation_payload(entity_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    root = data.get("data", {})
    key = {
        "PRODUCT": "productUpdate",
        "COLLECTION": "collectionUpdate",
        "PAGE": "pageUpdate",
    }[entity_type]
    payload = root.get(key) or {}
    return payload


def _update_template(client: ShopifyClient, entity_type: str, gid: str, template_suffix: Optional[str]) -> Dict[str, Any]:
    q = _mutation_for(entity_type)
    variables = _variables_for(entity_type, gid, template_suffix)
    data = client.graphql(q, variables)
    payload = _extract_mutation_payload(entity_type, data)
    user_errors = payload.get("userErrors") or []
    if user_errors:
        raise RuntimeError(f"Shopify userErrors: {json.dumps(user_errors, ensure_ascii=False)}")
    node = payload.get("product") or payload.get("collection") or payload.get("page") or {}
    return node


# -----------------------------
# Runlog
# -----------------------------

class RunLogger:
    def __init__(
        self,
        ws: Optional[gspread.Worksheet],
        run_id: str,
        job_name: str,
        site_code: str,
        enabled: bool = True,
    ):
        self.ws = ws
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.enabled = enabled and ws is not None
        self.headers = [
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
        if self.enabled:
            self._ensure_header()

    def _ensure_header(self) -> None:
        values = self.ws.get_all_values()
        if not values:
            self.ws.update("A1:R1", [self.headers])
            return
        current = [h.strip() for h in values[0][: len(self.headers)]]
        if current != self.headers:
            # Do not clear existing log. Just update header row.
            self.ws.update("A1:R1", [self.headers])

    def log(
        self,
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
        row = [
            self.run_id,
            _now_cn(),
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
        ]
        print(f"[{status}] {phase} | {entity_type} | {gid} | {field_key} | {message}")
        if self.enabled:
            self.ws.append_row(row, value_input_option="USER_ENTERED")


# -----------------------------
# Input parsing / validation
# -----------------------------

def _validate_headers(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Input worksheet missing columns: {missing}")


def _build_plan(
    df: pd.DataFrame,
    only_entity_types: Optional[set] = None,
    only_field_keys: Optional[set] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    _validate_headers(df)
    plans: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    only_entity_types = {x.upper() for x in (only_entity_types or SUPPORTED_ENTITY_TYPES)}
    only_field_keys = set(only_field_keys or SUPPORTED_FIELD_KEYS)

    for idx, row in df.iterrows():
        sheet_row = int(idx) + 2
        entity_type = _norm_entity(row.get("entity_type"))
        gid_or_handle = _as_str(row.get("gid_or_handle"))
        field_key = _norm_key(row.get("field_key"))
        desired_value = _as_str(row.get("desired_value"))
        action = _norm_action(row.get("action"))
        mode = _as_str(row.get("mode"))
        note = _as_str(row.get("note"))

        if not any([entity_type, gid_or_handle, field_key, desired_value, action, mode, note]):
            continue

        def add_error(reason: str, msg: str):
            errors.append({
                "sheet_row": sheet_row,
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "action": action,
                "error_reason": reason,
                "message": msg,
            })

        if entity_type not in SUPPORTED_ENTITY_TYPES:
            add_error("unsupported_entity_type", f"entity_type must be one of {sorted(SUPPORTED_ENTITY_TYPES)}")
            continue
        if entity_type not in only_entity_types:
            continue

        if field_key not in SUPPORTED_FIELD_KEYS:
            add_error("unsupported_field_key", "field_key must be core.template_suffix")
            continue
        if field_key not in only_field_keys:
            continue

        if action not in SUPPORTED_ACTIONS:
            add_error("unsupported_action", "action must be SET or CLEAR")
            continue

        if not gid_or_handle:
            add_error("blank_gid_or_handle", "gid_or_handle is required")
            continue

        if action == "SET" and not desired_value:
            add_error("blank_desired_value", "desired_value is required when action=SET")
            continue

        plans.append({
            "sheet_row": sheet_row,
            "entity_type": entity_type,
            "gid_or_handle": gid_or_handle,
            "field_key": field_key,
            "desired_value": desired_value,
            "template_suffix": _template_value_for_action(action, desired_value),
            "action": action,
            "mode": mode,
            "note": note,
        })

    return plans, errors


# -----------------------------
# Main runner
# -----------------------------

def run(
    *,
    site_code: str,
    job_name: str,
    console_core_url: str,
    console_gsheet_sa_b64_secret: str,
    input_sheet_label: str = "edit",
    runlog_sheet_label: str = "runlog_sheet",
    input_worksheet_title: str = "Edit__ThemeTemplate",
    cfg_sites_tab: str = "Cfg__Sites",
    cfg_account_tab: str = "Cfg__account_id",
    runlog_tab_name: str = "Ops__RunLog",
    dry_run: bool = True,
    confirmed: bool = False,
    only_entity_types: Optional[set] = None,
    only_field_keys: Optional[set] = None,
    sleep_seconds: float = 0.15,
) -> Dict[str, Any]:
    """
    Execute edit_theme_template job.
    """
    site_code = _as_str(site_code).upper()
    job_name = _as_str(job_name)
    rid = _run_id(job_name)

    print("====================================")
    print("RUN START")
    print("====================================")
    print("SITE_CODE              =", site_code)
    print("JOB_NAME               =", job_name)
    print("CONSOLE_CORE_URL       =", console_core_url)
    print("INPUT_SHEET_LABEL      =", input_sheet_label)
    print("INPUT_WORKSHEET_TITLE  =", input_worksheet_title)
    print("RUNLOG_SHEET_LABEL     =", runlog_sheet_label)
    print("RUNLOG_TAB_NAME        =", runlog_tab_name)
    print("DRY_RUN                =", dry_run)
    print("CONFIRMED              =", confirmed)
    print("ONLY_ENTITY_TYPES      =", only_entity_types)
    print("ONLY_FIELD_KEYS        =", only_field_keys)
    print("====================================")

    if not dry_run and not confirmed:
        raise ValueError("Blocked: CONFIRMED=True is required when DRY_RUN=False.")

    console_gc = _gspread_client_from_sa_secret(console_gsheet_sa_b64_secret)
    console_sh = _open_spreadsheet(console_gc, console_core_url)

    cfg_sites_ws = console_sh.worksheet(cfg_sites_tab)
    cfg_sites_df = _worksheet_to_df(cfg_sites_ws)

    account_ws = console_sh.worksheet(cfg_account_tab)
    account_cfg = _worksheet_to_key_value_config(account_ws, tab_name=cfg_account_tab)

    input_route = _find_site_route(cfg_sites_df, site_code, input_sheet_label)
    runlog_route = _find_site_route(cfg_sites_df, site_code, runlog_sheet_label)

    site_gsheet_secret = _pick_config(account_cfg, "GSHEET_SA_B64_SECRET", required=False, default=console_gsheet_sa_b64_secret)
    site_gc = _gspread_client_from_sa_secret(site_gsheet_secret)

    input_sh = _open_spreadsheet(site_gc, input_route.get("sheet_url") or input_route.get("sheet_id"))
    input_ws = input_sh.worksheet(input_worksheet_title)
    input_df = _worksheet_to_df(input_ws)

    runlog_ws = None
    try:
        runlog_sh = _open_spreadsheet(site_gc, runlog_route.get("sheet_url") or runlog_route.get("sheet_id"))
        try:
            runlog_ws = runlog_sh.worksheet(runlog_tab_name)
        except gspread.WorksheetNotFound:
            runlog_ws = runlog_sh.add_worksheet(title=runlog_tab_name, rows=1000, cols=30)
    except Exception as exc:
        print(f"[WARN] Runlog sheet unavailable: {exc}")

    logger = RunLogger(runlog_ws, rid, job_name, site_code, enabled=True)

    rows_loaded = len(input_df)
    logger.log(
        phase="load",
        log_type="summary",
        status="ok",
        rows_loaded=rows_loaded,
        message=f"Loaded {rows_loaded} rows from {input_worksheet_title}",
    )

    plans, validation_errors = _build_plan(input_df, only_entity_types, only_field_keys)
    rows_recognized = rows_loaded - len(validation_errors)
    rows_planned = len(plans)

    for err in validation_errors:
        logger.log(
            phase="validate",
            log_type="row",
            status="error",
            entity_type=err.get("entity_type", ""),
            gid=err.get("gid_or_handle", ""),
            field_key=err.get("field_key", ""),
            rows_loaded=rows_loaded,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_skipped=1,
            message=f"Row {err.get('sheet_row')}: {err.get('message')}",
            error_reason=err.get("error_reason", ""),
        )

    if validation_errors:
        logger.log(
            phase="validate",
            log_type="summary",
            status="error",
            rows_loaded=rows_loaded,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_skipped=len(validation_errors),
            message=f"Validation failed with {len(validation_errors)} error(s). No Shopify writes executed.",
            error_reason="validation_errors",
        )
        return {
            "status": "error",
            "run_id": rid,
            "rows_loaded": rows_loaded,
            "rows_planned": rows_planned,
            "rows_written": 0,
            "rows_skipped": len(validation_errors),
            "validation_errors": validation_errors,
        }

    logger.log(
        phase="plan",
        log_type="summary",
        status="ok",
        rows_loaded=rows_loaded,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        message=f"Planned {rows_planned} template update(s).",
    )

    shop_domain = _pick_config(account_cfg, "SHOP_DOMAIN", required=True)
    api_version = _pick_config(account_cfg, "SHOPIFY_API_VERSION", required=True)
    token_secret = _pick_config(account_cfg, "SHOPIFY_TOKEN_SECRET", required=True)
    access_token = _get_secret(token_secret)

    client = ShopifyClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=access_token,
    )

    rows_written = 0
    rows_skipped = 0
    row_results: List[Dict[str, Any]] = []

    for plan in plans:
        entity_type = plan["entity_type"]
        field_key = plan["field_key"]
        raw_gid = plan["gid_or_handle"]
        template_suffix = plan["template_suffix"]

        try:
            gid = _resolve_gid(client, entity_type, raw_gid)

            msg_value = "DEFAULT_TEMPLATE" if template_suffix is None else template_suffix
            if dry_run:
                rows_skipped += 1
                logger.log(
                    phase="dry_run",
                    log_type="row",
                    status="planned",
                    entity_type=entity_type,
                    gid=gid,
                    field_key=field_key,
                    rows_loaded=rows_loaded,
                    rows_planned=rows_planned,
                    rows_skipped=rows_skipped,
                    message=f"Would update templateSuffix to {msg_value}. Source row={plan['sheet_row']}",
                )
                row_results.append({**plan, "gid": gid, "status": "planned", "result": None})
                continue

            node = _update_template(client, entity_type, gid, template_suffix)
            rows_written += 1
            logger.log(
                phase="write",
                log_type="row",
                status="ok",
                entity_type=entity_type,
                gid=gid,
                field_key=field_key,
                rows_loaded=rows_loaded,
                rows_planned=rows_planned,
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                message=f"Updated templateSuffix to {msg_value}. Shopify returned templateSuffix={node.get('templateSuffix')}",
            )
            row_results.append({**plan, "gid": gid, "status": "written", "result": node})

            if sleep_seconds:
                time.sleep(sleep_seconds)

        except Exception as exc:
            rows_skipped += 1
            logger.log(
                phase="write" if not dry_run else "dry_run",
                log_type="row",
                status="error",
                entity_type=entity_type,
                gid=raw_gid,
                field_key=field_key,
                rows_loaded=rows_loaded,
                rows_planned=rows_planned,
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                message=f"Row {plan['sheet_row']} failed: {exc}",
                error_reason=type(exc).__name__,
            )
            row_results.append({**plan, "gid": raw_gid, "status": "error", "error": str(exc)})

    final_status = "dry_run" if dry_run else "ok"
    if any(r["status"] == "error" for r in row_results):
        final_status = "partial_error" if rows_written else "error"

    logger.log(
        phase="finish",
        log_type="summary",
        status=final_status,
        rows_loaded=rows_loaded,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message=f"Finished. dry_run={dry_run}, written={rows_written}, skipped={rows_skipped}",
    )

    print("====================================")
    print("JOB RESULT")
    print("====================================")
    print("status:", final_status)
    print("run_id:", rid)
    print("rows_loaded:", rows_loaded)
    print("rows_planned:", rows_planned)
    print("rows_written:", rows_written)
    print("rows_skipped:", rows_skipped)

    return {
        "status": final_status,
        "run_id": rid,
        "rows_loaded": rows_loaded,
        "rows_recognized": rows_recognized,
        "rows_planned": rows_planned,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "results": row_results,
    }
