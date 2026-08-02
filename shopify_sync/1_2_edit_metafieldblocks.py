"""shopify_sync/1_2_edit_metafieldblocks.py

Edit structured Shopify metafield blocks from Edit__MetafieldBlocks.
Business behavior is preserved from the former edit_metafieldblocks.py Current;
this module adds the verified Local/Colab runtime boundary and new identity.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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

MODULE_PATH = "shopify_sync.1_2_edit_metafieldblocks"
MODULE_VERSION = "2026-08-01-runtime-boundary-v1"
DEFAULT_JOB_NAME = "edit_metafieldblocks"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "VARIANT", "COLLECTION", "PAGE"}
SUPPORTED_ACTIONS = {"SET", "CLEAR", "SKIP"}
SUPPORTED_BLOCK_TYPES = {"list_item", "bullet", "feature", "paragraph", "multi_line_text", "single_line_text"}
FORBIDDEN_SHOPIFY_PREFIXES = ("mf.shopify.", "v_mf.shopify.", "v.mf.shopify.")

GOOGLE_SHEETS_CELL_LIMIT = 50000


# =========================================================
# Shopify GraphQL
# =========================================================

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

def _utc_run_id(prefix: str = "edit_metafieldblocks") -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


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


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        s = _norm_str(item)
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
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
    """
    Compatibility fallback only. Standard Wide_MFBs should use Display Name-1/-2/-3.
    """
    s = _norm_str(s)
    if not s:
        return []
    parts = re.split(r"[;\n|]+", s)
    return [p.strip() for p in parts if p and p.strip()]


def _is_blank_row(row: dict[str, Any]) -> bool:
    return all(_norm_str(row.get(c)) == "" for c in INPUT_HEADER)


# =========================================================
# Runtime / Secret / Workspace Registry / clients
# =========================================================

@dataclass(frozen=True)
class AccountConfig:
    shop_domain: str
    api_version: str
    gsheet_sa_b64_secret: str
    shopify_token_secret: str

@dataclass(frozen=True)
class SecretValue:
    value: str
    source_type: str
    source_detail: str

def _runtime_mode() -> str:
    try:
        import google.colab  # type: ignore  # noqa: F401
        return "COLAB"
    except Exception:
        return "LOCAL"

def _normalize_project_code(value: Any) -> str:
    return _norm_str(value).upper()

def _normalize_registry_header(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", _norm_str(value).lower()).strip()

def _extract_spreadsheet_id(value: Any) -> str:
    text = _norm_str(value)
    if not text:
        raise ValueError("Workspace Project Registry ID/URL is empty.")
    match = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", text)
    if match:
        return match.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]+", text):
        return text
    raise ValueError("Workspace Project Registry must be a Google Sheets ID or URL.")

def _workspace_secret_result_to_value(result: Any) -> SecretValue:
    source_detail = result.resolved_name
    if result.path is not None:
        source_detail = str(result.path)
        if result.key:
            source_detail += f"::{result.key}"
    elif result.key:
        source_detail = result.key
    return SecretValue(
        value=result.value,
        source_type=result.source_type,
        source_detail=source_detail,
    )

def read_secret(
    name: str,
    *,
    project_code: str,
    explicit_value: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> SecretValue:
    """Resolve one Secret without printing its value."""
    secret_name = _norm_str(name)
    resolved_project_code = _normalize_project_code(project_code)
    if not secret_name:
        raise RuntimeError("Secret name is empty.")
    if not resolved_project_code:
        raise RuntimeError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _norm_str(explicit_value):
        return SecretValue(_norm_str(explicit_value), "EXPLICIT_VALUE", "caller")

    if _runtime_mode() == "COLAB":
        try:
            from google.colab import userdata  # type: ignore
        except Exception as exc:
            raise RuntimeError("Colab Secret adapter is unavailable.") from exc
        value = userdata.get(secret_name)
        if value is None or not str(value).strip():
            raise RuntimeError(
                f"Colab Secret {secret_name!r} is missing or not enabled for this notebook."
            )
        return SecretValue(str(value).strip(), "COLAB_SECRETS", secret_name)

    try:
        from workspace_secret_resolver import WorkspaceSecretResolver
    except Exception as exc:
        raise RuntimeError(
            "Workspace Secret Resolver is required for Local execution. "
            "Install it once into the active Python environment with:\n"
            f"{sys.executable} -m pip install -e "
            "/Users/nikki/Documents/AI_Workspace/Projects/Workspace_Secret_Resolver"
        ) from exc

    resolver = WorkspaceSecretResolver(
        resolved_project_code,
        secret_home=secret_home,
    )
    aliases: Tuple[str, ...] = ()
    normalized_secret_name = secret_name.upper()
    canonical_suffixes = (
        "_GSHEET",
        "_SHOPIFY_ACCESS_TOKEN",
        "_SHOPIFY_TOKEN",
    )
    for suffix in canonical_suffixes:
        if normalized_secret_name.endswith(suffix):
            canonical_name = f"{resolved_project_code}{suffix}"
            if canonical_name != secret_name:
                aliases = (canonical_name,)
            break

    result = resolver.read(secret_name, aliases=aliases)
    return _workspace_secret_result_to_value(result)

def _parse_service_account_text(raw_value: str) -> Tuple[Dict[str, Any], str]:
    raw = _norm_str(raw_value)
    if not raw:
        raise RuntimeError("Google service-account Secret is empty.")

    try:
        info = json.loads(raw)
        secret_format = "RAW_JSON"
    except Exception:
        try:
            padded = raw + "=" * ((4 - len(raw) % 4) % 4)
            info = json.loads(base64.b64decode(padded).decode("utf-8"))
            secret_format = "BASE64_JSON"
        except Exception as exc:
            raise RuntimeError(
                "Google service-account Secret is neither valid raw JSON nor Base64 JSON."
            ) from exc

    required = {"type", "project_id", "private_key", "client_email", "token_uri"}
    missing = sorted(key for key in required if not info.get(key))
    if missing or info.get("type") != "service_account":
        raise RuntimeError(
            "Google Secret is not a complete service-account credential; "
            f"missing={missing}."
        )
    return info, secret_format

def _sheets_error_status(exc: BaseException) -> Optional[int]:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is None:
        status = getattr(response, "status", None)
    try:
        return int(status) if status is not None else None
    except Exception:
        return None

def _is_retryable_sheets_error(exc: BaseException) -> bool:
    status = _sheets_error_status(exc)
    if status == 429 or status in {500, 502, 503, 504}:
        return True
    text = str(exc).lower()
    retry_tokens = (
        "resource_exhausted",
        "ratelimitexceeded",
        "userratelimitexceeded",
        "rate limit exceeded",
        "quota exceeded",
        "too many requests",
    )
    return any(token in text for token in retry_tokens)

def _with_sheets_retry(
    operation,
    *,
    action: str,
    max_attempts: int = 5,
    max_delay: float = 16.0,
):
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if not _is_retryable_sheets_error(exc) or attempt >= attempts:
                raise
            delay = min(2 ** (attempt - 1), float(max_delay)) + random.random()
            status = _sheets_error_status(exc)
            reason = f"HTTP {status}" if status is not None else type(exc).__name__
            print(
                "[Sheets retry] "
                f"action={action} | attempt={attempt}/{attempts} | "
                f"reason={reason} | sleep={delay:.1f}s"
            )
            time.sleep(delay)
    raise RuntimeError(f"Sheets operation exhausted retries: {action}")

def _build_gspread_client_from_secret(
    secret: SecretValue,
) -> Tuple[gspread.Client, Dict[str, str]]:
    info, secret_format = _parse_service_account_text(secret.value)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds), {
        "source_type": secret.source_type,
        "source_detail": secret.source_detail,
        "secret_format": secret_format,
        "service_account_email": _norm_str(info.get("client_email")),
    }

def build_gsheet_client(gsheet_sa_value: str) -> gspread.Client:
    """Business-side Google client builder; consumes a resolved Secret value."""
    info, _secret_format = _parse_service_account_text(gsheet_sa_value)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def build_shopify_client(
    shopify_access_token: str,
    shop_domain: str,
    api_version: str,
    http_timeout: int = 60,
) -> ShopifyClient:
    """Business-side Shopify client builder; consumes a resolved token value."""
    token = _norm_str(shopify_access_token)
    if not token:
        raise RuntimeError("Resolved Shopify access token is empty.")
    return ShopifyClient(
        graph_url=f"https://{shop_domain}/admin/api/{api_version}/graphql.json",
        headers={
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        },
        timeout=http_timeout,
    )

def _load_account_config(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_account_id: str,
) -> AccountConfig:
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="account_config.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(tab_cfg_account_id),
        action=f"account_config.worksheet:{tab_cfg_account_id}",
    )
    values = _with_sheets_retry(
        ws.get_all_values,
        action=f"account_config.get_all_values:{tab_cfg_account_id}",
    )
    if not values:
        raise ValueError(f"{tab_cfg_account_id} is empty.")

    config: Dict[str, str] = {}
    duplicates: List[str] = []
    for row_number, row in enumerate(values, start=1):
        key = _norm_str(row[0] if row else "").upper()
        value = _norm_str(row[1] if len(row) > 1 else "")
        if not key:
            continue
        if row_number == 1 and key.lower() in {"key", "config_key", "name", "setting"}:
            continue
        if key in config:
            duplicates.append(f"{key}@row{row_number}")
        config[key] = value

    if duplicates:
        raise ValueError(f"Duplicated keys in {tab_cfg_account_id}: {duplicates}")

    required = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "GSHEET_SA_B64_SECRET",
        "SHOPIFY_TOKEN_SECRET",
    ]
    missing = [key for key in required if not config.get(key)]
    if missing:
        raise ValueError(f"{tab_cfg_account_id} missing required values: {missing}")

    return AccountConfig(
        shop_domain=config["SHOP_DOMAIN"],
        api_version=config["SHOPIFY_API_VERSION"],
        gsheet_sa_b64_secret=config["GSHEET_SA_B64_SECRET"],
        shopify_token_secret=config["SHOPIFY_TOKEN_SECRET"],
    )

def resolve_workspace_project(
    *,
    project_code: str,
    workspace_registry_id: str,
    workspace_gsheet_secret_name: str = "WORKSPACE_GSHEET",
    workspace_registry_tab: str = "Cfg__Projects",
    secret_home: Optional[str] = None,
    explicit_workspace_sa_value: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, str]:
    resolved_project_code = _normalize_project_code(project_code)
    if not resolved_project_code:
        raise ValueError("project_code is required.")

    if print_progress:
        print(
            "[Workspace Registry] resolve bootstrap Secret | "
            f"project={resolved_project_code} | secret={workspace_gsheet_secret_name}"
        )

    workspace_secret = read_secret(
        workspace_gsheet_secret_name,
        project_code="WORKSPACE",
        explicit_value=explicit_workspace_sa_value,
        secret_home=secret_home,
    )
    workspace_gc, auth_meta = _build_gspread_client_from_secret(workspace_secret)

    registry_file_id = _extract_spreadsheet_id(workspace_registry_id)
    registry_book = _with_sheets_retry(
        lambda: workspace_gc.open_by_key(registry_file_id),
        action="workspace_registry.open_by_key",
    )
    worksheet = _with_sheets_retry(
        lambda: registry_book.worksheet(workspace_registry_tab),
        action=f"workspace_registry.worksheet:{workspace_registry_tab}",
    )
    values = _with_sheets_retry(
        worksheet.get_all_values,
        action=f"workspace_registry.get_all_values:{workspace_registry_tab}",
    )
    if not values:
        raise ValueError(f"Workspace Project Registry tab {workspace_registry_tab!r} is empty.")

    header_map: Dict[str, int] = {}
    duplicate_headers: List[str] = []
    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if not normalized:
            continue
        if normalized in header_map:
            duplicate_headers.append(normalized)
        header_map[normalized] = index
    if duplicate_headers:
        raise ValueError(
            "Workspace Project Registry has duplicate normalized headers: "
            + ", ".join(sorted(set(duplicate_headers)))
        )

    def require_column(*aliases: str) -> int:
        for alias in aliases:
            normalized = _normalize_registry_header(alias)
            if normalized in header_map:
                return header_map[normalized]
        raise ValueError(
            "Workspace Project Registry is missing a required column; "
            f"accepted_aliases={aliases}."
        )

    project_col = require_column("project_code", "project code")
    active_col = require_column("active")
    console_url_col = require_column("console_core_url", "console core url")
    gsheet_secret_col = require_column("gsheet_secret_name", "gsheet secret name")
    account_tab_col = require_column("account_config_tab", "account config tab")
    timezone_col = require_column("timezone", "time zone")
    project_name_col = header_map.get(_normalize_registry_header("project_name"))

    matches: List[Tuple[int, List[Any]]] = []
    width = len(values[0])
    for row_number, raw_row in enumerate(values[1:], start=2):
        row = list(raw_row) + [""] * max(0, width - len(raw_row))
        if _normalize_project_code(row[project_col]) == resolved_project_code:
            matches.append((row_number, row))
    if not matches:
        raise ValueError(
            f"Workspace Project Registry has no row for project_code={resolved_project_code}."
        )
    if len(matches) > 1:
        raise ValueError(
            "Workspace Project Registry has duplicate rows for "
            f"project_code={resolved_project_code}; "
            f"rows={[row_number for row_number, _ in matches]}."
        )

    source_row, row = matches[0]
    if _norm_str(row[active_col]).lower() not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}, row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": _norm_str(row[project_name_col]) if project_name_col is not None else "",
        "console_core_url": _norm_str(row[console_url_col]),
        "gsheet_secret_name": _norm_str(row[gsheet_secret_col]),
        "account_config_tab": _norm_str(row[account_tab_col]),
        "timezone": _norm_str(row[timezone_col]),
        "registry_source_row": str(source_row),
        "workspace_auth_source_type": _norm_str(auth_meta.get("source_type")),
    }
    empty_required = [
        key
        for key in ("console_core_url", "gsheet_secret_name", "account_config_tab", "timezone")
        if not route[key]
    ]
    if empty_required:
        raise ValueError(
            "Workspace Project Registry route has empty required values: "
            f"project_code={resolved_project_code}; fields={empty_required}; row={source_row}."
        )

    if print_progress:
        print(
            "[Workspace Registry] resolved | "
            f"project={route['project_code']} | row={source_row} | "
            f"secret={route['gsheet_secret_name']} | "
            f"account_tab={route['account_config_tab']} | timezone={route['timezone']}"
        )
    return route

def resolve_runtime_context(
    *,
    project_code: str,
    workspace_registry_id: str,
    workspace_gsheet_secret_name: str = "WORKSPACE_GSHEET",
    workspace_registry_tab: str = "Cfg__Projects",
    secret_home: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, Any]:
    route = resolve_workspace_project(
        project_code=project_code,
        workspace_registry_id=workspace_registry_id,
        workspace_gsheet_secret_name=workspace_gsheet_secret_name,
        workspace_registry_tab=workspace_registry_tab,
        secret_home=secret_home,
        print_progress=print_progress,
    )

    project_google_secret = read_secret(
        route["gsheet_secret_name"],
        project_code=route["project_code"],
        secret_home=secret_home,
    )
    gc, google_auth_meta = _build_gspread_client_from_secret(project_google_secret)
    account = _load_account_config(
        gc,
        route["console_core_url"],
        route["account_config_tab"],
    )
    if account.gsheet_sa_b64_secret != route["gsheet_secret_name"]:
        raise ValueError(
            "Workspace Registry Google Secret does not match Cfg__account_id. "
            f"registry={route['gsheet_secret_name']}; cfg={account.gsheet_sa_b64_secret}"
        )

    shopify_secret = read_secret(
        account.shopify_token_secret,
        project_code=route["project_code"],
        secret_home=secret_home,
    )

    if print_progress:
        print(
            "[Runtime Auth] ready | "
            f"project={route['project_code']} | "
            f"google_source={google_auth_meta['source_type']} | "
            f"shopify_source={shopify_secret.source_type} | "
            f"shop={account.shop_domain} | api={account.api_version}"
        )

    return {
        "project_route": route,
        "account": {
            "shop_domain": account.shop_domain,
            "api_version": account.api_version,
            "gsheet_secret_name": account.gsheet_sa_b64_secret,
            "shopify_token_secret_name": account.shopify_token_secret,
        },
        "credentials": {
            "gsheet_sa_value": project_google_secret.value,
            "shopify_access_token": shopify_secret.value,
        },
        "auth": {
            "runtime_mode": _runtime_mode(),
            "project_google_secret_source_type": google_auth_meta["source_type"],
            "project_google_secret_format": google_auth_meta["secret_format"],
            "shopify_secret_source_type": shopify_secret.source_type,
            "service_account_email": google_auth_meta["service_account_email"],
        },
    }

def update_existing_notebook_registry_row(
    *,
    project_code: str,
    registry_mode: str,
    console_core_url: str,
    bootstrap_gsheet_secret_name: str,
    registry_tab: str,
    job_name: str,
    sheet_label: str,
    tab_name: str,
    current_colab_url: str = "",
    current_colab_name: str = "",
    secret_home: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, Any]:
    mode = _norm_str(registry_mode).upper() or "OFF"
    allowed = {"OFF", "CHECK", "UPDATE_URL", "UPDATE_URL_AND_NAME"}
    if mode not in allowed:
        raise ValueError(f"registry_mode must be one of {sorted(allowed)}.")
    if mode == "OFF":
        if print_progress:
            print(
                "[Registry] mode=OFF | "
                f"job_name={job_name} | sheet_label={sheet_label} | tab_name={tab_name}"
            )
        return {"status": "OFF", "changed_fields": [], "target_row": None}

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _norm_str(current_colab_url):
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _norm_str(current_colab_name):
        raise ValueError("UPDATE_URL_AND_NAME requires current_colab_name.")

    sa_secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        secret_home=secret_home,
    )
    gc, auth_meta = _build_gspread_client_from_secret(sa_secret)
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="registry.open_console",
    )
    worksheet = _with_sheets_retry(
        lambda: sh.worksheet(registry_tab),
        action=f"registry.worksheet:{registry_tab}",
    )
    values = _with_sheets_retry(
        worksheet.get_all_values,
        action=f"registry.get_all_values:{registry_tab}",
    )
    if not values:
        raise ValueError(f"Registry tab {registry_tab!r} is empty.")

    header_map: Dict[str, int] = {}
    duplicate_headers: List[str] = []
    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if not normalized:
            continue
        if normalized in header_map:
            duplicate_headers.append(normalized)
        header_map[normalized] = index
    if duplicate_headers:
        raise ValueError(
            "Registry tab has duplicate normalized headers: "
            + ", ".join(sorted(set(duplicate_headers)))
        )

    def require_column(*aliases: str) -> int:
        for alias in aliases:
            key = _normalize_registry_header(alias)
            if key in header_map:
                return header_map[key]
        raise ValueError(
            f"Registry tab is missing required column; accepted aliases={aliases}."
        )

    job_col = require_column("job_name", "job name")
    label_col = require_column("sheet_label", "sheet label")
    tab_col = require_column("Tab name", "sheet name", "sheet_name")
    url_col = require_column("colab_url", "colab url")
    name_col = require_column("colab_name", "colab name")

    wanted = (
        _norm_str(job_name).lower(),
        _norm_str(sheet_label).lower(),
        _norm_str(tab_name).lower(),
    )
    matches: List[int] = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(values[0]) - len(row))
        logical_key = (
            _norm_str(padded[job_col]).lower(),
            _norm_str(padded[label_col]).lower(),
            _norm_str(padded[tab_col]).lower(),
        )
        if logical_key == wanted:
            matches.append(row_index)
    if not matches:
        raise ValueError(
            "Registry target row was not found. This function never appends. "
            f"logical_key={wanted}"
        )
    if len(matches) > 1:
        raise ValueError(
            f"Registry logical key is duplicated at rows={matches}; no row was changed."
        )

    row_number = matches[0]
    current_row = values[row_number - 1] + [""] * max(
        0, len(values[0]) - len(values[row_number - 1])
    )
    changes: List[Tuple[str, int, str, str]] = []
    provided_url = _norm_str(current_colab_url)
    provided_name = _norm_str(current_colab_name)
    if provided_url and _norm_str(current_row[url_col]) != provided_url:
        changes.append(("colab_url", url_col + 1, _norm_str(current_row[url_col]), provided_url))
    if provided_name and _norm_str(current_row[name_col]) != provided_name:
        changes.append(("colab_name", name_col + 1, _norm_str(current_row[name_col]), provided_name))

    if mode == "CHECK":
        status = "CHANGE_DETECTED" if changes else "NO_CHANGE"
    else:
        permitted = {"colab_url"} if mode == "UPDATE_URL" else {"colab_url", "colab_name"}
        applied = [change for change in changes if change[0] in permitted]
        for field_name, column_number, _old_value, new_value in applied:
            _with_sheets_retry(
                lambda rn=row_number, cn=column_number, nv=new_value: worksheet.update_cell(rn, cn, nv),
                action=f"registry.update_cell:{field_name}",
            )
        changes = applied
        status = "UPDATED" if changes else "NO_CHANGE"

    if print_progress:
        print(
            "[Registry] "
            f"row={row_number} | status={status} | "
            f"changed_fields={[item[0] for item in changes]}"
        )
    return {
        "status": status,
        "target_row": row_number,
        "changed_fields": [item[0] for item in changes],
        "auth_source_type": auth_meta["source_type"],
    }

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

            try:
                data = r.json()
            except Exception:
                raise RuntimeError(f"Non-JSON response. HTTP {r.status_code}: {r.text[:500]}")

            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}: {data}")

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
# Sheet routing
# =========================================================

def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="cfg_sites.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(cfg_sites_tab),
        action=f"cfg_sites.worksheet:{cfg_sites_tab}",
    )
    rows = _with_sheets_retry(
        ws.get_all_records,
        action=f"cfg_sites.get_all_records:{cfg_sites_tab}",
    )
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"{cfg_sites_tab} is empty")

    for c in ["site_code", "label", "sheet_url"]:
        if c not in df.columns:
            raise ValueError(f"{cfg_sites_tab} missing required column: {c}")

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
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(sheet_url),
        action=f"target.open_by_url:{label}",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(worksheet_title),
        action=f"target.worksheet:{label}:{worksheet_title}",
    )
    return sh, ws, sheet_url


def load_account_config(
    gc_console: gspread.Client,
    console_core_url: str,
    cfg_account_tab: str,
) -> dict[str, str]:
    """
    Read Console Core / Cfg__account_id.

    Supports:
    1) header style:
       key | value

    2) header style:
       config_key | config_value

    3) no-header two-column key-value style:
       SHOP_DOMAIN | xxx
       SHOPIFY_API_VERSION | 2026-04
       GSHEET_SA_B64_SECRET | NRP_GSHEET
       SHOPIFY_TOKEN_SECRET | NRP_SHOPIFY_ACCESS_TOKEN
    """
    sh_console = _with_sheets_retry(
        lambda: gc_console.open_by_url(console_core_url),
        action="account_config.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh_console.worksheet(cfg_account_tab),
        action=f"account_config.worksheet:{cfg_account_tab}",
    )
    values = _with_sheets_retry(
        ws.get_all_values,
        action=f"account_config.get_all_values:{cfg_account_tab}",
    )

    if not values:
        raise ValueError(f"{cfg_account_tab} is empty in Console Core")

    values = [row for row in values if any(_norm_str(x) for x in row)]

    if not values:
        raise ValueError(f"{cfg_account_tab} has no non-empty rows in Console Core")

    norm_rows = []
    for row in values:
        r = list(row) + ["", ""]
        norm_rows.append([_norm_str(r[0]), _norm_str(r[1])])

    first_key = norm_rows[0][0].lower().strip()
    second_key = norm_rows[0][1].lower().strip()

    out: dict[str, str] = {}

    if first_key == "key" and second_key == "value":
        data_rows = norm_rows[1:]
    elif first_key == "config_key" and second_key == "config_value":
        data_rows = norm_rows[1:]
    else:
        data_rows = norm_rows

    for k, v in data_rows:
        if k:
            out[k] = v

    required_any = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "GSHEET_SA_B64_SECRET",
        "SHOPIFY_TOKEN_SECRET",
    ]

    found_required = [k for k in required_any if _norm_str(out.get(k))]
    if not found_required:
        raise ValueError(
            f"{cfg_account_tab} does not look like a valid account config table. "
            f"Expected key-value rows such as SHOP_DOMAIN / SHOPIFY_API_VERSION / "
            f"GSHEET_SA_B64_SECRET / SHOPIFY_TOKEN_SECRET. First rows: {norm_rows[:5]}"
        )

    return out


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
        self.gc = gc
        self.runlog_sheet_url = runlog_sheet_url
        self.runlog_tab_name = runlog_tab_name
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.flush_every = flush_every
        self._buf: list[list[Any]] = []

        sh = _with_sheets_retry(
            lambda: gc.open_by_url(runlog_sheet_url),
            action="runlog.open_by_url",
        )
        try:
            self.ws = _with_sheets_retry(
                lambda: sh.worksheet(runlog_tab_name),
                action=f"runlog.worksheet:{runlog_tab_name}",
            )
        except gspread.WorksheetNotFound:
            # Creation is intentionally not retried after an uncertain response.
            self.ws = sh.add_worksheet(
                title=runlog_tab_name,
                rows=1000,
                cols=len(RUNLOG_HEADER),
            )

        _with_sheets_retry(
            lambda: self.ws.update(range_name="A1:R1", values=[RUNLOG_HEADER]),
            action=f"runlog.header:{runlog_tab_name}",
        )

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

        pending = list(self._buf)
        _with_sheets_retry(
            lambda: self.ws.append_rows(
                pending,
                value_input_option="RAW",
                table_range="A:R",
            ),
            action=f"runlog.append_rows:{self.runlog_tab_name}",
            max_attempts=6,
            max_delay=20.0,
        )
        self._buf = []


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
                gid=_norm_str(row.get("gid") or row.get("owner_id")),
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


# =========================================================
# Input loading / filtering
# =========================================================

def load_blocks_sheet(ws_blocks) -> pd.DataFrame:
    rows = _with_sheets_retry(
        ws_blocks.get_all_records,
        action=f"input.get_all_records:{getattr(ws_blocks, 'title', 'Edit__MetafieldBlocks')}",
    )
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=INPUT_HEADER + ["_sheet_row", "_source_order"])

    missing = [c for c in INPUT_HEADER if c not in df.columns]
    if missing:
        raise ValueError(f"Edit__MetafieldBlocks missing required columns: {missing}")

    df = df[INPUT_HEADER].copy()
    df["_sheet_row"] = range(2, 2 + len(df))
    df["_source_order"] = range(len(df))

    for c in INPUT_HEADER:
        df[c] = df[c].apply(_norm_str)

    df = df[~df.apply(lambda r: _is_blank_row(r.to_dict()), axis=1)].copy()

    df["entity_type"] = df["entity_type"].str.upper().str.strip()
    df["block_type"] = df["block_type"].str.lower().str.strip()
    df["action"] = df["action"].str.upper().str.strip()
    df["mode"] = df["mode"].str.upper().str.strip()

    return df


def normalize_blocks_df(
    df: pd.DataFrame,
    mode_default: str = "STRICT",
    action_default: str = "SET",
    only_entity_types: Optional[set[str]] = None,
    only_field_prefixes: Optional[set[str]] = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if df.empty:
        return df.copy(), []

    d = df.copy()
    errors = []

    d["mode"] = d["mode"].replace("", mode_default.upper())
    d["action"] = d["action"].replace("", action_default.upper())

    if only_entity_types:
        allow = {x.upper().strip() for x in only_entity_types}
        d = d[d["entity_type"].isin(allow)].copy()

    if only_field_prefixes:
        prefixes = tuple(only_field_prefixes)
        d = d[d["field_key"].astype(str).str.startswith(prefixes)].copy()

    for r in d.to_dict("records"):
        sheet_row = r.get("_sheet_row")
        field_key = _norm_str(r.get("field_key")).lower()

        if r["entity_type"] not in SUPPORTED_ENTITY_TYPES:
            errors.append({
                "sheet_row": sheet_row,
                "entity_type": r.get("entity_type", ""),
                "field_key": r.get("field_key", ""),
                "error_reason": "invalid_entity_type",
                "message": f"entity_type={r['entity_type']}",
            })

        if not r["gid_or_handle"]:
            errors.append({
                "sheet_row": sheet_row,
                "entity_type": r.get("entity_type", ""),
                "field_key": r.get("field_key", ""),
                "error_reason": "missing_gid_or_handle",
                "message": "gid_or_handle is required",
            })

        if not r["field_key"]:
            errors.append({
                "sheet_row": sheet_row,
                "entity_type": r.get("entity_type", ""),
                "field_key": r.get("field_key", ""),
                "error_reason": "missing_field_key",
                "message": "field_key is required",
            })

        if field_key and any(field_key.startswith(p) for p in FORBIDDEN_SHOPIFY_PREFIXES):
            errors.append({
                "sheet_row": sheet_row,
                "entity_type": r.get("entity_type", ""),
                "field_key": r.get("field_key", ""),
                "error_reason": "forbidden_shopify_field_key",
                "message": f"field_key={r.get('field_key')}",
            })

        if r["action"] not in SUPPORTED_ACTIONS:
            errors.append({
                "sheet_row": sheet_row,
                "entity_type": r.get("entity_type", ""),
                "field_key": r.get("field_key", ""),
                "error_reason": "invalid_action",
                "message": f"action={r['action']}",
            })

        if r["action"] != "CLEAR":
            if r["block_type"] not in SUPPORTED_BLOCK_TYPES:
                errors.append({
                    "sheet_row": sheet_row,
                    "entity_type": r.get("entity_type", ""),
                    "field_key": r.get("field_key", ""),
                    "error_reason": "invalid_block_type",
                    "message": f"block_type={r['block_type']}",
                })

    if errors:
        return d, errors

    d = d[d["action"] != "SKIP"].copy()

    return d, []


# =========================================================
# Cfg Fields
# =========================================================

def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = _with_sheets_retry(
        ws_cfg_fields.get_all_records,
        action=f"cfg_fields.get_all_records:{getattr(ws_cfg_fields, 'title', 'Cfg__Fields')}",
    )
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    for c in ["entity_type", "field_key", "data_type", "source_type", "display_name"]:
        if c not in df.columns:
            df[c] = ""

    df["entity_type"] = df["entity_type"].astype(str).str.upper().str.strip()
    df["field_key"] = df["field_key"].astype(str).str.strip()
    df["data_type"] = df["data_type"].astype(str).str.strip().str.lower()
    df["source_type"] = df["source_type"].astype(str).str.strip().str.upper()
    df["display_name"] = df["display_name"].astype(str).str.strip()

    df = df[
        (df["source_type"].eq("METAFIELD"))
        | (df["field_key"].str.startswith("mf."))
        | (df["field_key"].str.startswith("v_mf."))
    ].copy()

    return df


def build_cfg_field_key_map(cfg_fields: pd.DataFrame) -> dict[tuple[str, str], str]:
    mp: dict[tuple[str, str], str] = {}

    for r in cfg_fields.to_dict("records"):
        et = _norm_str(r.get("entity_type")).upper()
        fk = _norm_str(r.get("field_key"))
        dt_ = _norm_str(r.get("data_type")).lower()
        if et and fk and dt_:
            key = (et, fk)
            if key in mp and mp[key] != dt_:
                raise ValueError({
                    "message": "Cfg__Fields has duplicate entity_type + field_key with different data_type",
                    "entity_type": et,
                    "field_key": fk,
                    "data_types": [mp[key], dt_],
                })
            mp[key] = dt_

    return mp


def cfg_data_type_for_row(
    entity_type: str,
    field_key: str,
    cfg_type_map: dict[tuple[str, str], str],
) -> str:
    et = _norm_str(entity_type).upper()
    fk = _norm_str(field_key)
    dt_ = cfg_type_map.get((et, fk))
    if dt_:
        return dt_

    # Accept Cfg__Fields stored as mf.* for VARIANT only if input uses v_mf.* and matching key exists.
    if fk.startswith("v_mf."):
        dt_ = cfg_type_map.get((et, "mf." + fk[len("v_mf."):]))
        if dt_:
            return dt_

    return ""


def _ref_scalar_default(reference_default_kind: str) -> str:
    k = _norm_str(reference_default_kind).lower() or "mixed"
    return "metaobject_reference" if k == "metaobject" else "mixed_reference"


def _ref_list_default(reference_default_kind: str) -> str:
    k = _norm_str(reference_default_kind).lower() or "mixed"
    return "list.metaobject_reference" if k == "metaobject" else "list.mixed_reference"


def map_cfg_dtype_to_shopify_type(cfg_dt: str, reference_default_kind: str) -> str:
    dt_ = _norm_str(cfg_dt).lower()

    explicit_scalars = {
        "boolean", "json",
        "multi_line_text_field", "number_decimal", "number_integer", "rich_text_field", "single_line_text_field",
        "product_reference", "variant_reference", "collection_reference", "metaobject_reference", "mixed_reference",
    }
    if dt_ in explicit_scalars:
        return dt_

    if dt_.startswith("list."):
        inner = dt_[5:].strip()
        explicit_list_inner = {
            "boolean", "json",
            "multi_line_text_field", "number_decimal", "number_integer", "rich_text_field", "single_line_text_field",
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


def validate_block_type_vs_data_type(block_type: str, cfg_dt: str) -> Optional[str]:
    bt = _norm_str(block_type).lower()
    dt_ = _norm_str(cfg_dt).lower()

    if bt == "list_item":
        if dt_.startswith("list."):
            return None
        return f"block_type=list_item requires data_type=list.*, got {cfg_dt}"

    if bt in {"bullet", "feature", "paragraph"}:
        if dt_ == "rich_text_field":
            return None
        return f"block_type={bt} requires data_type=rich_text_field, got {cfg_dt}"

    if bt == "multi_line_text":
        if dt_ in {"multi_line_text_field", "text"}:
            return None
        return f"block_type=multi_line_text requires data_type=multi_line_text_field, got {cfg_dt}"

    if bt == "single_line_text":
        if dt_ in {"single_line_text_field", "string"}:
            return None
        return f"block_type=single_line_text requires data_type=single_line_text_field, got {cfg_dt}"

    return f"unsupported block_type={block_type}"


# =========================================================
# Owner resolution
# =========================================================

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


def normalize_gid_or_numeric(entity_type: str, ref: str) -> Optional[str]:
    s = _norm_str(ref)
    et = _norm_str(entity_type).upper()

    if s.startswith("gid://"):
        return s

    if re.fullmatch(r"\d+", s):
        if et == "PRODUCT":
            return f"gid://shopify/Product/{s}"
        if et == "VARIANT":
            return f"gid://shopify/ProductVariant/{s}"
        if et == "COLLECTION":
            return f"gid://shopify/Collection/{s}"
        if et == "PAGE":
            return f"gid://shopify/Page/{s}"

    return None


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


def resolve_owner_ids(client: ShopifyClient, df_plan: pd.DataFrame) -> pd.DataFrame:
    d = df_plan.copy()
    d["owner_id"] = d.apply(lambda r: normalize_gid_or_numeric(r["entity_type"], r["gid_or_handle"]), axis=1)

    mask_need = d["owner_id"].isna() & d["gid_or_handle"].ne("")
    need = d.loc[mask_need, ["entity_type", "gid_or_handle"]].drop_duplicates()

    cache: dict[tuple[str, str], Optional[str]] = {}

    def resolve_one(et: str, ref: str) -> Optional[str]:
        k = (et, ref)
        if k in cache:
            return cache[k]

        v = None
        if et == "PRODUCT":
            v = resolve_product_by_handle(client, ref)
        elif et == "COLLECTION":
            v = resolve_collection_by_handle(client, ref)
        elif et == "PAGE":
            v = resolve_page_by_handle(client, ref)
        elif et == "VARIANT":
            v = resolve_variant_by_sku(client, ref)

        cache[k] = v
        return v

    resolved_map = {}
    for row in need.itertuples(index=False):
        resolved_map[(row.entity_type, row.gid_or_handle)] = resolve_one(row.entity_type, row.gid_or_handle)

    d["owner_id"] = d.apply(
        lambda r: r["owner_id"] if r["owner_id"] else resolved_map.get((r["entity_type"], r["gid_or_handle"])),
        axis=1,
    )

    d["_skip_reason"] = ""
    d.loc[d["owner_id"].isna() | (d["owner_id"].astype(str).str.strip() == ""), "_skip_reason"] = "cannot_resolve_owner_id"

    mask_has_owner = d["_skip_reason"].eq("")
    unique_owner_ids = d.loc[mask_has_owner, "owner_id"].astype(str).drop_duplicates().tolist()
    exist_map = nodes_exist_map(client, unique_owner_ids, chunk_size=80)

    d["_owner_exists"] = d["owner_id"].apply(lambda x: bool(exist_map.get(x, False)) if x else False)
    d.loc[mask_has_owner & (~d["_owner_exists"]), "_skip_reason"] = "owner_not_found_in_shop"

    return d


# =========================================================
# Value builders
# =========================================================

def build_rich_text_bullet(items: list[str]) -> str:
    clean_items = [x.strip() for x in items if x and x.strip()]
    root = {
        "type": "root",
        "children": [
            {
                "type": "list",
                "listType": "unordered",
                "children": [
                    {
                        "type": "list-item",
                        "children": [{"type": "text", "value": item}],
                    }
                    for item in clean_items
                ],
            }
        ],
    }
    return _json_dumps(root)


def build_rich_text_feature(rows: list[dict[str, Any]]) -> str:
    """
    Build a Shopify rich_text_field value for title/body feature blocks.

    Display rule:
      title + body -> "Title: Body" in the same paragraph.
      title only    -> bold title only.
      body only     -> body only.

    Input sheet stays unchanged:
      title column = heading text
      body column  = paragraph text
    """
    children = []

    for r in rows:
        title = _norm_str(r.get("title"))
        body = _norm_str(r.get("body"))
        if not title and not body:
            continue

        paragraph_children = []

        if title:
            paragraph_children.append({"type": "text", "value": title, "bold": True})

        if title and body:
            paragraph_children.append({"type": "text", "value": f": {body}"})
        elif body:
            paragraph_children.append({"type": "text", "value": body})

        children.append({"type": "paragraph", "children": paragraph_children})

    return _json_dumps({"type": "root", "children": children})


def build_rich_text_paragraph(rows: list[dict[str, Any]]) -> str:
    """
    Build a Shopify rich_text_field value for paragraph blocks.

    Display rule:
      title + body/value -> "Title: Body" in the same paragraph.
      title only         -> bold title only.
      body/value only    -> body/value only.
    """
    children = []

    for r in rows:
        title = _norm_str(r.get("title"))
        body = _norm_str(r.get("body") or r.get("value"))
        if not title and not body:
            continue

        paragraph_children = []

        if title:
            paragraph_children.append({"type": "text", "value": title, "bold": True})

        if title and body:
            paragraph_children.append({"type": "text", "value": f": {body}"})
        elif body:
            paragraph_children.append({"type": "text", "value": body})

        children.append({"type": "paragraph", "children": paragraph_children})

    return _json_dumps({"type": "root", "children": children})


def build_multi_line_text(rows: list[dict[str, Any]]) -> str:
    """
    Build a scalar Shopify multi_line_text_field value from Edit__MetafieldBlocks rows.

    Standard input from Wide_MFBs:
      block_type = multi_line_text
      value      = final multi-line text
      body       = blank

    Compatibility input:
      body can also be used when value is blank.
      Multiple rows for the same owner + field_key are joined by newline in block_seq order.
    """
    values = []
    for r in rows:
        v = _norm_str(r.get("value") or r.get("body"))
        if v:
            values.append(v)
    return "\n".join(values)


def build_single_line_text(rows: list[dict[str, Any]]) -> str:
    """
    Build a scalar Shopify single_line_text_field value.
    Multiple rows are joined with comma + space as a compatibility fallback.
    """
    values = []
    for r in rows:
        v = _norm_str(r.get("value") or r.get("body"))
        if v:
            values.append(v)
    return ", ".join(values)


# Reference normalization

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


def build_metafield_value(
    *,
    block_type: str,
    action: str,
    mf_type: str,
    rows: list[dict[str, Any]],
) -> str:
    action = _norm_str(action).upper()
    block_type = _norm_str(block_type).lower()
    mf_type = _norm_str(mf_type)

    if action == "CLEAR":
        return "[]" if mf_type.startswith("list.") else ""

    if block_type == "list_item":
        values = [_norm_str(r.get("value")) for r in rows]
        values = [v for v in values if v]

        # Compatibility: split if legacy row still contains ; / newline / |
        split_values = []
        for v in values:
            if re.search(r"[;\n|]", v):
                split_values.extend(_split_items(v))
            else:
                split_values.append(v)

        split_values = _dedupe_keep_order(split_values)
        split_values = normalize_reference_items_by_type(mf_type, split_values)
        return _json_dumps(split_values)

    if block_type == "bullet":
        items = [_norm_str(r.get("body") or r.get("value")) for r in rows]
        return build_rich_text_bullet(items)

    if block_type == "feature":
        return build_rich_text_feature(rows)

    if block_type == "paragraph":
        return build_rich_text_paragraph(rows)

    if block_type == "multi_line_text":
        return build_multi_line_text(rows)

    if block_type == "single_line_text":
        return build_single_line_text(rows)

    raise ValueError(f"Unsupported block_type: {block_type}")


# =========================================================
# Build plan
# =========================================================

def _sort_group_rows(g: pd.DataFrame) -> pd.DataFrame:
    d = g.copy()
    d["_seq_sort"] = d["block_seq"].apply(_safe_int_or_none)
    d["_seq_sort"] = d["_seq_sort"].apply(lambda x: 999999999 if x is None else x)
    return d.sort_values(["_seq_sort", "_source_order"], kind="stable").copy()


def build_plan(
    df_blocks: pd.DataFrame,
    cfg_type_map: dict[tuple[str, str], str],
    reference_default_kind: str,
) -> dict[str, Any]:
    errors = []
    preview_rows = []
    plan_rows = []

    if df_blocks.empty:
        return {"plan_rows": [], "preview_rows": [], "errors": []}

    group_cols = ["entity_type", "gid_or_handle", "field_key"]

    for group_key, g0 in df_blocks.groupby(group_cols, dropna=False, sort=False):
        entity_type, gid_or_handle, field_key = group_key
        g = _sort_group_rows(g0)

        actions = [a for a in sorted(set(g["action"].astype(str).str.upper().str.strip().tolist())) if a]
        if len(actions) > 1:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "mixed_actions_in_group",
                "message": f"actions={actions}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        action = actions[0] if actions else "SET"

        parsed = parse_field_key(field_key)
        if not parsed:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "field_key_not_recognized",
                "message": f"field_key={field_key}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        prefix, namespace, key = parsed

        if prefix == "v_mf." and entity_type != "VARIANT":
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "prefix_entity_mismatch",
                "message": f"v_mf. requires entity_type=VARIANT, got {entity_type}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        if prefix == "mf." and entity_type == "VARIANT":
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "prefix_entity_mismatch",
                "message": f"mf. cannot be used for entity_type=VARIANT; use v_mf.",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        cfg_dt = cfg_data_type_for_row(entity_type, field_key, cfg_type_map)
        if not cfg_dt:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "missing_cfg_field_key",
                "message": f"Cannot find field_key in Cfg__Fields for entity_type={entity_type}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        block_types = [x for x in g["block_type"].astype(str).str.lower().str.strip().tolist() if x]
        unique_block_types = sorted(set(block_types))

        if action == "CLEAR":
            block_type = unique_block_types[0] if unique_block_types else "list_item"
        else:
            if len(unique_block_types) != 1:
                errors.append({
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": field_key,
                    "error_reason": "mixed_block_types_in_group",
                    "message": f"block_types={unique_block_types}",
                    "sheet_rows": g["_sheet_row"].tolist(),
                })
                continue
            block_type = unique_block_types[0]

        if action != "CLEAR":
            msg = validate_block_type_vs_data_type(block_type, cfg_dt)
            if msg:
                errors.append({
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": field_key,
                    "error_reason": "block_type_data_type_mismatch",
                    "message": msg,
                    "sheet_rows": g["_sheet_row"].tolist(),
                })
                continue

        mf_type = map_cfg_dtype_to_shopify_type(cfg_dt, reference_default_kind)
        records = g.to_dict("records")

        try:
            value_to_write = build_metafield_value(
                block_type=block_type,
                action=action,
                mf_type=mf_type,
                rows=records,
            )
        except Exception as e:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "invalid_value",
                "message": str(e),
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        if len(value_to_write) >= GOOGLE_SHEETS_CELL_LIMIT:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "value_too_long",
                "message": f"value length={len(value_to_write)} exceeds Google Sheets cell limit reference threshold",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        plan_row = {
            "entity_type": entity_type,
            "gid_or_handle": gid_or_handle,
            "field_key": field_key,
            "namespace": namespace,
            "key": key,
            "mf_type": mf_type,
            "action": action,
            "block_type": block_type,
            "desired_value": value_to_write,
            "source_rows": g["_sheet_row"].tolist(),
        }
        plan_rows.append(plan_row)

        preview_rows.append({
            "entity_type": entity_type,
            "gid_or_handle": gid_or_handle,
            "field_key": field_key,
            "action": action,
            "block_type": block_type,
            "mf_type": mf_type,
            "value_preview": value_to_write[:300],
            "source_rows": ",".join(map(str, g["_sheet_row"].tolist()[:10])),
        })

    return {
        "plan_rows": plan_rows,
        "preview_rows": preview_rows,
        "errors": errors,
    }


# =========================================================
# Apply Shopify
# =========================================================

def parse_error_index(field_path):
    try:
        if isinstance(field_path, list) and len(field_path) >= 2 and str(field_path[0]) == "metafields":
            return int(field_path[1])
    except Exception:
        return None
    return None


def apply_plan(
    client: ShopifyClient,
    df_ready: pd.DataFrame,
    set_batch_size: int,
) -> dict[str, Any]:
    rows = df_ready[df_ready["_skip_reason"].eq("")].copy()

    set_inputs = []
    meta_rows = []

    for r in rows.to_dict("records"):
        set_inputs.append({
            "ownerId": r["owner_id"],
            "namespace": r["namespace"],
            "key": r["key"],
            "type": r["mf_type"],
            "value": str(r["desired_value"]),
        })
        meta_rows.append({
            "entity_type": r.get("entity_type", ""),
            "owner_id": r.get("owner_id", ""),
            "field_key": r.get("field_key", ""),
            "source_rows": r.get("source_rows", []),
        })

    total = len(set_inputs)
    if total == 0:
        return {
            "ok_count": 0,
            "fail_count": 0,
            "total": 0,
            "detail_fail_rows": [],
        }

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
                    "owner_id": r.get("owner_id", ""),
                    "field_key": r.get("field_key", ""),
                    "error_reason": "shopify_user_error",
                    "message": (
                        f"source_rows={r.get('source_rows')} | "
                        f"code={errs[0].get('code', '')} | "
                        f"msg={errs[0].get('message', '')} | "
                        f"field={errs[0].get('field')} | "
                        f"ns={inp.get('namespace')} key={inp.get('key')} "
                        f"type={inp.get('type')} value={str(inp.get('value'))[:120]}"
                    ),
                })

            if fail_items == 0:
                fail_count += len(batch)
                print(f"FAILED (fail={len(batch)})", flush=True)

                detail_fail_rows.append({
                    "entity_type": "",
                    "owner_id": "",
                    "field_key": "",
                    "error_reason": "shopify_batch_error",
                    "message": (
                        f"batch_error start={start_idx} size={len(batch)} | "
                        f"no per-item index returned | "
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
                        "owner_id": "",
                        "field_key": "",
                        "error_reason": "shopify_batch_error",
                        "message": (
                            f"batch_error start={start_idx} size={len(batch)} | "
                            f"code={e.get('code', '')} | "
                            f"msg={e.get('message', '')} | "
                            f"field={e.get('field')}"
                        ),
                    })

        except Exception as e:
            fail_count += len(batch)
            print("FAILED", flush=True)
            print(f"  exception: {e}", flush=True)

            for r, inp in zip(meta_batch, batch):
                detail_fail_rows.append({
                    "entity_type": r.get("entity_type", ""),
                    "owner_id": r.get("owner_id", ""),
                    "field_key": r.get("field_key", ""),
                    "error_reason": "batch_exception",
                    "message": (
                        f"source_rows={r.get('source_rows')} | exception: {e} | "
                        f"ns={inp.get('namespace')} key={inp.get('key')} type={inp.get('type')}"
                    ),
                })

    print(f"=== Apply done === total={total}, ok={ok_count}, fail={fail_count}", flush=True)

    return {
        "ok_count": ok_count,
        "fail_count": fail_count,
        "total": total,
        "detail_fail_rows": detail_fail_rows,
    }


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = DEFAULT_JOB_NAME,

    gsheet_sa_value: str,
    shopify_access_token: str,
    shop_domain: str,
    api_version: str,

    console_core_url: str,
    gsheet_sa_secret_name: str = "",
    shopify_token_secret_name: str = "",

    input_sheet_label: str = "edit",
    config_sheet_label: str = "config",
    runlog_sheet_label: str = "runlog_sheet",

    input_worksheet_title: str = "Edit__MetafieldBlocks",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,
    runlog_tab_name: str = "Ops__RunLog",

    run_id: Optional[str] = None,
    dry_run: bool = True,
    confirmed: bool = False,

    mode_default: str = "STRICT",
    action_default: str = "SET",

    only_entity_types: Optional[set[str]] = None,
    only_field_prefixes: Optional[set[str]] = None,

    reference_default_kind: str = "mixed",
    set_batch_size: int = 25,
    http_timeout: int = 60,
    preview_limit: int = 30,
    detail_max_per_reason: int = 3,
) -> dict[str, Any]:
    """
    Direct write structured metafield blocks to Shopify.

    Responsibility:
      edit / Edit__MetafieldBlocks -> Shopify metafieldsSet -> runlog

    It does NOT generate Edit__ValuesLong.
    """

    run_id = run_id or _utc_run_id("edit_metafieldblocks")

    resolved_shop_domain = _norm_str(shop_domain)
    resolved_api_version = _norm_str(api_version)
    if not resolved_shop_domain:
        raise ValueError("Resolved SHOP_DOMAIN is empty.")
    if not resolved_api_version:
        raise ValueError("Resolved SHOPIFY_API_VERSION is empty.")

    gc = build_gsheet_client(gsheet_sa_value)
    shopify = build_shopify_client(
        shopify_access_token=shopify_access_token,
        shop_domain=resolved_shop_domain,
        api_version=resolved_api_version,
        http_timeout=http_timeout,
    )

    _, ws_blocks, input_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_cfg_fields, cfg_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=config_sheet_label,
        worksheet_title=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
    )

    runlog_sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=runlog_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )

    logger = RunLogger(
        gc=gc,
        runlog_sheet_url=runlog_sheet_url,
        runlog_tab_name=runlog_tab_name,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
    )

    df_raw = load_blocks_sheet(ws_blocks)
    rows_loaded = int(len(df_raw))

    df_blocks, validation_errors = normalize_blocks_df(
        df=df_raw,
        mode_default=mode_default,
        action_default=action_default,
        only_entity_types=only_entity_types,
        only_field_prefixes=only_field_prefixes,
    )

    rows_pending = int(len(df_blocks))

    if validation_errors:
        rows_skipped = int(len(validation_errors))
        logger.log_row(
            phase="preview",
            log_type="summary",
            status="ERROR",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=rows_skipped,
            message=f"Validation errors: {len(validation_errors)}",
            error_reason="validation_error",
        )
        log_grouped_details(
            logger,
            phase="preview",
            status="FAIL",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=rows_skipped,
            detail_rows=validation_errors,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "error",
            "run_id": run_id,
            "job_name": job_name,
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": 0,
                "rows_planned": 0,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
                "errors": len(validation_errors),
            },
            "errors": validation_errors[:preview_limit],
            "warnings": [],
            "preview": [],
            "meta": {
                "site_code": site_code,
                "input_sheet_url": input_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "runlog_tab_name": runlog_tab_name,
                "shop_domain": resolved_shop_domain,
                "api_version": resolved_api_version,
                "gsheet_sa_b64_secret_from_cfg": _norm_str(gsheet_sa_secret_name),
                "shopify_token_secret_from_cfg": _norm_str(shopify_token_secret_name),
            },
        }

    cfg_fields = load_cfg_fields(ws_cfg_fields)
    cfg_type_map = build_cfg_field_key_map(cfg_fields)

    plan = build_plan(
        df_blocks=df_blocks,
        cfg_type_map=cfg_type_map,
        reference_default_kind=reference_default_kind,
    )

    build_errors = plan["errors"]
    plan_rows = plan["plan_rows"]
    preview_rows = plan["preview_rows"]

    rows_recognized = int(len(df_blocks))
    rows_planned = int(len(plan_rows))

    if build_errors:
        rows_skipped = int(len(build_errors))
        logger.log_row(
            phase="preview",
            log_type="summary",
            status="ERROR",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            message=f"Build plan errors: {len(build_errors)}",
            error_reason="build_plan_error",
        )
        log_grouped_details(
            logger,
            phase="preview",
            status="FAIL",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            detail_rows=build_errors,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "error",
            "run_id": run_id,
            "job_name": job_name,
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
                "errors": len(build_errors),
            },
            "errors": build_errors[:preview_limit],
            "warnings": [],
            "preview": preview_rows[:preview_limit],
            "meta": {
                "site_code": site_code,
                "input_sheet_url": input_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "runlog_tab_name": runlog_tab_name,
                "shop_domain": resolved_shop_domain,
                "api_version": resolved_api_version,
                "gsheet_sa_b64_secret_from_cfg": _norm_str(gsheet_sa_secret_name),
                "shopify_token_secret_from_cfg": _norm_str(shopify_token_secret_name),
            },
        }

    df_plan = pd.DataFrame(plan_rows)
    if df_plan.empty:
        df_ready = pd.DataFrame(columns=[
            "entity_type", "gid_or_handle", "field_key", "owner_id", "_skip_reason"
        ])
    else:
        df_ready = resolve_owner_ids(shopify, df_plan)

    df_unresolvable = df_ready[df_ready["_skip_reason"] != ""].copy() if not df_ready.empty else pd.DataFrame()
    unresolvable_detail_rows = []
    if not df_unresolvable.empty:
        unresolvable_detail_rows = [
            {
                "entity_type": _norm_str(r.get("entity_type")),
                "owner_id": _norm_str(r.get("owner_id")),
                "field_key": _norm_str(r.get("field_key")),
                "error_reason": _norm_str(r.get("_skip_reason")),
                "message": (
                    f"gid_or_handle={r.get('gid_or_handle')} | "
                    f"source_rows={r.get('source_rows')} | "
                    f"reason={r.get('_skip_reason')}"
                ),
            }
            for r in df_unresolvable.to_dict("records")
        ]

    rows_skipped = int(len(df_unresolvable))
    rows_resolvable = int(len(df_ready) - rows_skipped) if not df_ready.empty else 0

    preview_ready = []
    if not df_ready.empty:
        for r in df_ready.head(preview_limit).to_dict("records"):
            preview_ready.append({
                "entity_type": r.get("entity_type", ""),
                "gid_or_handle": r.get("gid_or_handle", ""),
                "owner_id": r.get("owner_id", ""),
                "field_key": r.get("field_key", ""),
                "action": r.get("action", ""),
                "block_type": r.get("block_type", ""),
                "mf_type": r.get("mf_type", ""),
                "skip_reason": r.get("_skip_reason", ""),
                "value_preview": _norm_str(r.get("desired_value"))[:300],
                "source_rows": r.get("source_rows", ""),
            })

    if not confirmed:
        status = "needs_confirmation"
        logger.log_row(
            phase="preview",
            log_type="summary",
            status="NEEDS_CONFIRMATION",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            message=(
                f"Preview generated | rows_loaded={rows_loaded} | rows_pending={rows_pending} | "
                f"rows_recognized={rows_recognized} | rows_planned={rows_planned} | "
                f"rows_resolvable={rows_resolvable} | rows_skipped={rows_skipped}"
            ),
            error_reason="",
        )
        log_grouped_details(
            logger,
            phase="preview",
            status="SKIP",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            detail_rows=unresolvable_detail_rows,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": status,
            "run_id": run_id,
            "job_name": job_name,
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_resolvable": rows_resolvable,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
                "errors": 0,
            },
            "errors": [],
            "warnings": [
                {
                    "type": "unresolvable_rows",
                    "count": int(len(unresolvable_detail_rows)),
                    "examples": unresolvable_detail_rows[:preview_limit],
                }
            ] if unresolvable_detail_rows else [],
            "preview": preview_ready,
            "meta": {
                "site_code": site_code,
                "input_sheet_url": input_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "runlog_tab_name": runlog_tab_name,
                "shop_domain": resolved_shop_domain,
                "api_version": resolved_api_version,
                "gsheet_sa_b64_secret_from_cfg": _norm_str(gsheet_sa_secret_name),
                "shopify_token_secret_from_cfg": _norm_str(shopify_token_secret_name),
                "dry_run": dry_run,
                "confirmed": confirmed,
            },
        }

    if dry_run:
        logger.log_row(
            phase="apply",
            log_type="summary",
            status="SUCCESS",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            message="Confirmed but DRY_RUN=True. No Shopify write executed.",
            error_reason="",
        )
        log_grouped_details(
            logger,
            phase="apply",
            status="SKIP",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            detail_rows=unresolvable_detail_rows,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "dry_run_confirmed_no_apply",
            "run_id": run_id,
            "job_name": job_name,
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_resolvable": rows_resolvable,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
                "errors": 0,
            },
            "errors": [],
            "warnings": [
                {
                    "type": "unresolvable_rows",
                    "count": int(len(unresolvable_detail_rows)),
                    "examples": unresolvable_detail_rows[:preview_limit],
                }
            ] if unresolvable_detail_rows else [],
            "preview": preview_ready,
            "meta": {
                "site_code": site_code,
                "input_sheet_url": input_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "runlog_tab_name": runlog_tab_name,
                "shop_domain": resolved_shop_domain,
                "api_version": resolved_api_version,
                "gsheet_sa_b64_secret_from_cfg": _norm_str(gsheet_sa_secret_name),
                "shopify_token_secret_from_cfg": _norm_str(shopify_token_secret_name),
                "dry_run": dry_run,
                "confirmed": confirmed,
            },
        }

    apply_result = apply_plan(
        client=shopify,
        df_ready=df_ready,
        set_batch_size=set_batch_size,
    )

    rows_written = int(apply_result["ok_count"])
    apply_fail_count = int(apply_result["fail_count"])

    final_status = "SUCCESS"
    if apply_fail_count > 0 and rows_written > 0:
        final_status = "PARTIAL_SUCCESS"
    elif apply_fail_count > 0 and rows_written == 0:
        final_status = "ERROR"

    all_fail_detail_rows = unresolvable_detail_rows + apply_result["detail_fail_rows"]
    rows_skipped_final = rows_skipped

    logger.log_row(
        phase="apply",
        log_type="summary",
        status=final_status,
        rows_loaded=rows_loaded,
        rows_pending=rows_pending,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped_final,
        message=(
            f"Apply completed | rows_planned={rows_planned} | rows_written={rows_written} | "
            f"rows_skipped={rows_skipped_final} | apply_fail_count={apply_fail_count}"
        ),
        error_reason="",
    )
    log_grouped_details(
        logger,
        phase="apply",
        status="FAIL",
        rows_loaded=rows_loaded,
        rows_pending=rows_pending,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped_final,
        detail_rows=all_fail_detail_rows,
        max_per_reason=detail_max_per_reason,
    )
    logger.flush()

    return {
        "status": "applied",
        "run_id": run_id,
        "job_name": job_name,
        "summary": {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_pending,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_resolvable": rows_resolvable,
            "rows_written": rows_written,
            "rows_skipped": rows_skipped_final,
            "apply_fail_count": apply_fail_count,
            "errors": int(apply_fail_count),
        },
        "errors": apply_result["detail_fail_rows"][:preview_limit],
        "warnings": [
            {
                "type": "unresolvable_rows",
                "count": int(len(unresolvable_detail_rows)),
                "examples": unresolvable_detail_rows[:preview_limit],
            }
        ] if unresolvable_detail_rows else [],
        "preview": preview_ready,
        "meta": {
            "site_code": site_code,
            "input_sheet_url": input_sheet_url,
            "cfg_sheet_url": cfg_sheet_url,
            "runlog_sheet_url": runlog_sheet_url,
            "runlog_tab_name": runlog_tab_name,
            "shop_domain": resolved_shop_domain,
            "api_version": resolved_api_version,
            "gsheet_sa_b64_secret_from_cfg": _norm_str(gsheet_sa_secret_name),
            "shopify_token_secret_from_cfg": _norm_str(shopify_token_secret_name),
            "dry_run": dry_run,
            "confirmed": confirmed,
            "final_status": final_status,
        },
    }
