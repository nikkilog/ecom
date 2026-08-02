"""shopify_sync/1_1_edit_metafields.py

Edit Shopify metafields from the configured Edit__ValuesLong input.
Business behavior is preserved from the former edit_metafields.py Current;
this module adds the verified Local/Colab runtime boundary and identity.
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



# =========================================================
# Constants
# =========================================================

CFG_SITES_TAB_DEFAULT = "Cfg__Sites"
CFG_FIELDS_TAB_DEFAULT = "Cfg__Fields"

MODULE_PATH = "shopify_sync.1_1_edit_metafields"
MODULE_VERSION = "2026-08-01-runtime-boundary-v1"
DEFAULT_JOB_NAME = "edit_metafields"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
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

SUPPORTED_ACTIONS = {"SET", "CLEAR"}
FORBIDDEN_SHOPIFY_PREFIXES = ("mf.shopify.", "v_mf.shopify.", "v.mf.shopify.")

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
    userErrors { field message }
  }
}
"""

M_DELETE = """
mutation deleteMf($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    deletedMetafields { ownerId namespace key }
    userErrors { field message }
  }
}
"""


# =========================================================
# Small data objects
# =========================================================

@dataclass
class ShopifyClient:
    graph_url: str
    headers: dict[str, str]
    timeout: int = 60


# =========================================================
# Generic utils
# =========================================================

def _utc_run_id(prefix: str = "edit") -> str:
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


def _chunk_list(items: list[Any], size: int):
    for i in range(0, len(items), size):
        yield i, items[i:i + size]


def _split_items(s: str) -> list[str]:
    s = _norm_str(s)
    if not s:
        return []
    parts = re.split(r"[,\n;|]+", s)
    return [p.strip() for p in parts if p and p.strip()]


def _is_json_array_string(s: str) -> bool:
    s = _norm_str(s)
    if not (s.startswith("[") and s.endswith("]")):
        return False
    try:
        return isinstance(json.loads(s), list)
    except Exception:
        return False


def _safe_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


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
            data = r.json()

            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}")

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
# Sheets locating
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

    m = df[(df["site_code"] == site_code.strip().upper()) & (df["label"] == label.strip())].copy()
    m = m[m["sheet_url"] != ""]

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


# =========================================================
# Runlog
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
        self.ws = _with_sheets_retry(
            lambda: sh.worksheet(runlog_tab_name),
            action=f"runlog.worksheet:{runlog_tab_name}",
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


# =========================================================
# Load input
# =========================================================

def load_edit_values_long(ws_edit) -> pd.DataFrame:
    rows = _with_sheets_retry(
        ws_edit.get_all_records,
        action=f"input.get_all_records:{ws_edit.title}",
    )
    df = pd.DataFrame(rows)

    required_cols = ["entity_type", "gid_or_handle", "field_key", "desired_value", "action", "mode", "note", "run_id"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Edit__ValuesLong missing columns: {missing}")

    df["_sheet_row"] = range(2, 2 + len(df))

    for c in required_cols:
        df[c] = df[c].astype(str).fillna("").replace("nan", "").str.strip()

    return df


def filter_pending_rows(
    df: pd.DataFrame,
    mode_default: str,
    only_entity_types: Optional[set[str]],
    only_field_prefixes: Optional[set[str]],
) -> pd.DataFrame:
    d = df.copy()

    d = d[d["run_id"].eq("")]
    d["entity_type"] = d["entity_type"].str.upper().str.strip()
    d["action"] = d["action"].str.upper().str.strip()
    d["mode"] = d["mode"].replace("", mode_default).str.upper().str.strip()

    if only_entity_types:
        allow = {x.upper() for x in only_entity_types}
        d = d[d["entity_type"].isin(allow)]

    if only_field_prefixes:
        prefixes = tuple(only_field_prefixes)
        d = d[d["field_key"].str.startswith(prefixes)]

    d = d[~d["action"].isin(["SKIP", ""])]

    return d


# =========================================================
# Recognition
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
        if entity_type == "CUSTOMER":
            return f"gid://shopify/Customer/{s}"

    return s


def recognize_rows(df_work: pd.DataFrame, mode_default: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    records = []
    bad_rows = []

    for idx, r in df_work.iterrows():
        entity_type = _norm_str(r.get("entity_type")).upper()
        field_key = _norm_str(r.get("field_key"))
        action = _norm_str(r.get("action")).upper()
        mode = _norm_str(r.get("mode") or mode_default).upper()
        desired = _norm_str(r.get("desired_value"))
        owner_raw = _norm_str(r.get("gid_or_handle"))
        owner_ref = normalize_owner_ref(entity_type, owner_raw)
        sheet_row = int(r.get("_sheet_row", -1))

        reason = ""
        parsed = parse_field_key(field_key)

        if not parsed:
            reason = "field_key_not_recognized"
        else:
            prefix, ns, key = parsed

            if prefix == "v_mf." and entity_type != "VARIANT":
                reason = f"prefix_entity_mismatch"
            if prefix == "mf." and entity_type == "VARIANT":
                reason = "prefix_entity_mismatch"

        if not reason and action not in SUPPORTED_ACTIONS:
            reason = f"action_not_supported"

        if reason:
            bad_rows.append({
                "sheet_row": sheet_row,
                "entity_type": entity_type,
                "gid_or_handle": owner_raw,
                "field_key": field_key,
                "action": action,
                "mode": mode,
                "reason": reason,
                "desired_value": desired,
            })
            continue

        records.append({
            "_row_index": idx,
            "sheet_row": sheet_row,
            "entity_type": entity_type,
            "owner_ref": owner_ref,
            "owner_raw": owner_raw,
            "prefix": prefix,
            "namespace": ns,
            "key": key,
            "field_key": field_key,
            "action": action,
            "mode": mode,
            "desired_value": desired,
            "note": _norm_str(r.get("note")),
        })

    return pd.DataFrame(records), pd.DataFrame(bad_rows)


def abort_if_forbidden_fieldkeys(df_src: pd.DataFrame):
    fk = df_src.get("field_key", pd.Series([], dtype=str)).astype(str).str.strip().str.lower()
    mask_bad = fk.apply(lambda s: any(s.startswith(p) for p in FORBIDDEN_SHOPIFY_PREFIXES))

    if mask_bad.any():
        offenders = df_src.loc[
            mask_bad,
            ["_sheet_row", "entity_type", "gid_or_handle", "field_key", "desired_value", "action", "mode", "note", "run_id"]
        ].copy()
        offenders = offenders.rename(columns={"_sheet_row": "sheet_row"})
        raise ValueError({
            "message": "Detected forbidden shopify-prefixed metafield in Edit__ValuesLong.field_key",
            "offenders": offenders.head(200).to_dict("records"),
        })


# =========================================================
# Owner resolution
# =========================================================

def normalize_gid_or_numeric(entity_type: str, ref: str) -> Optional[str]:
    s = _norm_str(ref)
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
        if entity_type == "CUSTOMER":
            return f"gid://shopify/Customer/{s}"
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


def resolve_owner_ids(client: ShopifyClient, df_parsed: pd.DataFrame) -> pd.DataFrame:
    df_ready = df_parsed.copy()
    df_ready["owner_id"] = df_ready.apply(
        lambda r: normalize_gid_or_numeric(r["entity_type"], r["owner_ref"]),
        axis=1,
    )

    mask_need = df_ready["owner_id"].isna() & df_ready["owner_ref"].ne("")
    need = df_ready.loc[mask_need, ["entity_type", "owner_ref"]].drop_duplicates()

    cache_product = {}
    cache_collection = {}
    cache_page = {}
    cache_variant = {}

    def resolve_one(entity_type: str, ref: str):
        if entity_type == "PRODUCT":
            if ref in cache_product:
                return cache_product[ref]
            v = resolve_product_by_handle(client, ref)
            cache_product[ref] = v
            return v

        if entity_type == "COLLECTION":
            if ref in cache_collection:
                return cache_collection[ref]
            v = resolve_collection_by_handle(client, ref)
            cache_collection[ref] = v
            return v

        if entity_type == "PAGE":
            if ref in cache_page:
                return cache_page[ref]
            v = resolve_page_by_handle(client, ref)
            cache_page[ref] = v
            return v

        if entity_type == "VARIANT":
            if ref in cache_variant:
                return cache_variant[ref]
            v = resolve_variant_by_sku(client, ref)
            cache_variant[ref] = v
            return v

        return None

    resolved_map = {}
    for row in need.itertuples(index=False):
        resolved_map[(row.entity_type, row.owner_ref)] = resolve_one(row.entity_type, row.owner_ref)

    df_ready["owner_id"] = df_ready.apply(
        lambda r: r["owner_id"] if r["owner_id"] else resolved_map.get((r["entity_type"], r["owner_ref"])),
        axis=1,
    )

    df_ready["_skip_reason"] = ""
    df_ready.loc[df_ready["owner_id"].isna() | (df_ready["owner_id"].astype(str).str.strip() == ""), "_skip_reason"] = "cannot_resolve_owner_id"

    mask_has_owner = df_ready["_skip_reason"].eq("")
    unique_owner_ids = df_ready.loc[mask_has_owner, "owner_id"].astype(str).drop_duplicates().tolist()

    exist_map = nodes_exist_map(client, unique_owner_ids, chunk_size=80)

    df_ready["_owner_exists"] = df_ready["owner_id"].apply(lambda x: bool(exist_map.get(x, False)) if x else False)
    df_ready.loc[mask_has_owner & (~df_ready["_owner_exists"]), "_skip_reason"] = "owner_not_found_in_shop"

    return df_ready


# =========================================================
# Cfg fields / type resolving
# =========================================================

def load_cfg_fields_map(ws_cfg_fields) -> dict[tuple[str, str], str]:
    rows = _with_sheets_retry(
        ws_cfg_fields.get_all_records,
        action=f"cfg_fields.get_all_records:{ws_cfg_fields.title}",
    )
    d = pd.DataFrame(rows)

    if d.empty:
        return {}

    for c in ["entity_type", "field_key", "data_type", "source_type"]:
        if c not in d.columns:
            d[c] = ""

    d["entity_type"] = d["entity_type"].astype(str).str.upper().str.strip()
    d["field_key"] = d["field_key"].astype(str).str.strip()
    d["data_type"] = d["data_type"].astype(str).str.strip().str.lower()
    d["source_type"] = d["source_type"].astype(str).str.strip().str.upper()

    d = d[
        (d["source_type"].eq("METAFIELD"))
        | (d["field_key"].str.startswith("mf."))
        | (d["field_key"].str.startswith("v_mf."))
    ].copy()

    mp = {}
    for r in d.to_dict("records"):
        et = _norm_str(r.get("entity_type"))
        fk = _norm_str(r.get("field_key"))
        dt_ = _norm_str(r.get("data_type")).lower()
        if et and fk and dt_:
            mp[(et, fk)] = dt_

    return mp


def build_cfg_keyonly_map(cfg_type_map: dict[tuple[str, str], str]) -> dict[str, str]:
    out = {}
    for (_, fk), dt_ in cfg_type_map.items():
        if fk and dt_ and fk not in out:
            out[fk] = dt_
    return out


def resolve_cfg_data_type(
    entity_type: str,
    field_key: str,
    cfg_type_map: dict[tuple[str, str], str],
    cfg_by_keyonly: dict[str, str],
) -> str:
    et = _norm_str(entity_type).upper()
    fk = _norm_str(field_key)

    v = cfg_type_map.get((et, fk))
    if v:
        return v

    if fk.startswith("v_mf."):
        v2 = cfg_type_map.get((et, "mf." + fk[len("v_mf."):]))
        if v2:
            return v2

    return cfg_by_keyonly.get(fk, "")


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


def mf_type_for_row(
    entity_type: str,
    field_key: str,
    cfg_type_map: dict[tuple[str, str], str],
    cfg_by_keyonly: dict[str, str],
    reference_default_kind: str,
    type_override_by_field_key: Optional[dict[str, str]] = None,
) -> str:
    fk = _norm_str(field_key)

    if isinstance(type_override_by_field_key, dict):
        ov = type_override_by_field_key.get(fk)
        if ov:
            return _norm_str(ov)

    cfg_dt = resolve_cfg_data_type(entity_type, fk, cfg_type_map, cfg_by_keyonly)
    if cfg_dt:
        return map_cfg_dtype_to_shopify_type(cfg_dt, reference_default_kind)

    return "single_line_text_field"


# =========================================================
# Value normalization
# =========================================================

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


def value_for_shopify(mf_type: str, desired: str, action: str) -> str:
    mf_type = _norm_str(mf_type)
    action = _norm_str(action).upper()
    desired = "" if desired is None else str(desired)

    if action == "CLEAR":
        return "[]" if mf_type.startswith("list.") else ""

    s = desired.strip()

    if mf_type.startswith("list."):
        if s == "":
            return "[]"

        if _is_json_array_string(s):
            arr = json.loads(s)
            arr = normalize_reference_items_by_type(mf_type, arr)
            return json.dumps(arr, ensure_ascii=False)

        items = _split_items(s)
        items = normalize_reference_items_by_type(mf_type, items)
        return json.dumps(items, ensure_ascii=False)

    if mf_type in {"product_reference", "variant_reference", "collection_reference"}:
        if s == "":
            return ""
        items = normalize_reference_items_by_type(mf_type, [s])
        return items[0]

    return desired


# =========================================================
# Planning
# =========================================================

def build_plan(
    df_ready: pd.DataFrame,
    cfg_type_map: dict[tuple[str, str], str],
    reference_default_kind: str,
    type_override_by_field_key: Optional[dict[str, str]],
) -> dict[str, Any]:
    """
    Build write plan.

    SET rows use Shopify metafieldsSet.
    CLEAR rows use Shopify metafieldsDelete by ownerId + namespace + key.
    This is intentional: writing an empty string through metafieldsSet is not a reliable clear/delete operation
    for Shopify metafields, especially when definitions or validations exist.
    """
    df_apply = df_ready[df_ready["_skip_reason"].eq("")].copy()
    cfg_by_keyonly = build_cfg_keyonly_map(cfg_type_map)

    set_inputs: list[dict[str, Any]] = []
    set_meta_rows: list[dict[str, Any]] = []
    delete_inputs: list[dict[str, Any]] = []
    delete_meta_rows: list[dict[str, Any]] = []
    preview_rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    missing_cfg_type = 0

    for r in df_apply.itertuples(index=False):
        action = _norm_str(getattr(r, "action", "")).upper()
        if action not in SUPPORTED_ACTIONS:
            invalid_rows.append({
                "sheet_row": getattr(r, "sheet_row", None),
                "entity_type": getattr(r, "entity_type", ""),
                "owner_id": getattr(r, "owner_id", ""),
                "field_key": getattr(r, "field_key", ""),
                "error_reason": "action_not_supported",
                "message": f"unsupported_action={action}",
            })
            continue

        et = getattr(r, "entity_type", "")
        fk = getattr(r, "field_key", "")
        owner_id = getattr(r, "owner_id", "")
        namespace = getattr(r, "namespace", "")
        key = getattr(r, "key", "")

        cfg_dt = resolve_cfg_data_type(et, fk, cfg_type_map, cfg_by_keyonly)
        has_ov = isinstance(type_override_by_field_key, dict) and bool(type_override_by_field_key.get(_norm_str(fk)))

        if (not cfg_dt) and (not has_ov):
            missing_cfg_type += 1

        mf_type = mf_type_for_row(
            entity_type=et,
            field_key=fk,
            cfg_type_map=cfg_type_map,
            cfg_by_keyonly=cfg_by_keyonly,
            reference_default_kind=reference_default_kind,
            type_override_by_field_key=type_override_by_field_key,
        )

        meta_row = {
            "sheet_row": getattr(r, "sheet_row", None),
            "entity_type": et,
            "owner_id": owner_id,
            "field_key": fk,
            "namespace": namespace,
            "key": key,
        }

        if action == "CLEAR":
            # Real clear/delete: remove the metafield itself. Do NOT write empty string via metafieldsSet.
            item = {
                "ownerId": owner_id,
                "namespace": namespace,
                "key": key,
            }
            delete_inputs.append(item)
            delete_meta_rows.append(meta_row)

            preview_rows.append({
                "sheet_row": getattr(r, "sheet_row", None),
                "entity_type": et,
                "owner_id": owner_id,
                "field_key": fk,
                "action": action,
                "mf_type": mf_type,
                "value_preview": "<DELETE metafield>",
            })
            continue

        desired = _norm_str(getattr(r, "desired_value", ""))
        try:
            value_to_write = value_for_shopify(mf_type, desired, action)
        except Exception as e:
            invalid_rows.append({
                "sheet_row": getattr(r, "sheet_row", None),
                "entity_type": getattr(r, "entity_type", ""),
                "owner_id": getattr(r, "owner_id", ""),
                "field_key": getattr(r, "field_key", ""),
                "error_reason": "invalid_value",
                "message": f"sheet_row={getattr(r, 'sheet_row', None)} | invalid_value={desired} | mf_type={mf_type} | {e}",
            })
            continue

        item = {
            "ownerId": owner_id,
            "namespace": namespace,
            "key": key,
            "type": mf_type,
            "value": str(value_to_write),
        }
        set_inputs.append(item)
        set_meta_rows.append(meta_row)

        preview_rows.append({
            "sheet_row": getattr(r, "sheet_row", None),
            "entity_type": et,
            "owner_id": owner_id,
            "field_key": fk,
            "action": action,
            "mf_type": mf_type,
            "value_preview": str(value_to_write)[:200],
        })

    rows_skipped_total = _safe_int((df_ready["_skip_reason"] != "").sum()) + len(invalid_rows)
    rows_planned_set = len(set_inputs)
    rows_planned_delete = len(delete_inputs)
    rows_planned_total = rows_planned_set + rows_planned_delete

    summary = {
        "rows_recognized": int(len(df_ready)),
        "rows_resolvable": int(len(df_apply)),
        "rows_planned_set": int(rows_planned_set),
        "rows_planned_delete": int(rows_planned_delete),
        "rows_planned_total": int(rows_planned_total),
        "rows_skipped_unresolvable": int((df_ready["_skip_reason"] != "").sum()),
        "rows_skipped_invalid": int(len(invalid_rows)),
        "rows_skipped_total": int(rows_skipped_total),
        "missing_cfg_type": int(missing_cfg_type),
    }

    return {
        "summary": summary,
        "set_inputs": set_inputs,
        "set_meta_rows": set_meta_rows,
        "delete_inputs": delete_inputs,
        "delete_meta_rows": delete_meta_rows,
        # Backward-compatible aliases for old callers/debug cells.
        "meta_rows": set_meta_rows,
        "preview_rows": preview_rows,
        "invalid_rows": invalid_rows,
        "df_apply": df_apply,
    }


# =========================================================
# Runlog helper: grouped detail rows
# =========================================================

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
    max_per_reason: int = 2,
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
# Apply
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
    set_inputs: list[dict[str, Any]],
    set_meta_rows: list[dict[str, Any]],
    set_batch_size: int,
    delete_inputs: Optional[list[dict[str, Any]]] = None,
    delete_meta_rows: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    delete_inputs = delete_inputs or []
    delete_meta_rows = delete_meta_rows or []

    set_total = len(set_inputs)
    delete_total = len(delete_inputs)
    total = set_total + delete_total

    ok_count = 0
    fail_count = 0
    detail_fail_rows: list[dict[str, Any]] = []

    # -----------------------------
    # 1) Apply SET rows via metafieldsSet
    # -----------------------------
    if set_total == 0:
        print("=== Applying metafieldsSet === total=0, batches=0, batch_size=0")
    else:
        total_batches = (set_total + set_batch_size - 1) // set_batch_size
        print(f"=== Applying metafieldsSet === total={set_total}, batches={total_batches}, batch_size={set_batch_size}")

        for batch_no, (start_idx, batch) in enumerate(_chunk_list(set_inputs, set_batch_size), start=1):
            meta_batch = set_meta_rows[start_idx:start_idx + len(batch)]
            print(f"SET Batch {batch_no}/{total_batches}: {len(batch)} items ... ", end="", flush=True)

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
                            f"sheet_row={r.get('sheet_row')} | action=SET | "
                            f""
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
                            f"SET batch_error start={start_idx} size={len(batch)} | "
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
                                f"SET batch_error start={start_idx} size={len(batch)} | "
                                f""
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
                            f"sheet_row={r.get('sheet_row')} | action=SET | exception: {e} | "
                            f"ns={inp.get('namespace')} key={inp.get('key')} type={inp.get('type')}"
                        ),
                    })

    # -----------------------------
    # 2) Apply CLEAR rows via metafieldsDelete
    # -----------------------------
    if delete_total == 0:
        print("=== Applying metafieldsDelete === total=0, batches=0, batch_size=0")
    else:
        total_batches = (delete_total + set_batch_size - 1) // set_batch_size
        print(f"=== Applying metafieldsDelete === total={delete_total}, batches={total_batches}, batch_size={set_batch_size}")

        for batch_no, (start_idx, batch) in enumerate(_chunk_list(delete_inputs, set_batch_size), start=1):
            meta_batch = delete_meta_rows[start_idx:start_idx + len(batch)]
            print(f"DELETE Batch {batch_no}/{total_batches}: {len(batch)} items ... ", end="", flush=True)

            try:
                data = gql(client, M_DELETE, {"metafields": batch})
                resp = data["metafieldsDelete"]
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
                            f"sheet_row={r.get('sheet_row')} | action=CLEAR_DELETE | "
                            f""
                            f"msg={errs[0].get('message', '')} | "
                            f"field={errs[0].get('field')} | "
                            f"ownerId={inp.get('ownerId')} ns={inp.get('namespace')} key={inp.get('key')}"
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
                            f"DELETE batch_error start={start_idx} size={len(batch)} | "
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
                                f"DELETE batch_error start={start_idx} size={len(batch)} | "
                                f""
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
                            f"sheet_row={r.get('sheet_row')} | action=CLEAR_DELETE | exception: {e} | "
                            f"ownerId={inp.get('ownerId')} ns={inp.get('namespace')} key={inp.get('key')}"
                        ),
                    })

    print(
        f"=== Apply done === total={total}, ok={ok_count}, fail={fail_count}, set={set_total}, delete={delete_total}",
        flush=True,
    )

    return {
        "ok_count": ok_count,
        "fail_count": fail_count,
        "total": total,
        "set_total": set_total,
        "delete_total": delete_total,
        "detail_fail_rows": detail_fail_rows,
    }


# =========================================================
# Colab preview helpers
# =========================================================

def build_preview_output(
    *,
    result_status: str,
    site_code: str,
    job_name: str,
    run_id: str,
    rows_loaded: int,
    rows_pending: int,
    rows_recognized: int,
    rows_planned: int,
    rows_skipped: int,
    preview_rows: list[dict[str, Any]],
    warning_groups: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": result_status,
        "site_code": site_code,
        "job_name": job_name,
        "run_id": run_id,
        "summary": {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_pending,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_skipped": rows_skipped,
        },
        "preview": preview_rows,
        "warnings": warning_groups,
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
    api_version: str = "2026-01",

    console_core_url: str,
    input_sheet_label: str = "edit",
    worksheet_title: str = "Edit__ValuesLong",

    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,

    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,

    run_id: Optional[str] = None,
    dry_run: bool = True,
    confirmed: bool = False,
    preview_limit: int = 50,

    mode_default: str = "STRICT",
    write_mode: str = "UPSERT",
    delete_empty: bool = False,

    only_entity_types: Optional[set[str]] = None,
    only_field_prefixes: Optional[set[str]] = None,

    reference_default_kind: str = "mixed",
    type_override_by_field_key: Optional[dict[str, str]] = None,

    set_batch_size: int = 25,
    http_timeout: int = 60,
    abort_if_fieldkey_contains: str = ".shopify.",

    detail_max_per_reason: int = 2,
) -> dict[str, Any]:
    if write_mode.upper() != "UPSERT":
        raise ValueError(f"Currently only WRITE_MODE='UPSERT' is supported, got: {write_mode}")

    run_id = run_id or _utc_run_id("edit")

    gc = build_gsheet_client(gsheet_sa_value)
    shopify = build_shopify_client(
        shopify_access_token=shopify_access_token,
        shop_domain=shop_domain,
        api_version=api_version,
        http_timeout=http_timeout,
    )

    _, ws_edit, edit_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_cfg_fields, cfg_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_sheet_label,
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

    df = load_edit_values_long(ws_edit)
    df_work = filter_pending_rows(
        df=df,
        mode_default=mode_default,
        only_entity_types=only_entity_types,
        only_field_prefixes=only_field_prefixes,
    )

    rows_loaded = int(len(df))
    rows_pending = int(len(df_work))

    if df_work.empty:
        logger.log_row(
            phase="preview",
            log_type="summary",
            status="SUCCESS",
            rows_loaded=rows_loaded,
            rows_pending=0,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=0,
            message="No pending rows in scope",
            error_reason="",
        )
        logger.flush()

        return {
            "status": "no_pending_rows",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": 0,
                "rows_recognized": 0,
                "rows_planned": 0,
                "rows_skipped": 0,
            },
            "preview": [],
            "warnings": [],
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "edit_sheet_url": edit_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
            },
        }

    abort_if_forbidden_fieldkeys(df_work)

    df_parsed, df_bad = recognize_rows(df_work, mode_default=mode_default)
    rows_recognized = int(len(df_parsed))

    if df_parsed.empty:
        rows_skipped = int(len(df_bad))

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
            message=f"No recognized rows. bad_rows={len(df_bad)}",
            error_reason="no_recognized_rows",
        )

        bad_detail_rows = [
            {
                "entity_type": _norm_str(r.get("entity_type")),
                "owner_id": "",
                "field_key": _norm_str(r.get("field_key")),
                "error_reason": _norm_str(r.get("reason")),
                "message": (
                    f"sheet_row={r.get('sheet_row')} | "
                    f"gid_or_handle={r.get('gid_or_handle')} | "
                    f"action={r.get('action')} | "
                    f"reason={r.get('reason')}"
                ),
            }
            for r in df_bad.to_dict("records")
        ]
        log_grouped_details(
            logger,
            phase="preview",
            status="SKIP",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=rows_skipped,
            detail_rows=bad_detail_rows,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "no_recognized_rows",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": 0,
                "rows_planned": 0,
                "rows_skipped": rows_skipped,
            },
            "preview": [],
            "warnings": [
                {
                    "type": "unrecognized_rows",
                    "count": int(len(df_bad)),
                    "examples": df_bad.head(preview_limit).to_dict("records"),
                }
            ],
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
            },
        }

    df_ready = resolve_owner_ids(shopify, df_parsed)
    cfg_type_map = load_cfg_fields_map(ws_cfg_fields)

    plan = build_plan(
        df_ready=df_ready,
        cfg_type_map=cfg_type_map,
        reference_default_kind=reference_default_kind,
        type_override_by_field_key=type_override_by_field_key,
    )

    rows_planned = int(plan["summary"].get("rows_planned_total", plan["summary"].get("rows_planned_set", 0)))
    rows_skipped = int(plan["summary"]["rows_skipped_total"])

    warnings = []

    bad_detail_rows = []
    if not df_bad.empty:
        warnings.append({
            "type": "unrecognized_rows",
            "count": int(len(df_bad)),
            "examples": df_bad.head(preview_limit).to_dict("records"),
        })

        bad_detail_rows = [
            {
                "entity_type": _norm_str(r.get("entity_type")),
                "owner_id": "",
                "field_key": _norm_str(r.get("field_key")),
                "error_reason": _norm_str(r.get("reason")),
                "message": (
                    f"sheet_row={r.get('sheet_row')} | "
                    f"gid_or_handle={r.get('gid_or_handle')} | "
                    f"action={r.get('action')} | "
                    f"reason={r.get('reason')}"
                ),
            }
            for r in df_bad.to_dict("records")
        ]

    df_unresolvable = df_ready[df_ready["_skip_reason"] != ""].copy()
    unresolvable_detail_rows = []
    if not df_unresolvable.empty:
        warnings.append({
            "type": "unresolvable_rows",
            "count": int(len(df_unresolvable)),
            "examples": df_unresolvable.head(preview_limit)[
                ["sheet_row", "entity_type", "owner_ref", "field_key", "_skip_reason"]
            ].to_dict("records"),
        })

        unresolvable_detail_rows = [
            {
                "entity_type": _norm_str(r.get("entity_type")),
                "owner_id": _norm_str(r.get("owner_id")),
                "field_key": _norm_str(r.get("field_key")),
                "error_reason": _norm_str(r.get("_skip_reason")),
                "message": (
                    f"sheet_row={r.get('sheet_row')} | "
                    f"owner_ref={r.get('owner_ref')} | "
                    f"reason={r.get('_skip_reason')}"
                ),
            }
            for r in df_unresolvable.to_dict("records")
        ]

    invalid_detail_rows = plan["invalid_rows"]
    if invalid_detail_rows:
        warnings.append({
            "type": "invalid_rows",
            "count": int(len(invalid_detail_rows)),
            "examples": invalid_detail_rows[:preview_limit],
        })

    preview = plan["preview_rows"][:preview_limit]

    if not confirmed:
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
                f"rows_recognized={rows_recognized} | rows_planned={rows_planned} | rows_skipped={rows_skipped}"
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
            detail_rows=bad_detail_rows + unresolvable_detail_rows + invalid_detail_rows,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "needs_confirmation",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_skipped": rows_skipped,
            },
            "preview": preview,
            "warnings": warnings,
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "edit_sheet_url": edit_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
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
            detail_rows=bad_detail_rows + unresolvable_detail_rows + invalid_detail_rows,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "dry_run_confirmed_no_apply",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
            },
            "preview": preview,
            "warnings": warnings,
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
            },
        }

    apply_result = apply_plan(
        client=shopify,
        set_inputs=plan["set_inputs"],
        set_meta_rows=plan["set_meta_rows"],
        set_batch_size=set_batch_size,
        delete_inputs=plan["delete_inputs"],
        delete_meta_rows=plan["delete_meta_rows"],
    )

    rows_written = int(apply_result["ok_count"])
    apply_fail_count = int(apply_result["fail_count"])

    final_status = "SUCCESS"
    if apply_fail_count > 0 and rows_written > 0:
        final_status = "PARTIAL_SUCCESS"
    elif apply_fail_count > 0 and rows_written == 0:
        final_status = "ERROR"

    logger.log_row(
        phase="apply",
        log_type="summary",
        status=final_status,
        rows_loaded=rows_loaded,
        rows_pending=rows_pending,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message=(
            f"Apply completed | rows_planned={rows_planned} | rows_written={rows_written} | "
            f"rows_skipped={rows_skipped} | apply_fail_count={apply_fail_count}"
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
        rows_skipped=rows_skipped,
        detail_rows=apply_result["detail_fail_rows"],
        max_per_reason=detail_max_per_reason,
    )
    logger.flush()

    return {
        "status": "applied",
        "summary": {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_pending,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_written": rows_written,
            "rows_skipped": rows_skipped,
            "apply_fail_count": apply_fail_count,
        },
        "preview": preview,
        "warnings": warnings,
        "meta": {
            "site_code": site_code,
            "job_name": job_name,
            "run_id": run_id,
            "runlog_sheet_url": runlog_sheet_url,
            "runlog_tab_name": runlog_tab_name,
            "final_status": final_status,
        },
    }
