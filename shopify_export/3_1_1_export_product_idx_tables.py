# -*- coding: utf-8 -*-
"""
shopify_export/3_1_1_export_product_idx_tables.py

用途：
- 从 Shopify 导出两张 IDX 薄索引表：
  - IDX__Products
  - IDX__Variants
- 配置来源：
  - console_core_url -> Cfg__Sites
  - label=config      -> Cfg__ExportTabFields
  - label=export_product -> 输出表
- 支持：
  - CALC 依赖展开
  - MF_VALUE("ns","key")
  - COALESCE(...)
  - JSON(...)
  - GET({fid}, n).name / .value
  - xxx[0] / nodes[0] 路径
  - core.tags 人类可读输出
  - 旧版 variant.weight / variant.weightUnit 自动 remap
  - 过滤 ARCHIVED
"""

from __future__ import annotations

import base64
import html
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


# =========================================================
# 基础
# =========================================================

TailStep = Union[str, int]

# Used by paths like:
# - product.media.nodes[].preview.image.url
# - variant.product.media.nodes[].preview.image.url
ALL_LIST_STEP = "__ALL__"

# Shopify product media gallery limit per exported row.
# Raise this if a product may have more than 50 media images.
DEFAULT_CONNECTION_LIST_FIRST = 15

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FIELD_DEF: Dict[str, Dict[str, Any]] = {}

EXPORT_IDX_TABLES_PATCH_VERSION = "2026-05-24-calc-fields-in-fetch-v2"
MODULE_PATH = "shopify_export.3_1_1_export_product_idx_tables"
MODULE_VERSION = EXPORT_IDX_TABLES_PATCH_VERSION
DEFAULT_JOB_NAME = "export_product_idx_tables"


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_int(x: Any, default: int = 999999) -> int:
    try:
        return int(str(x).strip())
    except Exception:
        return default


def _clean_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _is_blank(x: Any) -> bool:
    return _clean_str(x) == ""


def _gql_safe_alias(s: str) -> str:
    a = re.sub(r"[^0-9A-Za-z_]", "_", str(s or ""))
    if re.match(r"^\d", a):
        a = "f_" + a
    return a or "f_blank"


def _listify_records(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# =========================================================
# Runtime / Secret / Workspace Registry
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
    return _clean_str(value).upper()


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
    """Resolve one Secret without exposing its value.

    Colab uses the exact configured Colab Secret name. Local execution delegates
    to WorkspaceSecretResolver(project_code). Business export logic only receives
    resolved values and does not inspect the Runtime.
    """
    secret_name = _clean_str(name)
    resolved_project_code = _normalize_project_code(project_code)
    if not secret_name:
        raise RuntimeError("Secret name is empty.")
    if not resolved_project_code:
        raise RuntimeError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _clean_str(explicit_value):
        return SecretValue(_clean_str(explicit_value), "EXPLICIT_VALUE", "caller")

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
    raw = _clean_str(raw_value)
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


def _build_gspread_client_from_secret(
    secret: SecretValue,
) -> Tuple[gspread.Client, Dict[str, str]]:
    info, secret_format = _parse_service_account_text(secret.value)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds), {
        "source_type": secret.source_type,
        "source_detail": secret.source_detail,
        "secret_format": secret_format,
        "service_account_email": _clean_str(info.get("client_email")),
    }


def _load_account_config(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_account_id: str,
) -> AccountConfig:
    values = gc.open_by_url(console_core_url).worksheet(tab_cfg_account_id).get_all_values()
    if not values:
        raise ValueError(f"{tab_cfg_account_id} is empty.")

    config: Dict[str, str] = {}
    duplicates: List[str] = []
    for row_number, row in enumerate(values, start=1):
        key = _clean_str(row[0] if row else "").upper()
        value = _clean_str(row[1] if len(row) > 1 else "")
        if not key:
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


def _normalize_registry_header(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", _clean_str(value).lower()).strip()


def _extract_spreadsheet_id(value: Any) -> str:
    text = _clean_str(value)
    if not text:
        raise ValueError("Workspace Project Registry ID/URL is empty.")

    match = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", text)
    if match:
        return match.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]+", text):
        return text
    raise ValueError("Workspace Project Registry must be a Google Sheets ID or URL.")


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
    """Resolve exactly one active project from the Workspace Project Registry."""
    resolved_project_code = _normalize_project_code(project_code)
    if not resolved_project_code:
        raise ValueError("project_code is required.")
    registry_tab = _clean_str(workspace_registry_tab)
    if not registry_tab:
        raise ValueError("workspace_registry_tab is required.")

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
    registry_book = workspace_gc.open_by_key(registry_file_id)
    try:
        worksheet = registry_book.worksheet(registry_tab)
    except gspread.WorksheetNotFound as exc:
        raise ValueError(
            f"Workspace Project Registry tab {registry_tab!r} does not exist "
            f"in {registry_book.title!r}."
        ) from exc

    values = worksheet.get_all_values()
    if not values:
        raise ValueError(f"Workspace Project Registry tab {registry_tab!r} is empty.")

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
    notes_col = header_map.get(_normalize_registry_header("notes"))

    matches: List[Tuple[int, List[Any]]] = []
    width = len(values[0])
    for row_number, raw_row in enumerate(values[1:], start=2):
        row = list(raw_row) + [""] * max(0, width - len(raw_row))
        if _normalize_project_code(row[project_col]) == resolved_project_code:
            matches.append((row_number, row))
    if not matches:
        raise ValueError(
            "Workspace Project Registry has no row for "
            f"project_code={resolved_project_code}."
        )
    if len(matches) > 1:
        raise ValueError(
            "Workspace Project Registry has duplicate rows for "
            f"project_code={resolved_project_code}; "
            f"rows={[row_number for row_number, _ in matches]}."
        )

    source_row, row = matches[0]
    active_text = _clean_str(row[active_col]).lower()
    if active_text not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}, row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": _clean_str(row[project_name_col]) if project_name_col is not None else "",
        "console_core_url": _clean_str(row[console_url_col]),
        "gsheet_secret_name": _clean_str(row[gsheet_secret_col]),
        "account_config_tab": _clean_str(row[account_tab_col]),
        "timezone": _clean_str(row[timezone_col]),
        "notes": _clean_str(row[notes_col]) if notes_col is not None else "",
        "registry_id": registry_file_id,
        "registry_tab": registry_tab,
        "registry_source_row": str(source_row),
        "workspace_gsheet_secret_name": _clean_str(workspace_gsheet_secret_name),
        "workspace_auth_source_type": _clean_str(auth_meta.get("source_type")),
        "workspace_service_account_email": _clean_str(auth_meta.get("service_account_email")),
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
    """Resolve project route, account config, and the two required credentials."""
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
            f"registry={route['gsheet_secret_name']}; "
            f"cfg={account.gsheet_sa_b64_secret}"
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
            "workspace_secret_source_type": route["workspace_auth_source_type"],
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
    explicit_sa_value: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, Any]:
    """Check or update one existing registry row; never append a row."""
    mode = _clean_str(registry_mode).upper() or "OFF"
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

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _clean_str(current_colab_url):
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _clean_str(current_colab_name):
        raise ValueError("UPDATE_URL_AND_NAME requires current_colab_name.")

    sa_secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
    )
    gc, auth_meta = _build_gspread_client_from_secret(sa_secret)
    worksheet = gc.open_by_url(console_core_url).worksheet(registry_tab)
    values = worksheet.get_all_values()
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
        _clean_str(job_name).lower(),
        _clean_str(sheet_label).lower(),
        _clean_str(tab_name).lower(),
    )
    matches: List[int] = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(values[0]) - len(row))
        logical_key = (
            _clean_str(padded[job_col]).lower(),
            _clean_str(padded[label_col]).lower(),
            _clean_str(padded[tab_col]).lower(),
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
    provided_url = _clean_str(current_colab_url)
    provided_name = _clean_str(current_colab_name)
    if provided_url and _clean_str(current_row[url_col]) != provided_url:
        changes.append(("colab_url", url_col + 1, _clean_str(current_row[url_col]), provided_url))
    if provided_name and _clean_str(current_row[name_col]) != provided_name:
        changes.append(("colab_name", name_col + 1, _clean_str(current_row[name_col]), provided_name))

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


# =========================================================
# Google Sheets
# =========================================================

def build_gspread_client_from_b64(sa_b64: str) -> gspread.Client:
    """Backward-compatible entrypoint; accepts Base64 JSON or raw JSON."""
    info, _secret_format = _parse_service_account_text(sa_b64)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def open_ws(gc: gspread.Client, sheet_url: str, worksheet_title: str):
    sh = gc.open_by_url(sheet_url)
    return sh.worksheet(worksheet_title)


def ws_to_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    body = values[1:] if len(values) > 1 else []
    return pd.DataFrame(body, columns=header)


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


def ensure_ws(gc: gspread.Client, sheet_url: str, worksheet_title: str, rows: int = 2000, cols: int = 60):
    sh = gc.open_by_url(sheet_url)
    try:
        return sh.worksheet(worksheet_title)
    except gspread.WorksheetNotFound:
        # Worksheet creation is intentionally not retried: unlike writing to a
        # fixed range, create is not safely idempotent after an uncertain response.
        return sh.add_worksheet(title=worksheet_title, rows=rows, cols=cols)


def write_df(ws, df: pd.DataFrame, mode: str = "REPLACE"):
    """Write one dataframe with bounded retry while preserving payload semantics."""
    mode = _clean_str(mode).upper() or "REPLACE"
    if mode == "REPLACE":
        _with_sheets_retry(ws.clear, action=f"clear:{ws.title}")
        if df is None or df.empty:
            _with_sheets_retry(
                lambda: ws.update(range_name="A1", values=[[""]]),
                action=f"update_empty:{ws.title}",
            )
            return
        _with_sheets_retry(
            lambda: set_with_dataframe(
                ws,
                df,
                include_index=False,
                include_column_header=True,
                resize=True,
            ),
            action=f"set_with_dataframe:{ws.title}:replace",
        )
        return

    existing = ws.get_all_values()
    if not existing:
        _with_sheets_retry(
            lambda: set_with_dataframe(
                ws,
                df,
                include_index=False,
                include_column_header=True,
                resize=True,
            ),
            action=f"set_with_dataframe:{ws.title}:append_empty",
        )
        return

    start_row = len(existing) + 1
    _with_sheets_retry(
        lambda: set_with_dataframe(
            ws,
            df,
            row=start_row,
            col=1,
            include_index=False,
            include_column_header=False,
            resize=False,
        ),
        action=f"set_with_dataframe:{ws.title}:append",
    )


# =========================================================
# Shopify GraphQL
# =========================================================

@dataclass
class ShopifyClient:
    shop_domain: str
    api_version: str
    access_token: str
    timeout: int = 60
    min_sleep: float = 0.0

    def __post_init__(self):
        self.url = f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"
        self.sess = requests.Session()
        self.sess.headers.update({
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        })

    def gql(self, query: str, variables: Optional[dict] = None, max_retry: int = 5) -> dict:
        payload = {"query": query, "variables": variables or {}}

        for i in range(max_retry):
            resp = self.sess.post(self.url, json=payload, timeout=self.timeout)
            txt = resp.text

            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(2 ** i, 10)
                time.sleep(sleep_s)
                continue

            resp.raise_for_status()
            data = resp.json()

            if data.get("errors"):
                raise RuntimeError(f"Shopify GraphQL errors: {json.dumps(data['errors'], ensure_ascii=False)}")

            user_errors = []
            d = data.get("data") or {}
            if isinstance(d, dict):
                for v in d.values():
                    if isinstance(v, dict) and v.get("userErrors"):
                        user_errors.extend(v["userErrors"])
            if user_errors:
                raise RuntimeError(f"Shopify userErrors: {json.dumps(user_errors, ensure_ascii=False)}")

            ext = data.get("extensions", {}) or {}
            ts = (ext.get("cost", {}) or {}).get("throttleStatus", {}) or {}
            currently = ts.get("currentlyAvailable")
            restore = ts.get("restoreRate")
            if currently is not None and restore:
                if currently < 50:
                    time.sleep(max(0.2, (100 - currently) / max(restore, 1)))

            if self.min_sleep > 0:
                time.sleep(self.min_sleep)

            return d

        raise RuntimeError("GraphQL request failed after retries")


# =========================================================
# 配置读取
# =========================================================

def get_label_sheet_url(gc: gspread.Client, console_core_url: str, site_code: str, label: str) -> str:
    ws = open_ws(gc, console_core_url, "Cfg__Sites")
    df = ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()

    hit = df[
        (df["site_code"] == str(site_code).strip().upper()) &
        (df["label"] == str(label).strip())
    ]

    if hit.empty:
        raise ValueError(f"Cfg__Sites 中未找到 site_code={site_code}, label={label}")

    sheet_url = _clean_str(hit.iloc[0].get("sheet_url"))
    if not sheet_url:
        raise ValueError(f"Cfg__Sites 中 sheet_url 为空: site_code={site_code}, label={label}")

    return sheet_url


def load_cfg_fields(gc: gspread.Client, config_sheet_url: str, worksheet_title: str = "Cfg__Fields") -> pd.DataFrame:
    ws = open_ws(gc, config_sheet_url, worksheet_title)
    df = ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    need_cols = ["field_id", "entity_type", "field_key", "expr", "field_type", "data_type"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = ""

    return df.fillna("")


def load_export_tab_fields(gc: gspread.Client, config_sheet_url: str, worksheet_title: str = "Cfg__ExportTabFields") -> pd.DataFrame:
    ws = open_ws(gc, config_sheet_url, worksheet_title)
    df = ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__ExportTabFields is empty")

    need_cols = ["view_id", "field_id", "seq", "field_type", "entity_type", "field_key", "expr"]
    for c in need_cols:
        if c not in df.columns:
            raise ValueError(f"Cfg__ExportTabFields 缺少字段: {c}")

    return df.fillna("")


def build_field_def_map(cfg_fields_df: pd.DataFrame, cfg_export_df: Optional[pd.DataFrame] = None) -> Dict[str, Dict[str, Any]]:
    """
    FIELD_DEF 必须优先来自 Cfg__Fields。
    这样当某个依赖 field_id 不在当前 view 中时，仍然能补回它的 expr / entity_type / data_type。
    若 export 里存在同 field_id 的补充信息，则只做兜底合并，不覆盖 Cfg__Fields 主定义。
    """
    out: Dict[str, Dict[str, Any]] = {}

    for _, r in cfg_fields_df.fillna("").iterrows():
        fid = _clean_str(r.get("field_id"))
        if fid:
            out[fid] = dict(r)

    if cfg_export_df is not None and not cfg_export_df.empty:
        for _, r in cfg_export_df.fillna("").iterrows():
            fid = _clean_str(r.get("field_id"))
            if not fid:
                continue
            if fid not in out:
                out[fid] = dict(r)
            else:
                for k, v in dict(r).items():
                    if _is_blank(out[fid].get(k)) and not _is_blank(v):
                        out[fid][k] = v

    return out


def get_view_cfg(cfg_df: pd.DataFrame, view_id: str) -> pd.DataFrame:
    out = cfg_df[cfg_df["view_id"].astype(str).str.strip() == view_id].copy()
    if out.empty:
        raise ValueError(f"Cfg__ExportTabFields 中未找到 view_id={view_id}")
    out["__seq"] = out["seq"].apply(_to_int)
    out = out.sort_values("__seq").drop(columns=["__seq"])
    return out


# =========================================================
# CALC 依赖 / fetch_df
# =========================================================

_placeholder_re = re.compile(r"\{([^}]+)\}")
_mf_re = re.compile(r'MF_VALUE\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)')


def parse_mf_value_expr(expr: Any) -> Optional[Tuple[str, str]]:
    if not isinstance(expr, str):
        return None
    m = _mf_re.search(expr.strip())
    if not m:
        return None
    return m.group(1), m.group(2)


def expand_calc_dependencies(view_df: pd.DataFrame) -> set[str]:
    """
    找出当前 view 中所有 CALC expr 依赖到的 field_id。
    关键修复：
    - 递归展开时，不能只看当前 view；
    - 若占位符 field_id 只存在于 Cfg__Fields，也必须继续向下追。
    """
    deps = set()
    changed = True

    view_rows = {_clean_str(r.get("field_id")): dict(r) for _, r in view_df.iterrows()}

    def get_row(fid: str) -> Dict[str, Any]:
        return view_rows.get(fid) or FIELD_DEF.get(fid) or {}

    while changed:
        changed = False
        scan_rows: List[Dict[str, Any]] = [dict(r) for _, r in view_df.iterrows()]

        for dep in list(deps):
            dep_row = get_row(dep)
            if dep_row:
                scan_rows.append(dep_row)

        for r in scan_rows:
            ft = _clean_str(r.get("field_type")).upper()
            ex = _clean_str(r.get("expr"))
            if ft != "CALC" or not ex:
                continue

            refs = set(_placeholder_re.findall(ex))
            for fid in refs:
                if fid not in deps:
                    deps.add(fid)
                    changed = True

    return deps


def make_fetch_df(view_df: pd.DataFrame, deps: set[str]) -> pd.DataFrame:
    """
    Build the field plan used by both GraphQL fetching and row calculation.

    Important rule:
    - Keep ALL fields in the current view, including CALC rows.
      CALC rows do not generate GraphQL selections, but build_plan() needs them
      so node_to_row() can calculate them and build_export_df_filtered() can output them.
    - Also add dependency field_ids used by CALC expressions.
      If a dependency is not in the current view, read its definition from FIELD_DEF.

    This fixes cases like selectedOptions:
    - view contains CALC fields such as Option1 Name / Option1 Value
    - CALC depends on VARIANT|raw.variant.selected_options
    - final IDX__Variants must include both calculated option columns and fetched dependency data
    """
    rows: List[Dict[str, Any]] = []
    fid_map_view = {_clean_str(r.get("field_id")): dict(r) for _, r in view_df.iterrows()}
    seen = set()

    # Keep every field in the output view, including CALC.
    for _, r in view_df.iterrows():
        fid = _clean_str(r.get("field_id"))
        if not fid or fid in seen:
            continue
        rows.append(dict(r))
        seen.add(fid)

    # Add fields needed only as CALC dependencies.
    for dep in deps:
        if dep in seen:
            continue

        dep_row = fid_map_view.get(dep)
        if dep_row is None:
            dep_row = dict(FIELD_DEF.get(dep) or {})

        if not dep_row:
            print(f"⚠️ CALC dependency not found in Cfg__Fields / view, skipped: {dep}")
            continue

        dep_row.setdefault("view_id", "__FETCH_DEPS__")
        dep_row.setdefault("join key", "")
        dep_row.setdefault("join_key", "")
        dep_row.setdefault("seq", "999999")
        dep_row.setdefault("alias", "")
        dep_row.setdefault("required", "")
        dep_row.setdefault("notes", "auto-added dep for CALC / join")
        dep_row["field_id"] = dep
        dep_row["field_type"] = _clean_str(dep_row.get("field_type")).upper() or "RAW"

        rows.append(dep_row)
        seen.add(dep)

    out = pd.DataFrame(rows).fillna("")
    if out.empty:
        raise ValueError("make_fetch_df 结果为空")
    out["__seq"] = out["seq"].apply(_to_int)
    return out.sort_values("__seq").drop(columns=["__seq"])


# =========================================================
# expr 路径处理
# =========================================================

def strip_entity_prefix(expr: str, entity_type: str) -> str:
    """
    统一把 expr 转成 GraphQL 节点下路径。

    特殊规则：
    - PRODUCT product.description 改拉 descriptionHtml。
      后续在 node_to_row 里转成人类可读 text。
      原因：Shopify product.description 会丢掉 HTML 结构，容易把段落粘在一起。
    - VARIANT weight / weightUnit 兼容旧版写法。
    """
    pref = {
        "PRODUCT": "product.",
        "VARIANT": "variant.",
    }.get(entity_type, "")

    s = _clean_str(expr)
    if pref and s.startswith(pref):
        s = s[len(pref):]

    if entity_type == "PRODUCT":
        k = s.strip()
        if k == "description":
            return "descriptionHtml"

    # ---- 兼容旧版 weight 写法（与原 ipynb 一致：先去前缀，再 remap）----
    if entity_type == "VARIANT":
        k = s.strip()
        if k == "weight":
            return "inventoryItem.measurement.weight.value"
        if k == "weightUnit":
            return "inventoryItem.measurement.weight.unit"

    return s

_index_part_re = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\[(\d+)\]$")
_all_nodes_part_re = re.compile(r"^nodes\[\]$")


def build_nested_fields(parts: List[str]) -> str:
    if not parts:
        return "id"
    inner = parts[-1]
    for p in reversed(parts[:-1]):
        inner = f"{p} {{ {inner} }}"
    return inner


def build_selected_options_selection(alias: str) -> str:
    return f"""
{alias}: selectedOptions {{
  name
  value
}}
""".strip()


def extract_leaf(val: Any, tail: List[TailStep]) -> Any:
    """
    Extract nested values from GraphQL alias result.

    Supports:
    - normal object path: ["product", "id"]
    - indexed list path: ["nodes", 0, "preview", "image", "url"]
    - full list path: ["nodes", ALL_LIST_STEP, "preview", "image", "url"]
    """
    cur = val

    i = 0
    while i < len(tail):
        step = tail[i]

        if step == ALL_LIST_STEP:
            if not isinstance(cur, list):
                return []
            rest = tail[i + 1:]
            out = []
            for item in cur:
                v = extract_leaf(item, rest)
                if v not in ("", None, [], {}):
                    out.append(v)
            return out

        if isinstance(step, int):
            if not isinstance(cur, list) or step < 0 or step >= len(cur):
                return ""
            cur = cur[step]
        else:
            if not isinstance(cur, dict):
                return ""
            cur = cur.get(step, "")

        i += 1

    return "" if cur is None else cur


# =========================================================
# CALC evaluator
# =========================================================

_coalesce_re = re.compile(r"^COALESCE\((.*)\)\s*$", re.IGNORECASE)
_json_re = re.compile(r"^JSON\(\s*(\{[^}]+\})\s*\)\s*$", re.IGNORECASE)
_get_re = re.compile(r"^GET\(\s*(\{[^}]+\})\s*,\s*(\d+)\s*\)\.(name|value)\s*$", re.IGNORECASE)


def eval_calc(expr: str, row: Dict[str, Any]) -> Any:
    s = _clean_str(expr)
    if not s:
        return ""

    # Compatible with Google Sheets-style config expressions:
    # allow both GET(...) / JSON(...) and =GET(...) / =JSON(...).
    if s.startswith("="):
        s = s[1:].strip()

    m = _json_re.match(s)
    if m:
        fid = m.group(1)[1:-1].strip()
        val = row.get(fid, "")
        try:
            return json.dumps(val, ensure_ascii=False)
        except Exception:
            return json.dumps(str(val), ensure_ascii=False)

    m = _get_re.match(s)
    if m:
        fid = m.group(1)[1:-1].strip()
        idx = int(m.group(2))
        attr = m.group(3).lower()
        arr = row.get(fid, [])
        if not isinstance(arr, list) or idx <= 0 or idx > len(arr):
            return ""
        item = arr[idx - 1] or {}
        return item.get(attr, "")

    m = _coalesce_re.match(s)
    if m:
        inside = m.group(1)
        parts = [p.strip() for p in inside.split(",") if p.strip()]
        for p in parts:
            if p.startswith("{") and p.endswith("}"):
                fid = p[1:-1].strip()
                v = row.get(fid, "")
                if v not in ("", None):
                    return v
            else:
                if p not in ("", None):
                    return p
        return ""

    return ""


def _try_parse_json_list(s: str) -> Optional[list]:
    ss = _clean_str(s)
    if not (ss.startswith("[") and ss.endswith("]")):
        return None
    try:
        v = json.loads(ss)
        return v if isinstance(v, list) else None
    except Exception:
        return None


def tags_to_human(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        parts = [str(x).strip() for x in v if str(x).strip() not in ("", "None", "nan")]
        return ", ".join(parts)
    if isinstance(v, str):
        parsed = _try_parse_json_list(v)
        if parsed is not None:
            parts = [str(x).strip() for x in parsed if str(x).strip() not in ("", "None", "nan")]
            return ", ".join(parts)
        return v.strip()
    return str(v).strip()


def list_to_pipe(v: Any, sep: str = " | ") -> str:
    if v is None:
        return ""

    if isinstance(v, list):
        parts = [str(x).strip() for x in v if str(x).strip() not in ("", "None", "nan")]
        return sep.join(parts)

    if isinstance(v, str):
        parsed = _try_parse_json_list(v)
        if parsed is not None:
            parts = [str(x).strip() for x in parsed if str(x).strip() not in ("", "None", "nan")]
            return sep.join(parts)
        return v.strip()

    return str(v).strip()



def html_to_readable_text(html_text: Any) -> str:
    """
    把 Shopify descriptionHtml 转成人类可读文本：
    - <br>, </p>, </div>, </li>, </tr>, 标题标签 -> 换行
    - 去掉剩余 HTML 标签
    - 反解码 &amp; / &quot; / &#39; 等实体
    - 压缩多余空格，但保留段落换行
    """
    s = "" if html_text is None else str(html_text)
    if not s.strip():
        return ""

    # 去掉 script/style
    s = re.sub(r"(?is)<\s*(script|style)[^>]*>.*?</\s*\1\s*>", "", s)

    # 先把明显的结构标签转成换行
    s = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", s)
    s = re.sub(r"(?i)</\s*(p|div|section|article|li|tr|h1|h2|h3|h4|h5|h6)\s*>", "\n", s)
    s = re.sub(r"(?i)<\s*li[^>]*>", "• ", s)

    # 表格单元格之间给一点间隔
    s = re.sub(r"(?i)</\s*(td|th)\s*>", " ", s)

    # 块级起始标签也给轻微断开，避免前文直接粘到新块
    s = re.sub(r"(?i)<\s*(p|div|section|article|tr|h1|h2|h3|h4|h5|h6)[^>]*>", "\n", s)

    # 去掉剩余标签
    s = re.sub(r"(?s)<[^>]+>", "", s)

    # HTML entity 还原
    s = html.unescape(s)

    # 统一换行
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # 每一行内部压缩空格
    lines = []
    for line in s.split("\n"):
        line = re.sub(r"[ \t\u00a0]+", " ", line).strip()
        if line:
            lines.append(line)

    return "\n".join(lines).strip()


def normalize_sheet_cell(v: Any) -> Any:
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return v


# =========================================================
# build_plan
# =========================================================

def build_plan(fetch_df: pd.DataFrame, entity_type: str) -> Dict[str, Any]:
    gql_lines: List[str] = []
    alias_map: Dict[str, str] = {}
    leaf_tail: Dict[str, List[TailStep]] = {}
    raw_rows: List[Tuple[str, str]] = []
    calc_rows: List[Tuple[str, str]] = []
    join_fids = set()

    def find_index_seg(parts: List[str]):
        for i, seg in enumerate(parts):
            m = _index_part_re.match(seg)
            if m:
                return i, m.group(1), int(m.group(2))
        return None

    def find_all_nodes_seg(parts: List[str]):
        for i, seg in enumerate(parts):
            if _all_nodes_part_re.match(seg):
                return i
        return None

    for _, r in fetch_df.iterrows():
        ft = _clean_str(r.get("field_type")).upper()
        fid = _clean_str(r.get("field_id"))
        ex = _clean_str(r.get("expr"))
        jk = _clean_str(r.get("join key"))

        if not fid:
            continue

        if jk:
            join_fids.add(fid)

        a = _gql_safe_alias(fid)
        alias_map[fid] = a

        if ft == "CALC":
            calc_rows.append((fid, ex))
            continue

        mf = parse_mf_value_expr(ex)
        if mf:
            ns, key = mf
            gql_lines.append(f'{a}: metafield(namespace: "{ns}", key: "{key}") {{ value }}')
            raw_rows.append((fid, ex))
            continue

        if entity_type == "VARIANT" and ex.endswith("selectedOptions"):
            gql_lines.append(build_selected_options_selection(a))
            raw_rows.append((fid, ex))
            continue

        path = strip_entity_prefix(ex, entity_type)
        parts = [p for p in path.split(".") if p]
        if not parts:
            continue

        # =====================================================
        # Full connection list:
        # - media.nodes[].preview.image.url
        # - product.media.nodes[].preview.image.url
        # =====================================================
        all_hit = find_all_nodes_seg(parts)
        if all_hit is not None:
            k = all_hit
            if k <= 0:
                raise ValueError(f"Invalid nodes[] path for field_id={fid}: {ex}")

            conn = parts[k - 1]
            after = parts[k + 1:]
            node_fields = build_nested_fields(after)
            first_n = DEFAULT_CONNECTION_LIST_FIRST
            conn_sel = f"{conn}(first:{first_n}) {{ nodes {{ {node_fields} }} }}"

            # Case A:
            # PRODUCT expr after strip:
            # media.nodes[].preview.image.url
            if k - 1 == 0:
                gql_lines.append(f"{a}: {conn_sel}")
                leaf_tail[fid] = ["nodes", ALL_LIST_STEP] + after
                raw_rows.append((fid, ex))
                continue

            # Case B:
            # VARIANT expr after strip:
            # product.media.nodes[].preview.image.url
            head = parts[0]
            mid = parts[1:k - 1]

            inner = conn_sel
            for p in reversed(mid):
                inner = f"{p} {{ {inner} }}"

            gql_lines.append(f"{a}: {head} {{ {inner} }}")
            leaf_tail[fid] = mid + [conn, "nodes", ALL_LIST_STEP] + after
            raw_rows.append((fid, ex))
            continue

        # =====================================================
        # Existing indexed connection logic:
        # - media[0].preview.image.url
        # - media.nodes[0].preview.image.url
        # =====================================================
        idx_hit = find_index_seg(parts)
        if idx_hit:
            k, name, idx = idx_hit
            after = parts[k + 1:]
            node_fields = build_nested_fields(after)
            first_n = idx + 1

            # case A: nodes[0]
            if name.lower() == "nodes" and k > 0:
                conn = parts[k - 1]
                head = parts[0]
                mid = parts[1:k - 1]
                conn_sel = f"{conn}(first:{first_n}) {{ nodes {{ {node_fields} }} }}"

                if k - 1 == 0:
                    gql_lines.append(f"{a}: {conn}(first:{first_n}) {{ nodes {{ {node_fields} }} }}")
                    leaf_tail[fid] = ["nodes", idx] + after
                else:
                    inner = conn_sel
                    for p in reversed(mid):
                        inner = f"{p} {{ {inner} }}"
                    gql_lines.append(f"{a}: {head} {{ {inner} }}")
                    leaf_tail[fid] = mid + [conn, "nodes", idx] + after

                raw_rows.append((fid, ex))
                continue

            # case B: xxx[0]
            before = parts[:k]
            if before and before[-1] == name:
                before = before[:-1]

            conn_sel = f"{name}(first:{first_n}) {{ nodes {{ {node_fields} }} }}"

            if not before:
                gql_lines.append(f"{a}: {conn_sel}")
                leaf_tail[fid] = ["nodes", idx] + after
            else:
                head = before[0]
                mid = before[1:]
                inner = conn_sel
                for p in reversed(mid):
                    inner = f"{p} {{ {inner} }}"
                gql_lines.append(f"{a}: {head} {{ {inner} }}")
                leaf_tail[fid] = mid + [name, "nodes", idx] + after

            raw_rows.append((fid, ex))
            continue

        if len(parts) == 1:
            gql_lines.append(f"{a}: {parts[0]}")
        else:
            head = parts[0]
            tail = build_nested_fields(parts[1:])
            gql_lines.append(f"{a}: {head} {{ {tail} }}")
            leaf_tail[fid] = parts[1:]

        raw_rows.append((fid, ex))

    return {
        "gql_selections": "\n".join(gql_lines),
        "alias_map": alias_map,
        "leaf_tail": leaf_tail,
        "raw_rows": raw_rows,
        "calc_rows": calc_rows,
        "join_fids": join_fids,
    }


# =========================================================
# fetchers
# =========================================================

def fetch_all_products(client: ShopifyClient, page_size: int, plan: Dict[str, Any], debug_every: int = 5) -> List[dict]:
    sel = plan["gql_selections"]
    q = f"""
query Products($first:Int!, $after:String) {{
  products(first:$first, after:$after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      {sel}
    }}
  }}
}}
""".strip()

    out: List[dict] = []
    after = None
    page = 0
    while True:
        page += 1
        data = client.gql(q, {"first": page_size, "after": after})
        box = data["products"]
        out.extend(box.get("nodes") or [])
        if page % max(1, debug_every) == 0:
            print(f"Products pages={page} rows={len(out)}")
        if not box["pageInfo"]["hasNextPage"]:
            break
        after = box["pageInfo"]["endCursor"]
    return out


def fetch_all_variants(client: ShopifyClient, page_size: int, plan: Dict[str, Any], debug_every: int = 5) -> List[dict]:
    sel = plan["gql_selections"]
    q = f"""
query Variants($first:Int!, $after:String) {{
  productVariants(first:$first, after:$after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      {sel}
    }}
  }}
}}
""".strip()

    out: List[dict] = []
    after = None
    page = 0
    while True:
        page += 1
        data = client.gql(q, {"first": page_size, "after": after})
        box = data["productVariants"]
        out.extend(box.get("nodes") or [])
        if page % max(1, debug_every) == 0:
            print(f"Variants pages={page} rows={len(out)}")
        if not box["pageInfo"]["hasNextPage"]:
            break
        after = box["pageInfo"]["endCursor"]
    return out


# =========================================================
# row / export df
# =========================================================

def node_to_row(node: dict, plan: Dict[str, Any]) -> Dict[str, Any]:
    alias_map = plan["alias_map"]
    leaf_tail = plan["leaf_tail"]
    join_fids = plan["join_fids"]

    row: Dict[str, Any] = {}

    for fid, _ex in plan["raw_rows"]:
        a = alias_map[fid]
        val = node.get(a, "")

        if isinstance(val, dict) and "value" in val:
            row[fid] = val.get("value") or ""
        elif fid in leaf_tail:
            row[fid] = extract_leaf(val, leaf_tail[fid])
        else:
            row[fid] = val

        if fid in join_fids and isinstance(row[fid], (dict, list)):
            row[fid] = ""

    for fid, ex in plan["calc_rows"]:
        row[fid] = eval_calc(ex, row)

    for fid in list(row.keys()):
        fdef = FIELD_DEF.get(str(fid), {}) or {}
        fk = _clean_str(fdef.get("field_key", ""))
        dt = _clean_str(fdef.get("data_type", "")).lower()

        if fk == "core.tags":
            row[fid] = tags_to_human(row.get(fid, ""))

        # Product Description (text)
        # 配置里仍然可以是 product.description / core.description，
        # 但 strip_entity_prefix 已经改成实际拉 descriptionHtml；
        # 这里把 HTML 转成带换行的可读文本。
        if fk == "core.description":
            row[fid] = html_to_readable_text(row.get(fid, ""))

        # Human-readable image URL list:
        # - PRODUCT|core.product.images_urls
        # - VARIANT|core.variant.product_images_urls
        if fk.endswith("images_urls"):
            row[fid] = list_to_pipe(row.get(fid, ""))

        # JSON image URL list:
        # - PRODUCT|core.product.images_json
        # - VARIANT|core.variant.product_images_json
        # Keep as list here. normalize_sheet_cell() will serialize list -> JSON.
        if fk.endswith("images_json"):
            v = row.get(fid, "")
            if isinstance(v, str):
                parsed = _try_parse_json_list(v)
                row[fid] = parsed if parsed is not None else ([v] if v else [])
            elif v is None:
                row[fid] = []

    for k in list(row.keys()):
        row[k] = normalize_sheet_cell(row[k])

    return row


def find_synced_at_fids(view_df: pd.DataFrame) -> List[str]:
    out = []
    for _, r in view_df.iterrows():
        if _clean_str(r.get("field_key")) == "core.synced_at":
            fid = _clean_str(r.get("field_id"))
            if fid:
                out.append(fid)
    return out


def append_internal_status_fetch(fetch_df: pd.DataFrame, entity_type: str) -> Tuple[pd.DataFrame, str]:
    """
    确保 fetch_df 一定包含内部状态字段，用于过滤 archived
    """
    out = fetch_df.copy()

    if entity_type == "PRODUCT":
        internal_fid = "__FILTER_PRODUCT_STATUS__"
        internal_expr = "product.status"
    else:
        internal_fid = "__FILTER_PARENT_PRODUCT_STATUS__"
        internal_expr = "variant.product.status"

    for _, r in out.iterrows():
        fid = _clean_str(r.get("field_id"))
        ex = _clean_str(r.get("expr"))
        ft = _clean_str(r.get("field_type")).upper()
        if ft == "CALC":
            continue
        if ex == internal_expr:
            return out, fid

    add_row = pd.DataFrame([{
        "view_id": "__INTERNAL_FILTER__",
        "field_id": internal_fid,
        "join key": "",
        "seq": "999998",
        "field_type": "RAW",
        "entity_type": entity_type,
        "field_key": "__internal.status__",
        "expr": internal_expr,
        "alias": "",
        "data_type": "single_line_text_field",
        "required": "",
        "notes": "internal use: filter archived",
    }])

    out = pd.concat([out, add_row], ignore_index=True)
    out["__seq"] = out["seq"].apply(_to_int)
    out = out.sort_values("__seq").drop(columns=["__seq"])
    return out, internal_fid


def build_export_df_filtered(
    entity_type: str,
    view_df: pd.DataFrame,
    fetch_df: pd.DataFrame,
    nodes: List[dict],
    status_fid: str,
    exclude_archived: bool = True,
    only_first_row_synced_at: bool = True,
) -> Tuple[pd.DataFrame, int]:
    plan = build_plan(fetch_df, entity_type)
    rows = [node_to_row(n, plan) for n in nodes]
    df_all = pd.DataFrame(rows)

    removed_cnt = 0
    if exclude_archived:
        if status_fid in df_all.columns:
            status_ser = df_all[status_fid].astype(str).str.strip().str.upper()
            keep_mask = status_ser.ne("ARCHIVED")
            removed_cnt = int((~keep_mask).sum())
            df_all = df_all[keep_mask].copy()
        else:
            print(f"⚠️ status_fid 不在导出结果中，未执行 archived 过滤: {status_fid}")

    synced_fids = find_synced_at_fids(view_df)
    if synced_fids:
        ts = _now_iso_utc()
        for fid in synced_fids:
            if fid in df_all.columns:
                if only_first_row_synced_at:
                    df_all[fid] = ""
                    if len(df_all) > 0:
                        df_all.loc[df_all.index[0], fid] = ts
                else:
                    df_all[fid] = ts

    tmp = view_df.copy()
    tmp["__seq"] = tmp["seq"].apply(_to_int)
    tmp = tmp.sort_values("__seq").drop(columns=["__seq"])

    export_fids = [_clean_str(x) for x in tmp["field_id"].tolist() if _clean_str(x)]
    export_fids = [c for c in export_fids if c in df_all.columns]

    df_out = df_all[export_fids].copy() if export_fids else pd.DataFrame()
    if not df_out.empty:
        df_out.columns = export_fids

    return df_out, removed_cnt


# =========================================================
# 主入口
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    shop_domain: str,
    api_version: str,
    gsheet_sa_b64: str,
    shopify_access_token: str,
    cfg_export_tab_fields_ws: str = "Cfg__ExportTabFields",
    out_tab_idx_products: str = "IDX__Products",
    out_tab_idx_variants: str = "IDX__Variants",
    export_idx_products: bool = True,
    export_idx_variants: bool = True,
    write_mode: str = "REPLACE",
    write_mode_products: str = "",
    write_mode_variants: str = "",
    page_size: int = 100,
    debug_every: int = 5,
    exclude_archived: bool = True,
    only_first_row_synced_at: bool = True,
) -> Dict[str, Any]:
    """
    返回：
    {
      "site_code": ...,
      "config_sheet_url": ...,
      "out_sheet_url": ...,
      "products_rows": ...,
      "variants_rows": ...,
      "products_filtered_archived": ...,
      "variants_filtered_archived": ...,
    }
    """
    global FIELD_DEF

    site_code = _clean_str(site_code).upper()

    gc = build_gspread_client_from_b64(gsheet_sa_b64)
    client = ShopifyClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=shopify_access_token,
        timeout=60,
        min_sleep=0.0,
    )

    config_sheet_url = get_label_sheet_url(gc, console_core_url, site_code, "config")
    out_sheet_url = get_label_sheet_url(gc, console_core_url, site_code, "export_product")

    cfg_fields_df = load_cfg_fields(gc, config_sheet_url, worksheet_title="Cfg__Fields")
    cfg_df = load_export_tab_fields(gc, config_sheet_url, worksheet_title=cfg_export_tab_fields_ws)
    FIELD_DEF = build_field_def_map(cfg_fields_df, cfg_df)

    def get_effective_mode(default_mode: str, override: str) -> str:
        m = _clean_str(override).upper()
        if m in ("REPLACE", "APPEND"):
            return m
        return _clean_str(default_mode).upper() or "REPLACE"

    result = {
        "site_code": site_code,
        "config_sheet_url": config_sheet_url,
        "out_sheet_url": out_sheet_url,
        "products_rows": 0,
        "variants_rows": 0,
        "products_filtered_archived": 0,
        "variants_filtered_archived": 0,
    }

    # ---- Products ----
    if export_idx_products:
        view_p = get_view_cfg(cfg_df, "IDX__PRODUCTS")
        deps_p = expand_calc_dependencies(view_p)
        fetch_p = make_fetch_df(view_p, deps_p)
        fetch_p, status_fid_p = append_internal_status_fetch(fetch_p, "PRODUCT")

        print("CALC deps (products):", sorted(list(deps_p))[:30], "..." if len(deps_p) > 30 else "")
        plan_p = build_plan(fetch_p, "PRODUCT")
        nodes_p = fetch_all_products(client, page_size, plan_p, debug_every=debug_every)

        df_p, removed_p = build_export_df_filtered(
            "PRODUCT",
            view_p,
            fetch_p,
            nodes_p,
            status_fid_p,
            exclude_archived=exclude_archived,
            only_first_row_synced_at=only_first_row_synced_at,
        )

        mode_p = get_effective_mode(write_mode, write_mode_products)
        ws_p = ensure_ws(gc, out_sheet_url, out_tab_idx_products)
        write_df(ws_p, df_p, mode_p)

        result["products_rows"] = len(df_p)
        result["products_filtered_archived"] = removed_p
        print(f"✅ IDX__Products exported: rows={len(df_p)} cols={len(df_p.columns)} mode={mode_p} filtered_archived={removed_p}")

    # ---- Variants ----
    if export_idx_variants:
        view_v = get_view_cfg(cfg_df, "IDX__VARIANTS")
        deps_v = expand_calc_dependencies(view_v)
        fetch_v = make_fetch_df(view_v, deps_v)
        fetch_v, status_fid_v = append_internal_status_fetch(fetch_v, "VARIANT")

        print("CALC deps (variants):", sorted(list(deps_v))[:30], "..." if len(deps_v) > 30 else "")
        plan_v = build_plan(fetch_v, "VARIANT")
        nodes_v = fetch_all_variants(client, page_size, plan_v, debug_every=debug_every)

        df_v, removed_v = build_export_df_filtered(
            "VARIANT",
            view_v,
            fetch_v,
            nodes_v,
            status_fid_v,
            exclude_archived=exclude_archived,
            only_first_row_synced_at=only_first_row_synced_at,
        )

        mode_v = get_effective_mode(write_mode, write_mode_variants)
        ws_v = ensure_ws(gc, out_sheet_url, out_tab_idx_variants)
        write_df(ws_v, df_v, mode_v)

        result["variants_rows"] = len(df_v)
        result["variants_filtered_archived"] = removed_v
        print(f"✅ IDX__Variants exported: rows={len(df_v)} cols={len(df_v.columns)} mode={mode_v} filtered_archived={removed_v}")

    print(f"✅ export_idx_tables done. patch_version={EXPORT_IDX_TABLES_PATCH_VERSION}")
    return result
