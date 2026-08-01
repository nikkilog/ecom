# -*- coding: utf-8 -*-
"""
shopify_export/3_1_2_export_product_values_long.py

Purpose:
- Build DL__ValuesLong from configured Product / Variant / Metaobject dependencies.
- Keep business selection, filtering, extraction, dedupe, schema, and write semantics unchanged.
- Runtime/Auth/Registry helpers support the verified Local + Colab Console Core boundary.
"""

from __future__ import annotations

import base64
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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

DL_HEADERS = [
    "owner_entity_type",
    "owner_gid",
    "owner_legacy_id",
    "field_key",
    "value",
    "value_type",
]

VALID_ENTITY_PREFIXES = {"PRODUCT", "VARIANT", "COLLECTION", "PAGE", "METAOBJECT_ENTRY"}
LONG_PREFIXES = ("mf.", "v_mf.", "mo.")
MO_EXPR_RE = re.compile(r'^MO_REF\(\s*"([^"]+)"\s*\)\s*$', re.I)
PH_RE = re.compile(r"\{([^}]+)\}")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MODULE_PATH = "shopify_export.3_1_2_export_product_values_long"
MODULE_VERSION = "2026-08-01-runtime-boundary-v1"
DEFAULT_JOB_NAME = "export_product_values_long"


def _clean_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


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



@dataclass
class ShopifyClient:
    shop_domain: str
    api_version: str
    access_token: str
    timeout: int = 60
    max_retries: int = 5
    backoff_factor: float = 1.2

    def __post_init__(self):
        self.url = f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"
        self.session = requests.Session()
        retry = Retry(
            total=self.max_retries,
            connect=self.max_retries,
            read=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
            }
        )

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        resp = self.session.post(self.url, json=payload, timeout=self.timeout)
        try:
            data = resp.json()
        except Exception:
            raise RuntimeError(f"Shopify GraphQL non-json response: status={resp.status_code}")

        if resp.status_code >= 400:
            raise RuntimeError(f"Shopify GraphQL HTTP {resp.status_code}: {data}")

        if data.get("errors"):
            raise RuntimeError(f"Shopify GraphQL errors: {data['errors']}")

        out = data.get("data") or {}
        ext = data.get("extensions") or {}
        throttle = ((ext.get("cost") or {}).get("throttleStatus") or {})
        avail = throttle.get("currentlyAvailable")
        restore = throttle.get("restoreRate")
        if isinstance(avail, (int, float)) and isinstance(restore, (int, float)):
            if avail < 100:
                sleep_s = max(0.8, min(3.0, (100 - avail) / max(restore, 1)))
                time.sleep(sleep_s)
        return out


def build_gspread_client_from_b64(sa_b64: str) -> gspread.Client:
    """Backward-compatible entrypoint; accepts Base64 JSON or raw JSON."""
    info, _secret_format = _parse_service_account_text(sa_b64)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def now_cn_str() -> str:
    return pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")


def normalize_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def is_true(x: Any) -> bool:
    s = normalize_str(x).upper()
    return s in {"TRUE", "1", "YES", "Y", "T"}


def gql_safe_alias(s: str) -> str:
    a = re.sub(r"[^0-9A-Za-z_]", "_", str(s))
    if re.match(r"^\d", a):
        a = "f_" + a
    return a


def chunk_list(xs: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [xs]
    return [xs[i:i + size] for i in range(0, len(xs), size)]


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


def ensure_ws(gc: gspread.Client, sheet_url: str, tab_name: str):
    sh = gc.open_by_url(sheet_url)
    try:
        return sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab_name, rows=2000, cols=30)


def write_df_replace(ws, df: pd.DataFrame):
    """Replace the target Tab with bounded Sheets retry; payload semantics unchanged."""
    row_count = 0 if df is None else len(df)
    col_count = 0 if df is None else len(df.columns)
    print(f"[Sheets] write start | tab={ws.title} | rows={row_count} | cols={col_count}")
    started = time.time()

    _with_sheets_retry(ws.clear, action=f"clear:{ws.title}")
    if df is None or df.empty:
        _with_sheets_retry(
            lambda: ws.update(range_name="A1", values=[DL_HEADERS]),
            action=f"update_header:{ws.title}",
        )
        print(f"[Sheets] write done | tab={ws.title} | rows=0 | elapsed={time.time() - started:.1f}s")
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
    print(
        f"[Sheets] write done | tab={ws.title} | rows={row_count} | "
        f"elapsed={time.time() - started:.1f}s"
    )


def append_runlog_rows(gc: gspread.Client, runlog_url: str, runlog_tab: str, rows: List[List[Any]]):
    if not runlog_url or not runlog_tab:
        return
    print(f"[Sheets] runlog start | tab={runlog_tab} | rows={len(rows)}")
    started = time.time()
    ws = ensure_ws(gc, runlog_url, runlog_tab)
    existing = ws.get_all_values()
    if not existing:
        _with_sheets_retry(
            lambda: ws.update(range_name="A1", values=[RUNLOG_HEADERS]),
            action=f"runlog.update_header:{runlog_tab}",
        )
    elif existing[0] != RUNLOG_HEADERS:
        # Preserve Current behavior: reset the RunLog when the existing header differs.
        _with_sheets_retry(ws.clear, action=f"runlog.clear_header_mismatch:{runlog_tab}")
        _with_sheets_retry(
            lambda: ws.update(range_name="A1", values=[RUNLOG_HEADERS]),
            action=f"runlog.reset_header:{runlog_tab}",
        )
    if rows:
        _with_sheets_retry(
            lambda: ws.append_rows(rows, value_input_option="USER_ENTERED"),
            action=f"runlog.append_rows:{runlog_tab}",
        )
    print(
        f"[Sheets] runlog done | tab={runlog_tab} | rows={len(rows)} | "
        f"elapsed={time.time() - started:.1f}s"
    )


def open_ws(gc: gspread.Client, url: str, tab: str):
    sh = gc.open_by_url(url)
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"Worksheet not found: {tab}")


def read_table(gc: gspread.Client, url: str, tab: str) -> pd.DataFrame:
    print(f"[Read] table start | tab={tab}")
    started = time.time()
    ws = open_ws(gc, url, tab)
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        print(f"[Read] table done | tab={tab} | rows=0 | elapsed={time.time() - started:.1f}s")
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=header).replace({"": None})
    print(f"[Read] table done | tab={tab} | rows={len(df)} | elapsed={time.time() - started:.1f}s")
    return df


def _normalize_header(xs):
    return [normalize_str(x) for x in xs]


def _strip_entity_prefix(col: str) -> str:
    col = normalize_str(col)
    if "|" in col:
        pfx, rest = col.split("|", 1)
        if normalize_str(pfx).upper() in VALID_ENTITY_PREFIXES:
            return normalize_str(rest)
    return col


def _find_header_row(rows, required_any=("core.gid", "core.legacy_id"), max_scan=80):
    req = set(required_any)
    for i in range(min(len(rows), max_scan)):
        raw = _normalize_header(rows[i])
        stripped = [_strip_entity_prefix(c) for c in raw]
        if req.intersection(set(raw)) or req.intersection(set(stripped)):
            return i
    return 0


def _build_gid(owner_type: str, legacy_id: str) -> str:
    legacy_id = normalize_str(legacy_id)
    if not legacy_id:
        return ""
    ot = owner_type.upper()
    if ot == "PRODUCT":
        return f"gid://shopify/Product/{legacy_id}"
    if ot == "VARIANT":
        return f"gid://shopify/ProductVariant/{legacy_id}"
    if ot == "COLLECTION":
        return f"gid://shopify/Collection/{legacy_id}"
    if ot == "PAGE":
        return f"gid://shopify/Page/{legacy_id}"
    return legacy_id


def read_idx_df(gc: gspread.Client, sheet_url: str, tab: str, owner_type: str) -> pd.DataFrame:
    print(f"[Read] idx start | tab={tab} | owner={owner_type}")
    started = time.time()
    ws = open_ws(gc, sheet_url, tab)
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        raise RuntimeError(f"IDX tab empty: {tab}")

    header_i = _find_header_row(rows)
    header_raw = _normalize_header(rows[header_i])
    data = rows[header_i + 1:]
    df = pd.DataFrame(data, columns=header_raw).replace({"": None})

    rename_map = {}
    for c in df.columns:
        c2 = _strip_entity_prefix(c)
        if c2 != c:
            rename_map[c] = c2
    if rename_map:
        df = df.rename(columns=rename_map)

    if "core.legacy_id" not in df.columns:
        raise RuntimeError(f"IDX tab {tab} missing core.legacy_id")
    if "core.gid" not in df.columns:
        df["core.gid"] = df["core.legacy_id"].apply(lambda x: _build_gid(owner_type, x))

    df = df.dropna(subset=["core.legacy_id"]).copy()
    df["core.legacy_id"] = df["core.legacy_id"].astype(str).str.strip()
    df["core.gid"] = df["core.gid"].astype(str).str.strip()
    df = df[df["core.gid"].str.len() > 0].copy()
    print(
        f"[Read] idx done | tab={tab} | owner={owner_type} | rows={len(df)} | "
        f"elapsed={time.time() - started:.1f}s"
    )
    return df


def parse_mf_field_key(field_key: str) -> Tuple[str, str, str]:
    s = normalize_str(field_key)
    if s.startswith("mf."):
        parts = s.split(".", 2)
        if len(parts) == 3:
            return "mf", parts[1], parts[2]
    if s.startswith("v_mf."):
        parts = s.split(".", 2)
        if len(parts) == 3:
            return "v_mf", parts[1], parts[2]
    return "", "", ""


def parse_mo_field_key(field_key: str) -> Tuple[str, str]:
    s = normalize_str(field_key)
    if not s.startswith("mo."):
        return "", ""
    rest = s[3:]
    if "." not in rest:
        return "", ""
    metaobject_type, meta_field_key = rest.rsplit(".", 1)
    return metaobject_type.strip(), meta_field_key.strip()


def parse_mo_ref_expr(expr: str) -> str:
    s = normalize_str(expr)
    m = MO_EXPR_RE.match(s)
    if not m:
        return ""
    return normalize_str(m.group(1))


def build_alias_to_fieldkey(cfg_fields: pd.DataFrame) -> Dict[str, str]:
    out = {}
    for _, r in cfg_fields.fillna("").iterrows():
        fk = normalize_str(r.get("field_key"))
        alias = normalize_str(r.get("alias"))
        if fk:
            out[fk] = fk
        if alias:
            out[alias] = fk
    return out


def extract_placeholders(expr: str) -> List[str]:
    return [normalize_str(x) for x in PH_RE.findall(normalize_str(expr)) if normalize_str(x)]


def get_view_cfg(cfg_export: pd.DataFrame, view_id: str) -> pd.DataFrame:
    df = cfg_export[cfg_export["view_id"].astype(str).str.strip() == view_id].copy()
    if df.empty:
        return df
    if "seq" not in df.columns:
        df["seq"] = ""
    df["__seq"] = pd.to_numeric(df["seq"], errors="coerce").fillna(999999).astype(int)
    return df.sort_values("__seq").drop(columns=["__seq"])


def decide_enabled_views(cfg_tabs: pd.DataFrame, view_toggles: List[Tuple[str, bool]]) -> List[str]:
    if isinstance(view_toggles, list) and len(view_toggles) > 0:
        enabled = [normalize_str(v) for (v, on) in view_toggles if on and normalize_str(v)]
        return list(dict.fromkeys(enabled))

    if cfg_tabs is not None and not cfg_tabs.empty:
        cfg_tabs = cfg_tabs.copy()
        cfg_tabs.columns = [normalize_str(c) for c in cfg_tabs.columns]
        if "view_id" in cfg_tabs.columns:
            if "enabled" in cfg_tabs.columns:
                df = cfg_tabs[cfg_tabs["enabled"].apply(is_true)].copy()
                vids = [normalize_str(x) for x in df["view_id"].tolist() if normalize_str(x)]
                if vids:
                    return list(dict.fromkeys(vids))
            vids = [normalize_str(x) for x in cfg_tabs["view_id"].tolist() if normalize_str(x)]
            if vids:
                return list(dict.fromkeys(vids))
    return []


def parse_default_filters_from_tabs(cfg_tabs: pd.DataFrame, enabled_views: List[str]) -> Dict[str, Dict[str, Tuple[bool, str]]]:
    """
    当前按你的表结构支持：
    - fixed_filter_mode
    - fixed_filters_json

    兼容旧列名：
    - view_filters_json
    - filters_json
    """
    out: Dict[str, Dict[str, Tuple[bool, str]]] = {}
    if cfg_tabs is None or cfg_tabs.empty or "view_id" not in cfg_tabs.columns:
        return out

    work = cfg_tabs.copy().fillna("")
    work.columns = [normalize_str(c) for c in work.columns]

    for v in enabled_views:
        row = work[work["view_id"].astype(str).str.strip() == v]
        if row.empty:
            continue

        rr = row.iloc[0].to_dict()
        raw = normalize_str(
            rr.get("fixed_filters_json")
            or rr.get("view_filters_json")
            or rr.get("filters_json")
        )
        if not raw:
            continue

        try:
            obj = json.loads(raw)
        except Exception:
            continue

        if not isinstance(obj, dict):
            continue

        view_map: Dict[str, Tuple[bool, str]] = {}
        for k, val in obj.items():
            kk = normalize_str(k)
            if not kk:
                continue

            if isinstance(val, list) and len(val) >= 2:
                view_map[kk] = (bool(val[0]), normalize_str(val[1]))
            elif isinstance(val, dict):
                view_map[kk] = (bool(val.get("enabled")), normalize_str(val.get("value")))
            else:
                view_map[kk] = (True, normalize_str(val))

        if view_map:
            out[v] = view_map

    return out


def merge_view_filters(
    default_filters: Dict[str, Dict[str, Tuple[bool, str]]],
    override_filters: Dict[str, Dict[str, Tuple[bool, str]]],
    use_default_filters: bool,
) -> Dict[str, Dict[str, Tuple[bool, str]]]:
    if not use_default_filters:
        return override_filters or {}

    out = {}
    for v, mp in (default_filters or {}).items():
        out[v] = dict(mp)

    for v, mp in (override_filters or {}).items():
        if v not in out:
            out[v] = {}
        for fk, vv in mp.items():
            out[v][fk] = vv
    return out


def _parse_filter_key(k: str) -> Tuple[str, str]:
    s = normalize_str(k)
    if "|" not in s:
        return "", s
    a, b = s.split("|", 1)
    return normalize_str(a).upper(), normalize_str(b)


def _eval_one_filter(series: pd.Series, value_expr: str) -> pd.Series:
    s = series.fillna("").astype(str)
    v = normalize_str(value_expr)
    if v == "":
        return pd.Series([True] * len(s), index=s.index)

    if v.startswith("~"):
        pat = v[1:].strip()
        try:
            rgx = re.compile(pat, flags=re.IGNORECASE)
            return s.apply(lambda x: bool(rgx.search(str(x))))
        except re.error:
            return pd.Series([False] * len(s), index=s.index)

    items = [x.strip() for x in v.split(",") if x.strip()]
    if not items:
        return pd.Series([True] * len(s), index=s.index)
    items_set = set([x.lower() for x in items])
    return s.apply(lambda x: str(x).strip().lower() in items_set)


def apply_filters_for_owner(
    df: pd.DataFrame,
    owner_type: str,
    global_filters: Dict[str, Tuple[bool, str]],
    view_filters: Dict[str, Dict[str, Tuple[bool, str]]],
    enabled_views: List[str],
    warnings: List[str],
) -> pd.DataFrame:
    work = df.copy()
    owner_type = owner_type.upper()

    for k, (enabled, value) in (global_filters or {}).items():
        if not enabled:
            continue
        et, col = _parse_filter_key(k)
        if et and et != owner_type:
            continue
        if col not in work.columns:
            warnings.append(f"skip global filter missing col: {k}")
            continue
        work = work[_eval_one_filter(work[col], value)]

    for view_id in enabled_views:
        vf = (view_filters or {}).get(view_id) or {}
        if not vf:
            continue

        mask_view = pd.Series([True] * len(work), index=work.index)
        used_any = False

        for k, (enabled, value) in vf.items():
            if not enabled:
                continue
            et, col = _parse_filter_key(k)
            if et and et != owner_type:
                continue
            if col not in work.columns:
                warnings.append(f"skip view filter missing col: view={view_id} key={k}")
                continue
            used_any = True
            mask_view = mask_view & _eval_one_filter(work[col], value)

        if used_any:
            work = work[mask_view]

    return work.copy()


def build_long_need(
    cfg_fields: pd.DataFrame,
    cfg_export: pd.DataFrame,
    enabled_views_non_idx: List[str],
    idx_coverage_product: set,
    idx_coverage_variant: set,
) -> Tuple[Dict[str, Any], List[str]]:
    warnings: List[str] = []

    need_cols_export = ["view_id", "seq", "field_type", "entity_type", "field_key", "expr", "alias", "data_type", "required", "notes"]
    need_cols_fields = ["entity_type", "field_key", "expr", "alias", "source_type", "namespace", "key", "data_type", "field_type"]
    for c in need_cols_export:
        if c not in cfg_export.columns:
            cfg_export[c] = ""
    for c in need_cols_fields:
        if c not in cfg_fields.columns:
            cfg_fields[c] = ""

    alias_to_fieldkey = build_alias_to_fieldkey(cfg_fields)

    product_mf: set = set()
    variant_mf: set = set()
    mo_specs: Dict[str, Dict[str, Any]] = {}

    for view_id in enabled_views_non_idx:
        view_df = get_view_cfg(cfg_export, view_id)
        if view_df.empty:
            warnings.append(f"view not found in Cfg__ExportTabFields: {view_id}")
            continue

        for _, r in view_df.fillna("").iterrows():
            field_key = normalize_str(r.get("field_key"))
            expr = normalize_str(r.get("expr"))
            field_type = normalize_str(r.get("field_type")).upper()

            candidates: List[str] = []
            if field_key:
                candidates.append(field_key)

            if field_type == "CALC" and expr:
                for ph in extract_placeholders(expr):
                    mapped = alias_to_fieldkey.get(ph) or ph
                    if mapped:
                        candidates.append(mapped)

            for fk in candidates:
                if not fk.startswith(LONG_PREFIXES):
                    continue

                if fk.startswith("mf."):
                    if fk not in idx_coverage_product:
                        product_mf.add(fk)

                elif fk.startswith("v_mf."):
                    if fk not in idx_coverage_variant:
                        variant_mf.add(fk)

                elif fk.startswith("mo."):
                    mo_type, mo_field_key = parse_mo_field_key(fk)
                    if not mo_type or not mo_field_key:
                        warnings.append(f"bad mo field_key: {fk} view={view_id}")
                        continue

                    ref_fk = parse_mo_ref_expr(expr)
                    if not ref_fk:
                        warnings.append(f"mo field missing MO_REF expr: field_key={fk} view={view_id}")
                        continue

                    if not (ref_fk.startswith("mf.") or ref_fk.startswith("v_mf.")):
                        warnings.append(f"mo expr invalid source ref: field_key={fk} expr={expr} view={view_id}")
                        continue

                    _, ns, key = parse_mf_field_key(ref_fk)
                    if not ns or not key:
                        warnings.append(f"mo expr parse failed: field_key={fk} expr={expr} view={view_id}")
                        continue

                    if ref_fk.startswith("mf.") and ref_fk not in idx_coverage_product:
                        product_mf.add(ref_fk)
                    if ref_fk.startswith("v_mf.") and ref_fk not in idx_coverage_variant:
                        variant_mf.add(ref_fk)

                    base_row = cfg_fields[cfg_fields["field_key"].astype(str).str.strip() == fk]
                    if base_row.empty:
                        warnings.append(f"mo field not found in Cfg__Fields: {fk}")
                        continue

                    rr = base_row.iloc[0].fillna("").to_dict()
                    if normalize_str(rr.get("entity_type")).upper() != "METAOBJECT_ENTRY":
                        warnings.append(f"mo field entity_type not METAOBJECT_ENTRY: {fk}")
                        continue
                    if normalize_str(rr.get("source_type")).upper() != "METAOBJECT_REF":
                        warnings.append(f"mo field source_type not METAOBJECT_REF: {fk}")
                        continue

                    spec = mo_specs.get(fk) or {
                        "field_key": fk,
                        "metaobject_type": mo_type,
                        "meta_field_key": mo_field_key,
                        "source_ref_field_keys": set(),
                        "source_view_ids": set(),
                        "data_type": normalize_str(rr.get("data_type")),
                        "field_type": normalize_str(rr.get("field_type")),
                    }
                    spec["source_ref_field_keys"].add(ref_fk)
                    spec["source_view_ids"].add(view_id)
                    mo_specs[fk] = spec

    return {
        "product_mf": sorted(product_mf),
        "variant_mf": sorted(variant_mf),
        "mo_specs": mo_specs,
    }, warnings


def build_mf_selection_and_map(mf_field_keys: List[str]) -> Tuple[str, Dict[str, str]]:
    lines = []
    alias_to_fk = {}
    for fk in mf_field_keys:
        _, ns, key = parse_mf_field_key(fk)
        if not ns or not key:
            continue
        alias = gql_safe_alias(fk)
        alias_to_fk[alias] = fk
        lines.append(
            f'{alias}: metafield(namespace: "{ns}", key: "{key}") '
            '{ value type references(first: 50) { nodes { __typename '
            '... on Metaobject { id type handle } '
            '... on Collection { id } '
            '... on GenericFile { id url } '
            '... on MediaImage { id image { url altText } } '
            '} } }'
        )
    return "\n".join(lines), alias_to_fk


def fetch_nodes_products(client: ShopifyClient, ids: List[str], mf_field_keys: List[str], chunk_size_ids: int, chunk_size_mf: int) -> Dict[str, dict]:
    print(
        "[Shopify] Products start | "
        f"ids={len(ids)} | mf_keys={len(mf_field_keys)} | "
        f"id_chunk={chunk_size_ids} | mf_chunk={chunk_size_mf}"
    )
    if not ids or not mf_field_keys:
        print("[Shopify] Products done | owners=0 | rows=0")
        return {}

    out: Dict[str, dict] = {}
    id_chunks = chunk_list(ids, chunk_size_ids)
    mf_chunks = chunk_list(mf_field_keys, chunk_size_mf)
    total_chunks = len(id_chunks) * len(mf_chunks)
    chunk_no = 0
    started = time.time()

    for id_part in id_chunks:
        for mf_part in mf_chunks:
            chunk_no += 1
            sel, alias_to_fk = build_mf_selection_and_map(mf_part)
            q = f"""
query NodesProducts($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on Product {{
      id
      legacyResourceId
      updatedAt
      {sel}
    }}
  }}
}}
""".strip()
            data = client.gql(q, {"ids": id_part})
            for n in (data.get("nodes") or []):
                if not n:
                    continue
                gid = normalize_str(n.get("id"))
                if not gid:
                    continue
                if gid not in out:
                    out[gid] = {"id": gid, "legacyResourceId": n.get("legacyResourceId"), "updatedAt": n.get("updatedAt"), "__mf": {}}
                mf_map = out[gid]["__mf"]
                for alias, fk in alias_to_fk.items():
                    block = n.get(alias)
                    if block is None:
                        continue
                    refs = (((block.get("references") or {}).get("nodes")) or [])
                    mf_map[fk] = {
                        "value": "" if block.get("value") is None else str(block.get("value")),
                        "type": normalize_str(block.get("type")),
                        "references": refs,
                    }

            if chunk_no == 1 or chunk_no % 20 == 0 or chunk_no == total_chunks:
                row_count = sum(len((value.get("__mf") or {})) for value in out.values())
                print(f"[Shopify] Products chunks={chunk_no}/{total_chunks} | rows={row_count}")

    row_count = sum(len((value.get("__mf") or {})) for value in out.values())
    print(
        f"[Shopify] Products done | owners={len(out)} | rows={row_count} | "
        f"elapsed={time.time() - started:.1f}s"
    )
    return out

def fetch_nodes_variants(client: ShopifyClient, ids: List[str], mf_field_keys: List[str], chunk_size_ids: int, chunk_size_mf: int) -> Dict[str, dict]:
    print(
        "[Shopify] Variants start | "
        f"ids={len(ids)} | mf_keys={len(mf_field_keys)} | "
        f"id_chunk={chunk_size_ids} | mf_chunk={chunk_size_mf}"
    )
    if not ids or not mf_field_keys:
        print("[Shopify] Variants done | owners=0 | rows=0")
        return {}

    out: Dict[str, dict] = {}
    id_chunks = chunk_list(ids, chunk_size_ids)
    mf_chunks = chunk_list(mf_field_keys, chunk_size_mf)
    total_chunks = len(id_chunks) * len(mf_chunks)
    chunk_no = 0
    started = time.time()

    for id_part in id_chunks:
        for mf_part in mf_chunks:
            chunk_no += 1
            sel, alias_to_fk = build_mf_selection_and_map(mf_part)
            q = f"""
query NodesVariants($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on ProductVariant {{
      id
      legacyResourceId
      updatedAt
      {sel}
    }}
  }}
}}
""".strip()
            data = client.gql(q, {"ids": id_part})
            for n in (data.get("nodes") or []):
                if not n:
                    continue
                gid = normalize_str(n.get("id"))
                if not gid:
                    continue
                if gid not in out:
                    out[gid] = {"id": gid, "legacyResourceId": n.get("legacyResourceId"), "updatedAt": n.get("updatedAt"), "__mf": {}}
                mf_map = out[gid]["__mf"]
                for alias, fk in alias_to_fk.items():
                    block = n.get(alias)
                    if block is None:
                        continue
                    refs = (((block.get("references") or {}).get("nodes")) or [])
                    mf_map[fk] = {
                        "value": "" if block.get("value") is None else str(block.get("value")),
                        "type": normalize_str(block.get("type")),
                        "references": refs,
                    }

            if chunk_no == 1 or chunk_no % 20 == 0 or chunk_no == total_chunks:
                row_count = sum(len((value.get("__mf") or {})) for value in out.values())
                print(f"[Shopify] Variants chunks={chunk_no}/{total_chunks} | rows={row_count}")

    row_count = sum(len((value.get("__mf") or {})) for value in out.values())
    print(
        f"[Shopify] Variants done | owners={len(out)} | rows={row_count} | "
        f"elapsed={time.time() - started:.1f}s"
    )
    return out

def fetch_metaobjects_for_specs(
    client: ShopifyClient,
    ref_gid_to_expected_type: Dict[str, str],
    specs: List[Dict[str, Any]],
    chunk_size_ids: int,
) -> Dict[str, Dict[str, Any]]:
    print(
        "[Shopify] Metaobjects start | "
        f"ref_gids={len(ref_gid_to_expected_type)} | specs={len(specs)} | "
        f"id_chunk={chunk_size_ids}"
    )
    started = time.time()
    if not ref_gid_to_expected_type:
        print("[Shopify] Metaobjects done | entries=0")
        return {}

    all_meta_keys = sorted(
        {normalize_str(s["meta_field_key"]) for s in specs if normalize_str(s.get("meta_field_key"))}
    )
    field_lines = []
    for k in all_meta_keys:
        alias = gql_safe_alias(f"mo_field_{k}")
        field_lines.append(
            f'{alias}: field(key: "{k}") '
            '{ key value type '
            'reference { __typename '
            '... on Metaobject { id type handle } '
            '... on Collection { id } '
            '... on GenericFile { id url } '
            '... on MediaImage { id image { url altText } } '
            '} '
            'references(first: 50) { nodes { __typename '
            '... on Metaobject { id type handle } '
            '... on Collection { id } '
            '... on GenericFile { id url } '
            '... on MediaImage { id image { url altText } } '
            '} } } }'
        )
    sel = "\n".join(field_lines)

    out: Dict[str, Dict[str, Any]] = {}
    ids = list(ref_gid_to_expected_type.keys())
    for id_part in chunk_list(ids, chunk_size_ids):
        q = f"""
query MetaobjectsByIds($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on Metaobject {{
      id
      type
      handle
      {sel}
    }}
  }}
}}
""".strip()
        data = client.gql(q, {"ids": id_part})
        for n in (data.get("nodes") or []):
            if not n:
                continue
            gid = normalize_str(n.get("id"))
            if not gid:
                continue
            out[gid] = {
                "id": gid,
                "type": normalize_str(n.get("type")),
                "handle": normalize_str(n.get("handle")),
                "__fields": {},
            }
            for meta_field_key in all_meta_keys:
                alias = gql_safe_alias(f"mo_field_{meta_field_key}")
                blk = n.get(alias)
                if not blk:
                    continue
                refs = (((blk.get("references") or {}).get("nodes")) or [])
                out[gid]["__fields"][meta_field_key] = {
                    "value": "" if blk.get("value") is None else str(blk.get("value")),
                    "type": normalize_str(blk.get("type")),
                    "reference": blk.get("reference"),
                    "references": refs,
                }
    print(
        f"[Shopify] Metaobjects done | entries={len(out)} | "
        f"elapsed={time.time() - started:.1f}s"
    )
    return out


def serialize_reference_node(node: Dict[str, Any]) -> str:
    if not isinstance(node, dict):
        return ""
    t = normalize_str(node.get("__typename"))
    if t == "Metaobject":
        return normalize_str(node.get("id"))
    if t == "Collection":
        return normalize_str(node.get("id"))
    if t == "GenericFile":
        return normalize_str(node.get("url") or node.get("id"))
    if t == "MediaImage":
        img = node.get("image") or {}
        return normalize_str(img.get("url") or node.get("id"))
    return normalize_str(node.get("id"))


def extract_mo_value(meta_field_block: Dict[str, Any], join_sep: str) -> Tuple[str, str]:
    if not meta_field_block:
        return "", ""

    refs = meta_field_block.get("references") or []
    if refs:
        vals = [serialize_reference_node(x) for x in refs if serialize_reference_node(x)]
        return join_sep.join(vals), normalize_str(meta_field_block.get("type"))

    ref = meta_field_block.get("reference")
    if ref:
        return serialize_reference_node(ref), normalize_str(meta_field_block.get("type"))

    return normalize_str(meta_field_block.get("value")), normalize_str(meta_field_block.get("type"))


def dedupe_long_df(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return df_long
    key_cols = ["owner_entity_type", "owner_gid", "field_key", "value"]
    return df_long.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)


def run(
    *,
    site_code: str,
    shop_domain: str,
    api_version: str,
    shopify_access_token: str,
    gsheet_sa_b64: str,
    console_core_url: str,
    view_toggles: List[Tuple[str, bool]],
    use_default_filters: bool = True,
    view_filter_overrides: Optional[Dict[str, Dict[str, Tuple[bool, str]]]] = None,
    global_filters: Optional[Dict[str, Tuple[bool, str]]] = None,
    cfg_sites_tab: str = "Cfg__Sites",
    cfg_tabs_tab: str = "Cfg__ExportTabs",
    cfg_fields_tab: str = "Cfg__Fields",
    cfg_export_tab: str = "Cfg__ExportTabFields",
    idx_products_tab: str = "IDX__Products",
    idx_variants_tab: str = "IDX__Variants",
    values_long_tab: str = "DL__ValuesLong",
    idx_view_products: str = "IDX__Products",
    idx_view_variants: str = "IDX__Variants",
    runlog_tab: str = "Ops__RunLog",
    mo_list_join_sep: str = " , ",
    gql_ids_per_query: int = 50,
    gql_mf_per_query: int = 25,
    job_name: str = DEFAULT_JOB_NAME,
) -> Dict[str, Any]:

    job_name = normalize_str(job_name) or DEFAULT_JOB_NAME
    print(f"[ValuesLong] start | job={job_name} | site={site_code}")
    gc = build_gspread_client_from_b64(gsheet_sa_b64)
    client = ShopifyClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=shopify_access_token,
    )

    run_id = f"{job_name}_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}"
    ts_cn = now_cn_str()
    warnings: List[str] = []

    # 1) Cfg__Sites 只从 console_core 读
    cfg_sites = read_table(gc, console_core_url, cfg_sites_tab).fillna("")
    if cfg_sites.empty:
        raise RuntimeError(f"{cfg_sites_tab} is empty in console_core_url")

    site_rows = cfg_sites[cfg_sites["site_code"].astype(str).str.strip().str.upper() == site_code.upper()].copy()
    if site_rows.empty:
        raise RuntimeError(f"site_code not found in Cfg__Sites: {site_code}")

    def get_sheet_url(label: str) -> str:
        x = site_rows[site_rows["label"].astype(str).str.strip() == label]
        if x.empty:
            return ""
        return normalize_str(x.iloc[0].get("sheet_url"))

    config_sheet_url = get_sheet_url("config")
    data_sheet_url = get_sheet_url("export_product")
    runlog_sheet_url = get_sheet_url("runlog_sheet")

    if not config_sheet_url:
        raise RuntimeError(f"site {site_code} missing label=config in Cfg__Sites")
    if not data_sheet_url:
        raise RuntimeError(f"site {site_code} missing label=export_product in Cfg__Sites")

    # 2) 配置表从 label=config 的 sheet 读
    cfg_tabs = read_table(gc, config_sheet_url, cfg_tabs_tab).fillna("")
    cfg_fields = read_table(gc, config_sheet_url, cfg_fields_tab).fillna("")
    cfg_export = read_table(gc, config_sheet_url, cfg_export_tab).fillna("")

    if cfg_export.empty:
        raise RuntimeError(f"{cfg_export_tab} is empty in config sheet")

    enabled_views = decide_enabled_views(cfg_tabs, view_toggles)
    enabled_views_non_idx = [v for v in enabled_views if v not in (idx_view_products, idx_view_variants)]

    idx_p_cfg = get_view_cfg(cfg_export, idx_view_products)
    idx_v_cfg = get_view_cfg(cfg_export, idx_view_variants)
    idx_coverage_product = set(
        [normalize_str(x) for x in idx_p_cfg.get("field_key", pd.Series(dtype=str)).tolist() if normalize_str(x)]
    )
    idx_coverage_variant = set(
        [normalize_str(x) for x in idx_v_cfg.get("field_key", pd.Series(dtype=str)).tolist() if normalize_str(x)]
    )

    need_result, need_warnings = build_long_need(
        cfg_fields=cfg_fields,
        cfg_export=cfg_export,
        enabled_views_non_idx=enabled_views_non_idx,
        idx_coverage_product=idx_coverage_product,
        idx_coverage_variant=idx_coverage_variant,
    )
    warnings.extend(need_warnings)

    product_mf = need_result["product_mf"]
    variant_mf = need_result["variant_mf"]
    mo_specs = need_result["mo_specs"]

    # 3) IDX / Long 从 export_product 的 sheet 读写
    idx_products = read_idx_df(gc, data_sheet_url, idx_products_tab, "PRODUCT")
    idx_variants = read_idx_df(gc, data_sheet_url, idx_variants_tab, "VARIANT")

    default_view_filters = parse_default_filters_from_tabs(cfg_tabs, enabled_views_non_idx)
    merged_view_filters = merge_view_filters(
        default_filters=default_view_filters,
        override_filters=(view_filter_overrides or {}),
        use_default_filters=use_default_filters,
    )

    product_df = apply_filters_for_owner(
        df=idx_products,
        owner_type="PRODUCT",
        global_filters=global_filters or {},
        view_filters=merged_view_filters,
        enabled_views=enabled_views_non_idx,
        warnings=warnings,
    )

    variant_df = apply_filters_for_owner(
        df=idx_variants,
        owner_type="VARIANT",
        global_filters=global_filters or {},
        view_filters=merged_view_filters,
        enabled_views=enabled_views_non_idx,
        warnings=warnings,
    )

    product_ids = sorted(product_df["core.gid"].astype(str).str.strip().unique().tolist()) if not product_df.empty else []
    variant_ids = sorted(variant_df["core.gid"].astype(str).str.strip().unique().tolist()) if not variant_df.empty else []

    product_nodes = fetch_nodes_products(client, product_ids, product_mf, gql_ids_per_query, gql_mf_per_query)
    variant_nodes = fetch_nodes_variants(client, variant_ids, variant_mf, gql_ids_per_query, gql_mf_per_query)

    ref_gid_to_expected_type: Dict[str, str] = {}
    for owner_type, node_map in [("PRODUCT", product_nodes), ("VARIANT", variant_nodes)]:
        for owner_gid, nd in node_map.items():
            mf_map = nd.get("__mf") or {}
            for fk, spec in mo_specs.items():
                for src_ref_fk in sorted(spec["source_ref_field_keys"]):
                    if owner_type == "PRODUCT" and not src_ref_fk.startswith("mf."):
                        continue
                    if owner_type == "VARIANT" and not src_ref_fk.startswith("v_mf."):
                        continue

                    src_blk = mf_map.get(src_ref_fk) or {}
                    refs = src_blk.get("references") or []
                    for rr in refs:
                        if normalize_str(rr.get("__typename")) == "Metaobject":
                            ref_gid = normalize_str(rr.get("id"))
                            if ref_gid:
                                ref_gid_to_expected_type[ref_gid] = spec["metaobject_type"]

    metaobject_cache = fetch_metaobjects_for_specs(
        client=client,
        ref_gid_to_expected_type=ref_gid_to_expected_type,
        specs=list(mo_specs.values()),
        chunk_size_ids=gql_ids_per_query,
    )

    long_rows: List[Dict[str, Any]] = []

    # mf.*
    for owner_type, src_df, node_map, prefix in [
        ("PRODUCT", product_df, product_nodes, "mf."),
        ("VARIANT", variant_df, variant_nodes, "v_mf."),
    ]:
        if src_df.empty:
            continue
        for _, row in src_df.iterrows():
            owner_gid = normalize_str(row.get("core.gid"))
            owner_legacy_id = normalize_str(row.get("core.legacy_id"))
            nd = node_map.get(owner_gid) or {}
            mf_map = nd.get("__mf") or {}
            for fk, blk in mf_map.items():
                if not fk.startswith(prefix):
                    continue
                val = normalize_str(blk.get("value"))
                typ = normalize_str(blk.get("type"))
                if val == "":
                    continue
                long_rows.append(
                    {
                        "owner_entity_type": owner_type,
                        "owner_gid": owner_gid,
                        "owner_legacy_id": owner_legacy_id,
                        "field_key": fk,
                        "value": val,
                        "value_type": typ,
                    }
                )

    # mo.*
    for owner_type, src_df, node_map in [
        ("PRODUCT", product_df, product_nodes),
        ("VARIANT", variant_df, variant_nodes),
    ]:
        if src_df.empty:
            continue
        for _, row in src_df.iterrows():
            owner_gid = normalize_str(row.get("core.gid"))
            owner_legacy_id = normalize_str(row.get("core.legacy_id"))
            nd = node_map.get(owner_gid) or {}
            mf_map = nd.get("__mf") or {}

            for fk, spec in mo_specs.items():
                source_ref_candidates = sorted(spec["source_ref_field_keys"])
                valid_src = [
                    x for x in source_ref_candidates
                    if (owner_type == "PRODUCT" and x.startswith("mf."))
                    or (owner_type == "VARIANT" and x.startswith("v_mf."))
                ]
                if not valid_src:
                    continue

                for src_ref_fk in valid_src:
                    src_blk = mf_map.get(src_ref_fk) or {}
                    refs = src_blk.get("references") or []
                    if not refs:
                        continue

                    mo_vals = []
                    ref_gids = []
                    ref_types = []
                    for rr in refs:
                        if normalize_str(rr.get("__typename")) != "Metaobject":
                            continue
                        ref_gid = normalize_str(rr.get("id"))
                        if not ref_gid:
                            continue

                        mo_entry = metaobject_cache.get(ref_gid)
                        if not mo_entry:
                            warnings.append(f"mo ref target missing: owner={owner_gid} src_ref={src_ref_fk} ref_gid={ref_gid}")
                            continue

                        actual_type = normalize_str(mo_entry.get("type"))
                        expected_type = normalize_str(spec["metaobject_type"])
                        if actual_type != expected_type:
                            warnings.append(
                                f"mo ref type mismatch: owner={owner_gid} src_ref={src_ref_fk} "
                                f"field_key={fk} expected={expected_type} actual={actual_type}"
                            )
                            continue

                        meta_blk = (mo_entry.get("__fields") or {}).get(spec["meta_field_key"]) or {}
                        vv, _ = extract_mo_value(meta_blk, mo_list_join_sep)
                        if vv == "":
                            continue
                        mo_vals.append(vv)
                        ref_gids.append(ref_gid)
                        ref_types.append(actual_type)

                    if mo_vals:
                        long_rows.append(
                            {
                                "owner_entity_type": owner_type,
                                "owner_gid": owner_gid,
                                "owner_legacy_id": owner_legacy_id,
                                "field_key": fk,
                                "value": mo_list_join_sep.join(mo_vals),
                                "value_type": normalize_str(spec.get("data_type")) or "metaobject_ref_expanded",
                            }
                        )

    df_long = pd.DataFrame(long_rows, columns=DL_HEADERS).fillna("")
    df_long = dedupe_long_df(df_long)

    ws_long = ensure_ws(gc, data_sheet_url, values_long_tab)
    write_df_replace(ws_long, df_long)

    summary = {
        "run_id": run_id,
        "site_code": site_code,
        "job_name": job_name,
        "enabled_views": enabled_views_non_idx,
        "rows_loaded": len(idx_products) + len(idx_variants),
        "rows_pending": len(product_df) + len(variant_df),
        "rows_recognized": len(product_mf) + len(variant_mf) + len(mo_specs),
        "rows_planned": len(long_rows),
        "rows_written": len(df_long),
        "rows_skipped": 0,
        "warning_count": len(warnings),
        "warnings": warnings,
        "config_sheet_url": config_sheet_url,
        "data_sheet_url": data_sheet_url,
        "values_long_tab": values_long_tab,
    }

    runlog_rows = [
        [
            run_id,
            ts_cn,
            job_name,
            "apply",
            "summary",
            "OK",
            site_code,
            "",
            "",
            "",
            len(idx_products) + len(idx_variants),
            len(product_df) + len(variant_df),
            len(product_mf) + len(variant_mf) + len(mo_specs),
            len(long_rows),
            len(df_long),
            0,
            f"enabled_views={len(enabled_views_non_idx)} warnings={len(warnings)}",
            "",
        ]
    ]

    by_reason: Dict[str, List[str]] = {}
    for w in warnings:
        reason = w.split(":", 1)[0].strip() if ":" in w else "warning"
        by_reason.setdefault(reason, [])
        if len(by_reason[reason]) < 2:
            by_reason[reason].append(w)

    for reason, msgs in by_reason.items():
        for msg in msgs:
            runlog_rows.append(
                [
                    run_id,
                    ts_cn,
                    job_name,
                    "apply",
                    "detail",
                    "WARN",
                    site_code,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    msg,
                    reason,
                ]
            )

    append_runlog_rows(gc, runlog_sheet_url, runlog_tab, runlog_rows)
    print(
        "[ValuesLong] done | "
        f"rows_written={len(df_long)} | warnings={len(warnings)} | run_id={run_id}"
    )

    return {
        "summary": summary,
        "df_long": df_long,
        "warnings": warnings,
    }
