# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import datetime as dt
import json
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials


MODULE_PATH = "shopify_ops.config_metaobject_defs"
MODULE_VERSION = "2026-08-01-runtime-boundary-v1"
DEFAULT_JOB_NAME = "config_metaobject_defs"


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


Q_METAOBJECT_DEFS = """
query ($first: Int!, $after: String) {
  metaobjectDefinitions(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      type
      name
      fieldDefinitions {
        key
        name
        description
        required
        type { name }
      }
    }
  }
}
"""


def _now_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def _gen_run_id(job_name: str, tz_name: str) -> str:
    ts = dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d_%H%M%S")
    return f"{job_name}_{ts}"


def _extract_sheet_id(url_or_id: str) -> str:
    s = str(url_or_id or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
    return m.group(1) if m else s


def _a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v)


def _normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


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
    return _safe_str(value).strip().upper()


def _workspace_secret_result_to_value(result: Any) -> SecretValue:
    source_detail = getattr(result, "resolved_name", "") or ""
    path = getattr(result, "path", None)
    key = getattr(result, "key", None)
    if path is not None:
        source_detail = str(path)
        if key:
            source_detail += f"::{key}"
    elif key:
        source_detail = str(key)
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
    secret_name = _safe_str(name).strip()
    resolved_project_code = _normalize_project_code(project_code)
    if not secret_name:
        raise RuntimeError("Secret name is empty.")
    if not resolved_project_code:
        raise RuntimeError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _safe_str(explicit_value).strip():
        return SecretValue(_safe_str(explicit_value).strip(), "EXPLICIT_VALUE", "caller")

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
    raw = _safe_str(raw_value).strip()
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
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds), {
        "source_type": secret.source_type,
        "source_detail": secret.source_detail,
        "secret_format": secret_format,
        "service_account_email": _safe_str(info.get("client_email")).strip(),
    }


def _normalize_registry_header(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", _safe_str(value).lower()).strip()


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

    registry_file_id = _extract_sheet_id(workspace_registry_id)
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
    active_text = _safe_str(row[active_col]).strip().lower()
    if active_text not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}, row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": _safe_str(row[project_name_col]).strip() if project_name_col is not None else "",
        "console_core_url": _safe_str(row[console_url_col]).strip(),
        "gsheet_secret_name": _safe_str(row[gsheet_secret_col]).strip(),
        "account_config_tab": _safe_str(row[account_tab_col]).strip(),
        "timezone": _safe_str(row[timezone_col]).strip(),
        "registry_id": registry_file_id,
        "registry_tab": workspace_registry_tab,
        "registry_source_row": str(source_row),
        "workspace_auth_source_type": _safe_str(auth_meta.get("source_type")).strip(),
        "workspace_service_account_email": _safe_str(auth_meta.get("service_account_email")).strip(),
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
    mode = _safe_str(registry_mode).strip().upper() or "OFF"
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

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _safe_str(current_colab_url).strip():
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _safe_str(current_colab_name).strip():
        raise ValueError("UPDATE_URL_AND_NAME requires current_colab_name.")

    sa_secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
    )
    gc, auth_meta = _build_gspread_client_from_secret(sa_secret)
    book = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="registry.open_by_url",
    )
    worksheet = _with_sheets_retry(
        lambda: book.worksheet(registry_tab),
        action=f"registry.worksheet:{registry_tab}",
    )
    values = _with_sheets_retry(
        worksheet.get_all_values,
        action=f"registry.get_all_values:{registry_tab}",
    )
    if not values:
        raise ValueError(f"Registry tab {registry_tab!r} is empty.")

    header_map: Dict[str, int] = {}
    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if normalized:
            if normalized in header_map:
                raise ValueError(f"Registry tab has duplicate normalized header: {normalized}")
            header_map[normalized] = index

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
        _safe_str(job_name).strip().lower(),
        _safe_str(sheet_label).strip().lower(),
        _safe_str(tab_name).strip().lower(),
    )
    matches: List[int] = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(values[0]) - len(row))
        logical_key = (
            _safe_str(padded[job_col]).strip().lower(),
            _safe_str(padded[label_col]).strip().lower(),
            _safe_str(padded[tab_col]).strip().lower(),
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
    provided_url = _safe_str(current_colab_url).strip()
    provided_name = _safe_str(current_colab_name).strip()
    if provided_url and _safe_str(current_row[url_col]).strip() != provided_url:
        changes.append(("colab_url", url_col + 1, _safe_str(current_row[url_col]).strip(), provided_url))
    if provided_name and _safe_str(current_row[name_col]).strip() != provided_name:
        changes.append(("colab_name", name_col + 1, _safe_str(current_row[name_col]).strip(), provided_name))

    if mode == "CHECK":
        status = "CHANGE_DETECTED" if changes else "NO_CHANGE"
    else:
        permitted = {"colab_url"} if mode == "UPDATE_URL" else {"colab_url", "colab_name"}
        changes = [change for change in changes if change[0] in permitted]
        for field_name, column_number, _old_value, new_value in changes:
            _with_sheets_retry(
                lambda rn=row_number, cn=column_number, nv=new_value: worksheet.update_cell(rn, cn, nv),
                action=f"registry.update_cell:{field_name}",
            )
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


def _load_secret(
    secret_name: str,
    explicit_value: Optional[str] = None,
    *,
    project_code: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> str:
    if explicit_value is not None and _safe_str(explicit_value).strip():
        return _safe_str(explicit_value).strip()
    if not project_code:
        raise RuntimeError(
            f"Secret {secret_name!r} was not supplied explicitly. "
            "Use the Runtime resolver before calling business run()."
        )
    return read_secret(
        secret_name,
        project_code=project_code,
        secret_home=secret_home,
    ).value


def _build_gspread_client(sa_b64: str) -> gspread.Client:
    info, _secret_format = _parse_service_account_text(sa_b64)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

class ShopifyGraphQLClient:
    def __init__(self, shop_domain: str, api_version: str, access_token: str, timeout: int = 90):
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        self.timeout = timeout

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None, retry: int = 6) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        last_err = None
        for i in range(retry):
            try:
                r = requests.post(self.url, headers=self.headers, json=payload, timeout=self.timeout)
                if r.status_code in (429, 502, 503, 504):
                    time.sleep(min(2 ** i, 20) + random.random())
                    continue
                r.raise_for_status()
                data = r.json()
                if data.get("errors"):
                    raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))
                return data["data"]
            except Exception as e:
                last_err = e
                if i < retry - 1:
                    time.sleep(min(2 ** i, 20) + random.random())
                    continue
                raise
        raise RuntimeError(f"GraphQL failed: {last_err}")


@dataclass
class SiteRouting:
    config_sheet_url: str
    runlog_sheet_url: str


@dataclass
class AccountConfig:
    shop_domain: str
    api_version: str
    gsheet_sa_b64_secret: str
    shopify_token_secret: str
    storefront_base_url: str = ""
    admin_base_url: str = ""
    meta_ad_account_id: str = ""
    awin_advertiser_id: str = ""


def _resolve_site_routing(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_sites: str,
    site_code: str,
    config_sheet_label: str,
    runlog_sheet_label: str,
) -> SiteRouting:
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="site_routing.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(tab_cfg_sites),
        action=f"site_routing.worksheet:{tab_cfg_sites}",
    )
    records = _with_sheets_retry(
        ws.get_all_records,
        action=f"site_routing.get_all_records:{tab_cfg_sites}",
    )
    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError(f"{tab_cfg_sites} is empty: {console_core_url}")

    df = _normalize_df_columns(df)
    need = ["site_code", "sheet_url", "label"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"{tab_cfg_sites} missing columns: {miss}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    sub = df[df["site_code"] == site_code.strip().upper()].copy()
    if sub.empty:
        raise ValueError(f"No rows found in {tab_cfg_sites} for site_code={site_code}")

    def pick(label: str) -> str:
        x = sub[sub["label"] == label]
        if x.empty:
            raise ValueError(f"{tab_cfg_sites} missing label={label} for site_code={site_code}")
        url = str(x.iloc[0]["sheet_url"]).strip()
        if not url:
            raise ValueError(f"{tab_cfg_sites} label={label} has empty sheet_url for site_code={site_code}")
        return url

    return SiteRouting(
        config_sheet_url=pick(config_sheet_label),
        runlog_sheet_url=pick(runlog_sheet_label),
    )


def _load_account_config(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_account_id: str,
) -> AccountConfig:
    """
    Read account-level runtime config from Console Core / Cfg__account_id.

    Expected sheet shape:
      column A = config key
      column B = config value

    Required keys:
      SHOP_DOMAIN
      SHOPIFY_API_VERSION
      GSHEET_SA_B64_SECRET
      SHOPIFY_TOKEN_SECRET

    This function is intentionally strict:
    - missing tab -> error
    - duplicated keys -> error
    - missing required key -> error
    - empty required value -> error
    """
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
        raise ValueError(f"{tab_cfg_account_id} is empty: {console_core_url}")

    kv: Dict[str, str] = {}
    duplicates: List[str] = []

    for row_idx, row in enumerate(values, start=1):
        if not row:
            continue

        key = str(row[0]).strip() if len(row) >= 1 else ""
        val = str(row[1]).strip() if len(row) >= 2 else ""

        if not key:
            continue

        # Allow a human-readable header row without treating it as config.
        if row_idx == 1 and key.lower() in {"key", "config_key", "name", "setting"}:
            continue

        if key in kv:
            duplicates.append(key)
            continue

        kv[key] = val

    if duplicates:
        raise ValueError(f"{tab_cfg_account_id} has duplicated keys: {sorted(set(duplicates))}")

    required = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "GSHEET_SA_B64_SECRET",
        "SHOPIFY_TOKEN_SECRET",
    ]
    missing = [k for k in required if k not in kv]
    empty = [k for k in required if k in kv and not kv[k]]

    if missing:
        raise ValueError(f"{tab_cfg_account_id} missing required keys: {missing}")
    if empty:
        raise ValueError(f"{tab_cfg_account_id} has empty required values: {empty}")

    return AccountConfig(
        shop_domain=kv["SHOP_DOMAIN"],
        api_version=kv["SHOPIFY_API_VERSION"],
        gsheet_sa_b64_secret=kv["GSHEET_SA_B64_SECRET"],
        shopify_token_secret=kv["SHOPIFY_TOKEN_SECRET"],
        storefront_base_url=kv.get("STOREFRONT_BASE_URL", ""),
        admin_base_url=kv.get("ADMIN_BASE_URL", ""),
        meta_ad_account_id=kv.get("META_AD_ACCOUNT_ID", ""),
        awin_advertiser_id=kv.get("AWIN_ADVERTISER_ID", ""),
    )


def _is_excluded_metaobject_type(mo_type: str, excluded_type_contains: Tuple[str, ...]) -> bool:
    s = str(mo_type or "").strip().lower()
    return any(x and x.lower() in s for x in excluded_type_contains)


def _filter_metaobject_defs(
    nodes: List[Dict[str, Any]],
    excluded_type_contains: Tuple[str, ...],
) -> Tuple[List[Dict[str, Any]], int]:
    if not excluded_type_contains:
        return nodes, 0

    kept: List[Dict[str, Any]] = []
    excluded = 0

    for node in nodes:
        mo_type = _safe_str(node.get("type"))
        if _is_excluded_metaobject_type(mo_type, excluded_type_contains):
            excluded += 1
            continue
        kept.append(node)

    return kept, excluded


def _fetch_all_metaobject_defs(client: ShopifyGraphQLClient, page_size: int) -> List[Dict[str, Any]]:
    all_nodes: List[Dict[str, Any]] = []
    after = None
    while True:
        data = client.gql(Q_METAOBJECT_DEFS, {"first": page_size, "after": after})
        conn = data["metaobjectDefinitions"]
        nodes = conn.get("nodes") or []
        all_nodes.extend(nodes)

        page_info = conn["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        after = page_info["endCursor"]
    return all_nodes


def _build_defs_df(nodes: List[Dict[str, Any]], tz_name: str) -> pd.DataFrame:
    synced_at = _now_str(tz_name)
    rows: List[Dict[str, Any]] = []

    for d in nodes:
        mo_type = _safe_str(d.get("type"))
        mo_name = _safe_str(d.get("name"))
        gid = _safe_str(d.get("id"))

        for fd in (d.get("fieldDefinitions") or []):
            field_type_obj = fd.get("type") or {}
            field_type_name = ""
            if isinstance(field_type_obj, dict):
                field_type_name = _safe_str(field_type_obj.get("name"))
            else:
                field_type_name = _safe_str(field_type_obj)

            rows.append(
                {
                    "gid": gid,
                    "type": mo_type,
                    "type_name": mo_name,
                    "field_key": _safe_str(fd.get("key")),
                    "field_name": _safe_str(fd.get("name")),
                    "field_type": field_type_name,
                    "required": "TRUE" if bool(fd.get("required")) else "FALSE",
                    "description": _safe_str(fd.get("description")),
                    "updated_at": "",
                    "synced_at": synced_at,
                }
            )

    cols = [
        "gid",
        "type",
        "type_name",
        "field_key",
        "field_name",
        "field_type",
        "required",
        "description",
        "updated_at",
        "synced_at",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if not df.empty:
        df = df.sort_values(["type", "field_key"], kind="stable").reset_index(drop=True)
    return df


def _pick_col(actual_headers: List[str], aliases: List[str]) -> Optional[str]:
    lowers = {h.lower(): h for h in actual_headers}
    for a in aliases:
        if a.lower() in lowers:
            return lowers[a.lower()]
    return None


def _ensure_runlog_header(ws_log: gspread.Worksheet) -> None:
    current = _with_sheets_retry(
        lambda: ws_log.row_values(1),
        action=f"runlog.row_values:{ws_log.title}",
    )
    if current != RUNLOG_HEADER_18:
        _with_sheets_retry(
            lambda: ws_log.update(range_name="A1:R1", values=[RUNLOG_HEADER_18]),
            action=f"runlog.header_update:{ws_log.title}",
        )


class RunLogger18:
    def __init__(
        self,
        ws_log: gspread.Worksheet,
        run_id: str,
        job_name: str,
        site_code: str,
        tz_name: str,
        flush_every: int = 100,
    ):
        self.ws_log = ws_log
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.tz_name = tz_name
        self.flush_every = flush_every
        self.buf: List[List[Any]] = []
        _ensure_runlog_header(ws_log)

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
        self.buf.append(
            [
                self.run_id,
                _now_str(self.tz_name),
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
        )
        if len(self.buf) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        rows = list(self.buf)
        _with_sheets_retry(
            lambda: self.ws_log.append_rows(
                rows,
                value_input_option="RAW",
                table_range="A:R",
            ),
            action=f"runlog.append_rows:{self.ws_log.title}",
        )
        self.buf = []


def _worksheet_values_matrix(ws: gspread.Worksheet) -> List[List[str]]:
    return _with_sheets_retry(
        ws.get_all_values,
        action=f"worksheet.get_all_values:{ws.title}",
    )


def _write_matrix_preserve_shape(
    ws: gspread.Worksheet,
    matrix: List[List[Any]],
) -> None:
    matrix = [[_safe_str(v) for v in row] for row in matrix]
    old = _worksheet_values_matrix(ws)
    old_rows = len(old)
    old_cols = max((len(r) for r in old), default=0)

    new_rows = len(matrix)
    new_cols = max((len(r) for r in matrix), default=0)

    target_rows = max(old_rows, new_rows, 1)
    target_cols = max(old_cols, new_cols, 1)

    _with_sheets_retry(
        lambda: ws.resize(rows=target_rows, cols=target_cols),
        action=f"worksheet.resize:{ws.title}",
    )

    if new_rows > 0 and new_cols > 0:
        end_col = _a1_col(new_cols)
        _with_sheets_retry(
            lambda: ws.update(
                range_name=f"A1:{end_col}{new_rows}",
                values=matrix,
                value_input_option="RAW",
            ),
            action=f"worksheet.update_main:{ws.title}",
        )

    if old_rows > new_rows and new_cols > 0:
        end_col = _a1_col(new_cols)
        blank_rows = [[""] * new_cols for _ in range(old_rows - new_rows)]
        _with_sheets_retry(
            lambda: ws.update(
                range_name=f"A{new_rows+1}:{end_col}{old_rows}",
                values=blank_rows,
                value_input_option="RAW",
            ),
            action=f"worksheet.clear_tail_rows:{ws.title}",
        )

    if old_cols > new_cols and new_rows > 0:
        start_col = _a1_col(new_cols + 1)
        end_col = _a1_col(old_cols)
        blank_extra = [[""] * (old_cols - new_cols) for _ in range(new_rows)]
        _with_sheets_retry(
            lambda: ws.update(
                range_name=f"{start_col}1:{end_col}{new_rows}",
                values=blank_extra,
                value_input_option="RAW",
            ),
            action=f"worksheet.clear_tail_cols:{ws.title}",
        )


def _upsert_cfg_fields(
    ws_fields: gspread.Worksheet,
    df_defs: pd.DataFrame,
    strict: bool,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    raw = _with_sheets_retry(
        ws_fields.get_all_values,
        action=f"cfg_fields.get_all_values:{ws_fields.title}",
    )
    warnings: List[str] = []

    if not raw:
        raise ValueError("Cfg__Fields is empty; header row required.")

    header = [str(x).strip() for x in raw[0]]
    rows = raw[1:]
    df_cfg = pd.DataFrame(rows, columns=header)
    df_cfg = _normalize_df_columns(df_cfg)

    col_entity = _pick_col(header, ["entity_type", "owner_type", "entity"])
    col_field_key = _pick_col(header, ["field_key", "fieldkey", "system_key"])
    col_name = _pick_col(header, ["display_name", "display name", "name", "label"])
    col_source_type = _pick_col(header, ["source_type", "field_source", "source"])
    col_namespace = _pick_col(header, ["namespace", "ns"])
    col_key = _pick_col(header, ["key", "mf_key", "metafield_key"])
    col_field_type = _pick_col(header, ["field_type", "type", "value_type", "data_type"])
    col_required = _pick_col(header, ["required", "is_required", "required?"])
    col_desc = _pick_col(header, ["description", "desc", "notes"])
    col_entity_name = _pick_col(header, ["entity_name", "owner_name"])
    col_source_ref_type = _pick_col(header, ["source_ref_type", "ref_type"])

    must_missing = []
    if not col_entity:
        must_missing.append("entity_type")
    if not col_field_key:
        must_missing.append("field_key")

    if must_missing:
        raise ValueError(f"Cfg__Fields missing MUST columns: {must_missing}")

    optional_missing = []
    check_map = {
        "display_name": col_name,
        "source_type": col_source_type,
        "namespace": col_namespace,
        "key": col_key,
        "field_type": col_field_type,
        "required": col_required,
        "description": col_desc,
    }
    for k, v in check_map.items():
        if not v:
            optional_missing.append(k)

    if optional_missing:
        msg = f"Cfg__Fields optional columns not found: {optional_missing}"
        if strict:
            raise ValueError(msg)
        warnings.append(msg)

    if df_cfg.empty:
        df_cfg = pd.DataFrame(columns=header)

    if col_entity not in df_cfg.columns:
        df_cfg[col_entity] = ""
    if col_field_key not in df_cfg.columns:
        df_cfg[col_field_key] = ""

    df_cfg[col_entity] = df_cfg[col_entity].astype(str).str.strip().str.upper()
    df_cfg[col_field_key] = df_cfg[col_field_key].astype(str).str.strip()

    upserts: List[Dict[str, Any]] = []
    for _, r in df_defs.iterrows():
        mo_type = _safe_str(r["type"])
        fd_key = _safe_str(r["field_key"])
        field_name = _safe_str(r["field_name"])
        field_type = _safe_str(r["field_type"])
        required = _safe_str(r["required"])
        description = _safe_str(r["description"])

        x: Dict[str, Any] = {
            col_entity: "METAOBJECT_ENTRY",
            col_field_key: f"mo.{mo_type}.{fd_key}",
        }
        if col_name:
            x[col_name] = field_name or fd_key
        if col_source_type:
            x[col_source_type] = "METAOBJECT_REF"
        if col_namespace:
            x[col_namespace] = mo_type
        if col_key:
            x[col_key] = fd_key
        if col_field_type:
            x[col_field_type] = field_type
        if col_required:
            x[col_required] = required
        if col_desc:
            x[col_desc] = description
        if col_entity_name:
            x[col_entity_name] = mo_type
        if col_source_ref_type:
            x[col_source_ref_type] = "METAOBJECT_DEFINITION"

        upserts.append(x)

    df_up = pd.DataFrame(upserts)
    existing_keys = (df_cfg[col_entity] + "||" + df_cfg[col_field_key]).tolist()
    key_to_idx = {k: i for i, k in enumerate(existing_keys)}

    updated = 0
    inserted = 0

    for _, row in df_up.iterrows():
        k = f"{row[col_entity]}||{row[col_field_key]}"
        if k in key_to_idx:
            idx = key_to_idx[k]
            for col, val in row.items():
                df_cfg.at[idx, col] = _safe_str(val)
            updated += 1
        else:
            new_row = {c: "" for c in df_cfg.columns}
            for col, val in row.items():
                if col in new_row:
                    new_row[col] = _safe_str(val)
            df_cfg = pd.concat([df_cfg, pd.DataFrame([new_row])], ignore_index=True)
            inserted += 1

    stats = {
        "updated": updated,
        "inserted": inserted,
        "rows_after": len(df_cfg),
    }
    return df_cfg, stats, warnings


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    shop_domain: Optional[str] = None,
    api_version: Optional[str] = None,
    gsheet_sa_b64_secret: Optional[str] = None,
    shopify_token_secret: Optional[str] = None,
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_cfg_metaobject_defs: str = "Cfg__MetaobjectDefs",
    tab_runlog: str = "Ops__RunLog",
    config_sheet_label: str = "config",
    runlog_sheet_label: str = "runlog_sheet",
    page_size: int = 50,
    preview_rows: int = 20,
    sync_cfg_fields: bool = True,
    strict: bool = True,
    dry_run: bool = True,
    confirmed: bool = False,
    write_mode: str = "OVERWRITE",
    excluded_type_contains: Tuple[str, ...] = ("shopify",),
    tz_name: str = "Asia/Shanghai",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    project_code: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> Dict[str, Any]:
    phase = "preview" if dry_run else "apply"

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 7, f"Start Metaobject Definitions sync | site={site_code} | phase={phase}")

    if not dry_run and not confirmed:
        raise ValueError("Apply mode requires confirmed=True.")

    if write_mode.upper() != "OVERWRITE":
        raise ValueError("write_mode currently only supports OVERWRITE.")

    run_id = run_id or _gen_run_id(job_name, tz_name)

    # Bootstrap is required because Cfg__account_id itself lives in Google Sheets.
    # Do not hide mismatch: after reading Cfg__account_id, we validate the secret name.
    bootstrap_sa_b64 = _load_secret(
        bootstrap_gsheet_sa_b64_secret,
        explicit_value=sa_b64_value,
        project_code=project_code,
        secret_home=secret_home,
    )
    gc = _build_gspread_client(bootstrap_sa_b64)

    progress(2, 7, f"Read {tab_cfg_account_id} and resolve account config")
    account = _load_account_config(
        gc=gc,
        console_core_url=console_core_url,
        tab_cfg_account_id=tab_cfg_account_id,
    )

    resolved_shop_domain = (shop_domain or account.shop_domain).strip()
    resolved_api_version = (api_version or account.api_version).strip()
    resolved_gsheet_sa_secret = (gsheet_sa_b64_secret or account.gsheet_sa_b64_secret).strip()
    resolved_shopify_token_secret = (shopify_token_secret or account.shopify_token_secret).strip()

    if not resolved_shop_domain:
        raise ValueError("Resolved SHOP_DOMAIN is empty.")
    if not resolved_api_version:
        raise ValueError("Resolved SHOPIFY_API_VERSION is empty.")
    if not resolved_gsheet_sa_secret:
        raise ValueError("Resolved GSHEET_SA_B64_SECRET is empty.")
    if not resolved_shopify_token_secret:
        raise ValueError("Resolved SHOPIFY_TOKEN_SECRET is empty.")

    if resolved_gsheet_sa_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "BOOTSTRAP_GSHEET_SA_B64_SECRET does not match Cfg__account_id.GSHEET_SA_B64_SECRET. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}, cfg={resolved_gsheet_sa_secret}"
        )

    shopify_token = _load_secret(
        resolved_shopify_token_secret,
        explicit_value=shopify_token_value,
        project_code=project_code,
        secret_home=secret_home,
    )

    progress(3, 7, "Resolve config and RunLog sheet routes")
    route = _resolve_site_routing(
        gc=gc,
        console_core_url=console_core_url,
        tab_cfg_sites=tab_cfg_sites,
        site_code=site_code,
        config_sheet_label=config_sheet_label,
        runlog_sheet_label=runlog_sheet_label,
    )

    sh_cfg = _with_sheets_retry(
        lambda: gc.open_by_url(route.config_sheet_url),
        action="run.open_config_sheet",
    )
    sh_log = _with_sheets_retry(
        lambda: gc.open_by_url(route.runlog_sheet_url),
        action="run.open_runlog_sheet",
    )

    ws_defs = _with_sheets_retry(
        lambda: sh_cfg.worksheet(tab_cfg_metaobject_defs),
        action=f"run.worksheet:{tab_cfg_metaobject_defs}",
    )
    ws_log = _with_sheets_retry(
        lambda: sh_log.worksheet(tab_runlog),
        action=f"run.worksheet:{tab_runlog}",
    )
    logger = RunLogger18(
        ws_log=ws_log,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )

    gql_client = ShopifyGraphQLClient(
        shop_domain=resolved_shop_domain,
        api_version=resolved_api_version,
        access_token=shopify_token,
    )

    warnings: List[str] = []
    detail_counter: Dict[str, int] = {}

    def log_detail_once(error_reason: str, entity_type: str = "", gid: str = "", field_key: str = "", message: str = ""):
        n = detail_counter.get(error_reason, 0)
        if n >= 2:
            return
        logger.log(
            phase=phase,
            log_type="detail",
            status="WARN" if error_reason else "OK",
            entity_type=entity_type,
            gid=gid,
            field_key=field_key,
            message=message,
            error_reason=error_reason,
        )
        detail_counter[error_reason] = n + 1

    try:
        progress(4, 7, f"Fetch Shopify Metaobject Definitions | page_size={page_size}")
        nodes_all = _fetch_all_metaobject_defs(gql_client, page_size=page_size)
        nodes, rows_excluded = _filter_metaobject_defs(
            nodes_all,
            excluded_type_contains=excluded_type_contains,
        )
        df_defs = _build_defs_df(nodes, tz_name=tz_name)

        rows_loaded = len(nodes_all)
        rows_recognized = len(df_defs)
        rows_planned = len(df_defs)
        rows_written = 0
        rows_skipped = 0

        defs_header = [
            "gid",
            "type",
            "type_name",
            "field_key",
            "field_name",
            "field_type",
            "required",
            "description",
            "updated_at",
            "synced_at",
        ]
        defs_matrix = [defs_header] + df_defs[defs_header].fillna("").astype(str).values.tolist()

        cfg_fields_stats = {"updated": 0, "inserted": 0, "rows_after": 0}
        df_cfg_after = None

        progress(
            5,
            7,
            f"Build definitions and Cfg__Fields plan | defs={len(df_defs)} | excluded={rows_excluded}",
        )

        if sync_cfg_fields:
            ws_fields = _with_sheets_retry(
                lambda: sh_cfg.worksheet(tab_cfg_fields),
                action=f"run.worksheet:{tab_cfg_fields}",
            )
            df_cfg_after, cfg_fields_stats, field_warnings = _upsert_cfg_fields(
                ws_fields=ws_fields,
                df_defs=df_defs,
                strict=strict,
            )
            warnings.extend(field_warnings)
            for w in field_warnings:
                log_detail_once("CFG_FIELDS_OPTIONAL_COLUMNS_MISSING", entity_type="CFG_FIELDS", message=w)

        preview_defs = df_defs.head(preview_rows).copy()
        preview_fields = None
        if df_cfg_after is not None:
            col_entity = _pick_col(df_cfg_after.columns.tolist(), ["entity_type", "owner_type", "entity"])
            col_field_key = _pick_col(df_cfg_after.columns.tolist(), ["field_key", "fieldkey", "system_key"])
            if col_entity and col_field_key:
                preview_fields = (
                    df_cfg_after[
                        (df_cfg_after[col_entity].astype(str).str.upper() == "METAOBJECT_ENTRY")
                        & (df_cfg_after[col_field_key].astype(str).str.startswith("mo."))
                    ]
                    .head(preview_rows)
                    .copy()
                )

        if dry_run:
            progress(6, 7, "Preview only; no Cfg__MetaobjectDefs / Cfg__Fields values changed")
            rows_skipped = rows_planned
            logger.log(
                phase=phase,
                log_type="summary",
                status="OK",
                entity_type="METAOBJECT_DEF",
                rows_loaded=rows_loaded,
                rows_pending=rows_planned,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=0,
                rows_skipped=rows_skipped,
                message=(
                    f"dry_run preview only | defs_rows={len(df_defs)} | "
                    f"excluded_types={rows_excluded} | "
                    f"cfg_fields_updated={cfg_fields_stats['updated']} | "
                    f"cfg_fields_inserted={cfg_fields_stats['inserted']}"
                ),
                error_reason="",
            )
            logger.flush()
            progress(7, 7, "Preview complete")

            return {
                "ok": True,
                "phase": phase,
                "run_id": run_id,
                "job_name": job_name,
                "site_code": site_code,
                "summary": {
                    "rows_loaded": rows_loaded,
                    "rows_recognized": rows_recognized,
                    "rows_planned": rows_planned,
                    "rows_written": 0,
                    "rows_skipped": rows_skipped,
                    "rows_excluded": rows_excluded,
                    "cfg_fields_updated": cfg_fields_stats["updated"],
                    "cfg_fields_inserted": cfg_fields_stats["inserted"],
                    "warnings_count": len(warnings),
                },
                "preview": {
                    "metaobject_defs": preview_defs,
                    "cfg_fields": preview_fields,
                },
                "warnings": warnings,
                "targets": {
                    "config_sheet_url": route.config_sheet_url,
                    "runlog_sheet_url": route.runlog_sheet_url,
                    "tab_cfg_account_id": tab_cfg_account_id,
                    "tab_cfg_metaobject_defs": tab_cfg_metaobject_defs,
                    "tab_cfg_fields": tab_cfg_fields,
                    "tab_runlog": tab_runlog,
                    "shop_domain": resolved_shop_domain,
                    "api_version": resolved_api_version,
                    "gsheet_sa_b64_secret": resolved_gsheet_sa_secret,
                    "shopify_token_secret": resolved_shopify_token_secret,
                    "excluded_type_contains": list(excluded_type_contains),
                },
            }

        progress(
            6,
            7,
            f"Apply overwrite | {tab_cfg_metaobject_defs} rows={len(df_defs)} | sync_cfg_fields={sync_cfg_fields}",
        )
        _write_matrix_preserve_shape(ws_defs, defs_matrix)
        rows_written += len(df_defs)

        if sync_cfg_fields and df_cfg_after is not None:
            cfg_matrix = [df_cfg_after.columns.tolist()] + df_cfg_after.fillna("").astype(str).values.tolist()
            ws_fields = _with_sheets_retry(
                lambda: sh_cfg.worksheet(tab_cfg_fields),
                action=f"run.worksheet:{tab_cfg_fields}",
            )
            _write_matrix_preserve_shape(ws_fields, cfg_matrix)

        logger.log(
            phase=phase,
            log_type="summary",
            status="OK",
            entity_type="METAOBJECT_DEF",
            rows_loaded=rows_loaded,
            rows_pending=rows_planned,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=0,
            message=(
                f"applied | defs_rows={len(df_defs)} | "
                f"cfg_fields_updated={cfg_fields_stats['updated']} | "
                f"cfg_fields_inserted={cfg_fields_stats['inserted']}"
            ),
            error_reason="",
        )
        logger.flush()
        progress(7, 7, "Apply complete")

        return {
            "ok": True,
            "phase": phase,
            "run_id": run_id,
            "job_name": job_name,
            "site_code": site_code,
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_written": rows_written,
                "rows_skipped": 0,
                "rows_excluded": rows_excluded,
                "cfg_fields_updated": cfg_fields_stats["updated"],
                "cfg_fields_inserted": cfg_fields_stats["inserted"],
                "warnings_count": len(warnings),
            },
            "preview": {
                "metaobject_defs": preview_defs,
                "cfg_fields": preview_fields,
            },
            "warnings": warnings,
            "targets": {
                "config_sheet_url": route.config_sheet_url,
                "runlog_sheet_url": route.runlog_sheet_url,
                "tab_cfg_metaobject_defs": tab_cfg_metaobject_defs,
                "tab_cfg_fields": tab_cfg_fields,
                "tab_runlog": tab_runlog,
            },
        }

    except Exception as e:
        msg = str(e)
        logger.log(
            phase=phase,
            log_type="summary",
            status="FAIL",
            entity_type="METAOBJECT_DEF",
            rows_loaded="",
            rows_pending="",
            rows_recognized="",
            rows_planned="",
            rows_written="",
            rows_skipped="",
            message=msg,
            error_reason="JOB_FAILED",
        )
        logger.flush()
        raise
