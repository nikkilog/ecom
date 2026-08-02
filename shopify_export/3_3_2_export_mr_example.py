# shopify_export/export_mr_example.py

import os
import re
import json
import time
import base64
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Iterable, Tuple

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# Basic utils
# =========================================================

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

SUPPORTED_OWNER_ENTITY_TYPES = {"PRODUCT", "COLLECTION", "PAGE"}
JOB_NAME = "export_mr_example"
MODULE_PATH = "shopify_export.3_3_2_export_mr_example"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
DEFAULT_JOB_NAME = JOB_NAME

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _now_cn_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _norm(x) -> str:
    return str(x).strip() if x is not None else ""


def _norm_lower(x) -> str:
    return _norm(x).lower()


def _is_blank(x) -> bool:
    return _norm(x) == ""


def _pick_first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_lower = {_norm_lower(c): c for c in df.columns}
    for c in candidates:
        if _norm_lower(c) in cols_lower:
            return cols_lower[_norm_lower(c)]
    return None


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]
    return df


def _safe_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _json_dumps(v) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def _tail_numeric_from_gid(gid: str) -> str:
    s = _norm(gid)
    if "/" in s:
        return s.rsplit("/", 1)[-1]
    return s


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
    return _norm(value).upper()


def _normalize_registry_header(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", _norm(value).lower()).strip()


def _extract_spreadsheet_id(value: Any) -> str:
    text = _norm(value)
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
    """Resolve one Secret without exposing its value."""
    secret_name = _norm(name)
    resolved_project_code = _normalize_project_code(project_code)
    if not secret_name:
        raise RuntimeError("Secret name is empty.")
    if not resolved_project_code:
        raise RuntimeError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _norm(explicit_value):
        return SecretValue(_norm(explicit_value), "EXPLICIT_VALUE", "caller")

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
    raw = _norm(raw_value)
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
        "read requests per minute",
        "write requests per minute",
        "too many requests",
    )
    return any(token in text for token in retry_tokens)


def _with_sheets_retry(
    operation,
    *,
    action: str,
    max_attempts: int = 8,
    base_sleep: float = 1.2,
    max_delay: float = 20.0,
):
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if not _is_retryable_sheets_error(exc) or attempt >= attempts:
                raise
            delay = min(float(max_delay), float(base_sleep) * (2 ** (attempt - 1))) + random.random()
            status = _sheets_error_status(exc)
            reason = f"HTTP {status}" if status is not None else type(exc).__name__
            print(
                "[Sheets retry] "
                f"action={action} | attempt={attempt}/{attempts} | "
                f"reason={reason} | sleep={delay:.1f}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"Sheets operation exhausted retries: {action}")


def _build_gspread_client_from_value(raw_value: str) -> gspread.Client:
    info, _secret_format = _parse_service_account_text(raw_value)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def _build_gspread_client_from_secret(
    secret: SecretValue,
) -> Tuple[gspread.Client, Dict[str, str]]:
    info, secret_format = _parse_service_account_text(secret.value)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc, {
        "source_type": secret.source_type,
        "source_detail": secret.source_detail,
        "secret_format": secret_format,
        "service_account_email": _norm(info.get("client_email")),
    }


def _load_runtime_account_config(
    gc: gspread.Client,
    console_core_url: str,
    account_tab: str,
) -> AccountConfig:
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="account_config.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(account_tab),
        action=f"account_config.worksheet:{account_tab}",
    )
    rows = _with_sheets_retry(
        ws.get_all_values,
        action=f"account_config.get_all_values:{account_tab}",
    )

    out: Dict[str, str] = {}
    duplicates: List[str] = []
    for row_number, row in enumerate(rows, start=1):
        if not row:
            continue
        key = _norm(row[0] if len(row) >= 1 else "")
        value = _norm(row[1] if len(row) >= 2 else "")
        if not key or key.lower() in {"key", "config_key", "field_key", "name"}:
            continue
        if key in out:
            duplicates.append(f"{key}@row{row_number}")
        out[key] = value

    if duplicates:
        raise ValueError(f"Duplicated keys in {account_tab}: {duplicates}")

    required = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "GSHEET_SA_B64_SECRET",
        "SHOPIFY_TOKEN_SECRET",
    ]
    missing = [key for key in required if not _norm(out.get(key))]
    if missing:
        raise ValueError(f"{account_tab} missing required values: {missing}")

    return AccountConfig(
        shop_domain=out["SHOP_DOMAIN"],
        api_version=out["SHOPIFY_API_VERSION"],
        gsheet_sa_b64_secret=out["GSHEET_SA_B64_SECRET"],
        shopify_token_secret=out["SHOPIFY_TOKEN_SECRET"],
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
    duplicates: List[str] = []
    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if not normalized:
            continue
        if normalized in header_map:
            duplicates.append(normalized)
        header_map[normalized] = index
    if duplicates:
        raise ValueError(
            "Workspace Project Registry has duplicate normalized headers: "
            + ", ".join(sorted(set(duplicates)))
        )

    def require_col(*aliases: str) -> int:
        for alias in aliases:
            key = _normalize_registry_header(alias)
            if key in header_map:
                return header_map[key]
        raise ValueError(
            "Workspace Project Registry is missing required column; "
            f"accepted aliases={aliases}."
        )

    project_col = require_col("project_code", "project code")
    active_col = require_col("active", "is_active", "enabled")
    console_url_col = require_col("console_core_url", "console core url", "console_core")
    gsheet_secret_col = require_col("gsheet_secret_name", "gsheet secret name", "google secret name")
    account_tab_col = require_col("account_config_tab", "account config tab", "cfg account tab")
    timezone_col = require_col("timezone", "time zone")
    project_name_col = header_map.get(_normalize_registry_header("project_name"))

    width = len(values[0])
    matches: List[Tuple[int, List[str]]] = []
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
    if _norm(row[active_col]).lower() not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}, row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": _norm(row[project_name_col]) if project_name_col is not None else "",
        "console_core_url": _norm(row[console_url_col]),
        "gsheet_secret_name": _norm(row[gsheet_secret_col]),
        "account_config_tab": _norm(row[account_tab_col]),
        "timezone": _norm(row[timezone_col]),
        "registry_source_row": str(source_row),
        "workspace_auth_source_type": _norm(auth_meta.get("source_type")),
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
    account = _load_runtime_account_config(
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
        "google_client": gc,
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
    explicit_sa_value: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, Any]:
    """Check/update exactly one existing Registry row; never append."""
    mode = _norm(registry_mode).upper() or "OFF"
    allowed = {"OFF", "CHECK", "UPDATE_URL", "UPDATE_URL_AND_NAME"}
    if mode not in allowed:
        raise ValueError(f"registry_mode must be one of {sorted(allowed)}.")

    if mode == "OFF":
        if print_progress:
            print(
                "[Registry] mode=OFF | "
                f"job_name={job_name} | sheet_label={sheet_label} | tab_name={tab_name}"
            )
        return {"status": "OFF", "target_row": None, "changed_fields": []}

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _norm(current_colab_url):
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _norm(current_colab_name):
        raise ValueError("UPDATE_URL_AND_NAME requires current_colab_name.")

    secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
    )
    gc = _build_gspread_client_from_value(secret.value)

    book = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="registry.open_console",
    )
    ws = _with_sheets_retry(
        lambda: book.worksheet(registry_tab),
        action="registry.open_tab",
    )
    values = _with_sheets_retry(
        ws.get_all_values,
        action="registry.read",
    )
    if not values:
        raise ValueError(f"Registry tab {registry_tab!r} is empty.")

    header_map: Dict[str, int] = {}
    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if not normalized:
            continue
        if normalized in header_map:
            raise ValueError(f"Registry tab has duplicate normalized header: {normalized}.")
        header_map[normalized] = index

    def require_column(*aliases: str) -> int:
        for alias in aliases:
            key = _normalize_registry_header(alias)
            if key in header_map:
                return header_map[key]
        raise ValueError(
            "Registry tab is missing required column; "
            f"accepted aliases={aliases}."
        )

    job_col = require_column("job_name", "job name")
    label_col = require_column("sheet_label", "sheet label")
    tab_col = require_column("Tab name", "sheet name", "sheet_name")
    url_col = require_column("colab_url", "colab url")
    name_col = require_column("colab_name", "colab name")

    wanted = (
        _norm(job_name).lower(),
        _norm(sheet_label).lower(),
        _norm(tab_name).lower(),
    )
    matches: List[int] = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(values[0]) - len(row))
        logical_key = (
            _norm(padded[job_col]).lower(),
            _norm(padded[label_col]).lower(),
            _norm(padded[tab_col]).lower(),
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
    provided_url = _norm(current_colab_url)
    provided_name = _norm(current_colab_name)

    if provided_url and _norm(current_row[url_col]) != provided_url:
        changes.append(("colab_url", url_col + 1, _norm(current_row[url_col]), provided_url))
    if provided_name and _norm(current_row[name_col]) != provided_name:
        changes.append(("colab_name", name_col + 1, _norm(current_row[name_col]), provided_name))

    if mode == "CHECK":
        status = "CHANGE_DETECTED" if changes else "NO_CHANGE"
    else:
        permitted = {"colab_url"} if mode == "UPDATE_URL" else {"colab_url", "colab_name"}
        applied = [change for change in changes if change[0] in permitted]
        for field_name, column_number, _old_value, new_value in applied:
            _with_sheets_retry(
                lambda rn=row_number, cn=column_number, nv=new_value: ws.update_cell(rn, cn, nv),
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
    }



# =========================================================
# Auth / Clients
# =========================================================

def build_gspread_client_from_b64_secret(gsheet_sa_b64: str):
    """Backward-compatible client builder; accepts Base64 JSON or raw JSON."""
    return _build_gspread_client_from_value(gsheet_sa_b64)



def shopify_graphql(
    shop_domain: str,
    api_version: str,
    token: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    timeout: int = 120,
) -> Dict[str, Any]:
    url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }
    resp = requests.post(
        url,
        headers=headers,
        json={"query": query, "variables": variables or {}},
        timeout=timeout,
    )

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Shopify 返回非 JSON：HTTP {resp.status_code} / {resp.text[:500]}")

    if resp.status_code != 200:
        raise RuntimeError(f"Shopify GraphQL HTTP {resp.status_code}: {_json_dumps(data)[:1200]}")

    if data.get("errors"):
        raise RuntimeError(f"Shopify GraphQL errors: {_json_dumps(data['errors'])[:1600]}")

    return data.get("data") or {}


# =========================================================
# Google Sheets helpers
# =========================================================

def open_ws_by_url_and_title(gc, spreadsheet_url: str, worksheet_title: str):
    ss = _with_sheets_retry(
        lambda: gc.open_by_url(spreadsheet_url),
        action=f"sheet.open:{worksheet_title}",
    )
    return _with_sheets_retry(
        lambda: ss.worksheet(worksheet_title),
        action=f"worksheet.open:{worksheet_title}",
    )



def open_first_existing_ws_by_urls_and_title(gc, spreadsheet_urls: List[str], worksheet_title: str):
    """Open a worksheet title from the first spreadsheet URL that contains it.

    This is used for Console Core runtime tabs that may live either in the
    Console Core itself or in the sheet routed by Cfg__Sites label=config.
    """
    checked = []
    seen = set()
    for url in spreadsheet_urls:
        url = _norm(url)
        if not url or url in seen:
            continue
        seen.add(url)
        checked.append(url)
        try:
            return open_ws_by_url_and_title(gc, url, worksheet_title)
        except Exception as e:
            # gspread raises WorksheetNotFound when the spreadsheet exists but
            # the requested tab is absent. Continue to the next candidate URL.
            if e.__class__.__name__ == "WorksheetNotFound":
                continue
            raise

    raise RuntimeError(
        f"找不到 worksheet={worksheet_title}；已检查 spreadsheet 数量={len(checked)}。"
        "请确认该 tab 在 Console Core 或 config label 指向的表内。"
    )


def open_ss_by_url(gc, spreadsheet_url: str):
    return _with_sheets_retry(
        lambda: gc.open_by_url(spreadsheet_url),
        action="sheet.open_by_url",
    )



def ws_records(ws) -> pd.DataFrame:
    rows = _with_sheets_retry(
        lambda: ws.get_all_records(default_blank=""),
        action=f"worksheet.get_all_records:{getattr(ws, 'title', '')}",
    )
    return pd.DataFrame(rows)



def ensure_runlog_header(ws):
    values = _with_sheets_retry(
        ws.get_all_values,
        action="runlog.get_all_values",
    )
    if not values:
        _with_sheets_retry(
            lambda: ws.update(
                values=[RUNLOG_HEADERS],
                range_name="A1",
                value_input_option="RAW",
            ),
            action="runlog.write_header",
        )
        return

    header = values[0]
    if header[: len(RUNLOG_HEADERS)] != RUNLOG_HEADERS:
        _with_sheets_retry(ws.clear, action="runlog.clear_for_header")
        _with_sheets_retry(
            lambda: ws.update(
                values=[RUNLOG_HEADERS],
                range_name="A1",
                value_input_option="RAW",
            ),
            action="runlog.rewrite_header",
        )



def append_runlog_rows(ws, rows: List[Dict[str, Any]]):
    ensure_runlog_header(ws)
    data = []
    for r in rows:
        data.append([_norm(r.get(h, "")) for h in RUNLOG_HEADERS])

    if data:
        existing = _with_sheets_retry(
            ws.get_all_values,
            action="runlog.read_before_append",
        )
        start_row = len(existing) + 1
        _with_sheets_retry(
            lambda: ws.update(
                values=data,
                range_name=f"A{start_row}",
                value_input_option="RAW",
            ),
            action="runlog.append_rows",
        )



def write_df_to_ws(ws, df: pd.DataFrame, clear_first: bool = True):
    if clear_first:
        _with_sheets_retry(ws.clear, action=f"worksheet.clear:{getattr(ws, 'title', '')}")

    if df is None or df.empty:
        _with_sheets_retry(
            lambda: ws.update(
                values=[list(df.columns) if df is not None else []],
                range_name="A1",
                value_input_option="RAW",
            ),
            action=f"worksheet.write_empty:{getattr(ws, 'title', '')}",
        )
        return

    values = [df.columns.tolist()] + df.astype(str).fillna("").values.tolist()
    required_rows = max(1, len(values))
    required_cols = max(1, len(values[0]) if values else 1)
    current_rows = int(getattr(ws, "row_count", 0) or 0)
    current_cols = int(getattr(ws, "col_count", 0) or 0)
    if current_rows < required_rows or current_cols < required_cols:
        _with_sheets_retry(
            lambda: ws.resize(
                rows=max(current_rows, required_rows),
                cols=max(current_cols, required_cols),
            ),
            action=f"worksheet.resize:{getattr(ws, 'title', '')}",
        )
    _with_sheets_retry(
        lambda: ws.update(
            values=values,
            range_name="A1",
            value_input_option="RAW",
        ),
        action=f"worksheet.write_df:{getattr(ws, 'title', '')}",
    )



# =========================================================
# Config discovery
# =========================================================

def get_site_targets(df_sites: pd.DataFrame, site_code: str) -> Dict[str, str]:
    """
    Resolve sheet routing from Cfg__Sites.

    Console Core standard:
    - Cfg__Sites controls label -> sheet_url routing.
    - Shopify runtime values such as SHOP_DOMAIN normally live in Cfg__account_id.

    Older versions incorrectly required shop_domain directly in Cfg__Sites.
    This function now keeps shop_domain optional and lets run() resolve it from
    Cfg__account_id when it is not present in Cfg__Sites.
    """
    df = _normalize_headers(df_sites)

    col_site = _pick_first_existing_col(df, ["site_code", "site", "code"])
    col_label = _pick_first_existing_col(df, ["label"])
    col_sheet_url = _pick_first_existing_col(df, ["sheet_url"])
    col_site_url = _pick_first_existing_col(df, ["site_url"])
    col_shop_domain = _pick_first_existing_col(
        df,
        ["shop_domain", "shopify_domain", "myshopify_domain", "shop", "domain"],
    )

    if not col_site or not col_label or not col_sheet_url:
        raise RuntimeError("Cfg__Sites 缺少必要字段：site_code / label / sheet_url")

    dfx = df[df[col_site].astype(str).str.strip().str.upper() == site_code.strip().upper()].copy()
    if dfx.empty:
        raise RuntimeError(f"Cfg__Sites 找不到 site_code={site_code}")

    targets = {}
    site_url = ""
    shop_domain = ""

    for _, r in dfx.iterrows():
        label = _norm(r[col_label])
        sheet_url = _norm(r[col_sheet_url])
        if label:
            targets[label] = sheet_url

        if col_site_url and not site_url:
            site_url = _norm(r[col_site_url])

        if col_shop_domain and not shop_domain:
            shop_domain = _norm(r[col_shop_domain])

    required_labels = ["config", "export_other", "runlog_sheet"]
    miss = [x for x in required_labels if _is_blank(targets.get(x))]
    if miss:
        raise RuntimeError(f"Cfg__Sites 缺少这些 label 对应的 sheet_url：{miss}")

    return {
        "site_url": site_url,
        "shop_domain": normalize_shop_domain(shop_domain) if not _is_blank(shop_domain) else "",
        "config_url": targets["config"],
        "export_other_url": targets["export_other"],
        "runlog_url": targets["runlog_sheet"],
    }


def normalize_shop_domain(value: str) -> str:
    """Normalize Shopify shop domain to xxx.myshopify.com."""
    s = _norm(value)
    if not s:
        return ""

    s = re.sub(r"^https?://", "", s, flags=re.I).strip()

    # Allow an Admin URL such as admin.shopify.com/store/apollolift-us/ if it is
    # accidentally supplied instead of the pure myshopify domain.
    m = re.search(r"admin\.shopify\.com/store/([^/?#]+)", s, flags=re.I)
    if m:
        s = m.group(1)
    else:
        s = s.split("/", 1)[0]

    s = s.strip().strip("/")
    if not s:
        return ""

    if ".myshopify.com" not in s.lower():
        s = f"{s}.myshopify.com"
    return s


def read_key_value_config_ws(ws) -> Dict[str, str]:
    """Read a two-column key/value worksheet such as Cfg__account_id."""
    rows = ws.get_all_values()
    cfg = {}
    for row in rows:
        if not row:
            continue
        key = _norm(row[0])
        val = _norm(row[1]) if len(row) > 1 else ""
        if not key:
            continue
        # Keep the first nonblank value if duplicate keys exist.
        if key not in cfg or _is_blank(cfg.get(key)):
            cfg[key] = val
    return cfg


def resolve_shopify_runtime_config(
    *,
    targets: Dict[str, str],
    account_cfg: Dict[str, str],
    shopify_api_version: str,
) -> Tuple[str, str]:
    """
    Resolve Shopify runtime values from the correct config source.

    Priority:
    1. shop_domain in Cfg__Sites, if present for backward compatibility.
    2. SHOP_DOMAIN / MYSHOPIFY_DOMAIN / SHOPIFY_DOMAIN in Cfg__account_id.

    API version keeps the explicit Colab parameter first, then falls back to
    Cfg__account_id.SHOPIFY_API_VERSION.
    """
    shop_domain = _norm(targets.get("shop_domain"))
    if _is_blank(shop_domain):
        shop_domain = (
            account_cfg.get("SHOP_DOMAIN")
            or account_cfg.get("MYSHOPIFY_DOMAIN")
            or account_cfg.get("SHOPIFY_DOMAIN")
            or account_cfg.get("shop_domain")
            or account_cfg.get("myshopify_domain")
            or account_cfg.get("shopify_domain")
            or ""
        )

    shop_domain = normalize_shop_domain(shop_domain)
    if _is_blank(shop_domain):
        raise RuntimeError(
            "无法解析 Shopify 店铺域名：Cfg__Sites 没有 shop_domain，且 Cfg__account_id 没有 SHOP_DOMAIN / MYSHOPIFY_DOMAIN / SHOPIFY_DOMAIN"
        )

    api_version = _norm(shopify_api_version) or _norm(account_cfg.get("SHOPIFY_API_VERSION"))
    if _is_blank(api_version):
        raise RuntimeError("缺少 Shopify API Version：请在 Cell 1 或 Cfg__account_id.SHOPIFY_API_VERSION 配置")

    targets["shop_domain"] = shop_domain
    return shop_domain, api_version


def parse_metafield_key(metafield_key: str) -> Tuple[str, str]:
    s = _norm(metafield_key)
    if not s.startswith("mf."):
        raise RuntimeError(f"METAFIELD_KEY 只接受 mf. 前缀，当前为：{metafield_key}")

    s = s[3:]
    parts = s.split(".", 1)
    if len(parts) != 2:
        raise RuntimeError(f"METAFIELD_KEY 格式不对，应类似 mf.custom.breadcrumb_leaf，当前：{metafield_key}")
    return parts[0], parts[1]


def get_cfg_fields_info(
    df_fields: pd.DataFrame,
    owner_entity_type: str,
    metaobject_type: str,
    metafield_key: str,
) -> Dict[str, Any]:
    df = _normalize_headers(df_fields)

    col_entity_type = _pick_first_existing_col(df, ["entity_type"])
    col_field_key = _pick_first_existing_col(df, ["field_key"])
    col_source_type = _pick_first_existing_col(df, ["source_type"])
    col_namespace = _pick_first_existing_col(df, ["namespace"])
    col_key = _pick_first_existing_col(df, ["key"])
    col_seq = _pick_first_existing_col(df, ["seq"])
    col_display_name = _pick_first_existing_col(df, ["display_name", "name"])

    required = {
        "entity_type": col_entity_type,
        "field_key": col_field_key,
        "source_type": col_source_type,
        "namespace": col_namespace,
        "key": col_key,
    }
    miss = [k for k, v in required.items() if not v]
    if miss:
        raise RuntimeError(f"Cfg__Fields 缺少必要字段：{', '.join(miss)}")

    owner = owner_entity_type.strip().upper()

    df_mf = df[
        (df[col_entity_type].astype(str).str.strip().str.upper() == owner)
        & (df[col_field_key].astype(str).str.strip() == metafield_key)
    ].copy()

    if df_mf.empty:
        raise RuntimeError(
            f"Cfg__Fields 找不到精确匹配：entity_type={owner} + field_key={metafield_key}"
        )
    if len(df_mf) > 1:
        raise RuntimeError(
            f"Cfg__Fields 出现重复匹配：entity_type={owner} + field_key={metafield_key}，请先去重"
        )

    df_def = df[
        (df[col_entity_type].astype(str).str.strip().str.upper() == "METAOBJECT_ENTRY")
        & (df[col_source_type].astype(str).str.strip().str.upper() == "METAOBJECT_REF")
        & (df[col_namespace].astype(str).str.strip() == metaobject_type)
    ].copy()

    if df_def.empty:
        raise RuntimeError(
            f"Cfg__Fields 找不到 METAOBJECT_ENTRY + METAOBJECT_REF + namespace={metaobject_type} 的定义字段"
        )

    if col_seq:
        seq_series = pd.to_numeric(df_def[col_seq], errors="coerce").fillna(10**9)
        df_def = df_def.assign(__seq=seq_series).sort_values(["__seq", col_key], kind="stable")
    else:
        df_def = df_def.sort_values([col_key], kind="stable")

    field_order = [_norm(x) for x in df_def[col_key].tolist() if not _is_blank(x)]
    display_name_map = {}
    if col_display_name:
        for _, r in df_def.iterrows():
            display_name_map[_norm(r[col_key])] = _norm(r[col_display_name])

    namespace, key = parse_metafield_key(metafield_key)

    return {
        "namespace": namespace,
        "key": key,
        "field_order": field_order,
        "display_name_map": display_name_map,
    }


# =========================================================
# GraphQL queries
# =========================================================

PRODUCTS_QUERY = """
query ProductsWithTargetMetafield(
  $first: Int!,
  $after: String,
  $namespace: String!,
  $key: String!,
  $fieldRefListFirst: Int!,
  $metaobjectListFirst: Int!
) {
  products(first: $first, after: $after, sortKey: ID) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      legacyResourceId
      handle
      title
      metafield(namespace: $namespace, key: $key) {
        id
        type
        value
        reference {
          ... on Metaobject {
            id
            type
            handle
            displayName
            fields {
              key
              type
              value
              jsonValue
              reference {
                ... on Metaobject {
                  id
                  type
                  handle
                  displayName
                }
              }
              references(first: $fieldRefListFirst) {
                nodes {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
              }
            }
          }
        }
        references(first: $metaobjectListFirst) {
          nodes {
            ... on Metaobject {
              id
              type
              handle
              displayName
              fields {
                key
                type
                value
                jsonValue
                reference {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
                references(first: $fieldRefListFirst) {
                  nodes {
                    ... on Metaobject {
                      id
                      type
                      handle
                      displayName
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

COLLECTIONS_QUERY = """
query CollectionsWithTargetMetafield(
  $first: Int!,
  $after: String,
  $namespace: String!,
  $key: String!,
  $fieldRefListFirst: Int!,
  $metaobjectListFirst: Int!
) {
  collections(first: $first, after: $after, sortKey: ID) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      legacyResourceId
      handle
      title
      metafield(namespace: $namespace, key: $key) {
        id
        type
        value
        reference {
          ... on Metaobject {
            id
            type
            handle
            displayName
            fields {
              key
              type
              value
              jsonValue
              reference {
                ... on Metaobject {
                  id
                  type
                  handle
                  displayName
                }
              }
              references(first: $fieldRefListFirst) {
                nodes {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
              }
            }
          }
        }
        references(first: $metaobjectListFirst) {
          nodes {
            ... on Metaobject {
              id
              type
              handle
              displayName
              fields {
                key
                type
                value
                jsonValue
                reference {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
                references(first: $fieldRefListFirst) {
                  nodes {
                    ... on Metaobject {
                      id
                      type
                      handle
                      displayName
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

PAGES_QUERY = """
query PagesWithTargetMetafield(
  $first: Int!,
  $after: String,
  $namespace: String!,
  $key: String!,
  $fieldRefListFirst: Int!,
  $metaobjectListFirst: Int!
) {
  pages(first: $first, after: $after, sortKey: ID) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      metafield(namespace: $namespace, key: $key) {
        id
        type
        value
        reference {
          ... on Metaobject {
            id
            type
            handle
            displayName
            fields {
              key
              type
              value
              jsonValue
              reference {
                ... on Metaobject {
                  id
                  type
                  handle
                  displayName
                }
              }
              references(first: $fieldRefListFirst) {
                nodes {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
              }
            }
          }
        }
        references(first: $metaobjectListFirst) {
          nodes {
            ... on Metaobject {
              id
              type
              handle
              displayName
              fields {
                key
                type
                value
                jsonValue
                reference {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
                references(first: $fieldRefListFirst) {
                  nodes {
                    ... on Metaobject {
                      id
                      type
                      handle
                      displayName
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


# =========================================================
# Entity iterators
# =========================================================

def iter_owner_nodes(
    owner_entity_type: str,
    shop_domain: str,
    api_version: str,
    token: str,
    namespace: str,
    key: str,
    page_size: int = 80,
    metaobject_list_first: int = 50,
    field_ref_list_first: int = 50,
    sleep_seconds: float = 0.0,
) -> Iterable[Dict[str, Any]]:
    owner = owner_entity_type.strip().upper()

    if owner == "ORDER":
        raise RuntimeError("OWNER_ENTITY_TYPE=ORDER 暂未实现。原因：需要额外时间范围/查询条件，不能无界全量扫。")

    if owner == "PRODUCT":
        root_key = "products"
        query = PRODUCTS_QUERY
    elif owner == "COLLECTION":
        root_key = "collections"
        query = COLLECTIONS_QUERY
    elif owner == "PAGE":
        root_key = "pages"
        query = PAGES_QUERY
    else:
        raise RuntimeError(f"不支持的 OWNER_ENTITY_TYPE：{owner}")

    after = None
    while True:
        data = shopify_graphql(
            shop_domain=shop_domain,
            api_version=api_version,
            token=token,
            query=query,
            variables={
                "first": page_size,
                "after": after,
                "namespace": namespace,
                "key": key,
                "metaobjectListFirst": metaobject_list_first,
                "fieldRefListFirst": field_ref_list_first,
            },
        )
        root = (data or {}).get(root_key) or {}
        nodes = root.get("nodes") or []
        for node in nodes:
            yield node

        page_info = root.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


# =========================================================
# Metaobject flattening
# =========================================================

def _metaobject_brief_to_text(obj: Optional[Dict[str, Any]]) -> str:
    if not obj:
        return ""
    parts = [
        _norm(obj.get("type")),
        _norm(obj.get("handle")),
        _norm(obj.get("displayName")),
    ]
    parts = [x for x in parts if x]
    return " | ".join(parts)


def _field_value_to_text(field: Dict[str, Any], list_join_sep: str = " | ") -> str:
    if not field:
        return ""

    ref = field.get("reference")
    if ref:
        return _metaobject_brief_to_text(ref)

    refs = ((field.get("references") or {}).get("nodes")) or []
    if refs:
        return list_join_sep.join([_metaobject_brief_to_text(x) for x in refs if x])

    json_value = field.get("jsonValue")
    if isinstance(json_value, (dict, list)):
        return _json_dumps(json_value)

    value = field.get("value")
    if value is not None:
        return _norm(value)

    return ""


def flatten_metaobject_node(
    metaobject_node: Dict[str, Any],
    field_order: List[str],
    list_join_sep: str = " | ",
) -> Dict[str, Any]:
    fmap = {}
    for f in (metaobject_node.get("fields") or []):
        key = _norm(f.get("key"))
        fmap[key] = _field_value_to_text(f, list_join_sep=list_join_sep)

    ordered_map = {fk: _norm(fmap.get(fk, "")) for fk in field_order}

    return {
        "metaobject_gid": _norm(metaobject_node.get("id")),
        "metaobject_type": _norm(metaobject_node.get("type")),
        "metaobject_handle": _norm(metaobject_node.get("handle")),
        "metaobject_display_name": _norm(metaobject_node.get("displayName")),
        "field_map": ordered_map,
    }


def metafield_to_examples(
    metafield_obj: Dict[str, Any],
    field_order: List[str],
    list_join_sep: str = " | ",
) -> List[Dict[str, Any]]:
    if not metafield_obj:
        return []

    out = []

    ref = metafield_obj.get("reference")
    if ref and isinstance(ref, dict):
        ex = flatten_metaobject_node(ref, field_order=field_order, list_join_sep=list_join_sep)
        ex["metafield_value_text"] = _norm(metafield_obj.get("value"))
        out.append(ex)

    refs = ((metafield_obj.get("references") or {}).get("nodes")) or []
    for node in refs:
        if node and isinstance(node, dict):
            ex = flatten_metaobject_node(node, field_order=field_order, list_join_sep=list_join_sep)
            ex["metafield_value_text"] = _norm(metafield_obj.get("value"))
            out.append(ex)

    return out


# =========================================================
# Owner row building
# =========================================================

def owner_node_to_base_row(owner_entity_type: str, node: Dict[str, Any], metafield_key: str) -> Dict[str, Any]:
    owner = owner_entity_type.strip().upper()

    if owner == "PRODUCT":
        gid = _norm(node.get("id"))
        legacy_id = _norm(node.get("legacyResourceId"))
        handle = _norm(node.get("handle"))
        title = _norm(node.get("title"))
        extra = ""
    elif owner == "COLLECTION":
        gid = _norm(node.get("id"))
        legacy_id = _norm(node.get("legacyResourceId"))
        handle = _norm(node.get("handle"))
        title = _norm(node.get("title"))
        extra = ""
    elif owner == "PAGE":
        gid = _norm(node.get("id"))
        legacy_id = _tail_numeric_from_gid(gid)
        handle = _norm(node.get("handle"))
        title = _norm(node.get("title"))
        extra = ""
    else:
        raise RuntimeError(f"不支持的 OWNER_ENTITY_TYPE：{owner}")

    return {
        "Owner Entity Type": owner,
        "Owner GID": gid,
        "Owner ID (numeric)": legacy_id,
        "Owner Handle": handle,
        "Owner Title": title,
        "Owner Extra": extra,
        "Metafield Key": metafield_key,
    }


def build_output_df(
    owner_entity_type: str,
    owner_nodes: List[Dict[str, Any]],
    metafield_key: str,
    field_order: List[str],
    list_join_sep: str = " | ",
) -> Tuple[pd.DataFrame, Dict[str, int], List[Dict[str, Any]]]:
    rows = []
    detail_errors = []

    owners_scanned = 0
    owners_hit = 0
    examples_total = 0

    for node in owner_nodes:
        owners_scanned += 1
        try:
            mf = node.get("metafield")
            if not mf:
                continue

            examples = metafield_to_examples(mf, field_order=field_order, list_join_sep=list_join_sep)
            if not examples:
                continue

            owners_hit += 1

            base = owner_node_to_base_row(
                owner_entity_type=owner_entity_type,
                node=node,
                metafield_key=metafield_key,
            )

            for ex in examples:
                row = dict(base)
                row["Metafield Value"] = _norm(ex.get("metafield_value_text"))
                row["Metaobject GID"] = _norm(ex.get("metaobject_gid"))
                row["Metaobject Type"] = _norm(ex.get("metaobject_type"))
                row["Metaobject Handle"] = _norm(ex.get("metaobject_handle"))
                row["Metaobject Display Name"] = _norm(ex.get("metaobject_display_name"))

                fmap = ex.get("field_map") or {}
                for fk in field_order:
                    row[fk] = _norm(fmap.get(fk))

                rows.append(row)
                examples_total += 1

        except Exception as e:
            detail_errors.append({
                "gid": _norm(node.get("id")),
                "field_key": metafield_key,
                "error_reason": "ROW_BUILD_ERROR",
                "message": f"{type(e).__name__}: {e}",
            })

    cols = [
        "Owner Entity Type",
        "Owner GID",
        "Owner ID (numeric)",
        "Owner Handle",
        "Owner Title",
        "Owner Extra",
        "Metafield Key",
        "Metafield Value",
        "Metaobject GID",
        "Metaobject Type",
        "Metaobject Handle",
        "Metaobject Display Name",
    ] + field_order

    df = pd.DataFrame(rows, columns=cols).fillna("")

    stats = {
        "owners_scanned": owners_scanned,
        "owners_hit": owners_hit,
        "examples_total": examples_total,
        "rows_output": len(df),
    }
    return df, stats, detail_errors


# =========================================================
# Runlog builders
# =========================================================

def build_summary_runlog_row(
    run_id: str,
    site_code: str,
    owner_entity_type: str,
    field_key: str,
    status: str,
    rows_loaded: int,
    rows_recognized: int,
    rows_planned: int,
    rows_written: int,
    rows_skipped: int,
    message: str,
    error_reason: str = "",
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "ts_cn": _now_cn_str(),
        "job_name": JOB_NAME,
        "phase": "apply",
        "log_type": "summary",
        "status": status,
        "site_code": site_code,
        "entity_type": owner_entity_type,
        "gid": "",
        "field_key": field_key,
        "rows_loaded": rows_loaded,
        "rows_pending": 0,
        "rows_recognized": rows_recognized,
        "rows_planned": rows_planned,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "message": message,
        "error_reason": error_reason,
    }


def build_detail_runlog_rows(
    run_id: str,
    site_code: str,
    owner_entity_type: str,
    detail_errors: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out = []
    grouped = {}
    for e in detail_errors:
        reason = _norm(e.get("error_reason")) or "ERROR"
        grouped.setdefault(reason, []).append(e)

    for reason, items in grouped.items():
        for e in items[:2]:
            out.append({
                "run_id": run_id,
                "ts_cn": _now_cn_str(),
                "job_name": JOB_NAME,
                "phase": "apply",
                "log_type": "detail",
                "status": "FAILED",
                "site_code": site_code,
                "entity_type": owner_entity_type,
                "gid": _norm(e.get("gid")),
                "field_key": _norm(e.get("field_key")),
                "rows_loaded": "",
                "rows_pending": "",
                "rows_recognized": "",
                "rows_planned": "",
                "rows_written": "",
                "rows_skipped": "",
                "message": _norm(e.get("message")),
                "error_reason": reason,
            })
    return out


# =========================================================
# Main run
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    shopify_token: str,
    shopify_api_version: str,
    owner_entity_type: str,
    metaobject_type: str,
    metafield_key: str,
    cfg_sites_tab: str = "Cfg__Sites",
    cfg_account_id_tab: str = "Cfg__account_id",
    cfg_fields_tab: str = "Cfg__Fields",
    export_tab: str = "MR-Example",
    runlog_tab: str = "Ops__RunLog",
    owner_page_size: int = 80,
    metaobject_list_first: int = 50,
    field_ref_list_first: int = 50,
    overwrite_export_sheet: bool = True,
    list_join_sep: str = " | ",
    sleep_seconds: float = 0.0,
) -> Dict[str, Any]:
    owner = _norm(owner_entity_type).upper()
    if owner == "ORDER":
        raise RuntimeError("OWNER_ENTITY_TYPE=ORDER 暂未实现。")
    if owner not in SUPPORTED_OWNER_ENTITY_TYPES:
        raise RuntimeError(
            f"OWNER_ENTITY_TYPE 只支持 {sorted(SUPPORTED_OWNER_ENTITY_TYPES)}，当前为：{owner}"
        )

    if not _norm(metaobject_type):
        raise RuntimeError("METAOBJECT_TYPE 不能为空")
    if not _norm(metafield_key):
        raise RuntimeError("METAFIELD_KEY 不能为空")

    gc = build_gspread_client_from_b64_secret(gsheet_sa_b64)
    run_id = f"{JOB_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # 1) console core -> Cfg__Sites
    ws_sites = open_ws_by_url_and_title(gc, console_core_url, cfg_sites_tab)
    df_sites = ws_records(ws_sites)
    if df_sites.empty:
        raise RuntimeError("Cfg__Sites 为空")

    targets = get_site_targets(df_sites=df_sites, site_code=site_code)

    # 2) Console Core/config -> Cfg__account_id
    #    Shopify runtime config normally lives in Console Core. Some older
    #    setups may keep it in the sheet routed by label=config, so check both.
    ws_account = open_first_existing_ws_by_urls_and_title(
        gc,
        [console_core_url, targets["config_url"]],
        cfg_account_id_tab,
    )
    account_cfg = read_key_value_config_ws(ws_account)
    shop_domain, resolved_api_version = resolve_shopify_runtime_config(
        targets=targets,
        account_cfg=account_cfg,
        shopify_api_version=shopify_api_version,
    )

    # 3) config -> Cfg__Fields
    ws_fields = open_ws_by_url_and_title(gc, targets["config_url"], cfg_fields_tab)
    df_fields = ws_records(ws_fields)
    if df_fields.empty:
        raise RuntimeError("Cfg__Fields 为空")

    cfg = get_cfg_fields_info(
        df_fields=df_fields,
        owner_entity_type=owner,
        metaobject_type=metaobject_type,
        metafield_key=metafield_key,
    )

    namespace = cfg["namespace"]
    key = cfg["key"]
    field_order = cfg["field_order"]

    # 4) Shopify pull
    owner_nodes = list(
        iter_owner_nodes(
            owner_entity_type=owner,
            shop_domain=shop_domain,
            api_version=resolved_api_version,
            token=shopify_token,
            namespace=namespace,
            key=key,
            page_size=owner_page_size,
            metaobject_list_first=metaobject_list_first,
            field_ref_list_first=field_ref_list_first,
            sleep_seconds=sleep_seconds,
        )
    )

    # 5) build output
    df_out, stats, detail_errors = build_output_df(
        owner_entity_type=owner,
        owner_nodes=owner_nodes,
        metafield_key=metafield_key,
        field_order=field_order,
        list_join_sep=list_join_sep,
    )

    # 6) write export sheet
    ws_export = open_ws_by_url_and_title(gc, targets["export_other_url"], export_tab)
    write_df_to_ws(ws_export, df_out, clear_first=overwrite_export_sheet)

    # 7) runlog
    summary_status = "SUCCESS"
    summary_error_reason = ""
    summary_message = (
        f"导出完成；owner_scanned={stats['owners_scanned']}, "
        f"owner_hit={stats['owners_hit']}, rows={stats['rows_output']}"
    )
    if stats["rows_output"] == 0:
        summary_message = (
            f"执行成功但无命中；owner_scanned={stats['owners_scanned']}, "
            f"owner_hit={stats['owners_hit']}, rows=0"
        )

    summary_row = build_summary_runlog_row(
        run_id=run_id,
        site_code=site_code,
        owner_entity_type=owner,
        field_key=metafield_key,
        status=summary_status,
        rows_loaded=stats["owners_scanned"],
        rows_recognized=stats["owners_hit"],
        rows_planned=stats["rows_output"],
        rows_written=stats["rows_output"],
        rows_skipped=max(stats["owners_scanned"] - stats["owners_hit"], 0),
        message=summary_message,
        error_reason=summary_error_reason,
    )
    detail_rows = build_detail_runlog_rows(
        run_id=run_id,
        site_code=site_code,
        owner_entity_type=owner,
        detail_errors=detail_errors,
    )

    ws_runlog = open_ws_by_url_and_title(gc, targets["runlog_url"], runlog_tab)
    append_runlog_rows(ws_runlog, [summary_row] + detail_rows)

    return {
        "ok": True,
        "run_id": run_id,
        "job_name": JOB_NAME,
        "site_code": site_code,
        "owner_entity_type": owner,
        "metaobject_type": metaobject_type,
        "metafield_key": metafield_key,
        "shop_domain": shop_domain,
        "shopify_api_version": resolved_api_version,
        "targets": targets,
        "summary": {
            "owners_scanned": stats["owners_scanned"],
            "owners_hit": stats["owners_hit"],
            "rows_output": stats["rows_output"],
            "detail_error_count": len(detail_errors),
        },
        "preview": df_out.head(20).copy(),
        "warnings": [
            "OWNER_ENTITY_TYPE=ORDER 暂未实现",
        ] if owner == "ORDER" else [],
        "df_out": df_out,
    }
