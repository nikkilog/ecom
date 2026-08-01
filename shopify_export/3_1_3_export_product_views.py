# shopify_export/3_1_3_export_product_views.py
# delivery_name: build_product_views_variant_mf_backfill_v6.py
# feature: generic EXPAND_LIST support + Product/Variant metafield backfill from DL__ValuesLong
# -*- coding: utf-8 -*-

import re
import io
import json
import time
import base64
import random
import sys
from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# 基础工具
# =========================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MODULE_PATH = "shopify_export.3_1_3_export_product_views"
MODULE_VERSION = "2026-08-01-runtime-boundary-v1"
DEFAULT_JOB_NAME = "export_product_views"


def _now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _safe_str(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s


def _safe_json_loads(x):
    s = _safe_str(x)
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}



# =========================================================
# Runtime / Secret / Workspace Registry / Sheets retry
# =========================================================

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
    return _safe_str(value).upper()


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
    """Resolve one Google credential Secret without exposing its value."""
    secret_name = _safe_str(name)
    resolved_project_code = _normalize_project_code(project_code)
    if not secret_name:
        raise RuntimeError("Secret name is empty.")
    if not resolved_project_code:
        raise RuntimeError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _safe_str(explicit_value):
        return SecretValue(_safe_str(explicit_value), "EXPLICIT_VALUE", "caller")

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
    if secret_name.upper().endswith("_GSHEET"):
        canonical_name = f"{resolved_project_code}_GSHEET"
        if canonical_name != secret_name:
            aliases = (canonical_name,)

    result = resolver.read(secret_name, aliases=aliases)
    return _workspace_secret_result_to_value(result)


def _parse_service_account_text(raw_value: str) -> Tuple[Dict[str, Any], str]:
    raw = _safe_str(raw_value)
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
        "service_account_email": _safe_str(info.get("client_email")),
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


def _normalize_registry_header(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", _safe_str(value).lower()).strip()


def _extract_spreadsheet_id(value: Any) -> str:
    text = _safe_str(value)
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
    registry_tab = _safe_str(workspace_registry_tab)
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
    registry_book = _with_sheets_retry(
        lambda: workspace_gc.open_by_key(registry_file_id),
        action="workspace_registry.open_by_key",
    )
    try:
        worksheet = _with_sheets_retry(
            lambda: registry_book.worksheet(registry_tab),
            action=f"workspace_registry.worksheet:{registry_tab}",
        )
    except gspread.WorksheetNotFound as exc:
        raise ValueError(
            f"Workspace Project Registry tab {registry_tab!r} does not exist "
            f"in {registry_book.title!r}."
        ) from exc

    values = _with_sheets_retry(
        worksheet.get_all_values,
        action=f"workspace_registry.get_all_values:{registry_tab}",
    )
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
    active_text = _safe_str(row[active_col]).lower()
    if active_text not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}, row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": _safe_str(row[project_name_col]) if project_name_col is not None else "",
        "console_core_url": _safe_str(row[console_url_col]),
        "gsheet_secret_name": _safe_str(row[gsheet_secret_col]),
        "account_config_tab": _safe_str(row[account_tab_col]),
        "timezone": _safe_str(row[timezone_col]),
        "notes": _safe_str(row[notes_col]) if notes_col is not None else "",
        "registry_id": registry_file_id,
        "registry_tab": registry_tab,
        "registry_source_row": str(source_row),
        "workspace_gsheet_secret_name": _safe_str(workspace_gsheet_secret_name),
        "workspace_auth_source_type": _safe_str(auth_meta.get("source_type")),
        "workspace_service_account_email": _safe_str(auth_meta.get("service_account_email")),
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


def _load_project_google_secret_name(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_account_id: str,
) -> str:
    book = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="account_config.open_console",
    )
    worksheet = _with_sheets_retry(
        lambda: book.worksheet(tab_cfg_account_id),
        action=f"account_config.worksheet:{tab_cfg_account_id}",
    )
    values = _with_sheets_retry(
        worksheet.get_all_values,
        action=f"account_config.get_all_values:{tab_cfg_account_id}",
    )
    if not values:
        raise ValueError(f"{tab_cfg_account_id} is empty.")

    config: Dict[str, str] = {}
    duplicates: List[str] = []
    for row_number, row in enumerate(values, start=1):
        key = _safe_str(row[0] if row else "").upper()
        value = _safe_str(row[1] if len(row) > 1 else "")
        if not key:
            continue
        if key in config:
            duplicates.append(f"{key}@row{row_number}")
        config[key] = value
    if duplicates:
        raise ValueError(f"Duplicated keys in {tab_cfg_account_id}: {duplicates}")

    secret_name = _safe_str(config.get("GSHEET_SA_B64_SECRET"))
    if not secret_name:
        raise ValueError(f"{tab_cfg_account_id} missing required value: GSHEET_SA_B64_SECRET")
    return secret_name


def resolve_runtime_context(
    *,
    project_code: str,
    workspace_registry_id: str,
    workspace_gsheet_secret_name: str = "WORKSPACE_GSHEET",
    workspace_registry_tab: str = "Cfg__Projects",
    secret_home: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, Any]:
    """Resolve project route and the Google credential required by Product Views."""
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
    cfg_google_secret_name = _load_project_google_secret_name(
        gc,
        route["console_core_url"],
        route["account_config_tab"],
    )
    if cfg_google_secret_name != route["gsheet_secret_name"]:
        raise ValueError(
            "Workspace Registry Google Secret does not match Cfg__account_id. "
            f"registry={route['gsheet_secret_name']}; cfg={cfg_google_secret_name}"
        )

    if print_progress:
        print(
            "[Runtime Auth] ready | "
            f"project={route['project_code']} | "
            f"google_source={google_auth_meta['source_type']}"
        )

    return {
        "project_route": route,
        "account": {
            "gsheet_secret_name": cfg_google_secret_name,
        },
        "credentials": {
            "gsheet_sa_value": project_google_secret.value,
        },
        "auth": {
            "runtime_mode": _runtime_mode(),
            "workspace_secret_source_type": route["workspace_auth_source_type"],
            "project_google_secret_source_type": google_auth_meta["source_type"],
            "project_google_secret_format": google_auth_meta["secret_format"],
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
    mode = _safe_str(registry_mode).upper() or "OFF"
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

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _safe_str(current_colab_url):
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _safe_str(current_colab_name):
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
        action="notebook_registry.open_console",
    )
    worksheet = _with_sheets_retry(
        lambda: book.worksheet(registry_tab),
        action=f"notebook_registry.worksheet:{registry_tab}",
    )
    values = _with_sheets_retry(
        worksheet.get_all_values,
        action=f"notebook_registry.get_all_values:{registry_tab}",
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
        _safe_str(job_name).lower(),
        _safe_str(sheet_label).lower(),
        _safe_str(tab_name).lower(),
    )
    matches: List[int] = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(values[0]) - len(row))
        logical_key = (
            _safe_str(padded[job_col]).lower(),
            _safe_str(padded[label_col]).lower(),
            _safe_str(padded[tab_col]).lower(),
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
    provided_url = _safe_str(current_colab_url)
    provided_name = _safe_str(current_colab_name)
    if provided_url and _safe_str(current_row[url_col]) != provided_url:
        changes.append(("colab_url", url_col + 1, _safe_str(current_row[url_col]), provided_url))
    if provided_name and _safe_str(current_row[name_col]) != provided_name:
        changes.append(("colab_name", name_col + 1, _safe_str(current_row[name_col]), provided_name))

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



# Image-list fields are intentionally kept as a single raw JSON/list column.
# Final views no longer auto-expand them into Image 01 / Product Image 01 columns.


def _norm_bool(x) -> bool:
    s = _safe_str(x).upper()
    return s in ("TRUE", "1", "YES", "Y", "ON")


def _make_unique_cols(cols: List[str]) -> List[str]:
    """
    内部 DataFrame 列名唯一化：
    SKU, SKU -> SKU, SKU__2
    """
    seen = {}
    out = []
    for c in cols:
        k = _safe_str(c)
        if not k:
            k = "__blank__"
        seen[k] = seen.get(k, 0) + 1
        if seen[k] == 1:
            out.append(k)
        else:
            out.append(f"{k}__{seen[k]}")
    return out


def _make_display_headers(headers: List[str]) -> List[str]:
    """
    最终显示表头：
    SKU, SKU -> SKU, SKU-2
    """
    seen = {}
    out = []
    for h in headers:
        k = _safe_str(h) or "Unnamed"
        seen[k] = seen.get(k, 0) + 1
        if seen[k] == 1:
            out.append(k)
        else:
            out.append(f"{k}-{seen[k]}")
    return out


def _dedupe_columns_keep_first(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _as_scalar(v):
    """
    若同名列导致 row[col] 返回 Series，则取第一个非空值
    """
    if isinstance(v, pd.Series):
        for x in v.tolist():
            s = _safe_str(x)
            if s:
                return s
        return _safe_str(v.iloc[0]) if len(v) else ""
    return _safe_str(v)




def _extract_gid_like(v):
    """
    支持：
    - 纯 gid: gid://shopify/Product/123
    - JSON 字符串: {"id":"gid://shopify/Product/123"}
    - dict: {"id":"gid://shopify/Product/123"}
    其他情况回退为字符串本身
    """
    if isinstance(v, dict):
        if "id" in v:
            return _safe_str(v.get("id"))
        return _safe_str(v)
    s = _safe_str(v)
    if not s:
        return ""
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and "id" in obj:
                return _safe_str(obj.get("id"))
        except Exception:
            pass
    return s

def col_to_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# =========================================================
# Google auth / gspread
# =========================================================

def make_gspread_client_from_b64(service_account_b64: str):
    """Backward-compatible entrypoint; accepts Base64 JSON or raw JSON."""
    info, _secret_format = _parse_service_account_text(service_account_b64)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def ws_update(ws, a1_range: str, values, value_input_option="RAW", max_retry=5):
    return _with_sheets_retry(
        lambda: ws.update(
            a1_range,
            values,
            value_input_option=value_input_option,
        ),
        action=f"worksheet.update:{getattr(ws, 'title', '')}:{a1_range}",
        max_attempts=max_retry,
    )


def clear_worksheet(ws):
    return _with_sheets_retry(
        ws.clear,
        action=f"worksheet.clear:{getattr(ws, 'title', '')}",
    )


def resize_worksheet(ws, rows: int, cols: int):
    rows = max(int(rows or 1), 1)
    cols = max(int(cols or 1), 1)
    if ws.row_count != rows or ws.col_count != cols:
        return _with_sheets_retry(
            lambda: ws.resize(rows=rows, cols=cols),
            action=f"worksheet.resize:{getattr(ws, 'title', '')}:{rows}x{cols}",
        )
    return None


def ensure_worksheet(sh, title: str, rows: int = 1000, cols: int = 50):
    try:
        return _with_sheets_retry(
            lambda: sh.worksheet(title),
            action=f"worksheet.open:{title}",
        )
    except gspread.WorksheetNotFound:
        # Creation is intentionally not retried automatically because it is not
        # guaranteed idempotent. Existing business behavior still allows the
        # configured target worksheet to be created when truly absent.
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def read_ws_df(ws) -> pd.DataFrame:
    values = _with_sheets_retry(
        ws.get_all_values,
        action=f"worksheet.get_all_values:{getattr(ws, 'title', '')}",
    )
    if not values:
        return pd.DataFrame()

    header = values[0]
    header = [str(x).strip() for x in header]
    header = _make_unique_cols(header)

    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)

    max_len = max(len(header), max((len(r) for r in rows), default=0))
    header = header + [f"__extra_col_{i}" for i in range(len(header) + 1, max_len + 1)]

    norm_rows = []
    for r in rows:
        rr = list(r) + [""] * (max_len - len(r))
        norm_rows.append(rr[:max_len])

    df = pd.DataFrame(norm_rows, columns=header)
    df = df.fillna("")
    return df



def _open_by_url(gc: gspread.Client, url: str, *, action: str):
    return _with_sheets_retry(
        lambda: gc.open_by_url(url),
        action=action,
    )


def _get_worksheet(sh, title: str, *, action: Optional[str] = None):
    return _with_sheets_retry(
        lambda: sh.worksheet(title),
        action=action or f"worksheet.open:{title}",
    )


# =========================================================
# Console / sheet 定位
# =========================================================

def get_label_sheet_url_from_cfg_sites(gc, console_core_url: str, site_code: str, label: str) -> str:
    sh = _open_by_url(gc, console_core_url, action="cfg_sites.open_console")
    ws = _get_worksheet(sh, "Cfg__Sites", action="cfg_sites.worksheet")
    df = read_ws_df(ws)

    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()

    hit = df[
        (df["site_code"] == site_code.strip().upper()) &
        (df["label"] == label.strip())
    ]

    if hit.empty:
        raise ValueError(f"Cfg__Sites 未找到 site_code={site_code}, label={label}")

    url = _safe_str(hit.iloc[0].get("sheet_url"))
    if not url:
        raise ValueError(f"Cfg__Sites 命中但 sheet_url 为空: site_code={site_code}, label={label}")
    return url


# =========================================================
# Header / field / filter
# =========================================================

def normalize_idx_columns(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    把 IDX 表中的 core./mf./v_mf./raw./mo. 补成实体前缀：
    PRODUCT|core.title
    VARIANT|core.sku
    """
    if df is None or df.empty:
        return df

    existing = set(df.columns.astype(str))
    rename_map = {}

    for c in df.columns:
        c0 = str(c).strip()
        if c0.startswith(prefix + "|"):
            continue
        if c0.startswith(("core.", "mf.", "v_mf.", "raw.", "mo.")):
            new_name = f"{prefix}|{c0}"
            if new_name in existing:
                continue
            rename_map[c] = new_name

    if rename_map:
        df = df.rename(columns=rename_map)

    if df.columns.duplicated().any():
        df.columns = _make_unique_cols([str(x) for x in df.columns])

    return df


def split_filters_by_entity(filters: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    pf, vf = {}, {}
    for k, v in (filters or {}).items():
        kk = _safe_str(k)
        if kk.startswith("PRODUCT|"):
            pf[kk] = v
        elif kk.startswith("VARIANT|"):
            vf[kk] = v
    return pf, vf


def apply_entity_filters(df: pd.DataFrame, filters: Dict[str, Any], mode: str = "AND") -> pd.DataFrame:
    if df is None or df.empty or not filters:
        return df

    mode = _safe_str(mode).upper() or "AND"
    masks = []

    for col, want in filters.items():
        if col not in df.columns:
            continue

        series = df[col].astype(str).fillna("").str.strip()

        if isinstance(want, list):
            want_set = set([_safe_str(x) for x in want])
            mask = series.isin(want_set)
        else:
            mask = series == _safe_str(want)

        masks.append(mask)

    if not masks:
        return df

    final_mask = masks[0]
    for m in masks[1:]:
        final_mask = (final_mask & m) if mode == "AND" else (final_mask | m)

    return df[final_mask].copy()


# =========================================================
# Formula
# =========================================================

TOKEN_RE = re.compile(r"\{([^}]+)\}")
EXPAND_LIST_RE = re.compile(
    r"^=?\s*EXPAND_LIST\(\s*\{([^{}]+)\}\s*,\s*(\d+)\s*\)\s*$",
    re.IGNORECASE,
)


def _parse_expand_list_expr(expr: str) -> Optional[Tuple[str, int]]:
    """
    Parse:
      EXPAND_LIST({PRODUCT|core.tags},10)

    Returns:
      ("PRODUCT|core.tags", 10)

    The second argument is capped to prevent an accidental huge worksheet.
    """
    match = EXPAND_LIST_RE.match(_safe_str(expr))
    if not match:
        return None

    source_field_id = _safe_str(match.group(1))
    column_count = int(match.group(2))

    if not source_field_id:
        raise ValueError(f"EXPAND_LIST source field is blank: {expr}")
    if column_count < 1 or column_count > 200:
        raise ValueError(
            f"EXPAND_LIST column count must be between 1 and 200: "
            f"expr={expr}, count={column_count}"
        )
    return source_field_id, column_count


def _coerce_expand_list(value: Any) -> List[Any]:
    """
    Normalize a source list for EXPAND_LIST.

    Supported inputs:
    - Python list / tuple
    - JSON list string: ["tag1","tag2"]
    - Human-readable IDX tags: tag1, tag2, tag3
    - Pipe-separated list: tag1 | tag2 | tag3
    """
    if value is None:
        return []

    if isinstance(value, list):
        return [x for x in value if _safe_str(x)]

    if isinstance(value, tuple):
        return [x for x in value if _safe_str(x)]

    text = _safe_str(value)
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [x for x in parsed if _safe_str(x)]
        except Exception:
            pass

    # export_idx_tables currently writes core.tags as a human-readable
    # comma-separated string. Pipe handling also keeps this generic for
    # other list fields used by future views.
    separator = "|" if "|" in text and "," not in text else ","
    return [
        part.strip()
        for part in text.split(separator)
        if part.strip()
    ]


def _collect_expand_dependencies(vf: pd.DataFrame) -> Dict[str, set]:
    """
    Collect source field_ids required by EXPAND rows.

    Example:
      PRODUCT|derived.tags_split
      -> EXPAND_LIST({PRODUCT|core.tags},10)
      -> PRODUCT dependency: PRODUCT|core.tags
    """
    dependencies: Dict[str, set] = {
        "PRODUCT": set(),
        "VARIANT": set(),
    }

    for _, row in vf.iterrows():
        field_type = _safe_str(row.get("field_type")).upper()
        if field_type != "EXPAND":
            continue

        field_id = _safe_str(row.get("field_id"))
        expr = _safe_str(row.get("expr"))
        parsed = _parse_expand_list_expr(expr)
        if parsed is None:
            raise ValueError(
                f"EXPAND field must use EXPAND_LIST({{field_id}},count): "
                f"field_id={field_id}, expr={expr}"
            )

        source_field_id, _ = parsed
        if "|" not in source_field_id:
            raise ValueError(
                f"EXPAND_LIST source must use an entity-prefixed field_id: "
                f"field_id={field_id}, source={source_field_id}"
            )

        source_entity = _safe_str(source_field_id.split("|", 1)[0]).upper()
        if source_entity not in dependencies:
            raise ValueError(
                f"build_product_views only supports PRODUCT/VARIANT EXPAND sources: "
                f"field_id={field_id}, source={source_field_id}"
            )
        dependencies[source_entity].add(source_field_id)

    return dependencies


def _expand_field_rows_for_output(
    field_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Convert one configured EXPAND row into multiple physical output columns.

    Config:
      field_id   = PRODUCT|derived.tags_split
      alias      = Tag
      field_type = EXPAND
      expr       = EXPAND_LIST({PRODUCT|core.tags},10)

    Output headers:
      Tag-1 ... Tag-10
    """
    expanded: List[Dict[str, Any]] = []

    for field_row in field_rows:
        field_type = _safe_str(field_row.get("field_type")).upper()
        if field_type != "EXPAND":
            expanded.append(field_row)
            continue

        field_id = _safe_str(field_row.get("field_id"))
        parsed = _parse_expand_list_expr(field_row.get("expr"))
        if parsed is None:
            raise ValueError(
                f"Invalid EXPAND_LIST expression: field_id={field_id}, "
                f"expr={field_row.get('expr')}"
            )

        source_field_id, column_count = parsed
        alias_base = (
            _safe_str(field_row.get("alias"))
            or _safe_str(field_row.get("field_id"))
            or "Expanded"
        )
        output_key_base = (
            _safe_str(field_row.get("output_key"))
            or alias_base
        )

        for index in range(1, column_count + 1):
            item = dict(field_row)
            item["field_type"] = "EXPAND_ITEM"
            item["source_field_id"] = source_field_id
            item["source_index"] = index
            item["expand_parent_field_id"] = field_id
            item["field_id"] = f"{field_id}__item_{index}"
            item["alias"] = f"{alias_base}-{index}"
            item["output_key"] = f"{output_key_base}__expand_{index}"
            expanded.append(item)

    return expanded


def compile_formula(expr: str, row_num_1based: int, token_to_col_letter: Dict[str, str]) -> str:
    s = _safe_str(expr)
    if not s.startswith("="):
        return s

    def repl(m):
        token = m.group(1).strip()
        col = token_to_col_letter.get(token)
        if not col:
            return ""
        return f"{col}{row_num_1based}"

    return TOKEN_RE.sub(repl, s)


def _group_contiguous_cols(formula_cols):
    cols = sorted(formula_cols, key=lambda x: x[0])
    blocks = []
    cur = []
    prev = None
    for item in cols:
        ci = item[0]
        if prev is None or ci == prev + 1:
            cur.append(item)
        else:
            blocks.append(cur)
            cur = [item]
        prev = ci
    if cur:
        blocks.append(cur)
    return blocks


def write_formula_columns_filldown(
    ws,
    formula_cols,      # [(ci, expr, output_key), ...]
    start_row_1based: int,
    nrows: int,
    token_to_col_letter: Dict[str, str],
):
    if nrows <= 0 or not formula_cols:
        return

    blocks = _group_contiguous_cols(formula_cols)

    # 先只写第 2 行（或 start_row_1based 指定行）
    for block in blocks:
        c0 = block[0][0]
        c1 = block[-1][0]
        a1 = f"{col_to_letter(c0)}{start_row_1based}:{col_to_letter(c1)}{start_row_1based}"
        values = [[compile_formula(expr0, start_row_1based, token_to_col_letter) for (_, expr0, _) in block]]
        ws_update(ws, a1, values, value_input_option="USER_ENTERED")

    # 只有一行数据时，到这里就够了
    if nrows <= 1:
        return

    requests = []
    for block in blocks:
        c0 = block[0][0]
        c1 = block[-1][0]
        requests.append({
            "copyPaste": {
                "source": {
                    "sheetId": ws.id,
                    "startRowIndex": start_row_1based - 1,
                    "endRowIndex": start_row_1based,
                    "startColumnIndex": c0 - 1,
                    "endColumnIndex": c1,
                },
                "destination": {
                    "sheetId": ws.id,
                    "startRowIndex": start_row_1based,
                    "endRowIndex": start_row_1based - 1 + nrows,
                    "startColumnIndex": c0 - 1,
                    "endColumnIndex": c1,
                },
                "pasteType": "PASTE_FORMULA",
                "pasteOrientation": "NORMAL",
            }
        })

    if requests:
        _with_sheets_retry(
            lambda: ws.spreadsheet.batch_update({"requests": requests}),
            action=f"spreadsheet.batch_update:formula_filldown:{getattr(ws, 'title', '')}",
        )


# =========================================================
# field / join / value 解析
# =========================================================

def _prepare_field_rows(vf: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    每一列内部唯一 output_key；
    token 支持：
    1) field_id，例如 {PRODUCT|core.legacy_id}
    2) alias，例如 {Product Image}
    3) output_key（内部）
    """
    rows = []
    output_seen = {}

    for _, r in vf.iterrows():
        field_id = _safe_str(r.get("field_id"))
        alias = _safe_str(r.get("alias")) or field_id
        expr = _safe_str(r.get("expr"))
        field_type = _safe_str(r.get("field_type")).upper() or "RAW"
        entity_type = _safe_str(r.get("entity_type")).upper()
        field_key = _safe_str(r.get("field_key"))
        data_type = _safe_str(r.get("data_type"))

        # 兼容 "join_key" 和 "join key"
        join_key = _safe_str(r.get("join_key"))
        if not join_key:
            join_key = _safe_str(r.get("join key"))

        agg = _safe_str(r.get("agg"))

        base_key = alias or field_id or "col"
        output_seen[base_key] = output_seen.get(base_key, 0) + 1
        if output_seen[base_key] == 1:
            output_key = base_key
        else:
            output_key = f"{base_key}__{output_seen[base_key]}"

        rows.append({
            "field_id": field_id,
            "alias": alias,
            "expr": expr,
            "field_type": field_type,
            "entity_type": entity_type,
            "field_key": field_key,
            "data_type": data_type,
            "join_key": join_key,
            "agg": agg,
            "output_key": output_key,
        })

    return rows


def _build_row_maps(df_products: pd.DataFrame, df_variants: pd.DataFrame):
    """
    建立 PRODUCT / VARIANT 的常用索引
    """
    product_by_gid = {}
    product_by_legacy_id = {}
    variant_by_gid = {}

    product_gid_col = "PRODUCT|core.gid" if "PRODUCT|core.gid" in df_products.columns else None
    product_legacy_cols = [
        "PRODUCT|core.legacy_id",
        "PRODUCT|core.legacyResourceId",
        "PRODUCT|product.legacyResourceId",
    ]

    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None

    if product_gid_col:
        for _, r in df_products.iterrows():
            gid = _as_scalar(r.get(product_gid_col, ""))
            if gid:
                product_by_gid[gid] = r

    for _, r in df_products.iterrows():
        pid = ""
        for c in product_legacy_cols:
            if c in df_products.columns:
                pid = _as_scalar(r.get(c, ""))
                if pid:
                    break
        if pid:
            product_by_legacy_id[pid] = r

    if variant_gid_col:
        for _, r in df_variants.iterrows():
            gid = _as_scalar(r.get(variant_gid_col, ""))
            if gid:
                variant_by_gid[gid] = r

    return {
        "product_by_gid": product_by_gid,
        "product_by_legacy_id": product_by_legacy_id,
        "variant_by_gid": variant_by_gid,
    }


def _build_variant_product_bridge(
    df_variants: pd.DataFrame,
    row_maps: Dict[str, Any],
) -> Dict[str, str]:
    """
    bridge[variant_gid] = product_gid
    兼容 VARIANT|core.product_gid 为纯 gid / JSON 字符串 / dict
    """
    bridge = {}
    if df_variants is None or df_variants.empty:
        return bridge

    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None
    if not variant_gid_col:
        return bridge

    candidate_product_gid_cols = [
        "VARIANT|core.product_gid",
        "VARIANT|core.product.gid",
        "VARIANT|core.parent.gid",
        "PRODUCT|core.gid",
    ]

    candidate_product_legacy_cols = [
        "VARIANT|core.product.legacy_id",
        "VARIANT|core.product.legacyResourceId",
        "VARIANT|product.legacyResourceId",
        "PRODUCT|core.legacy_id",
        "PRODUCT|core.legacyResourceId",
    ]

    for _, r in df_variants.iterrows():
        vg = _extract_gid_like(r.get(variant_gid_col, ""))
        if not vg:
            continue

        pg = ""
        for c in candidate_product_gid_cols:
            if c in df_variants.columns:
                pg = _extract_gid_like(r.get(c, ""))
                if pg:
                    break

        if not pg:
            product_legacy_id = ""
            for c in candidate_product_legacy_cols:
                if c in df_variants.columns:
                    product_legacy_id = _as_scalar(r.get(c, ""))
                    if product_legacy_id:
                        break

            if product_legacy_id:
                product_row = row_maps.get("product_by_legacy_id", {}).get(product_legacy_id)
                if product_row is not None:
                    pg = _extract_gid_like(product_row.get("PRODUCT|core.gid", ""))

        if pg:
            bridge[vg] = pg

    return bridge

    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None
    if not variant_gid_col:
        return bridge

    candidate_product_gid_cols = [
        "VARIANT|core.product_gid",
        "VARIANT|core.product.gid",
        "VARIANT|core.parent.gid",
        "PRODUCT|core.gid",
    ]

    candidate_product_legacy_cols = [
        "VARIANT|core.product.legacy_id",
        "VARIANT|core.product.legacyResourceId",
        "VARIANT|product.legacyResourceId",
        "PRODUCT|core.legacy_id",
        "PRODUCT|core.legacyResourceId",
    ]

    for _, r in df_variants.iterrows():
        vg = _as_scalar(r.get(variant_gid_col, ""))
        if not vg:
            continue

        # 1) 先直接找 product gid
        pg = ""
        for c in candidate_product_gid_cols:
            if c in df_variants.columns:
                pg = _as_scalar(r.get(c, ""))
                if pg:
                    break

        # 2) gid 没找到，再用 legacy_id 反查
        if not pg:
            product_legacy_id = ""
            for c in candidate_product_legacy_cols:
                if c in df_variants.columns:
                    product_legacy_id = _as_scalar(r.get(c, ""))
                    if product_legacy_id:
                        break

            if product_legacy_id:
                product_row = row_maps.get("product_by_legacy_id", {}).get(product_legacy_id)
                if product_row is not None:
                    pg = _as_scalar(product_row.get("PRODUCT|core.gid", ""))

        if pg:
            bridge[vg] = pg

    return bridge


def _normalize_long_field_keys(owner_type: str, fk: str) -> List[str]:
    owner_type = _safe_str(owner_type).upper()
    fk0 = _safe_str(fk)
    if not fk0:
        return []

    keys = [fk0]
    if fk0.startswith("custom."):
        if owner_type == "PRODUCT":
            keys.append("mf." + fk0)
        elif owner_type == "VARIANT":
            keys.append("v_mf." + fk0)
    if fk0.startswith("mf.custom."):
        keys.append(fk0.replace("mf.", "", 1))
    if fk0.startswith("v_mf.custom."):
        keys.append(fk0.replace("v_mf.", "", 1))

    out = []
    seen = set()
    for k in keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _build_long_value_map(df_dl_values_long: pd.DataFrame) -> Dict[Tuple[str, str, str], str]:
    """
    兼容两种 DL 结构：
    A) owner_entity_type / owner_gid / field_key / value
    B) entity_type / gid_or_handle / field_key / desired_value
    """
    dl = df_dl_values_long.copy()
    if dl is None or dl.empty:
        return {}

    dl.columns = [_safe_str(c) for c in dl.columns]

    if {"owner_entity_type", "owner_gid", "field_key", "value"}.issubset(set(dl.columns)):
        et_col = "owner_entity_type"
        gid_col = "owner_gid"
        val_col = "value"
    elif {"owner_type", "owner_gid", "field_key", "value"}.issubset(set(dl.columns)):
        et_col = "owner_type"
        gid_col = "owner_gid"
        val_col = "value"
    elif {"entity_type", "gid_or_handle", "field_key", "desired_value"}.issubset(set(dl.columns)):
        et_col = "entity_type"
        gid_col = "gid_or_handle"
        val_col = "desired_value"
    else:
        return {}

    mp = {}
    for _, r in dl.iterrows():
        et = _safe_str(r.get(et_col)).upper()
        gid = _extract_gid_like(r.get(gid_col))
        fk = _safe_str(r.get("field_key"))
        val = _safe_str(r.get(val_col))
        if not (et and gid and fk):
            continue
        for fk_norm in _normalize_long_field_keys(et, fk):
            mp[(et, gid, fk_norm)] = val
    return mp


def _resolve_related_rows(
    base_entity_type: str,
    base_row: pd.Series,
    row_maps: Dict[str, Any],
    variant_product_bridge: Dict[str, str],
) -> Dict[str, Optional[pd.Series]]:
    """
    给当前 base row 同时解析出 product_row / variant_row
    """
    product_row = None
    variant_row = None

    if base_entity_type == "PRODUCT":
        product_row = base_row

    elif base_entity_type == "VARIANT":
        variant_row = base_row

        # 先尝试直接从当前 variant 行里拿 product gid
        direct_product_gid_candidates = [
            "VARIANT|core.product_gid",
            "VARIANT|core.product.gid",
            "VARIANT|core.parent.gid",
            "PRODUCT|core.gid",
        ]
        direct_product_gid = _extract_gid_like(
            _get_from_row_by_candidates(base_row, direct_product_gid_candidates)
        )

        if direct_product_gid:
            product_row = row_maps["product_by_gid"].get(direct_product_gid)

        # 再用 bridge
        if product_row is None:
            variant_gid = _extract_gid_like(base_row.get("VARIANT|core.gid", ""))
            product_gid = variant_product_bridge.get(variant_gid, "")
            if product_gid:
                product_row = row_maps["product_by_gid"].get(product_gid)

        # 再 fallback：直接用 product legacy id 反查
        if product_row is None:
            direct_product_legacy_candidates = [
                "VARIANT|core.product.legacy_id",
                "VARIANT|core.product.legacyResourceId",
                "VARIANT|product.legacyResourceId",
                "PRODUCT|core.legacy_id",
                "PRODUCT|core.legacyResourceId",
            ]
            product_legacy_id = _get_from_row_by_candidates(base_row, direct_product_legacy_candidates)
            if product_legacy_id:
                product_row = row_maps.get("product_by_legacy_id", {}).get(product_legacy_id)

    return {
        "PRODUCT": product_row,
        "VARIANT": variant_row,
    }


def _get_from_row_by_candidates(row: Optional[pd.Series], candidates: List[str]) -> str:
    if row is None:
        return ""
    for c in candidates:
        if c in row.index:
            v = _as_scalar(row[c])
            if v != "":
                return v
    return ""


def _resolve_raw_value(
    *,
    field_row: Dict[str, Any],
    related_rows: Dict[str, Optional[pd.Series]],
    long_value_map: Dict[Tuple[str, str, str], str],
    base_row: Optional[pd.Series] = None,
) -> str:
    """
    RAW 取值规则：
    1. 先按 field_id / field_key 到对应实体 row 找
    2. 若当前是 VARIANT base，且要取 PRODUCT 字段，允许直接从 base_row 上取 PRODUCT|... 列
    3. 找不到再按 expr（非公式）指向的列名找
    4. 还找不到再去 DL__ValuesLong 按 (entity_type, gid, field_key) 找
    """
    field_id = _safe_str(field_row.get("field_id"))
    field_key = _safe_str(field_row.get("field_key"))
    expr = _safe_str(field_row.get("expr"))
    entity_type = _safe_str(field_row.get("entity_type")).upper()

    row = related_rows.get(entity_type)

    # 1) 直接按 field_id / field_key 查实体 row
    candidates = []
    if field_id:
        candidates.append(field_id)
        if "|" not in field_id and entity_type:
            candidates.append(f"{entity_type}|{field_id}")
    if field_key:
        candidates.append(field_key)
        if "|" not in field_key and entity_type:
            candidates.append(f"{entity_type}|{field_key}")

    v = _get_from_row_by_candidates(row, candidates)
    if v != "":
        return v

    # 2) fallback：当前 base_row 里如果已经带了 PRODUCT|... / VARIANT|... 列，也直接拿
    if base_row is not None:
        v = _get_from_row_by_candidates(base_row, candidates)
        if v != "":
            return v

    # 3) expr 非公式时，允许 expr 作为“源列名”
    if expr and not expr.startswith("="):
        expr_candidates = [expr]
        if "|" not in expr and entity_type:
            expr_candidates.append(f"{entity_type}|{expr}")

        v = _get_from_row_by_candidates(row, expr_candidates)
        if v != "":
            return v

        if base_row is not None:
            v = _get_from_row_by_candidates(base_row, expr_candidates)
            if v != "":
                return v

    # 4) 去 DL__ValuesLong
    if row is not None and field_key:
        gid = _get_from_row_by_candidates(
            row,
            [f"{entity_type}|core.gid", f"{entity_type}|gid", "core.gid", "gid"]
        )
        if gid:
            v = long_value_map.get((entity_type, gid, field_key), "")
            if v != "":
                return v

    return ""



def _field_id_to_long_field_key(field_id: str) -> str:
    if "|" not in field_id:
        return _safe_str(field_id)
    return _safe_str(field_id.split("|", 1)[1])


def _agg_series_values(s: pd.Series, agg_name: str):
    agg_name = _safe_str(agg_name).upper()
    ss = s.fillna("").astype(str)
    ss = ss[ss.str.strip() != ""]
    if len(ss) == 0:
        return ""
    if agg_name in ("", "FIRST"):
        return ss.iloc[0]
    if agg_name == "FIRST_SORTED":
        return ss.sort_values().iloc[0]
    if agg_name == "LIST":
        return ", ".join(ss.tolist())
    if agg_name == "LIST_DISTINCT":
        seen = []
        for x in ss.tolist():
            if x not in seen:
                seen.append(x)
        return ", ".join(seen)
    return ss.iloc[0]


def _aggregate_variant_fields_to_product(df_variants: pd.DataFrame, variant_rows: pd.DataFrame) -> pd.DataFrame:
    if df_variants is None or df_variants.empty or variant_rows is None or variant_rows.empty:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    if "VARIANT|core.product_gid" not in df_variants.columns:
        raise ValueError("IDX__Variants 缺 VARIANT|core.product_gid，无法把 VARIANT 聚合到 PRODUCT")

    agg_groups: Dict[str, List[str]] = defaultdict(list)
    seen_fids = set()
    for _, r in variant_rows.iterrows():
        fid = _safe_str(r.get("field_id"))
        agg_name = _safe_str(r.get("agg")).upper() or "FIRST"
        if not fid or fid not in df_variants.columns or fid in seen_fids:
            continue
        agg_groups[agg_name].append(fid)
        seen_fids.add(fid)

    if not agg_groups:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    needed_cols = sorted(seen_fids)
    variant_work = df_variants[["VARIANT|core.product_gid"] + needed_cols].copy()
    variant_work["__product_gid_norm"] = variant_work["VARIANT|core.product_gid"].map(_extract_gid_like)
    variant_work = variant_work[variant_work["__product_gid_norm"].astype(str).str.strip() != ""].copy()
    if variant_work.empty:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    pieces = []
    for agg_name, fids in agg_groups.items():
        sub = variant_work[["__product_gid_norm"] + fids].copy()
        grouped = sub.groupby("__product_gid_norm", sort=False, dropna=False)
        agg_df = grouped[fids].agg(lambda s, agg_name=agg_name: _agg_series_values(s, agg_name))
        pieces.append(agg_df)

    if not pieces:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    variant_agg_df = pd.concat(pieces, axis=1)
    variant_agg_df = variant_agg_df.loc[:, ~variant_agg_df.columns.duplicated()].reset_index()
    variant_agg_df = variant_agg_df.rename(columns={"__product_gid_norm": "PRODUCT|core.gid"})
    return variant_agg_df


def ensure_columns_from_long(
    df: pd.DataFrame,
    needed_cols: List[str],
    long_value_map: Dict[Tuple[str, str, str], str],
) -> pd.DataFrame:
    """
    Ensure configured metafield columns can be resolved from DL__ValuesLong.

    v6 behavior:
    - add a configured mf./v_mf./custom. column when it is absent;
    - backfill individual blank cells when the column already exists in IDX;
    - preserve every non-blank IDX value as the first-priority source.

    The previous implementation only added completely missing columns. Therefore,
    an existing-but-empty VARIANT|v_mf.* column blocked the DL__ValuesLong fallback.
    """
    if df is None or df.empty or not needed_cols:
        return df

    work = df.copy()

    var_gid_col = "VARIANT|core.gid"
    prd_gid_col = "PRODUCT|core.gid"
    var_prd_gid = "VARIANT|core.product_gid"

    has_var_gid = var_gid_col in work.columns
    has_prd_gid = prd_gid_col in work.columns
    has_var_prd = var_prd_gid in work.columns

    var_gids = (
        [_extract_gid_like(x) for x in work[var_gid_col].tolist()]
        if has_var_gid else None
    )
    prd_gids = (
        [_extract_gid_like(x) for x in work[prd_gid_col].tolist()]
        if has_prd_gid
        else (
            [_extract_gid_like(x) for x in work[var_prd_gid].tolist()]
            if has_var_prd else None
        )
    )

    for col in needed_cols:
        if not isinstance(col, str) or "|" not in col:
            continue

        ent = _safe_str(col.split("|", 1)[0]).upper()
        fk = _field_id_to_long_field_key(col)
        if not (
            fk.startswith("mf.")
            or fk.startswith("v_mf.")
            or fk.startswith("custom.")
        ):
            continue

        gids = None
        owner_type = ""
        if ent == "VARIANT":
            gids = var_gids
            owner_type = "VARIANT"
        elif ent == "PRODUCT":
            gids = prd_gids
            owner_type = "PRODUCT"

        if not gids:
            continue

        fallback = pd.Series(
            [long_value_map.get((owner_type, gid, fk), "") for gid in gids],
            index=work.index,
            dtype="object",
        )

        if col not in work.columns:
            work[col] = fallback
            continue

        blank_mask = work[col].map(lambda x: _safe_str(x) == "")
        if blank_mask.any():
            work.loc[blank_mask, col] = fallback.loc[blank_mask]

    return work



def normalize_controller_config(
    *,
    view_toggles,
    filters=None,
    filters_by_view=None,
) -> Dict[str, Any]:
    """Normalize the existing operator-facing Controller shapes for run()."""
    view_map: Dict[str, bool] = {}
    for item in (view_toggles or []):
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            view_id = _safe_str(item[0])
            if view_id:
                view_map[view_id] = bool(item[1])

    def parse_filter_value(raw_value):
        s = _safe_str(raw_value)
        if not s:
            return ""
        if s.startswith("~"):
            return s
        if "," in s:
            return [x.strip() for x in s.split(",") if x.strip()]
        return s

    global_filters: Dict[str, Any] = {}
    for field_name, rule in (filters or {}).items():
        if not isinstance(rule, (list, tuple)) or len(rule) < 2:
            continue
        enabled, raw_value = rule[0], rule[1]
        if bool(enabled):
            global_filters[_safe_str(field_name)] = parse_filter_value(raw_value)

    view_filters: Dict[str, Dict[str, Any]] = {}
    for view_id, field_rules in (filters_by_view or {}).items():
        one: Dict[str, Any] = {}
        for field_name, rule in (field_rules or {}).items():
            if not isinstance(rule, (list, tuple)) or len(rule) < 2:
                continue
            enabled, raw_value = rule[0], rule[1]
            if bool(enabled):
                one[_safe_str(field_name)] = parse_filter_value(raw_value)
        if one:
            view_filters[_safe_str(view_id)] = one

    return {
        "view_toggles": view_map,
        "global_filters": global_filters,
        "view_filter_overrides": view_filters,
    }


# =========================================================
# 主流程
# =========================================================

def build_and_write_view(
    *,
    sh_data,
    cfg_tabs_df: pd.DataFrame,
    cfg_fields_df: pd.DataFrame,
    df_idx_products: pd.DataFrame,
    df_idx_variants: pd.DataFrame,
    view_id: str,
    long_value_map: Optional[Dict[Tuple[str, str, str], str]] = None,
    df_dl_values_long: Optional[pd.DataFrame] = None,
    global_filters: Optional[Dict[str, Any]] = None,
    filter_mode: str = "AND",
    view_filter_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    verbose: bool = True,
) -> Dict[str, Any]:

    view_id = _safe_str(view_id)
    global_filters = global_filters or {}
    view_filter_overrides = view_filter_overrides or {}
    if long_value_map is None:
        long_value_map = _build_long_value_map(df_dl_values_long if df_dl_values_long is not None else pd.DataFrame())

    tab_row = cfg_tabs_df[cfg_tabs_df["view_id"].astype(str).str.strip() == view_id]
    if tab_row.empty:
        raise ValueError(f"Cfg__ExportTabs 找不到 view_id={view_id}")

    tab = tab_row.iloc[0].to_dict()

    target_sheet = _safe_str(tab.get("target_sheet")) or view_id
    layout = _safe_str(tab.get("layout")) or "WIDE"
    base_entity_type = _safe_str(tab.get("base_entity_type")).upper()
    base_sheet = _safe_str(tab.get("base_sheet")).upper()
    base_key_field_id = _safe_str(tab.get("base_key_field_id"))
    fixed_filter_mode = _safe_str(tab.get("fixed_filter_mode")) or filter_mode or "AND"
    fixed_filters_json = _safe_json_loads(tab.get("fixed_filters_json"))

    if layout != "WIDE":
        raise ValueError(f"当前只支持 WIDE，view_id={view_id}, layout={layout}")

    if base_entity_type not in ("PRODUCT", "VARIANT"):
        raise ValueError(f"view_id={view_id} 的 base_entity_type 非 PRODUCT/VARIANT")

    vf = cfg_fields_df[cfg_fields_df["view_id"].astype(str).str.strip() == view_id].copy()
    if vf.empty:
        raise ValueError(f"Cfg__ExportTabFields 找不到 view_id={view_id} 的字段")

    if "seq" in vf.columns:
        vf["seq"] = pd.to_numeric(vf["seq"], errors="coerce")
        vf = vf.sort_values(["seq", "field_id"], na_position="last")

    if "agg" not in vf.columns:
        vf["agg"] = ""

    df_products = _dedupe_columns_keep_first(df_idx_products.copy())
    df_variants = _dedupe_columns_keep_first(df_idx_variants.copy())

    global_pf, global_vf = split_filters_by_entity(global_filters)
    fixed_pf, fixed_vf = split_filters_by_entity(fixed_filters_json)
    override_pf, override_vf = split_filters_by_entity(view_filter_overrides.get(view_id, {}))

    merged_pf = {}
    merged_pf.update(global_pf)
    merged_pf.update(fixed_pf)
    merged_pf.update(override_pf)

    merged_vf = {}
    merged_vf.update(global_vf)
    merged_vf.update(fixed_vf)
    merged_vf.update(override_vf)

    expand_dependencies = _collect_expand_dependencies(vf)

    needed_product_cols = set(vf[vf["entity_type"].astype(str).str.upper().eq("PRODUCT")]["field_id"].astype(str).tolist())
    needed_variant_cols = set(vf[vf["entity_type"].astype(str).str.upper().eq("VARIANT")]["field_id"].astype(str).tolist())
    needed_product_cols |= set(merged_pf.keys())
    needed_variant_cols |= set(merged_vf.keys())
    needed_product_cols |= expand_dependencies["PRODUCT"]
    needed_variant_cols |= expand_dependencies["VARIANT"]

    df_products = ensure_columns_from_long(df_products, list(needed_product_cols), long_value_map)
    df_variants = ensure_columns_from_long(df_variants, list(needed_variant_cols), long_value_map)

    product_filters_exist = len(merged_pf) > 0
    if base_entity_type == "VARIANT" and product_filters_exist:
        df_products_for_filter = apply_entity_filters(df_products.copy(), merged_pf, fixed_filter_mode)

        if "PRODUCT|core.gid" not in df_products_for_filter.columns:
            raise ValueError("IDX__Products 缺 PRODUCT|core.gid，无法按 PRODUCT filters 预筛 VARIANT")

        allowed_product_gids = set(_extract_gid_like(x) for x in df_products_for_filter["PRODUCT|core.gid"].tolist())

        if "VARIANT|core.product_gid" not in df_variants.columns:
            raise ValueError("IDX__Variants 缺 VARIANT|core.product_gid，无法按 PRODUCT filters 预筛 VARIANT")

        before_n = len(df_variants)
        df_variants = df_variants[
            df_variants["VARIANT|core.product_gid"].map(_extract_gid_like).isin(allowed_product_gids)
        ].copy()
        if verbose:
            print(f"prefilter variants by product filters: {before_n} -> {len(df_variants)}")

    if base_entity_type == "PRODUCT":
        base_df = apply_entity_filters(df_products.copy(), merged_pf, fixed_filter_mode)
    else:
        base_df = apply_entity_filters(df_variants.copy(), merged_vf, fixed_filter_mode)

    if base_entity_type == "PRODUCT":
        variant_rows = vf[vf["entity_type"].astype(str).str.upper().eq("VARIANT")].copy()
        if not variant_rows.empty:
            if "PRODUCT|core.gid" not in base_df.columns:
                raise ValueError("IDX__Products 缺 PRODUCT|core.gid，无法 merge 聚合后的 VARIANT 字段")

            variant_agg_df = _aggregate_variant_fields_to_product(df_variants, variant_rows)
            if not variant_agg_df.empty:
                base_df = base_df.merge(variant_agg_df, how="left", on="PRODUCT|core.gid")

    if base_entity_type == "VARIANT":
        if "VARIANT|core.product_gid" not in base_df.columns:
            raise ValueError("IDX__Variants 缺 VARIANT|core.product_gid，无法 join PRODUCT 字段")
        if "PRODUCT|core.gid" not in df_products.columns:
            raise ValueError("IDX__Products 缺 PRODUCT|core.gid，无法 join 到 VARIANT")

        needed_product_cols2 = set(vf[vf["entity_type"].astype(str).str.upper().eq("PRODUCT")]["field_id"].astype(str).tolist())
        needed_product_cols2 |= set(merged_pf.keys())
        needed_product_cols2 |= expand_dependencies["PRODUCT"]

        take_cols = ["PRODUCT|core.gid"] + [c for c in needed_product_cols2 if c in df_products.columns]
        prod_take = df_products[take_cols].drop_duplicates(subset=["PRODUCT|core.gid"]).copy()
        prod_take["__product_gid_norm"] = prod_take["PRODUCT|core.gid"].map(_extract_gid_like)

        base_df = base_df.copy()
        base_df["__variant_product_gid_norm"] = base_df["VARIANT|core.product_gid"].map(_extract_gid_like)
        base_df = base_df.merge(
            prod_take.drop(columns=["PRODUCT|core.gid"]).rename(columns={"__product_gid_norm": "__variant_product_gid_norm"}),
            how="left",
            on="__variant_product_gid_norm",
        )

        if product_filters_exist:
            base_df = apply_entity_filters(base_df, merged_pf, fixed_filter_mode)

    if base_key_field_id and base_key_field_id not in base_df.columns:
        short_key = base_key_field_id.split("|", 1)[-1] if "|" in base_key_field_id else base_key_field_id
        if short_key in base_df.columns:
            base_key_field_id = short_key

    if base_key_field_id and base_key_field_id not in base_df.columns:
        raise ValueError(f"view_id={view_id} 的 base_key_field_id 在 base_df 中不存在：{base_key_field_id}")

    field_rows_raw = _prepare_field_rows(vf)

    # Keep ordinary image-list fields exactly as configured.
    # EXPAND rows are intentionally converted into multiple physical columns.
    field_rows = _expand_field_rows_for_output(field_rows_raw)

    # Build related Product/Variant row maps after metafield backfill so every
    # RAW field can use IDX first and DL__ValuesLong as a cell-level fallback.
    row_maps = _build_row_maps(df_products, df_variants)
    variant_product_bridge = _build_variant_product_bridge(
        df_variants,
        row_maps,
    )

    required_expand_sources = sorted(
        expand_dependencies["PRODUCT"] | expand_dependencies["VARIANT"]
    )
    missing_expand_sources = [
        field_id
        for field_id in required_expand_sources
        if field_id not in base_df.columns
    ]
    if missing_expand_sources:
        raise ValueError(
            "EXPAND_LIST source field is missing from the prepared Product/Variant data: "
            f"{missing_expand_sources}. Ensure the source field, such as "
            "PRODUCT|core.tags, is present in IDX__Products / IDX__Variants, "
            "then rerun export_idx_tables before build_product_views."
        )

    output_keys = [fr["output_key"] for fr in field_rows]
    display_headers_raw = [fr["alias"] or fr["field_id"] or fr["output_key"] for fr in field_rows]
    display_headers = _make_display_headers(display_headers_raw)

    out_rows = []
    for _, base_row in base_df.iterrows():
        base_row = base_row.copy()
        related_rows = _resolve_related_rows(
            base_entity_type=base_entity_type,
            base_row=base_row,
            row_maps=row_maps,
            variant_product_bridge=variant_product_bridge,
        )
        one = {}

        for fr in field_rows:
            field_type = _safe_str(fr["field_type"]).upper()
            output_key = fr["output_key"]

            if field_type == "EXPAND_ITEM":
                source_field_id = _safe_str(fr.get("source_field_id"))
                source_index = int(fr.get("source_index") or 0)
                source_items = _coerce_expand_list(
                    base_row.get(source_field_id, "")
                )
                if source_index >= 1 and source_index <= len(source_items):
                    one[output_key] = _safe_str(source_items[source_index - 1])
                else:
                    one[output_key] = ""
            elif field_type == "CALC" and _safe_str(fr["expr"]).startswith("="):
                one[output_key] = ""
            else:
                one[output_key] = _resolve_raw_value(
                    field_row=fr,
                    related_rows=related_rows,
                    long_value_map=long_value_map,
                    base_row=base_row,
                )

        out_rows.append(one)

    out_df = pd.DataFrame(out_rows, columns=output_keys).fillna("")

    target_rows = max(len(out_df) + 20, 1000)
    target_cols = max(len(display_headers) + 10, 50)
    ws = ensure_worksheet(sh_data, target_sheet, rows=target_rows, cols=target_cols)
    resize_worksheet(ws, target_rows, target_cols)
    clear_worksheet(ws)

    if len(display_headers) == 0:
        ws_update(ws, "A1", [["No columns"]])
        return {
            "view_id": view_id,
            "target_sheet": target_sheet,
            "rows_written": 0,
            "cols_written": 0,
        }

    ws_update(ws, f"A1:{col_to_letter(len(display_headers))}1", [display_headers])

    if len(out_df) > 0:
        body_values = out_df[output_keys].astype(str).values.tolist()
        ws_update(ws, f"A2:{col_to_letter(len(display_headers))}{len(body_values)+1}", body_values)

    token_to_col_letter = {}
    for i, fr in enumerate(field_rows, start=1):
        col_letter = col_to_letter(i)
        output_key = fr["output_key"]
        field_id = _safe_str(fr["field_id"])
        alias = _safe_str(fr["alias"])
        token_to_col_letter[output_key] = col_letter
        if field_id:
            token_to_col_letter[field_id] = col_letter
        if alias:
            token_to_col_letter[alias] = col_letter

    formula_cols = []
    for i, fr in enumerate(field_rows, start=1):
        expr = _safe_str(fr["expr"])
        field_type = _safe_str(fr["field_type"]).upper()
        if field_type == "CALC" and expr.startswith("="):
            formula_cols.append((i, expr, fr["output_key"]))

    if formula_cols and len(out_df) > 0:
        write_formula_columns_filldown(
            ws=ws,
            formula_cols=formula_cols,
            start_row_1based=2,
            nrows=len(out_df),
            token_to_col_letter=token_to_col_letter,
        )

    if verbose:
        print(f"view={view_id}")
        print(f"target_sheet={target_sheet}")
        print(f"base_entity_type={base_entity_type}")
        print(f"base_sheet={base_sheet}")
        print(f"base_key_field_id={base_key_field_id}")
        print(f"rows={len(out_df)} cols={len(display_headers)}")

    return {
        "view_id": view_id,
        "target_sheet": target_sheet,
        "rows_written": int(len(out_df)),
        "cols_written": int(len(display_headers)),
    }


def _get_view_target_sheet_label(
    cfg_tabs_df: pd.DataFrame,
    view_id: str,
    default_label: str = "export_product",
) -> str:
    """
    Resolve which spreadsheet label a view should write to.

    Cfg__ExportTabs can include target_sheet_label.
    - blank / missing target_sheet_label => export_product
    - non-blank target_sheet_label => lookup that label in Console Core Cfg__Sites

    This keeps IDX__Products / IDX__Variants / DL__ValuesLong in export_product,
    while allowing heavy output views to be split into export_product_view_2,
    export_product_view_3, etc.
    """
    view_id = _safe_str(view_id)
    if cfg_tabs_df is None or cfg_tabs_df.empty:
        raise ValueError("Cfg__ExportTabs is empty")

    if "view_id" not in cfg_tabs_df.columns:
        raise ValueError("Cfg__ExportTabs 缺少必要字段：view_id")

    hit = cfg_tabs_df[cfg_tabs_df["view_id"].astype(str).str.strip() == view_id]
    if hit.empty:
        raise ValueError(f"Cfg__ExportTabs 找不到 view_id={view_id}")

    row = hit.iloc[0]
    if "target_sheet_label" in cfg_tabs_df.columns:
        label = _safe_str(row.get("target_sheet_label"))
        if label:
            return label

    return default_label


def run(
    *,
    site_code: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    view_toggles: Dict[str, bool],
    global_filters: Optional[Dict[str, Any]] = None,
    filter_mode: str = "AND",
    view_filter_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    verbose: bool = True,
):
    gc = make_gspread_client_from_b64(gsheet_sa_b64)

    config_url = get_label_sheet_url_from_cfg_sites(gc, console_core_url, site_code, "config")

    # Source sheet: always read IDX / DL from the canonical export_product file.
    # Output sheets: resolved per view by Cfg__ExportTabs.target_sheet_label.
    source_export_product_url = get_label_sheet_url_from_cfg_sites(
        gc,
        console_core_url,
        site_code,
        "export_product",
    )

    sh_cfg = _open_by_url(gc, config_url, action="run.open_config")
    sh_source = _open_by_url(gc, source_export_product_url, action="run.open_export_product")

    ws_tabs = _get_worksheet(sh_cfg, "Cfg__ExportTabs", action="run.worksheet:Cfg__ExportTabs")
    ws_fields = _get_worksheet(sh_cfg, "Cfg__ExportTabFields", action="run.worksheet:Cfg__ExportTabFields")
    cfg_tabs_df = read_ws_df(ws_tabs)
    cfg_fields_df = read_ws_df(ws_fields)

    ws_idx_products = _get_worksheet(sh_source, "IDX__Products", action="run.worksheet:IDX__Products")
    ws_idx_variants = _get_worksheet(sh_source, "IDX__Variants", action="run.worksheet:IDX__Variants")
    ws_dl = _get_worksheet(sh_source, "DL__ValuesLong", action="run.worksheet:DL__ValuesLong")

    df_idx_products = read_ws_df(ws_idx_products)
    df_idx_variants = read_ws_df(ws_idx_variants)
    df_dl_values_long = read_ws_df(ws_dl)

    df_idx_products = normalize_idx_columns(df_idx_products, "PRODUCT")
    df_idx_variants = normalize_idx_columns(df_idx_variants, "VARIANT")
    long_value_map = _build_long_value_map(df_dl_values_long)

    if verbose:
        print("loaded:")
        print("  cfg tabs rows        :", len(cfg_tabs_df))
        print("  cfg fields rows      :", len(cfg_fields_df))
        print("  source label         :", "export_product")
        print("  source spreadsheet   :", getattr(sh_source, "title", ""))
        print("  idx products rows    :", len(df_idx_products))
        print("  idx variants rows    :", len(df_idx_variants))
        print("  dl rows              :", len(df_dl_values_long))
        print("  dl long keys         :", len(long_value_map))
        print("  products dup cols    :", df_idx_products.columns.duplicated().any())
        print("  variants dup cols    :", df_idx_variants.columns.duplicated().any())
        print("  has product_gid      :", "VARIANT|core.product_gid" in df_idx_variants.columns)
        print("  has product.gid      :", "VARIANT|core.product.gid" in df_idx_variants.columns)
        print("  has target label col :", "target_sheet_label" in cfg_tabs_df.columns)

    enabled_view_ids = [k for k, v in (view_toggles or {}).items() if bool(v)]
    if not enabled_view_ids:
        raise ValueError("没有任何启用的 view。请在 VIEW_TOGGLES 里至少打开一个 True。")

    # Cache opened output spreadsheets by Cfg__Sites label to avoid repeated open_by_url calls.
    output_sheet_cache: Dict[str, Any] = {
        "export_product": sh_source,
    }
    output_url_cache: Dict[str, str] = {
        "export_product": source_export_product_url,
    }

    results = []
    for vid in enabled_view_ids:
        target_sheet_label = _get_view_target_sheet_label(
            cfg_tabs_df=cfg_tabs_df,
            view_id=vid,
            default_label="export_product",
        )

        if target_sheet_label not in output_sheet_cache:
            target_url = get_label_sheet_url_from_cfg_sites(
                gc,
                console_core_url,
                site_code,
                target_sheet_label,
            )
            output_sheet_cache[target_sheet_label] = _open_by_url(
                gc,
                target_url,
                action=f"run.open_output:{target_sheet_label}",
            )
            output_url_cache[target_sheet_label] = target_url

        sh_out = output_sheet_cache[target_sheet_label]

        if verbose:
            print(f"\n=== build view: {vid} ===")
            print("target_sheet_label:", target_sheet_label)
            print("target_spreadsheet:", getattr(sh_out, "title", ""))

        res = build_and_write_view(
            sh_data=sh_out,
            cfg_tabs_df=cfg_tabs_df,
            cfg_fields_df=cfg_fields_df,
            df_idx_products=df_idx_products,
            df_idx_variants=df_idx_variants,
            view_id=vid,
            long_value_map=long_value_map,
            global_filters=global_filters or {},
            filter_mode=filter_mode,
            view_filter_overrides=view_filter_overrides or {},
            verbose=verbose,
        )
        res["target_sheet_label"] = target_sheet_label
        res["target_spreadsheet_title"] = getattr(sh_out, "title", "")
        results.append(res)

        if verbose:
            print("done:", res)

    return {
        "site_code": site_code,
        "source_sheet_label": "export_product",
        "source_spreadsheet_title": getattr(sh_source, "title", ""),
        "view_count": len(results),
        "output_sheet_labels": sorted(output_sheet_cache.keys()),
        "results": results,
        "finished_at": _now_ts(),
    }
