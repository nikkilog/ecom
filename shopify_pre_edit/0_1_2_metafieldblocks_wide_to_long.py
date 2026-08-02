# shopify_pre_edit/0_1_2_metafieldblocks_wide_to_long.py

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Optional

import gspread
import pandas as pd
from google.oauth2 import service_account

# =========================================================
# Constants
# =========================================================

CFG_SITES_TAB_DEFAULT = "Cfg__Sites"
CFG_ACCOUNT_TAB_DEFAULT = "Cfg__account_id"
CFG_FIELDS_TAB_DEFAULT = "Cfg__Fields"

MODULE_PATH = "shopify_pre_edit.0_1_2_metafieldblocks_wide_to_long"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"

JOB_NAME = "wide_to_metafieldblocks"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

WIDE_INPUT_TAB_DEFAULT = "Wide_MFBs"
LONG_OUTPUT_TAB_DEFAULT = "Long_MFBs"

LONG_HEADER = [
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

DEFAULT_IGNORE_COLUMNS = {
    "",
    "来源",
    "source",
    "SKU",
    "sku",
    "Product ID (numeric)",
    "Product ID",
    "Product GID",
    "gid_or_handle",
    "entity_type",
    "Variant ID (numeric)",
    "Variant ID",
    "Collection ID (numeric)",
    "Page ID (numeric)",
    "Variant Base",
    "Variant Root",
    "Product Title",
    "Handle",
    "handle",
    "note",
    "Note",
    "备注",
}

SPLIT_COMPAT_SEPARATORS = (";", "|", "\n")

SUPPORTED_MFB_SCALAR_TEXT_TYPES = {
    "multi_line_text_field",
    "single_line_text_field",
}


def _is_list_data_type(data_type: str) -> bool:
    return _norm_str(data_type).lower().startswith("list.")


def _is_supported_mfb_data_type(data_type: str) -> bool:
    dt_ = _norm_str(data_type).lower()
    return (
        _is_list_data_type(dt_)
        or dt_ == "rich_text_field"
        or dt_ in SUPPORTED_MFB_SCALAR_TEXT_TYPES
    )


# =========================================================
# Small helpers
# =========================================================

def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _utc_run_id(prefix: str = JOB_NAME) -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    return s


def _norm_key(x: Any) -> str:
    return re.sub(r"\s+", " ", _norm_str(x)).strip().lower()


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _safe_int(x: Any) -> Optional[int]:
    s = _norm_str(x)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        v = _norm_str(item)
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _split_list_cell(value: Any) -> list[str]:
    s = _norm_str(value)
    if not s:
        return []

    # JSON array compatibility, only for legacy/automation input.
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [_norm_str(x) for x in arr if _norm_str(x)]
        except Exception:
            pass

    parts = [s]
    for sep in SPLIT_COMPAT_SEPARATORS:
        new_parts = []
        for p in parts:
            new_parts.extend(p.split(sep))
        parts = new_parts

    return [_norm_str(p) for p in parts if _norm_str(p)]


def _has_legacy_separator(value: Any) -> bool:
    s = _norm_str(value)
    return any(sep in s for sep in SPLIT_COMPAT_SEPARATORS)


def _col_to_a1(col_num: int) -> str:
    if col_num <= 0:
        raise ValueError(f"Invalid column number: {col_num}")
    result = ""
    n = col_num
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


# =========================================================
# Runtime / Secret / Workspace Registry / Google routing
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
    """Resolve one Secret without exposing its value."""
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
    aliases: tuple[str, ...] = ()
    normalized_secret_name = secret_name.upper()
    if normalized_secret_name.endswith("_GSHEET"):
        canonical_name = f"{resolved_project_code}_GSHEET"
        if canonical_name != secret_name:
            aliases = (canonical_name,)

    result = resolver.read(secret_name, aliases=aliases)
    return _workspace_secret_result_to_value(result)


def _parse_service_account_text(raw_value: str) -> tuple[dict[str, Any], str]:
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
) -> tuple[gspread.Client, dict[str, str]]:
    info, secret_format = _parse_service_account_text(secret.value)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds), {
        "source_type": secret.source_type,
        "source_detail": secret.source_detail,
        "secret_format": secret_format,
        "service_account_email": _norm_str(info.get("client_email")),
    }


def build_gsheet_client(
    gsheet_sa_secret_or_value: str,
    *,
    project_code: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> gspread.Client:
    """
    Compatibility helper.

    Accepts either:
    - raw/Base64 service-account JSON value; or
    - a Secret name such as PBS_GSHEET when project_code is supplied.
    """
    raw = _norm_str(gsheet_sa_secret_or_value)
    if not raw:
        raise ValueError("Google service-account Secret/value is empty.")

    # Fast path: already a credential value.
    try:
        info, _ = _parse_service_account_text(raw)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)
    except RuntimeError:
        pass

    if not project_code:
        raise ValueError(
            "A Secret name was supplied to build_gsheet_client but project_code is missing."
        )

    secret = read_secret(
        raw,
        project_code=project_code,
        secret_home=secret_home,
    )
    gc, _ = _build_gspread_client_from_secret(secret)
    return gc


def resolve_workspace_project(
    *,
    project_code: str,
    workspace_registry_id: str,
    workspace_gsheet_secret_name: str = "WORKSPACE_GSHEET",
    workspace_registry_tab: str = "Cfg__Projects",
    secret_home: Optional[str] = None,
    explicit_workspace_sa_value: Optional[str] = None,
    print_progress: bool = True,
) -> dict[str, str]:
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

    header_map: dict[str, int] = {}
    duplicate_headers: list[str] = []
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

    matches: list[tuple[int, list[Any]]] = []
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
) -> dict[str, Any]:
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

    account_cfg = load_account_config(
        gc_console=gc,
        console_core_url=route["console_core_url"],
        site_code=route["project_code"],
        cfg_account_tab=route["account_config_tab"],
    )
    cfg_gsheet_secret_name = (
        account_cfg.get("GSHEET_SA_B64_SECRET")
        or account_cfg.get("gsheet_sa_b64_secret")
        or ""
    )
    if not cfg_gsheet_secret_name:
        raise ValueError(
            f"{route['account_config_tab']} missing required GSHEET_SA_B64_SECRET."
        )
    if cfg_gsheet_secret_name != route["gsheet_secret_name"]:
        raise ValueError(
            "Workspace Registry Google Secret does not match Cfg__account_id. "
            f"registry={route['gsheet_secret_name']}; cfg={cfg_gsheet_secret_name}"
        )

    if print_progress:
        print(
            "[Runtime Auth] ready | "
            f"project={route['project_code']} | "
            f"google_source={google_auth_meta['source_type']} | "
            f"gsheet_secret={route['gsheet_secret_name']}"
        )

    return {
        "project_route": route,
        "account": {
            "gsheet_secret_name": cfg_gsheet_secret_name,
        },
        "credentials": {
            "gsheet_sa_value": project_google_secret.value,
        },
        "google_client": gc,
        "auth": {
            "runtime_mode": _runtime_mode(),
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
    gsheet_secret_name: str,
    registry_tab: str,
    job_name: str,
    sheet_label: str,
    tab_name: str,
    current_colab_url: str = "",
    current_colab_name: str = "",
    secret_home: Optional[str] = None,
    print_progress: bool = True,
) -> dict[str, Any]:
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
        gsheet_secret_name,
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

    header_map: dict[str, int] = {}
    duplicate_headers: list[str] = []
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
    matches: list[int] = []
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
    changes: list[tuple[str, int, str, str]] = []
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


def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="sheet_route.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(cfg_sites_tab),
        action=f"sheet_route.worksheet:{cfg_sites_tab}",
    )
    rows = _with_sheets_retry(
        ws.get_all_records,
        action=f"sheet_route.get_all_records:{cfg_sites_tab}",
    )
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
    create_if_missing: bool = False,
    default_rows: int = 1000,
    default_cols: int = 30,
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
        action=f"sheet.open_by_url:{label}",
    )

    try:
        ws = _with_sheets_retry(
            lambda: sh.worksheet(worksheet_title),
            action=f"sheet.worksheet:{worksheet_title}",
        )
    except gspread.WorksheetNotFound:
        if not create_if_missing:
            raise
        ws = _with_sheets_retry(
            lambda: sh.add_worksheet(
                title=worksheet_title,
                rows=default_rows,
                cols=default_cols,
            ),
            action=f"sheet.add_worksheet:{worksheet_title}",
        )

    return sh, ws, sheet_url


def load_account_config(
    gc_console: gspread.Client,
    console_core_url: str,
    site_code: str,
    config_sheet_label: str = "config",
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
) -> dict[str, str]:
    """
    Read account/runtime secrets from Console Core / Cfg__account_id.

    Important:
      Cfg__account_id belongs to Console Core itself.
      Do NOT route it through sheet_label=config.
      sheet_label=config is only for business config tabs such as Cfg__Fields.
    """
    sh_console = _with_sheets_retry(
        lambda: gc_console.open_by_url(console_core_url),
        action="account_config.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh_console.worksheet(cfg_account_tab),
        action=f"account_config.worksheet:{cfg_account_tab}",
    )
    rows = _with_sheets_retry(
        ws.get_all_values,
        action=f"account_config.get_all_values:{cfg_account_tab}",
    )
    rows = [row for row in rows if any(_norm_str(x) for x in row)]
    if not rows:
        raise ValueError(f"{cfg_account_tab} is empty in Console Core")

    norm_rows = []
    for row in rows:
        padded = list(row) + ["", ""]
        norm_rows.append([_norm_str(padded[0]), _norm_str(padded[1])])

    first_key = norm_rows[0][0].lower()
    second_key = norm_rows[0][1].lower()
    if first_key == "key" and second_key == "value":
        data_rows = norm_rows[1:]
    elif first_key == "config_key" and second_key == "config_value":
        data_rows = norm_rows[1:]
    else:
        data_rows = norm_rows

    out: dict[str, str] = {}
    duplicates: list[str] = []
    for row_number, (key, value) in enumerate(data_rows, start=2 if data_rows is not norm_rows else 1):
        if not key:
            continue
        if key in out:
            duplicates.append(f"{key}@row{row_number}")
        out[key] = value
    if duplicates:
        raise ValueError(f"Duplicated keys in {cfg_account_tab}: {duplicates}")
    return out


# =========================================================
# Cfg__Fields dictionary
# =========================================================

@dataclass
class FieldDef:
    display_name: str
    field_key: str
    data_type: str
    entity_type: str
    source_type: str


def load_cfg_fields(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    config_sheet_label: str = "config",
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> pd.DataFrame:
    _, ws, _ = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=config_sheet_label,
        worksheet_title=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
        create_if_missing=False,
    )

    rows = _with_sheets_retry(
        ws.get_all_records,
        action=f"cfg_fields.get_all_records:{cfg_tab_fields}",
    )
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(f"{cfg_tab_fields} is empty")

    for c in ["display_name", "field_key", "data_type", "entity_type", "source_type"]:
        if c not in df.columns:
            raise ValueError(f"{cfg_tab_fields} missing required column: {c}")

    d = df.copy()
    d["display_name"] = d["display_name"].astype(str).str.strip()
    d["field_key"] = d["field_key"].astype(str).str.strip()
    d["data_type"] = d["data_type"].astype(str).str.strip().str.lower()
    d["entity_type"] = d["entity_type"].astype(str).str.strip().str.upper()
    d["source_type"] = d["source_type"].astype(str).str.strip().str.upper()

    d = d[
        (d["display_name"] != "")
        & (d["field_key"] != "")
        & (
            d["source_type"].eq("METAFIELD")
            | d["field_key"].str.startswith("mf.")
            | d["field_key"].str.startswith("v_mf.")
        )
    ].copy()

    return d


def build_cfg_display_map(
    cfg_fields: pd.DataFrame,
    target_entity_type: str = "PRODUCT",
) -> dict[str, FieldDef]:
    """
    Build display_name -> FieldDef mapping for the current owner entity type.

    Cfg__Fields may legitimately contain the same display_name for different owners,
    for example:
      PRODUCT / Variant Base -> mf.custom.variant_base
      VARIANT / Variant Base -> v_mf.custom.variant_base

    That is not an error. For Wide_MFBs, the mapping must be unique only within
    the target entity_type currently being generated.
    """
    d = cfg_fields.copy()

    target_entity_type = _norm_str(target_entity_type).upper() or "PRODUCT"

    if "entity_type" not in d.columns:
        raise ValueError("Cfg__Fields missing required column: entity_type")

    d["entity_type"] = d["entity_type"].astype(str).str.strip().str.upper()
    d = d[d["entity_type"].eq(target_entity_type)].copy()

    if d.empty:
        raise ValueError(f"Cfg__Fields has no rows for entity_type={target_entity_type}")

    d["_display_key"] = d["display_name"].apply(_norm_key)

    dup = d[d.duplicated("_display_key", keep=False)].copy()
    if not dup.empty:
        examples = dup[["display_name", "field_key", "data_type", "entity_type"]].head(50).to_dict("records")
        raise ValueError({
            "message": (
                "Cfg__Fields has duplicate display_name within the same entity_type. "
                "For Wide_MFBs mapping, display_name must be unique after filtering by entity_type."
            ),
            "target_entity_type": target_entity_type,
            "examples": examples,
        })

    out = {}
    for r in d.to_dict("records"):
        out[r["_display_key"]] = FieldDef(
            display_name=_norm_str(r.get("display_name")),
            field_key=_norm_str(r.get("field_key")),
            data_type=_norm_str(r.get("data_type")).lower(),
            entity_type=_norm_str(r.get("entity_type")).upper(),
            source_type=_norm_str(r.get("source_type")).upper(),
        )
    return out


def lookup_field_def(
    cfg_map: dict[str, FieldDef],
    display_name: str,
) -> Optional[FieldDef]:
    return cfg_map.get(_norm_key(display_name))


# =========================================================
# Wide header parsing
# =========================================================

@dataclass
class ParsedWideColumn:
    source_col: str
    display_name: str
    block_seq: int
    role: str  # "", "title", "body"
    field_def: FieldDef


def parse_number_suffix(header: str) -> tuple[str, Optional[int]]:
    """
    Parse the trailing sequence number from a Wide_MFBs header.

    Supported examples:
      Key Features-Title-1 -> (Key Features-Title, 1)
      Key Features Body 1  -> (Key Features Body, 1)
      Compatible Brand-1   -> (Compatible Brand, 1)

    The trailing number is block_seq. It is never part of Cfg__Fields.display_name.
    """
    h = _norm_str(header)
    if not h:
        return "", None

    m = re.match(r"^(.*?)[\s_-]+(\d+)$", h)
    if not m:
        return h, None

    base = _norm_str(m.group(1))
    seq = _safe_int(m.group(2))
    return base, seq


def parse_role_from_display(base: str) -> tuple[str, str]:
    """
    Parse rich_text feature role from the base header after removing block_seq.

    Supported standard format:
      Key Features-Title-1 -> parent=Key Features, role=title, block_seq=1
      Key Features-Body-1  -> parent=Key Features, role=body,  block_seq=1

    Backward-compatible format also works:
      Key Features Title 1 -> parent=Key Features, role=title, block_seq=1
      Key Features Body 1  -> parent=Key Features, role=body,  block_seq=1

    Important:
      parent must exist in Cfg__Fields.display_name.
      Title/Body itself should NOT be added as a separate display_name.
    """
    b = _norm_str(base)
    m = re.match(r"^(.*?)[\s_-]+(Title|Body)$", b, flags=re.I)
    if not m:
        return b, ""

    parent = _norm_str(m.group(1))
    role = _norm_str(m.group(2)).lower()
    return parent, role


def classify_block_type(
    data_type: str,
    role: str,
    rich_text_default_block_type: str = "bullet",
) -> str:
    dt_ = _norm_str(data_type).lower()
    role = _norm_str(role).lower()

    if _is_list_data_type(dt_):
        return "list_item"

    if dt_ == "rich_text_field":
        if role in {"title", "body"}:
            return "feature"
        return rich_text_default_block_type

    if dt_ == "multi_line_text_field":
        return "multi_line_text"

    if dt_ == "single_line_text_field":
        return "single_line_text"

    return ""


def parse_wide_columns(
    columns: list[str],
    cfg_map: dict[str, FieldDef],
    ignore_columns: Optional[set[str]] = None,
    rich_text_default_block_type: str = "bullet",
    error_on_unsupported_datatype: bool = True,
) -> tuple[list[ParsedWideColumn], list[dict[str, Any]], list[dict[str, Any]]]:
    ignore = set(DEFAULT_IGNORE_COLUMNS)
    if ignore_columns:
        ignore.update(ignore_columns)

    parsed = []
    errors = []
    warnings = []

    for col in columns:
        col_s = _norm_str(col)
        if not col_s or col_s in ignore:
            continue

        base, seq = parse_number_suffix(col_s)
        if seq is None:
            # This flow is for block/list fields. Single fields should not silently enter this flow.
            if col_s in ignore:
                continue
            # Try exact config only to give a clearer unsupported/single error.
            fd_exact = lookup_field_def(cfg_map, col_s)
            if fd_exact:
                block_type = classify_block_type(
                    fd_exact.data_type,
                    role="",
                    rich_text_default_block_type=rich_text_default_block_type,
                )
                if block_type:
                    errors.append({
                        "source_col": col_s,
                        "error_reason": "missing_block_seq_suffix",
                        "message": "MFB wide columns must use -1/-2/-3 suffix. For rich_text feature pairs, use Display Name-Title-1 and Display Name-Body-1.",
                    })
                elif error_on_unsupported_datatype:
                    errors.append({
                        "source_col": col_s,
                        "display_name": fd_exact.display_name,
                        "field_key": fd_exact.field_key,
                        "data_type": fd_exact.data_type,
                        "error_reason": "unsupported_data_type_for_mfb",
                        "message": "Wide_MFBs supports list.*, rich_text_field, multi_line_text_field, and single_line_text_field fields.",
                    })
            else:
                errors.append({
                    "source_col": col_s,
                    "error_reason": "unknown_wide_column",
                    "message": "Column is not ignored and cannot be found in Cfg__Fields.display_name. If it is metadata, add it to ignore_columns.",
                })
            continue

        # Try exact display name first.
        role = ""
        fd = lookup_field_def(cfg_map, base)
        display_for_lookup = base

        # If exact is not found, try stripping Title/Body role.
        if not fd:
            parent, role_candidate = parse_role_from_display(base)
            if role_candidate:
                fd2 = lookup_field_def(cfg_map, parent)
                if fd2:
                    fd = fd2
                    role = role_candidate
                    display_for_lookup = parent

        if not fd:
            errors.append({
                "source_col": col_s,
                "base_display_name": base,
                "error_reason": "display_name_not_found_in_cfg_fields",
                "message": "Base display_name after removing sequence suffix was not found in Cfg__Fields.",
            })
            continue

        # If exact match exists and role not yet set, still detect Title/Body for rich_text feature columns
        # only when exact display name is not itself the intended standalone config key.
        if not role:
            parent, role_candidate = parse_role_from_display(base)
            if role_candidate and _norm_key(parent) == _norm_key(fd.display_name):
                role = role_candidate

        block_type = classify_block_type(
            fd.data_type,
            role=role,
            rich_text_default_block_type=rich_text_default_block_type,
        )

        if not block_type:
            if error_on_unsupported_datatype:
                errors.append({
                    "source_col": col_s,
                    "display_name": display_for_lookup,
                    "field_key": fd.field_key,
                    "data_type": fd.data_type,
                    "error_reason": "unsupported_data_type_for_mfb",
                    "message": "Wide_MFBs supports list.*, rich_text_field, multi_line_text_field, and single_line_text_field fields.",
                })
            continue

        if (fd.data_type.startswith("list.") or fd.data_type in SUPPORTED_MFB_SCALAR_TEXT_TYPES) and role:
            errors.append({
                "source_col": col_s,
                "display_name": display_for_lookup,
                "field_key": fd.field_key,
                "data_type": fd.data_type,
                "error_reason": "role_column_used_for_non_rich_text_field",
                "message": "Title/Body role columns are only valid for rich_text_field.",
            })
            continue

        parsed.append(ParsedWideColumn(
            source_col=col_s,
            display_name=fd.display_name,
            block_seq=seq,
            role=role,
            field_def=fd,
        ))

    return parsed, errors, warnings


# =========================================================
# Wide loading / long building
# =========================================================

def load_wide_sheet(
    ws_wide,
    header_row: int = 1,
    field_key_row: int = 2,
    data_start_row: int = 3,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Read Wide_MFBs with a two-row header convention.

    Row 1: human wide column names, for example:
      Product ID (numeric), Compatible Brand-1, Compatible Models-1

    Row 2: generated field_key mapping row. It is NOT a data row.
      This row is written by this job as a process record.

    Row 3+: product data rows.
    """
    values = _with_sheets_retry(
        ws_wide.get_all_values,
        action="wide_input.get_all_values",
    )
    if not values or len(values) < header_row:
        return pd.DataFrame(), [], []

    header_idx = header_row - 1
    mapping_idx = field_key_row - 1
    data_idx = data_start_row - 1

    headers = [_norm_str(x) for x in values[header_idx]]
    while headers and headers[-1] == "":
        headers.pop()

    if not headers:
        return pd.DataFrame(), [], []

    # Existing mapping row is returned only for diagnostics; the job regenerates it.
    existing_mapping = []
    if len(values) > mapping_idx:
        existing_mapping = [_norm_str(x) for x in values[mapping_idx][:len(headers)]]
        existing_mapping += [""] * (len(headers) - len(existing_mapping))

    data_rows = values[data_idx:] if len(values) > data_idx else []
    normalized_rows = []
    for row in data_rows:
        padded = list(row[:len(headers)]) + [""] * max(0, len(headers) - len(row))
        normalized_rows.append([_norm_str(x) for x in padded])

    df = pd.DataFrame(normalized_rows, columns=headers)
    if df.empty:
        return df, headers, existing_mapping

    # Drop rows that are completely blank across the known wide columns.
    mask_not_blank = df.apply(lambda r: any(_norm_str(x) for x in r.tolist()), axis=1)
    df = df[mask_not_blank].copy()

    for c in df.columns:
        df[c] = df[c].apply(_norm_str)

    return df, headers, existing_mapping


def build_field_key_mapping_row(
    columns: list[str],
    parsed_cols: list[ParsedWideColumn],
) -> list[str]:
    """Build the row-2 field_key process record for Wide_MFBs."""
    by_col = {p.source_col: p.field_def.field_key for p in parsed_cols}
    return [by_col.get(_norm_str(c), "") for c in columns]


def write_field_key_mapping_row(
    ws_wide,
    mapping_values: list[str],
    field_key_row: int = 2,
) -> dict[str, Any]:
    if not mapping_values:
        return {"mapping_row_written": 0}
    last_col = _col_to_a1(len(mapping_values))
    _with_sheets_retry(
        lambda: ws_wide.update(
            range_name=f"A{field_key_row}:{last_col}{field_key_row}",
            values=[mapping_values],
            value_input_option="RAW",
        ),
        action=f"wide_input.update_mapping_row:{field_key_row}",
    )
    return {"mapping_row_written": 1, "mapping_cols_written": len(mapping_values)}


def get_owner_from_wide_row(
    row: dict[str, Any],
    default_entity_type: str = "PRODUCT",
) -> tuple[str, str]:
    entity_type = _norm_str(row.get("entity_type") or default_entity_type).upper()
    if entity_type not in SUPPORTED_ENTITY_TYPES:
        raise ValueError(f"Unsupported entity_type: {entity_type}")

    # Prefer explicit gid_or_handle.
    gid_or_handle = _norm_str(row.get("gid_or_handle"))

    if not gid_or_handle:
        if entity_type == "PRODUCT":
            gid_or_handle = (
                _norm_str(row.get("Product ID (numeric)"))
                or _norm_str(row.get("Product ID"))
                or _norm_str(row.get("Product GID"))
                or _norm_str(row.get("Handle"))
                or _norm_str(row.get("handle"))
            )
        elif entity_type == "VARIANT":
            gid_or_handle = (
                _norm_str(row.get("Variant ID (numeric)"))
                or _norm_str(row.get("Variant ID"))
                or _norm_str(row.get("SKU"))
            )
        elif entity_type == "COLLECTION":
            gid_or_handle = (
                _norm_str(row.get("Collection ID (numeric)"))
                or _norm_str(row.get("Collection ID"))
                or _norm_str(row.get("Handle"))
                or _norm_str(row.get("handle"))
            )
        elif entity_type == "PAGE":
            gid_or_handle = (
                _norm_str(row.get("Page ID (numeric)"))
                or _norm_str(row.get("Page ID"))
                or _norm_str(row.get("Handle"))
                or _norm_str(row.get("handle"))
            )

    return entity_type, gid_or_handle


def build_long_rows(
    df_wide: pd.DataFrame,
    parsed_cols: list[ParsedWideColumn],
    *,
    data_start_row: int = 3,
    default_entity_type: str = "PRODUCT",
    action_default: str = "SET",
    mode_default: str = "STRICT",
    add_source_note: bool = True,
    dedupe_list_values_per_field: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    out = []
    errors = []
    warnings = []

    if df_wide.empty:
        return out, errors, warnings

    # Group parsed columns by source_col for lookup.
    parsed_by_col = {p.source_col: p for p in parsed_cols}

    for row_idx, row in enumerate(df_wide.to_dict("records"), start=data_start_row):
        try:
            entity_type, gid_or_handle = get_owner_from_wide_row(row, default_entity_type=default_entity_type)
        except Exception as e:
            errors.append({
                "sheet_row": row_idx,
                "error_reason": "invalid_owner",
                "message": str(e),
            })
            continue

        if not gid_or_handle:
            errors.append({
                "sheet_row": row_idx,
                "error_reason": "missing_gid_or_handle",
                "message": "Cannot determine gid_or_handle from wide row.",
            })
            continue

        base_note = _norm_str(row.get("note") or row.get("Note") or row.get("备注"))

        # Feature rows need combining title/body cells into a single long row.
        # Scalar text fields also need combining Wide columns such as
        # Compatible Fitment-1 / Compatible Fitment-2 into one metafield value.
        feature_groups: dict[tuple[str, int], dict[str, Any]] = {}
        scalar_text_groups: dict[str, dict[str, Any]] = {}
        list_seen: dict[str, set[str]] = {}

        for col_name, value in row.items():
            col_name = _norm_str(col_name)
            if col_name not in parsed_by_col:
                continue

            p = parsed_by_col[col_name]
            v = _norm_str(value)
            if not v:
                continue

            fd = p.field_def
            block_type = classify_block_type(fd.data_type, p.role)

            note_parts = []
            if base_note:
                note_parts.append(base_note)
            if add_source_note:
                note_parts.append(f"source_col={col_name}")
            note = " | ".join(note_parts)

            if block_type == "list_item":
                values = _split_list_cell(v)
                if _has_legacy_separator(v) and len(values) > 1:
                    warnings.append({
                        "sheet_row": row_idx,
                        "source_col": col_name,
                        "field_key": fd.field_key,
                        "warning_type": "legacy_separator_split",
                        "message": "List cell contains separator. Standard input should use Display Name-1/-2/-3 columns.",
                        "split_values": values,
                    })

                if dedupe_list_values_per_field:
                    seen_key = fd.field_key
                    list_seen.setdefault(seen_key, set())

                for item in values:
                    if dedupe_list_values_per_field:
                        if item in list_seen[fd.field_key]:
                            continue
                        list_seen[fd.field_key].add(item)

                    out.append({
                        "entity_type": entity_type,
                        "gid_or_handle": gid_or_handle,
                        "field_key": fd.field_key,
                        "block_type": "list_item",
                        "block_seq": str(p.block_seq),
                        "title": "",
                        "body": "",
                        "value": item,
                        "action": action_default,
                        "mode": mode_default,
                        "note": note,
                    })

            elif block_type in {"multi_line_text", "single_line_text"}:
                # Treat each suffixed Wide column as one candidate value, then write one
                # final row per field_key after all columns in this wide row are scanned.
                key = fd.field_key
                g = scalar_text_groups.setdefault(key, {
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": fd.field_key,
                    "block_type": block_type,
                    "block_seq": "",
                    "title": "",
                    "body": "",
                    "value": "",
                    "action": action_default,
                    "mode": mode_default,
                    "note": note,
                    "data_type": fd.data_type,
                    "_items": [],
                    "_source_cols": [],
                })

                # For multi-line text, split legacy separators too, because old sheets may
                # paste multiple fitments into one cell. For single-line text, keep the cell
                # as one logical item and join later with comma.
                if fd.data_type == "multi_line_text_field":
                    values = _split_list_cell(v)
                    if _has_legacy_separator(v) and len(values) > 1:
                        warnings.append({
                            "sheet_row": row_idx,
                            "source_col": col_name,
                            "field_key": fd.field_key,
                            "warning_type": "legacy_separator_split",
                            "message": "Multi-line text cell contains separator. Values were split and joined with line breaks.",
                            "split_values": values,
                        })
                else:
                    values = [v]

                for item in values:
                    item = _norm_str(item)
                    if item:
                        g["_items"].append((p.block_seq, item))
                g["_source_cols"].append(col_name)

            elif block_type == "bullet":
                out.append({
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": fd.field_key,
                    "block_type": "bullet",
                    "block_seq": str(p.block_seq),
                    "title": "",
                    "body": v,
                    "value": "",
                    "action": action_default,
                    "mode": mode_default,
                    "note": note,
                })

            elif block_type == "feature":
                key = (fd.field_key, p.block_seq)
                g = feature_groups.setdefault(key, {
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": fd.field_key,
                    "block_type": "feature",
                    "block_seq": str(p.block_seq),
                    "title": "",
                    "body": "",
                    "value": "",
                    "action": action_default,
                    "mode": mode_default,
                    "note": note,
                    "_source_cols": [],
                })

                if p.role == "title":
                    g["title"] = v
                elif p.role == "body":
                    g["body"] = v
                else:
                    # Rich text field with no Title/Body role uses bullet by default, so this should not happen.
                    g["body"] = v

                g["_source_cols"].append(col_name)

            elif block_type == "paragraph":
                out.append({
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": fd.field_key,
                    "block_type": "paragraph",
                    "block_seq": str(p.block_seq),
                    "title": "",
                    "body": v,
                    "value": "",
                    "action": action_default,
                    "mode": mode_default,
                    "note": note,
                })

        for _, g in sorted(scalar_text_groups.items(), key=lambda kv: kv[0]):
            items_with_seq = sorted(g.get("_items", []), key=lambda x: (_safe_int(x[0]) or 999999999))
            values = [item for _, item in items_with_seq]
            values = _dedupe_keep_order(values)

            data_type = _norm_str(g.get("data_type")).lower()
            if data_type == "multi_line_text_field":
                final_value = "\n".join(values)
            elif data_type == "single_line_text_field":
                final_value = ", ".join(values)
            else:
                final_value = "\n".join(values)

            source_cols = g.get("_source_cols") or []
            note_parts = []
            if base_note:
                note_parts.append(base_note)
            if add_source_note and source_cols:
                note_parts.append("source_cols=" + ",".join(source_cols))

            g["value"] = final_value
            g["body"] = ""
            g["note"] = " | ".join(note_parts)
            g.pop("_items", None)
            g.pop("_source_cols", None)
            g.pop("data_type", None)

            if final_value:
                out.append(g)

        for (_, _), g in sorted(feature_groups.items(), key=lambda kv: kv[0]):
            title = _norm_str(g.get("title"))
            body = _norm_str(g.get("body"))

            if title and not body:
                warnings.append({
                    "sheet_row": row_idx,
                    "field_key": g["field_key"],
                    "block_seq": g["block_seq"],
                    "warning_type": "feature_title_without_body",
                    "message": "Feature title exists but body is empty.",
                })
            if body and not title:
                warnings.append({
                    "sheet_row": row_idx,
                    "field_key": g["field_key"],
                    "block_seq": g["block_seq"],
                    "warning_type": "feature_body_without_title",
                    "message": "Feature body exists but title is empty.",
                })

            g.pop("_source_cols", None)
            if title or body:
                out.append(g)

    out = sorted(
        out,
        key=lambda r: (
            _norm_str(r.get("entity_type")),
            _norm_str(r.get("gid_or_handle")),
            _norm_str(r.get("field_key")),
            _safe_int(r.get("block_seq")) or 999999999,
            _norm_str(r.get("block_type")),
        ),
    )

    return out, errors, warnings


# =========================================================
# Output write
# =========================================================

def ensure_long_header(ws_output):
    values = _with_sheets_retry(
        ws_output.get_all_values,
        action="long_output.get_all_values",
    )
    if not values:
        _with_sheets_retry(
            lambda: ws_output.update(range_name="A1:K1", values=[LONG_HEADER]),
            action="long_output.write_header",
        )
        return

    header = values[0]
    if header[:len(LONG_HEADER)] != LONG_HEADER:
        _with_sheets_retry(
            lambda: ws_output.update(range_name="A1:K1", values=[LONG_HEADER]),
            action="long_output.repair_header",
        )


def write_long_output(
    ws_output,
    long_rows: list[dict[str, Any]],
    clear_output_first: bool = True,
) -> dict[str, Any]:
    df = pd.DataFrame(long_rows)
    if df.empty:
        df = pd.DataFrame(columns=LONG_HEADER)

    for c in LONG_HEADER:
        if c not in df.columns:
            df[c] = ""

    values = [LONG_HEADER] + df[LONG_HEADER].fillna("").astype(str).values.tolist()

    if clear_output_first:
        _with_sheets_retry(
            ws_output.clear,
            action="long_output.clear",
        )

    _with_sheets_retry(
        lambda: ws_output.update(
            range_name=f"A1:K{len(values)}",
            values=values,
            value_input_option="RAW",
        ),
        action=f"long_output.write_rows:{len(values) - 1}",
    )

    return {
        "rows_written": int(len(df)),
    }


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    console_gsheet_sa_b64_secret: str,
    resolved_gsheet_secret_name: str = "",

    input_sheet_label: str = "pre_edit",
    output_sheet_label: str = "pre_edit",
    config_sheet_label: str = "config",

    input_worksheet_title: str = WIDE_INPUT_TAB_DEFAULT,
    output_worksheet_title: str = LONG_OUTPUT_TAB_DEFAULT,

    wide_header_row: int = 1,
    wide_field_key_row: int = 2,
    wide_data_start_row: int = 3,
    write_wide_field_key_row: bool = True,

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,

    default_entity_type: str = "PRODUCT",
    action_default: str = "SET",
    mode_default: str = "STRICT",

    preview_only: bool = True,
    create_output_tab_if_missing: bool = True,
    clear_output_first: bool = True,

    rich_text_default_block_type: str = "bullet",
    ignore_columns: Optional[set[str]] = None,
    error_on_unsupported_datatype: bool = True,

    dedupe_list_values_per_field: bool = True,
    add_source_note: bool = True,

    preview_limit: int = 50,
) -> dict[str, Any]:
    """
    Convert human-friendly Wide_MFBs into Long_MFBs / Edit__MetafieldBlocks-compatible rows.

    Flow:
      pre_edit / Wide_MFBs
        -> 0_1_2_metafieldblocks_wide_to_long.py
      pre_edit / Long_MFBs

    Important:
      - field_key and data_type come only from config / Cfg__Fields.
      - Wide_MFBs row 1 is human header; row 2 is generated field_key mapping; row 3+ is data.
      - Wide column suffix -1/-2/-3 is block_seq, not display_name.
      - Standard list/scalar-text input is Display Name-1 / Display Name-2 / Display Name-3.
      - Standard rich_text feature input is Display Name-Title-1 / Display Name-Body-1.
        Example: if Cfg__Fields.display_name is Key Features, use Key Features-Title-1 and Key Features-Body-1.
      - Backward-compatible rich_text feature input Display Name Title 1 / Display Name Body 1 is also supported.
      - For multi_line_text_field, suffixed columns are merged into one newline-delimited value.
      - Legacy cell separators ;, |, newline are supported with warning.
    """

    run_id = _utc_run_id(JOB_NAME)

    # Bootstrap access to Console Core.
    gc_console = build_gsheet_client(
        console_gsheet_sa_b64_secret,
        project_code=site_code,
    )

    # Read account config from routed config sheet; use site GSHEET SA for business sheets.
    account_cfg = load_account_config(
        gc_console=gc_console,
        console_core_url=console_core_url,
        site_code=site_code,
        config_sheet_label=config_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
        cfg_account_tab=cfg_account_tab,
    )

    site_gsheet_secret = (
        account_cfg.get("GSHEET_SA_B64_SECRET")
        or account_cfg.get("gsheet_sa_b64_secret")
        or resolved_gsheet_secret_name
    )
    if not site_gsheet_secret:
        raise ValueError("Missing GSHEET_SA_B64_SECRET in Cfg__account_id.")

    if resolved_gsheet_secret_name and site_gsheet_secret != resolved_gsheet_secret_name:
        raise ValueError(
            "Resolved project Google Secret does not match Cfg__account_id. "
            f"resolved={resolved_gsheet_secret_name}; cfg={site_gsheet_secret}"
        )

    # The formal Runtime Context already authenticated with the project Service Account.
    # Reuse that client when the configured Secret matches; do not perform duplicate auth.
    if resolved_gsheet_secret_name and site_gsheet_secret == resolved_gsheet_secret_name:
        gc_site = gc_console
    else:
        gc_site = build_gsheet_client(
            site_gsheet_secret,
            project_code=site_code,
        )

    _, ws_wide, input_sheet_url = open_ws_by_label_and_title(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
        create_if_missing=False,
    )

    _, ws_output, output_sheet_url = open_ws_by_label_and_title(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        label=output_sheet_label,
        worksheet_title=output_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
        create_if_missing=create_output_tab_if_missing,
        default_rows=1000,
        default_cols=len(LONG_HEADER),
    )

    cfg_fields = load_cfg_fields(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        config_sheet_label=config_sheet_label,
        cfg_tab_fields=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
    )
    cfg_map = build_cfg_display_map(cfg_fields, target_entity_type=default_entity_type)

    df_wide, wide_columns, existing_field_key_row = load_wide_sheet(
        ws_wide,
        header_row=wide_header_row,
        field_key_row=wide_field_key_row,
        data_start_row=wide_data_start_row,
    )

    parsed_cols, parse_errors, parse_warnings = parse_wide_columns(
        columns=wide_columns,
        cfg_map=cfg_map,
        ignore_columns=ignore_columns,
        rich_text_default_block_type=rich_text_default_block_type,
        error_on_unsupported_datatype=error_on_unsupported_datatype,
    )

    mapping_values = build_field_key_mapping_row(wide_columns, parsed_cols)
    mapping_write_result = {"mapping_row_written": 0, "mapping_cols_written": 0}
    if write_wide_field_key_row:
        mapping_write_result = write_field_key_mapping_row(
            ws_wide,
            mapping_values=mapping_values,
            field_key_row=wide_field_key_row,
        )

    if parse_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "parsed_columns": int(len(parsed_cols)),
                "parse_errors": int(len(parse_errors)),
                "warnings": int(len(parse_warnings)),
                "rows_generated": 0,
                "written": 0,
                **mapping_write_result,
            },
            "errors": parse_errors[:preview_limit],
            "warnings": parse_warnings[:preview_limit],
            "meta": {
                "input_sheet_label": input_sheet_label,
                "output_sheet_label": output_sheet_label,
                "config_sheet_label": config_sheet_label,
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "input_worksheet_title": input_worksheet_title,
                "output_worksheet_title": output_worksheet_title,
                "cfg_tab_fields": cfg_tab_fields,
                "site_gsheet_secret": site_gsheet_secret,
                "wide_header_row": wide_header_row,
                "wide_field_key_row": wide_field_key_row,
                "wide_data_start_row": wide_data_start_row,
            },
        }

    long_rows, build_errors, build_warnings = build_long_rows(
        df_wide=df_wide,
        parsed_cols=parsed_cols,
        data_start_row=wide_data_start_row,
        default_entity_type=default_entity_type,
        action_default=action_default,
        mode_default=mode_default,
        add_source_note=add_source_note,
        dedupe_list_values_per_field=dedupe_list_values_per_field,
    )

    warnings = parse_warnings + build_warnings

    if build_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "parsed_columns": int(len(parsed_cols)),
                "build_errors": int(len(build_errors)),
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "errors": build_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "meta": {
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "input_worksheet_title": input_worksheet_title,
                "output_worksheet_title": output_worksheet_title,
                "cfg_tab_fields": cfg_tab_fields,
                "site_gsheet_secret": site_gsheet_secret,
                "wide_header_row": wide_header_row,
                "wide_field_key_row": wide_field_key_row,
                "wide_data_start_row": wide_data_start_row,
            },
        }

    if preview_only:
        return {
            "status": "preview",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "parsed_columns": int(len(parsed_cols)),
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "parsed_columns": [
                {
                    "source_col": p.source_col,
                    "display_name": p.field_def.display_name,
                    "field_key": p.field_def.field_key,
                    "data_type": p.field_def.data_type,
                    "block_seq": p.block_seq,
                    "role": p.role,
                    "block_type": classify_block_type(p.field_def.data_type, p.role, rich_text_default_block_type),
                }
                for p in parsed_cols[:preview_limit]
            ],
            "meta": {
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "input_worksheet_title": input_worksheet_title,
                "output_worksheet_title": output_worksheet_title,
                "cfg_tab_fields": cfg_tab_fields,
                "generated_at_cn": _now_cn_str(),
                "site_gsheet_secret": site_gsheet_secret,
                "wide_header_row": wide_header_row,
                "wide_field_key_row": wide_field_key_row,
                "wide_data_start_row": wide_data_start_row,
            },
        }

    write_result = write_long_output(
        ws_output=ws_output,
        long_rows=long_rows,
        clear_output_first=clear_output_first,
    )

    return {
        "status": "written",
        "job_name": JOB_NAME,
        "run_id": run_id,
        "site_code": site_code,
        "summary": {
            "rows_loaded": int(len(df_wide)),
            "parsed_columns": int(len(parsed_cols)),
            "warnings": int(len(warnings)),
            "rows_generated": int(len(long_rows)),
            **write_result,
            **mapping_write_result,
        },
        "warnings": warnings[:preview_limit],
        "preview": long_rows[:preview_limit],
        "parsed_columns": [
            {
                "source_col": p.source_col,
                "display_name": p.field_def.display_name,
                "field_key": p.field_def.field_key,
                "data_type": p.field_def.data_type,
                "block_seq": p.block_seq,
                "role": p.role,
                "block_type": classify_block_type(p.field_def.data_type, p.role, rich_text_default_block_type),
            }
            for p in parsed_cols[:preview_limit]
        ],
        "meta": {
            "input_sheet_url": input_sheet_url,
            "output_sheet_url": output_sheet_url,
            "input_worksheet_title": input_worksheet_title,
            "output_worksheet_title": output_worksheet_title,
            "cfg_tab_fields": cfg_tab_fields,
            "generated_at_cn": _now_cn_str(),
            "site_gsheet_secret": site_gsheet_secret,
            "wide_header_row": wide_header_row,
            "wide_field_key_row": wide_field_key_row,
            "wide_data_start_row": wide_data_start_row,
        },
    }
