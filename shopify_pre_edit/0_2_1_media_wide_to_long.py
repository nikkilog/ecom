# shopify_pre_edit/0_2_1_media_wide_to_long.py

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

MODULE_PATH = "shopify_pre_edit.0_2_1_media_wide_to_long"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"

JOB_NAME = "media_wide_to_long"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

WIDE_INPUT_TAB_DEFAULT = "Wide_Media"
LONG_OUTPUT_TAB_DEFAULT = "Long_Media"

LONG_HEADER = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "desired_value",
    "action",
]

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "VARIANT"}
SUPPORTED_ACTIONS = {"SET", "CLEAR", "SKIP"}

PRODUCT_FIELD_KEY = "core.product.images_urls"
VARIANT_FIELD_KEY = "core.variant.image_url"

PRODUCT_ID_HEADERS = (
    "Product ID (numeric)",
    "Product ID",
    "Product GID",
    "Product Handle",
    "Handle",
    "handle",
)
VARIANT_ID_HEADERS = (
    "Variant ID (numeric)",
    "Variant ID",
    "Variant GID",
    "SKU",
    "sku",
)

ENTITY_TYPE_HEADER = "entity_type"
ACTION_HEADER = "action"
VARIANT_IMAGE_HEADER = "Variant Image URL"

PRODUCT_IMAGE_HEADER_RE = re.compile(r"^Product\s+Image\s+URL(?:\s*[-_]\s*|\s+)(\d+)$", re.I)
HTTP_URL_RE = re.compile(r"^https?://", re.I)


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


def _norm_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        value = _norm_str(item)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _col_to_a1(col_num: int) -> str:
    if col_num <= 0:
        raise ValueError(f"Invalid column number: {col_num}")
    result = ""
    n = col_num
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _first_nonempty(row: dict[str, Any], headers: tuple[str, ...]) -> str:
    for header in headers:
        value = _norm_str(row.get(header))
        if value:
            return value
    return ""


def _is_http_url(value: str) -> bool:
    return bool(HTTP_URL_RE.match(_norm_str(value)))

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
# Wide sheet parsing
# =========================================================

def load_wide_sheet(
    ws_wide,
    header_row: int = 1,
    field_key_row: int = 2,
    data_start_row: int = 3,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Row 1: human-friendly headers.
    Row 2: generated field_key mapping row.
    Row 3+: data.
    """
    values = _with_sheets_retry(
        ws_wide.get_all_values,
        action="wide.get_all_values",
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

    duplicate_headers = sorted({h for h in headers if h and headers.count(h) > 1})
    if duplicate_headers:
        raise ValueError(f"Wide_Media has duplicate headers: {duplicate_headers}")

    existing_mapping: list[str] = []
    if len(values) > mapping_idx:
        existing_mapping = [_norm_str(x) for x in values[mapping_idx][: len(headers)]]
        existing_mapping += [""] * (len(headers) - len(existing_mapping))

    data_rows = values[data_idx:] if len(values) > data_idx else []
    normalized_rows: list[list[str]] = []
    for raw in data_rows:
        row = list(raw[: len(headers)]) + [""] * max(0, len(headers) - len(raw))
        normalized_rows.append([_norm_str(x) for x in row])

    df = pd.DataFrame(normalized_rows, columns=headers)
    if df.empty:
        return df, headers, existing_mapping

    not_blank = df.apply(lambda r: any(_norm_str(x) for x in r.tolist()), axis=1)
    df = df[not_blank].copy()

    for col in df.columns:
        df[col] = df[col].apply(_norm_str)

    return df, headers, existing_mapping


def parse_media_columns(headers: list[str]) -> dict[str, Any]:
    product_columns: list[tuple[int, str]] = []
    variant_columns: list[str] = []
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for header in headers:
        h = _norm_str(header)
        if not h:
            continue

        match = PRODUCT_IMAGE_HEADER_RE.match(h)
        if match:
            seq = int(match.group(1))
            if seq <= 0:
                errors.append(
                    {
                        "source_col": h,
                        "error_reason": "invalid_product_image_sequence",
                        "message": "Product Image URL sequence must start at 1.",
                    }
                )
            else:
                product_columns.append((seq, h))
            continue

        if h.lower() == VARIANT_IMAGE_HEADER.lower():
            variant_columns.append(h)

    seq_to_columns: dict[int, list[str]] = {}
    for seq, col in product_columns:
        seq_to_columns.setdefault(seq, []).append(col)

    duplicate_sequences = {seq: cols for seq, cols in seq_to_columns.items() if len(cols) > 1}
    if duplicate_sequences:
        errors.append(
            {
                "error_reason": "duplicate_product_image_sequence",
                "message": f"Duplicate Product Image URL sequence numbers: {duplicate_sequences}",
            }
        )

    if len(variant_columns) > 1:
        errors.append(
            {
                "error_reason": "duplicate_variant_image_column",
                "message": f"Multiple Variant Image URL columns found: {variant_columns}",
            }
        )

    product_columns = sorted(product_columns, key=lambda item: item[0])

    if not product_columns:
        warnings.append(
            {
                "warning_type": "no_product_image_columns",
                "message": "No Product Image URL-N columns were found.",
            }
        )
    if not variant_columns:
        warnings.append(
            {
                "warning_type": "no_variant_image_column",
                "message": "No Variant Image URL column was found.",
            }
        )

    return {
        "product_columns": product_columns,
        "variant_image_column": variant_columns[0] if variant_columns else "",
        "errors": errors,
        "warnings": warnings,
    }


def build_field_key_mapping_row(headers: list[str]) -> list[str]:
    mapping: list[str] = []
    for header in headers:
        h = _norm_str(header)
        if PRODUCT_IMAGE_HEADER_RE.match(h):
            mapping.append(PRODUCT_FIELD_KEY)
        elif h.lower() == VARIANT_IMAGE_HEADER.lower():
            mapping.append(VARIANT_FIELD_KEY)
        else:
            mapping.append("")
    return mapping


def write_field_key_mapping_row(
    ws_wide,
    mapping_values: list[str],
    field_key_row: int = 2,
) -> dict[str, int]:
    if not mapping_values:
        return {"mapping_row_written": 0, "mapping_cols_written": 0}

    last_col = _col_to_a1(len(mapping_values))
    _with_sheets_retry(
        lambda: ws_wide.update(
            range_name=f"A{field_key_row}:{last_col}{field_key_row}",
            values=[mapping_values],
            value_input_option="RAW",
        ),
        action="wide.write_field_key_mapping_row",
    )
    return {
        "mapping_row_written": 1,
        "mapping_cols_written": len(mapping_values),
    }


# =========================================================
# Long row builder
# =========================================================

def _infer_entity_type(row: dict[str, Any]) -> str:
    explicit = _norm_str(row.get(ENTITY_TYPE_HEADER)).upper()
    if explicit:
        return explicit

    product_id = _first_nonempty(row, PRODUCT_ID_HEADERS)
    variant_id = _first_nonempty(row, VARIANT_ID_HEADERS)

    if product_id and not variant_id:
        return "PRODUCT"
    if variant_id and not product_id:
        return "VARIANT"
    return ""


def build_long_rows(
    df_wide: pd.DataFrame,
    product_columns: list[tuple[int, str]],
    variant_image_column: str,
    *,
    data_start_row: int = 3,
    action_default: str = "SET",
    dedupe_product_urls: bool = True,
    strict_url_validation: bool = True,
) -> tuple[list[dict[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    out: list[dict[str, str]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    if df_wide.empty:
        return out, errors, warnings

    action_default = _norm_str(action_default).upper() or "SET"
    if action_default not in SUPPORTED_ACTIONS:
        raise ValueError(f"Invalid action_default: {action_default}")

    for sheet_row, row in enumerate(df_wide.to_dict("records"), start=data_start_row):
        entity_type = _infer_entity_type(row)
        action = _norm_str(row.get(ACTION_HEADER)).upper() or action_default

        product_id = _first_nonempty(row, PRODUCT_ID_HEADERS)
        variant_id = _first_nonempty(row, VARIANT_ID_HEADERS)
        variant_url = _norm_str(row.get(variant_image_column)) if variant_image_column else ""
        product_urls_raw = [_norm_str(row.get(col)) for _, col in product_columns]
        product_urls = [url for url in product_urls_raw if url]

        if action not in SUPPORTED_ACTIONS:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "error_reason": "invalid_action",
                    "message": f"action={action}; supported={sorted(SUPPORTED_ACTIONS)}",
                }
            )
            continue

        if action == "SKIP":
            continue

        if entity_type not in SUPPORTED_ENTITY_TYPES:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "error_reason": "invalid_or_ambiguous_entity_type",
                    "message": (
                        "entity_type must be PRODUCT or VARIANT. If blank, exactly one of "
                        "Product ID / Variant ID must be filled."
                    ),
                }
            )
            continue

        if entity_type == "PRODUCT":
            if not product_id:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "error_reason": "missing_product_id",
                        "message": "PRODUCT row requires Product ID (numeric), Product GID, or Product Handle.",
                    }
                )
                continue

            if variant_id or variant_url:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "gid_or_handle": product_id,
                        "error_reason": "product_row_contains_variant_values",
                        "message": "PRODUCT row must leave Variant ID and Variant Image URL blank.",
                    }
                )
                continue

            if action == "SET" and not product_urls:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "gid_or_handle": product_id,
                        "field_key": PRODUCT_FIELD_KEY,
                        "error_reason": "missing_product_image_urls",
                        "message": "PRODUCT SET requires at least one Product Image URL-N value.",
                    }
                )
                continue

            invalid_urls = [url for url in product_urls if strict_url_validation and not _is_http_url(url)]
            if invalid_urls:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "gid_or_handle": product_id,
                        "field_key": PRODUCT_FIELD_KEY,
                        "error_reason": "invalid_product_image_url",
                        "message": f"Non-HTTP(S) Product Image URL values: {invalid_urls[:5]}",
                    }
                )
                continue

            if dedupe_product_urls:
                deduped = _dedupe_keep_order(product_urls)
                if len(deduped) != len(product_urls):
                    warnings.append(
                        {
                            "sheet_row": sheet_row,
                            "entity_type": entity_type,
                            "gid_or_handle": product_id,
                            "warning_type": "duplicate_product_image_url_removed",
                            "message": f"Removed {len(product_urls) - len(deduped)} duplicate Product Image URL value(s).",
                        }
                    )
                product_urls = deduped

            desired_value = "[]" if action == "CLEAR" else _json_dumps(product_urls)
            out.append(
                {
                    "entity_type": "PRODUCT",
                    "gid_or_handle": product_id,
                    "field_key": PRODUCT_FIELD_KEY,
                    "desired_value": desired_value,
                    "action": action,
                }
            )
            continue

        # VARIANT
        if not variant_id:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "error_reason": "missing_variant_id",
                    "message": "VARIANT row requires Variant ID (numeric), Variant GID, or SKU.",
                }
            )
            continue

        if product_id or product_urls:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": variant_id,
                    "error_reason": "variant_row_contains_product_values",
                    "message": "VARIANT row must leave Product ID and Product Image URL-N columns blank.",
                }
            )
            continue

        if action == "SET" and not variant_url:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": variant_id,
                    "field_key": VARIANT_FIELD_KEY,
                    "error_reason": "missing_variant_image_url",
                    "message": "VARIANT SET requires Variant Image URL.",
                }
            )
            continue

        if strict_url_validation and variant_url and not _is_http_url(variant_url):
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": variant_id,
                    "field_key": VARIANT_FIELD_KEY,
                    "error_reason": "invalid_variant_image_url",
                    "message": f"Variant Image URL is not HTTP(S): {variant_url}",
                }
            )
            continue

        out.append(
            {
                "entity_type": "VARIANT",
                "gid_or_handle": variant_id,
                "field_key": VARIANT_FIELD_KEY,
                "desired_value": "" if action == "CLEAR" else variant_url,
                "action": action,
            }
        )

    return out, errors, warnings


# =========================================================
# Output
# =========================================================

def write_long_output(
    ws_output,
    long_rows: list[dict[str, Any]],
    clear_output_first: bool = True,
) -> dict[str, int]:
    df = pd.DataFrame(long_rows)
    if df.empty:
        df = pd.DataFrame(columns=LONG_HEADER)

    for col in LONG_HEADER:
        if col not in df.columns:
            df[col] = ""

    values = [LONG_HEADER] + df[LONG_HEADER].fillna("").astype(str).values.tolist()

    required_rows = max(1, len(values))
    required_cols = len(LONG_HEADER)
    if ws_output.row_count < required_rows or ws_output.col_count < required_cols:
        _with_sheets_retry(
            lambda: ws_output.resize(
                rows=max(ws_output.row_count, required_rows),
                cols=max(ws_output.col_count, required_cols),
            ),
            action="long.resize_output",
        )

    if clear_output_first:
        _with_sheets_retry(
            ws_output.clear,
            action="long.clear_output",
        )

    _with_sheets_retry(
        lambda: ws_output.update(
            range_name=f"A1:E{required_rows}",
            values=values,
            value_input_option="RAW",
        ),
        action="long.write_output",
    )

    return {"rows_written": int(len(df))}

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

    input_worksheet_title: str = WIDE_INPUT_TAB_DEFAULT,
    output_worksheet_title: str = LONG_OUTPUT_TAB_DEFAULT,

    wide_header_row: int = 1,
    wide_field_key_row: int = 2,
    wide_data_start_row: int = 3,
    write_wide_field_key_row: bool = True,

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,

    action_default: str = "SET",
    preview_only: bool = True,
    create_output_tab_if_missing: bool = True,
    clear_output_first: bool = True,

    dedupe_product_urls: bool = True,
    strict_url_validation: bool = True,
    preview_limit: int = 50,
) -> dict[str, Any]:
    """
    Convert human-friendly Wide_Media into Long_Media.

    Wide_Media standard:
      Row 1: entity_type, Product ID (numeric), Variant ID (numeric),
             Product Image URL-1 ... Product Image URL-N,
             Variant Image URL, action
      Row 2: generated field_key mapping row
      Row 3+: data

    Conversion:
      PRODUCT -> one Long_Media row with core.product.images_urls and an ordered JSON array.
      VARIANT -> one Long_Media row with core.variant.image_url and a scalar URL.

    Product Image URL-N suffix controls final image order. No separate sort field is needed.
    """
    run_id = _utc_run_id(JOB_NAME)

    # Runtime/Auth is resolved outside the business algorithm.
    gc_console = build_gsheet_client(
        console_gsheet_sa_b64_secret,
        project_code=site_code,
    )
    account_cfg = load_account_config(
        gc_console=gc_console,
        console_core_url=console_core_url,
        site_code=site_code,
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

    # Reuse the already-authenticated project client when identity matches.
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

    df_wide, wide_headers, existing_mapping = load_wide_sheet(
        ws_wide,
        header_row=wide_header_row,
        field_key_row=wide_field_key_row,
        data_start_row=wide_data_start_row,
    )

    parsed = parse_media_columns(wide_headers)
    parse_errors = parsed["errors"]
    warnings = list(parsed["warnings"])

    mapping_values = build_field_key_mapping_row(wide_headers)
    mapping_write_result = {"mapping_row_written": 0, "mapping_cols_written": 0}
    if write_wide_field_key_row:
        mapping_write_result = write_field_key_mapping_row(
            ws_wide,
            mapping_values=mapping_values,
            field_key_row=wide_field_key_row,
        )

    base_meta = {
        "input_sheet_label": input_sheet_label,
        "output_sheet_label": output_sheet_label,
        "input_sheet_url": input_sheet_url,
        "output_sheet_url": output_sheet_url,
        "input_worksheet_title": input_worksheet_title,
        "output_worksheet_title": output_worksheet_title,
        "site_gsheet_secret": site_gsheet_secret,
        "wide_header_row": wide_header_row,
        "wide_field_key_row": wide_field_key_row,
        "wide_data_start_row": wide_data_start_row,
        "generated_at_cn": _now_cn_str(),
        "product_field_key": PRODUCT_FIELD_KEY,
        "variant_field_key": VARIANT_FIELD_KEY,
    }

    if parse_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "product_image_columns": int(len(parsed["product_columns"])),
                "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
                "parse_errors": int(len(parse_errors)),
                "warnings": int(len(warnings)),
                "rows_generated": 0,
                "written": 0,
                **mapping_write_result,
            },
            "errors": parse_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": [],
            "meta": base_meta,
        }

    long_rows, build_errors, build_warnings = build_long_rows(
        df_wide=df_wide,
        product_columns=parsed["product_columns"],
        variant_image_column=parsed["variant_image_column"],
        data_start_row=wide_data_start_row,
        action_default=action_default,
        dedupe_product_urls=dedupe_product_urls,
        strict_url_validation=strict_url_validation,
    )
    warnings.extend(build_warnings)

    if build_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "product_image_columns": int(len(parsed["product_columns"])),
                "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
                "build_errors": int(len(build_errors)),
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "errors": build_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "meta": base_meta,
        }

    if preview_only:
        return {
            "status": "preview",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "product_image_columns": int(len(parsed["product_columns"])),
                "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "errors": [],
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "parsed_columns": {
                "product": [
                    {"sequence": seq, "source_col": col, "field_key": PRODUCT_FIELD_KEY}
                    for seq, col in parsed["product_columns"]
                ],
                "variant": (
                    {
                        "source_col": parsed["variant_image_column"],
                        "field_key": VARIANT_FIELD_KEY,
                    }
                    if parsed["variant_image_column"]
                    else None
                ),
            },
            "meta": base_meta,
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
            "product_image_columns": int(len(parsed["product_columns"])),
            "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
            "warnings": int(len(warnings)),
            "rows_generated": int(len(long_rows)),
            **write_result,
            **mapping_write_result,
        },
        "errors": [],
        "warnings": warnings[:preview_limit],
        "preview": long_rows[:preview_limit],
        "parsed_columns": {
            "product": [
                {"sequence": seq, "source_col": col, "field_key": PRODUCT_FIELD_KEY}
                for seq, col in parsed["product_columns"]
            ],
            "variant": (
                {
                    "source_col": parsed["variant_image_column"],
                    "field_key": VARIANT_FIELD_KEY,
                }
                if parsed["variant_image_column"]
                else None
            ),
        },
        "meta": base_meta,
    }
