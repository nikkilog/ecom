from __future__ import annotations

SCRIPT_BUILD = "2026-07-05-customer-order-owner-support"

MODULE_PATH = "shopify_pre_edit.0_1_1_metafields_wide_to_long"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
DEFAULT_JOB_NAME = "wide_to_long"

import base64
import json
import random
import re
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import gspread
from google.oauth2 import service_account
from gspread.exceptions import APIError, WorksheetNotFound

# ============================================================
# Defaults
# ============================================================

CFG_ACCOUNT_TAB_DEFAULT = "Cfg__account_id"
CFG_SITES_TAB_DEFAULT = "Cfg__Sites"

CFG_MATCH_COLUMN_CANDIDATES_DEFAULT = ["display_name", "field_name", "name", "field", "字段名"]
CFG_KEY_COLUMN_CANDIDATES_DEFAULT = ["field_key", "key", "api_key", "字段key", "字段_key", "field_id"]
CFG_ENTITY_COLUMN_CANDIDATES_DEFAULT = ["entity_type", "entity type", "Entity Type", "实体类型"]
CFG_SOURCE_TYPE_COLUMN_CANDIDATES_DEFAULT = ["source_type", "source type"]
CFG_FIELD_TYPE_COLUMN_CANDIDATES_DEFAULT = ["field_type", "field type"]
CFG_DATA_TYPE_COLUMN_CANDIDATES_DEFAULT = ["data_type", "data type"]
CFG_PURPOSE_COLUMN_CANDIDATES_DEFAULT = ["purpose_1", "purpose", "usage", "用途"]

WIDE_HEADER_ROW_DEFAULT = 1
WIDE_WRITE_ROW_DEFAULT = 2

TARGET_WRITES_PER_MIN_DEFAULT = 40
MAX_RETRIES_DEFAULT = 8
BASE_BACKOFF_DEFAULT = 1.2
JITTER_DEFAULT = 0.25

LONG_HEADER = ["entity_type", "gid_or_handle", "field_key", "desired_value", "note", "error_reason"]
RUNLOG_HEADER = [
    "run_id", "ts_cn", "job_name", "phase", "log_type", "status",
    "site_code", "entity_type", "gid", "field_key",
    "rows_loaded", "rows_pending", "rows_recognized", "rows_planned",
    "rows_written", "rows_skipped", "message", "error_reason",
]

# wide_to_long 只读写 Google Sheets，不访问 Shopify / Ads。
# 本 job 不读取 Cfg__account_id，也不校验站点账号字段。
REQUIRED_ACCOUNT_FIELDS: list[str] = []

OWNER_ID_KEYS = {
    "PRODUCT": "product_id",
    "VARIANT": "variant_id",
    "PRODUCTVARIANT": "variant_id",
    "COLLECTION": "collection_id",
    "PAGE": "page_id",
    "CUSTOMER": "customer_id",
    "ORDER": "order_id",
}

OWNER_HEADER_TO_KEY = {
    "PRODUCT": "product_id",
    "VARIANT": "variant_id",
    "COLLECTION": "collection_id",
    "PAGE": "page_id",
    "CUSTOMER": "customer_id",
    "ORDER": "order_id",
}

OWNER_KEY_TO_ENTITY = {
    "product_id": "PRODUCT",
    "variant_id": "VARIANT",
    "collection_id": "COLLECTION",
    "page_id": "PAGE",
    "customer_id": "CUSTOMER",
    "order_id": "ORDER",
}

OWNER_TYPE_WORDS = {
    "PRODUCT": re.compile(r"(?<![a-z0-9])product(?![a-z0-9])", re.I),
    "VARIANT": re.compile(r"(?<![a-z0-9])variant(?![a-z0-9])", re.I),
    "COLLECTION": re.compile(r"(?<![a-z0-9])collection(?![a-z0-9])", re.I),
    "PAGE": re.compile(r"(?<![a-z0-9])page(?![a-z0-9])", re.I),
    "CUSTOMER": re.compile(r"(?<![a-z0-9])customer(?![a-z0-9])", re.I),
    "ORDER": re.compile(r"(?<![a-z0-9])order(?![a-z0-9])", re.I),
}

ID_WORD_RE = re.compile(
    r"(?<![a-z0-9])("
    r"id|gid|legacy[\s_\-]*id|numeric[\s_\-]*id"
    r")(?![a-z0-9])",
    re.I,
)

GENERIC_OWNER_HEADERS_BLOCKLIST = {
    "id",
    "gid",
    "legacy id",
    "legacy_id",
    "core.legacy_id",
    "numeric id",
    "numeric_id",
    "gid_or_handle",
    "shopify id",
    "shopify_id",
}

_last_write_ts = 0.0


# ============================================================
# Basic helpers
# ============================================================

def _is_blank(x: Any) -> bool:
    return x is None or str(x).strip() == ""


def norm_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip().lower()


def norm_header(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def norm_key(x: Any) -> str:
    return "" if x is None else str(x).strip()


def normalize_entity_type(x: Any) -> str:
    s = str(x or "").strip().upper()
    if s in {"PRODUCTVARIANT", "PRODUCT_VARIANT", "VARIATION", "VARIANT"}:
        return "VARIANT"
    if s in {"PRODUCT"}:
        return "PRODUCT"
    if s in {"COLLECTION", "SMART_COLLECTION", "CUSTOM_COLLECTION"}:
        return "COLLECTION"
    if s in {"PAGE"}:
        return "PAGE"
    if s in {"CUSTOMER", "CUSTOMERS"}:
        return "CUSTOMER"
    if s in {"ORDER", "ORDERS"}:
        return "ORDER"
    return s


def trim_text(x: Any, max_len: int = 50000) -> str:
    s = "" if x is None else str(x)
    return s if len(s) <= max_len else s[:max_len]


def summarize_error(e: Exception, max_len: int = 1000) -> str:
    return trim_text(repr(e), max_len=max_len)


def cn_now_str() -> str:
    tz_cn = timezone(timedelta(hours=8))
    return datetime.now(tz_cn).strftime("%Y-%m-%d %H:%M:%S")


def make_run_id(job_name: str) -> str:
    tz_cn = timezone(timedelta(hours=8))
    ts = datetime.now(tz_cn).strftime("%Y%m%d_%H%M%S")
    tail = uuid.uuid4().hex[:8]
    return f"{job_name}__{ts}__{tail}"


class SecretValue:
    def __init__(self, value: str, source_type: str, source_detail: str):
        self.value = str(value).strip()
        self.source_type = str(source_type).strip()
        self.source_detail = str(source_detail).strip()


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _runtime_mode() -> str:
    try:
        import google.colab  # type: ignore  # noqa: F401
        return "COLAB"
    except Exception:
        return "LOCAL"


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
        value=str(result.value).strip(),
        source_type=str(result.source_type).strip(),
        source_detail=str(source_detail).strip(),
    )


def read_secret(
    secret_name: str,
    *,
    project_code: str,
    explicit_value: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> SecretValue:
    """Resolve one Secret without printing its value."""
    name = str(secret_name or "").strip()
    resolved_project_code = str(project_code or "").strip().upper()

    if not name:
        raise ValueError("Secret name is blank.")
    if not resolved_project_code:
        raise ValueError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and str(explicit_value).strip():
        return SecretValue(
            str(explicit_value).strip(),
            "EXPLICIT_VALUE",
            "caller",
        )

    if _runtime_mode() == "COLAB":
        try:
            from google.colab import userdata  # type: ignore
        except Exception as exc:
            raise RuntimeError("Colab Secret adapter is unavailable.") from exc

        value = userdata.get(name)
        if value is None or not str(value).strip():
            raise ValueError(
                f"Colab Secret {name!r} is missing or not enabled for this notebook."
            )
        return SecretValue(
            str(value).strip(),
            "COLAB_SECRETS",
            name,
        )

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

    aliases = ()
    normalized_name = name.upper()
    for suffix in (
        "_GSHEET",
        "_SHOPIFY_ACCESS_TOKEN",
        "_SHOPIFY_TOKEN",
    ):
        if normalized_name.endswith(suffix):
            canonical_name = f"{resolved_project_code}{suffix}"
            if canonical_name != name:
                aliases = (canonical_name,)
            break

    result = resolver.read(name, aliases=aliases)
    return _workspace_secret_result_to_value(result)


def _parse_service_account_text(raw_value: str) -> tuple[dict[str, Any], str]:
    raw = str(raw_value or "").strip()
    if not raw:
        raise ValueError("Google service-account Secret is empty.")

    try:
        info = json.loads(raw)
        secret_format = "RAW_JSON"
    except Exception:
        try:
            padded = raw + "=" * ((4 - len(raw) % 4) % 4)
            info = json.loads(base64.b64decode(padded).decode("utf-8"))
            secret_format = "BASE64_JSON"
        except Exception as exc:
            raise ValueError(
                "Google service-account Secret is neither valid raw JSON nor Base64 JSON."
            ) from exc

    required = {"type", "project_id", "private_key", "client_email", "token_uri"}
    missing = sorted(key for key in required if not info.get(key))

    if missing or info.get("type") != "service_account":
        raise ValueError(
            "Google Secret is not a complete service-account credential; "
            f"missing={missing}."
        )

    return info, secret_format


def build_gsheet_client(
    gsheet_sa_b64_secret: str,
    *,
    project_code: str,
    secret_home: Optional[str] = None,
    explicit_value: Optional[str] = None,
) -> gspread.Client:
    secret = read_secret(
        gsheet_sa_b64_secret,
        project_code=project_code,
        explicit_value=explicit_value,
        secret_home=secret_home,
    )
    sa_info, _secret_format = _parse_service_account_text(secret.value)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


def _build_gsheet_client_from_value(raw_value: str) -> gspread.Client:
    sa_info, _secret_format = _parse_service_account_text(raw_value)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)



def _sheets_error_status(exc: BaseException) -> Optional[int]:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is None:
        status = getattr(response, "status", None)

    try:
        return int(status) if status is not None else None
    except Exception:
        return None


def _is_retryable_sheets_error(
    exc: BaseException,
    *,
    retry_5xx: bool = True,
) -> bool:
    status = _sheets_error_status(exc)

    if status == 429:
        return True
    if retry_5xx and status in {500, 502, 503, 504}:
        return True

    err_text = str(exc).lower()
    return any(
        token in err_text
        for token in (
            "resource_exhausted",
            "ratelimitexceeded",
            "userratelimitexceeded",
            "rate limit exceeded",
            "quota exceeded",
            "read requests per minute",
            "write requests per minute",
            "too many requests",
        )
    )


def _with_sheets_retry(
    operation,
    *,
    action: str,
    max_attempts: int = 8,
    base_sleep: float = 1.2,
    max_delay: float = 20.0,
    retry_5xx: bool = True,
):
    attempts = max(1, int(max_attempts))

    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            retryable = _is_retryable_sheets_error(
                exc,
                retry_5xx=retry_5xx,
            )
            if (not retryable) or attempt >= attempts:
                raise

            delay = min(
                float(max_delay),
                float(base_sleep) * (2 ** (attempt - 1)),
            ) + random.random()

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


def _normalize_registry_header(value: Any) -> str:
    return re.sub(
        r"[\s_]+",
        " ",
        str(value or "").strip().lower(),
    ).strip()


def _extract_spreadsheet_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Workspace Project Registry ID/URL is empty.")

    match = re.search(
        r"/spreadsheets/d/([A-Za-z0-9_-]+)",
        raw,
    )
    if match:
        return match.group(1)

    if re.fullmatch(r"[A-Za-z0-9_-]+", raw):
        return raw

    raise ValueError(
        "Workspace Project Registry must be a Google Sheets ID or URL."
    )


def resolve_workspace_project(
    *,
    project_code: str,
    workspace_registry_id: str,
    workspace_gsheet_secret_name: str = "WORKSPACE_GSHEET",
    workspace_registry_tab: str = "Cfg__Projects",
    secret_home: Optional[str] = None,
    print_progress: bool = True,
) -> dict[str, str]:
    resolved_project_code = str(project_code or "").strip().upper()
    if not resolved_project_code:
        raise ValueError("project_code is required.")

    workspace_secret = read_secret(
        workspace_gsheet_secret_name,
        project_code="WORKSPACE",
        secret_home=secret_home,
    )
    workspace_gc = _build_gsheet_client_from_value(
        workspace_secret.value
    )

    registry_file_id = _extract_spreadsheet_id(
        workspace_registry_id
    )

    registry_book = _with_sheets_retry(
        lambda: workspace_gc.open_by_key(registry_file_id),
        action="workspace.open_registry",
    )
    worksheet = _with_sheets_retry(
        lambda: registry_book.worksheet(workspace_registry_tab),
        action="workspace.open_registry_tab",
    )
    values = _with_sheets_retry(
        lambda: worksheet.get_all_values(),
        action="workspace.read_registry",
    )

    if not values:
        raise ValueError(
            f"Workspace Project Registry tab {workspace_registry_tab!r} is empty."
        )

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
    console_url_col = require_column(
        "console_core_url",
        "console core url",
    )
    gsheet_secret_col = require_column(
        "gsheet_secret_name",
        "gsheet secret name",
    )
    account_tab_col = require_column(
        "account_config_tab",
        "account config tab",
    )

    project_name_col = header_map.get(
        _normalize_registry_header("project_name")
    )

    width = len(values[0])
    matches: list[tuple[int, list[Any]]] = []

    for row_number, raw_row in enumerate(
        values[1:],
        start=2,
    ):
        row = list(raw_row) + [""] * max(
            0,
            width - len(raw_row),
        )

        if str(row[project_col]).strip().upper() == resolved_project_code:
            matches.append((row_number, row))

    if not matches:
        raise ValueError(
            f"Workspace Project Registry has no row for project_code={resolved_project_code}."
        )

    if len(matches) > 1:
        raise ValueError(
            "Workspace Project Registry has duplicate project rows: "
            f"project_code={resolved_project_code}; "
            f"rows={[row_number for row_number, _ in matches]}."
        )

    source_row, row = matches[0]
    active_text = str(row[active_col] or "").strip().lower()

    if active_text not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}; row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": (
            str(row[project_name_col] or "").strip()
            if project_name_col is not None
            else ""
        ),
        "console_core_url": str(row[console_url_col] or "").strip(),
        "gsheet_secret_name": str(row[gsheet_secret_col] or "").strip(),
        "account_config_tab": str(row[account_tab_col] or "").strip(),
        "registry_source_row": str(source_row),
        "workspace_auth_source_type": workspace_secret.source_type,
    }

    missing = [
        key
        for key in (
            "console_core_url",
            "gsheet_secret_name",
            "account_config_tab",
        )
        if not route[key]
    ]

    if missing:
        raise ValueError(
            "Workspace Project Registry route has empty required values: "
            f"project_code={resolved_project_code}; "
            f"fields={missing}; row={source_row}."
        )

    if print_progress:
        print(
            "[Workspace Registry] resolved | "
            f"project={route['project_code']} | "
            f"row={source_row} | "
            f"secret={route['gsheet_secret_name']} | "
            f"account_tab={route['account_config_tab']}"
        )

    return route


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
) -> dict[str, Any]:
    """Check/update exactly one existing Registry row; never append."""
    mode = str(registry_mode or "").strip().upper() or "OFF"
    allowed = {
        "OFF",
        "CHECK",
        "UPDATE_URL",
        "UPDATE_URL_AND_NAME",
    }

    if mode not in allowed:
        raise ValueError(
            f"registry_mode must be one of {sorted(allowed)}."
        )

    if mode == "OFF":
        if print_progress:
            print(
                "[Registry] mode=OFF | "
                f"job_name={job_name} | "
                f"sheet_label={sheet_label} | "
                f"tab_name={tab_name}"
            )
        return {
            "status": "OFF",
            "target_row": None,
            "changed_fields": [],
        }

    if (
        mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"}
        and not str(current_colab_url or "").strip()
    ):
        raise ValueError(
            f"registry_mode={mode} requires current_colab_url."
        )

    if (
        mode == "UPDATE_URL_AND_NAME"
        and not str(current_colab_name or "").strip()
    ):
        raise ValueError(
            "UPDATE_URL_AND_NAME requires current_colab_name."
        )

    secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
    )
    gc = _build_gsheet_client_from_value(secret.value)

    book = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="registry.open_console",
    )
    ws = _with_sheets_retry(
        lambda: book.worksheet(registry_tab),
        action="registry.open_tab",
    )
    values = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="registry.read",
    )

    if not values:
        raise ValueError(
            f"Registry tab {registry_tab!r} is empty."
        )

    header_map: dict[str, int] = {}

    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if not normalized:
            continue
        if normalized in header_map:
            raise ValueError(
                "Registry tab has duplicate normalized header: "
                f"{normalized}."
            )
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
    label_col = require_column(
        "sheet_label",
        "sheet label",
    )
    tab_col = require_column(
        "Tab name",
        "sheet name",
        "sheet_name",
    )
    url_col = require_column(
        "colab_url",
        "colab url",
    )
    name_col = require_column(
        "colab_name",
        "colab name",
    )

    wanted = (
        str(job_name or "").strip().lower(),
        str(sheet_label or "").strip().lower(),
        str(tab_name or "").strip().lower(),
    )

    matches: list[int] = []

    for row_index, row in enumerate(
        values[1:],
        start=2,
    ):
        padded = list(row) + [""] * max(
            0,
            len(values[0]) - len(row),
        )

        logical_key = (
            str(padded[job_col] or "").strip().lower(),
            str(padded[label_col] or "").strip().lower(),
            str(padded[tab_col] or "").strip().lower(),
        )

        if logical_key == wanted:
            matches.append(row_index)

    if not matches:
        raise ValueError(
            "Registry target row was not found. "
            "This function never appends. "
            f"logical_key={wanted}"
        )

    if len(matches) > 1:
        raise ValueError(
            "Registry logical key is duplicated at "
            f"rows={matches}; no row was changed."
        )

    row_number = matches[0]
    current_row = values[row_number - 1] + [""] * max(
        0,
        len(values[0]) - len(values[row_number - 1]),
    )

    changes: list[tuple[str, int, str, str]] = []

    provided_url = str(current_colab_url or "").strip()
    provided_name = str(current_colab_name or "").strip()

    if (
        provided_url
        and str(current_row[url_col] or "").strip() != provided_url
    ):
        changes.append(
            (
                "colab_url",
                url_col + 1,
                str(current_row[url_col] or "").strip(),
                provided_url,
            )
        )

    if (
        provided_name
        and str(current_row[name_col] or "").strip() != provided_name
    ):
        changes.append(
            (
                "colab_name",
                name_col + 1,
                str(current_row[name_col] or "").strip(),
                provided_name,
            )
        )

    if mode == "CHECK":
        status = (
            "CHANGE_DETECTED"
            if changes
            else "NO_CHANGE"
        )
    else:
        permitted = (
            {"colab_url"}
            if mode == "UPDATE_URL"
            else {"colab_url", "colab_name"}
        )
        applied = [
            change
            for change in changes
            if change[0] in permitted
        ]

        for field_name, column_number, _old_value, new_value in applied:
            _with_sheets_retry(
                lambda rn=row_number, cn=column_number, nv=new_value: (
                    ws.update_cell(rn, cn, nv)
                ),
                action=f"registry.update_cell:{field_name}",
                retry_5xx=True,
            )

        changes = applied
        status = (
            "UPDATED"
            if changes
            else "NO_CHANGE"
        )

    if print_progress:
        print(
            "[Registry] "
            f"row={row_number} | "
            f"status={status} | "
            f"changed_fields={[item[0] for item in changes]}"
        )

    return {
        "status": status,
        "target_row": row_number,
        "changed_fields": [
            item[0]
            for item in changes
        ],
    }


def open_ss_by_url(gc: gspread.Client, url: str):
    if _is_blank(url):
        raise ValueError("❌ spreadsheet url 为空")
    return _with_sheets_retry(
        lambda: gc.open_by_url(str(url).strip()),
        action="spreadsheet.open_by_url",
    )


def pick_col_index(header_row: list[Any], candidates: list[str]) -> Optional[int]:
    norm = [norm_header(h) for h in header_row]
    cand_norm = [norm_header(c) for c in candidates]
    for c in cand_norm:
        if c in norm:
            return norm.index(c)
    return None


def _pick_required_idx(header_norm: list[str], candidates: list[str], col_desc: str, tab_name: str) -> int:
    cand_norm = [norm_header(c) for c in candidates]
    for c in cand_norm:
        if c in header_norm:
            return header_norm.index(c)
    raise ValueError(f"❌ {tab_name} 找不到列：{col_desc}，候选={candidates}")


# ============================================================
# Console Core config
# ============================================================

def read_cfg_account(
    gc: gspread.Client,
    console_core_url: str,
    *,
    site_code: str,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    required_fields: Optional[list[str]] = None,
) -> dict[str, str]:
    sh = open_ss_by_url(gc, console_core_url)
    ws = _with_sheets_retry(
        lambda: sh.worksheet(cfg_account_tab),
        action="cfg_account.open_tab",
    )
    values = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="cfg_account.read",
    )

    if not values or len(values) < 2:
        raise ValueError(f"❌ {cfg_account_tab} 为空或不可读")

    required_fields = required_fields or REQUIRED_ACCOUNT_FIELDS

    header = values[0]
    header_norm = [norm_header(x) for x in header]

    # Structure 1: horizontal table with site_code column
    if "site code" in header_norm or "site_code" in [str(x).strip().lower() for x in header]:
        site_idx = None
        for i, h in enumerate(header):
            if norm_header(h) == "site code" or str(h).strip().lower() == "site_code":
                site_idx = i
                break
        if site_idx is None:
            raise ValueError(f"❌ {cfg_account_tab} 检测到横向表，但找不到 site_code 列")

        target = norm_text(site_code)
        matched_row = None
        for r in values[1:]:
            v = r[site_idx] if site_idx < len(r) else ""
            if norm_text(v) == target:
                matched_row = r
                break

        if matched_row is None:
            raise ValueError(f"❌ {cfg_account_tab} 未找到 site_code={site_code} 的账号配置")

        cfg: dict[str, str] = {}
        for i, col in enumerate(header):
            key = str(col).strip()
            if not key:
                continue
            cfg[key] = (matched_row[i].strip() if i < len(matched_row) else "")

    # Structure 2: key / value table, no site_code
    else:
        key_idx = pick_col_index(header, ["key", "name", "field", "config_key", "配置项"])
        val_idx = pick_col_index(header, ["value", "val", "config_value", "配置值"])
        if key_idx is None or val_idx is None:
            # Allow first two columns as key/value.
            key_idx, val_idx = 0, 1

        cfg = {}
        for r in values[1:]:
            k = r[key_idx].strip() if key_idx < len(r) else ""
            v = r[val_idx].strip() if val_idx < len(r) else ""
            if k:
                cfg[k] = v

    missing = [k for k in required_fields if _is_blank(cfg.get(k, ""))]
    if missing:
        raise ValueError(f"❌ {cfg_account_tab} 缺少必填字段或值为空：{missing}")

    return cfg


def assert_bootstrap_secret_matches_account(
    *,
    bootstrap_gsheet_sa_b64_secret: str,
    account_cfg: dict[str, str],
) -> None:
    expected = account_cfg.get("GSHEET_SA_B64_SECRET", "").strip()
    actual = str(bootstrap_gsheet_sa_b64_secret).strip()
    if not expected:
        raise ValueError("❌ Cfg__account_id.GSHEET_SA_B64_SECRET 为空")
    if actual != expected:
        raise ValueError(
            "❌ BOOTSTRAP_GSHEET_SA_B64_SECRET 与 Cfg__account_id.GSHEET_SA_B64_SECRET 不一致。\n"
            f"Cell1={actual}\n"
            f"Cfg__account_id={expected}\n"
            "不允许 fallback，不自动替换。"
        )


def build_runtime_context(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    secret_home: Optional[str] = None,
) -> tuple[gspread.Client, dict[str, str]]:
    del console_core_url, cfg_account_tab  # kept for run() compatibility

    # wide_to_long 是 Google Sheets 内部整理 job：
    # 只需要项目 Google Sheets credential 读取 Console Core / Cfg__Sites / 业务表。
    # 不读取 Cfg__account_id，不校验 Shopify / Ads / Storefront / Admin 字段。
    runtime_gc = build_gsheet_client(
        bootstrap_gsheet_sa_b64_secret,
        project_code=str(site_code),
        secret_home=secret_home,
    )
    return runtime_gc, {}


def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    cfg_sites_sh = open_ss_by_url(gc, console_core_url)
    ws_cfg_sites = _with_sheets_retry(
        lambda: cfg_sites_sh.worksheet(cfg_sites_tab),
        action="cfg_sites.open_tab",
    )

    values = _with_sheets_retry(
        lambda: ws_cfg_sites.get_all_values(),
        action="cfg_sites.read",
    )
    if not values or len(values) < 2:
        raise ValueError(f"❌ {cfg_sites_tab} 为空或不可读")

    header = values[0]
    header_norm = [norm_header(x) for x in header]

    site_code_idx = _pick_required_idx(header_norm, ["site_code", "site code"], "site_code", cfg_sites_tab)
    label_idx = _pick_required_idx(header_norm, ["label"], "label", cfg_sites_tab)
    sheet_url_idx = _pick_required_idx(header_norm, ["sheet_url", "sheet url"], "sheet_url", cfg_sites_tab)

    site_code_n = norm_text(site_code)
    label_n = norm_text(label)

    for r in values[1:]:
        sc = norm_text(r[site_code_idx] if site_code_idx < len(r) else "")
        lb = norm_text(r[label_idx] if label_idx < len(r) else "")
        su = (r[sheet_url_idx] if sheet_url_idx < len(r) else "").strip()
        if sc == site_code_n and lb == label_n:
            if _is_blank(su):
                raise ValueError(f"❌ {cfg_sites_tab} 命中 site_code={site_code}, label={label}，但 sheet_url 为空")
            return su

    raise ValueError(f"❌ {cfg_sites_tab} 未找到 site_code={site_code}, label={label} 的记录")


def open_ws_by_label_and_title(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    worksheet_title: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    create_if_missing: bool = False,
    rows: int = 1000,
    cols: int = 6,
):
    sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=label,
        cfg_sites_tab=cfg_sites_tab,
    )
    sh = open_ss_by_url(gc, sheet_url)
    try:
        ws = _with_sheets_retry(
            lambda: sh.worksheet(worksheet_title),
            action=f"worksheet.open:{worksheet_title}",
        )
    except WorksheetNotFound:
        if not create_if_missing:
            raise
        ws = _with_sheets_retry(
            lambda: sh.add_worksheet(
                title=worksheet_title,
                rows=rows,
                cols=cols,
            ),
            action=f"worksheet.add:{worksheet_title}",
            retry_5xx=False,
        )
    return sh, ws, sheet_url


# ============================================================
# Google Sheets write helpers
# ============================================================

def _throttle(target_writes_per_min: int) -> None:
    global _last_write_ts
    min_interval = 60.0 / max(1, target_writes_per_min)
    now = time.time()
    wait = min_interval - (now - _last_write_ts)
    if wait > 0:
        time.sleep(wait)
    _last_write_ts = time.time()


def _is_quota_429(err: Exception) -> bool:
    s = str(err)
    return ("[429]" in s) or ("Quota exceeded" in s) or ("RATE_LIMIT_EXCEEDED" in s)


def safe_clear(
    ws,
    *,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.clear()
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit (clear). retry in {backoff:.1f}s")
                time.sleep(backoff)
                continue
            raise


def safe_resize(
    ws,
    rows: int,
    cols: int,
    *,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.resize(rows=rows, cols=cols)
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit (resize). retry in {backoff:.1f}s")
                time.sleep(backoff)
                continue
            raise


def safe_update_range(
    ws,
    a1_range: str,
    values: list[list[Any]],
    *,
    value_input_option: str = "RAW",
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.update(range_name=a1_range, values=values, value_input_option=value_input_option)
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit. retry in {backoff:.1f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff)
                continue
            raise


def append_rows_safe(
    ws,
    rows: list[list[Any]],
    *,
    value_input_option: str = "RAW",
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    if not rows:
        return
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.append_rows(rows, value_input_option=value_input_option)
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit (append_rows). retry in {backoff:.1f}s")
                time.sleep(backoff)
                continue
            raise


def write_chunk(
    ws,
    start_row: int,
    rows: list[list[Any]],
    *,
    n_cols: int,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    if not rows:
        return
    end_row = start_row + len(rows) - 1
    end_col = gspread.utils.rowcol_to_a1(1, n_cols).replace("1", "")
    safe_update_range(
        ws,
        f"A{start_row}:{end_col}{end_row}",
        rows,
        value_input_option="RAW",
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )


# ============================================================
# RunLog
# ============================================================

def ensure_runlog_header(
    ws,
    *,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    row1 = _with_sheets_retry(
        lambda: ws.row_values(1),
        action="runlog.read_header",
    )
    if row1[:len(RUNLOG_HEADER)] != RUNLOG_HEADER:
        end_col = gspread.utils.rowcol_to_a1(1, len(RUNLOG_HEADER)).replace("1", "")
        safe_update_range(
            ws,
            f"A1:{end_col}1",
            [RUNLOG_HEADER],
            value_input_option="RAW",
            target_writes_per_min=target_writes_per_min,
            max_retries=max_retries,
            base_backoff=base_backoff,
            jitter=jitter,
        )


def write_runlog(
    ws,
    *,
    run_id: str,
    ts_cn: str,
    job_name: str,
    phase: str,
    log_type: str,
    status: str,
    site_code: str,
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
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    ensure_runlog_header(
        ws,
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )
    row = [[
        run_id,
        ts_cn,
        job_name,
        phase,
        log_type,
        status,
        site_code,
        entity_type,
        gid,
        field_key,
        rows_loaded,
        rows_pending,
        rows_recognized,
        rows_planned,
        rows_written,
        rows_skipped,
        trim_text(message, 1000),
        trim_text(error_reason, 1000),
    ]]
    append_rows_safe(
        ws,
        row,
        value_input_option="RAW",
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )


# ============================================================
# Cfg__Fields maps
# ============================================================

def build_cfg_maps(
    ws_cfg,
    *,
    cfg_match_column_candidates: list[str],
    cfg_key_column_candidates: list[str],
    cfg_entity_column_candidates: list[str],
) -> tuple[dict[str, str], dict[str, str], dict[str, dict[str, str]], dict[str, str]]:
    values = _with_sheets_retry(
        lambda: ws_cfg.get_all_values(),
        action="cfg_fields.read",
    )
    if not values or len(values) < 2:
        raise ValueError("❌ Cfg__Fields 为空或不可读")

    header = values[0]
    match_idx = pick_col_index(header, cfg_match_column_candidates)
    key_idx = pick_col_index(header, cfg_key_column_candidates)
    entity_idx = pick_col_index(header, cfg_entity_column_candidates)

    if match_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到匹配列：{cfg_match_column_candidates}")
    if key_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到 field_key/key 列：{cfg_key_column_candidates}")
    if entity_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到 entity_type 列：{cfg_entity_column_candidates}")

    optional_idxs = {
        "source_type": pick_col_index(header, CFG_SOURCE_TYPE_COLUMN_CANDIDATES_DEFAULT),
        "field_type": pick_col_index(header, CFG_FIELD_TYPE_COLUMN_CANDIDATES_DEFAULT),
        "data_type": pick_col_index(header, CFG_DATA_TYPE_COLUMN_CANDIDATES_DEFAULT),
        "purpose": pick_col_index(header, CFG_PURPOSE_COLUMN_CANDIDATES_DEFAULT),
    }

    map_name_to_key: dict[str, str] = {}
    map_key_to_entity: dict[str, str] = {}
    map_key_meta: dict[str, dict[str, str]] = {}

    for r in values[1:]:
        key = r[key_idx].strip() if key_idx < len(r) else ""
        if _is_blank(key):
            continue

        entity = normalize_entity_type(r[entity_idx] if entity_idx < len(r) else "")
        name = r[match_idx] if match_idx < len(r) else ""

        if not _is_blank(name):
            map_name_to_key[norm_header(name)] = key

        # Also allow header to be the field_key directly.
        map_name_to_key[norm_header(key)] = key

        if not _is_blank(entity):
            map_key_to_entity[key] = entity

        meta = {
            "field_key": key,
            "entity_type": entity,
            "display_name": str(name).strip(),
        }
        for meta_name, idx in optional_idxs.items():
            meta[meta_name] = r[idx].strip() if idx is not None and idx < len(r) else ""
        map_key_meta[key] = meta

    cfg_meta = {
        "match_col": header[match_idx],
        "key_col": header[key_idx],
        "entity_col": header[entity_idx],
    }

    print(
        "✅ CFG maps loaded:"
        f" name/key->field_key={len(map_name_to_key)}"
        f" | field_key->entity={len(map_key_to_entity)}"
        f" | match_col={cfg_meta['match_col']}"
        f" | key_col={cfg_meta['key_col']}"
        f" | entity_col={cfg_meta['entity_col']}"
    )

    return map_name_to_key, map_key_to_entity, map_key_meta, cfg_meta


def should_output_field(field_key: str, meta: dict[str, str], *, exclude_display_only: bool = True) -> bool:
    k = str(field_key or "").strip().lower()
    if not k:
        return False

    # calc.* is derived/display logic, never a write target.
    if k.startswith("calc."):
        return False

    # core.* must be allowed for Edit__Core / core field writes, for example:
    #   core.title
    #   core.seo_title
    #   core.seo_description
    # Owner identity fields are still blocked because they should be used as gid_or_handle,
    # not emitted as editable rows.
    if k.startswith("core."):
        if k in {
            "core.id",
            "core.gid",
            "core.legacy_id",
            "core.product_id",
            "core.variant_id",
            "core.collection_id",
            "core.page_id",
            "core.customer_id",
            "core.order_id",
        }:
            return False
        return True

    if exclude_display_only:
        joined = " ".join([
            meta.get("source_type", ""),
            meta.get("field_type", ""),
            meta.get("data_type", ""),
            meta.get("purpose", ""),
        ]).lower()
        if "display-only" in joined or "display only" in joined or "display_only" in joined:
            return False

    return True


# ============================================================
# Owner column recognition
# ============================================================

def detect_owner_header(header_value: Any) -> Optional[str]:
    raw = "" if header_value is None else str(header_value).strip()
    if not raw:
        return None

    nh = norm_header(raw)
    if nh in GENERIC_OWNER_HEADERS_BLOCKLIST:
        return None

    has_id_word = bool(ID_WORD_RE.search(nh))
    if not has_id_word:
        return None

    matched_owner_types = [owner_type for owner_type, pattern in OWNER_TYPE_WORDS.items() if pattern.search(nh)]
    if len(matched_owner_types) != 1:
        return None

    return OWNER_HEADER_TO_KEY[matched_owner_types[0]]


def scan_owner_columns(header_row: list[Any], *, last_col: int) -> dict[str, int]:
    owner_cols: dict[str, int] = {}
    duplicates: dict[str, list[int]] = {}

    for idx in range(last_col):
        owner_key = detect_owner_header(header_row[idx] if idx < len(header_row) else "")
        if not owner_key:
            continue

        if owner_key in owner_cols:
            duplicates.setdefault(owner_key, [owner_cols[owner_key] + 1]).append(idx + 1)
        else:
            owner_cols[owner_key] = idx

    if duplicates:
        raise ValueError(f"❌ owner 身份列重复，无法安全判断：{duplicates}")

    return owner_cols


# ============================================================
# Wide row2 field_key write
# ============================================================

def write_wide_keys_from_cfg(
    ws_wide,
    map_name_to_key: dict[str, str],
    *,
    wide_header_row: int,
    wide_write_row: int,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
) -> dict[str, Any]:
    values = _with_sheets_retry(
        lambda: ws_wide.get_all_values(),
        action="wide.read_for_key_mapping",
    )
    if not values:
        raise ValueError("❌ Wide 为空")

    header_row = values[wide_header_row - 1] if len(values) >= wide_header_row else []
    if not header_row:
        raise ValueError("❌ Wide 第1行为空")

    last_non_empty = 0
    for i, v in enumerate(header_row):
        if not _is_blank(v):
            last_non_empty = i + 1
    if last_non_empty == 0:
        raise ValueError("❌ Wide 第1行没有任何字段名")

    out: list[str] = []
    hit = 0
    miss = 0
    owner_cols = 0
    misses: list[dict[str, Any]] = []

    for i in range(last_non_empty):
        h = header_row[i]
        if detect_owner_header(h):
            out.append("")
            owner_cols += 1
            continue

        nh = norm_header(h)
        if nh and nh in map_name_to_key:
            out.append(map_name_to_key[nh])
            hit += 1
        else:
            out.append("")
            miss += 1
            misses.append({"col_num": i + 1, "header": h})

    start = gspread.utils.rowcol_to_a1(wide_write_row, 1)
    end = gspread.utils.rowcol_to_a1(wide_write_row, last_non_empty)
    safe_update_range(
        ws_wide,
        f"{start}:{end}",
        [out],
        value_input_option="RAW",
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )

    print(
        f"✅ Wide row{wide_write_row} written:"
        f" cols=1..{last_non_empty} | mapped={hit} | unmapped={miss} | owner_cols={owner_cols}"
    )

    return {
        "last_non_empty_col": last_non_empty,
        "hit": hit,
        "miss": miss,
        "owner_cols": owner_cols,
        "misses": misses,
    }


# ============================================================
# Build Long
# ============================================================

def build_long_rows(
    values_2d: list[list[Any]],
    map_key_to_entity: dict[str, str],
    map_key_meta: dict[str, dict[str, str]],
    *,
    include_empty: bool,
    dedup_output: bool,
    exclude_display_only: bool,
) -> tuple[list[list[str]], dict[str, Any]]:
    """
    Convert Wide to Long.

    Important rule:
    The owner identity column in Wide is authoritative.

    Example:
        Product ID (numeric) => PRODUCT / product_id

    When Wide contains exactly one owner identity column, every editable field
    in that Wide sheet is written against that owner type. Cfg__Fields may
    still provide field metadata, but it is not allowed to redirect a PRODUCT
    Wide sheet to VARIANT / COLLECTION / PAGE / CUSTOMER / ORDER.
    """
    if len(values_2d) < 3:
        raise ValueError(
            "❌ 宽表至少需要：第1行(header) + 第2行(field_key) + 第3行起(数据)"
        )

    header1 = values_2d[0]
    key_row = values_2d[1]
    data_rows = values_2d[2:]

    last_col = 0
    for r in values_2d[: min(len(values_2d), 2000)]:
        last_col = max(last_col, len(r))
    if last_col < 1:
        raise ValueError("❌ Wide 没有任何列")

    header1 = header1 + [""] * max(0, last_col - len(header1))
    key_row = key_row + [""] * max(0, last_col - len(key_row))

    owner_cols = scan_owner_columns(header1, last_col=last_col)
    owner_col_indexes = set(owner_cols.values())

    if not owner_cols:
        raise ValueError(
            "❌ Wide 没有识别到任何 owner 身份列。\n"
            "owner 列表头必须同时包含 owner 类型词(Product/Variant/Collection/Page/Customer/Order)"
            " 和 ID 类型词(ID/GID/Legacy ID/Numeric ID)。"
        )

    # Print exactly what the Python file recognized from row 1.
    for owner_id_key, col_idx in owner_cols.items():
        recognized_entity = OWNER_KEY_TO_ENTITY.get(owner_id_key, "")
        original_header = header1[col_idx] if col_idx < len(header1) else ""
        print(
            "✅ Owner header recognized:"
            f" column={col_idx + 1}"
            f" | header={original_header!r}"
            f" | entity_type={recognized_entity}"
            f" | owner_key={owner_id_key}"
        )

    single_owner_mode = len(owner_cols) == 1
    forced_owner_id_key = ""
    forced_entity = ""

    if single_owner_mode:
        forced_owner_id_key = next(iter(owner_cols))
        forced_entity = OWNER_KEY_TO_ENTITY.get(forced_owner_id_key, "")
        if not forced_entity:
            raise ValueError(
                f"❌ 无法把 owner_key={forced_owner_id_key} 转换成 entity_type"
            )
        print(
            "✅ Single-owner mode:"
            f" Wide row 1 is authoritative"
            f" | entity_type={forced_entity}"
            f" | owner_key={forced_owner_id_key}"
        )

    out: list[list[str]] = []
    seen = set() if dedup_output else None

    skipped_missing_owner = 0
    skipped_empty_value = 0
    skipped_excluded_field = 0
    skipped_blank_key = 0
    planned_fields = 0
    nonblank_source_values = 0

    field_cols: list[dict[str, Any]] = []
    entity_overrides: list[dict[str, str]] = []

    for col_idx in range(last_col):
        if col_idx in owner_col_indexes:
            continue

        k = norm_key(key_row[col_idx])
        if _is_blank(k):
            skipped_blank_key += 1
            continue

        meta = map_key_meta.get(
            k,
            {
                "field_key": k,
                "entity_type": map_key_to_entity.get(k, ""),
            },
        )

        if not should_output_field(
            k,
            meta,
            exclude_display_only=exclude_display_only,
        ):
            skipped_excluded_field += 1
            continue

        cfg_entity = normalize_entity_type(
            map_key_to_entity.get(k, meta.get("entity_type", ""))
        )

        if single_owner_mode:
            # The Wide owner header wins. This is the key fix.
            entity = forced_entity
            owner_id_key = forced_owner_id_key

            if cfg_entity and cfg_entity != forced_entity:
                entity_overrides.append(
                    {
                        "field_key": k,
                        "cfg_entity": cfg_entity,
                        "wide_entity": forced_entity,
                    }
                )
        else:
            # Multiple owner columns: Cfg__Fields must tell us which owner to use.
            entity = cfg_entity
            owner_id_key = OWNER_ID_KEYS.get(entity, "")

            if not owner_id_key:
                raise ValueError(
                    "❌ Wide 同时有多个 owner 身份列，但字段无法确定 owner。\n"
                    f"column={col_idx + 1}"
                    f" | header={header1[col_idx]!r}"
                    f" | field_key={k!r}"
                    f" | cfg_entity={cfg_entity!r}"
                )

            if owner_id_key not in owner_cols:
                raise ValueError(
                    "❌ 字段要求的 owner 身份列不在 Wide 中。\n"
                    f"column={col_idx + 1}"
                    f" | header={header1[col_idx]!r}"
                    f" | field_key={k!r}"
                    f" | cfg_entity={cfg_entity!r}"
                    f" | required_owner_key={owner_id_key!r}"
                    f" | available_owner_keys={sorted(owner_cols)}"
                )

        field_cols.append(
            {
                "col_idx": col_idx,
                "field_key": k,
                "entity_type": entity,
                "owner_id_key": owner_id_key,
            }
        )
        planned_fields += 1

    if entity_overrides:
        print(
            "⚠️ Cfg entity overridden by Wide owner header:"
            f" count={len(entity_overrides)}"
        )
        for item in entity_overrides[:20]:
            print(
                "   "
                f"field_key={item['field_key']}"
                f" | cfg_entity={item['cfg_entity']}"
                f" | using_wide_entity={item['wide_entity']}"
            )

    if not field_cols:
        raise ValueError(
            "❌ 没有可输出字段列。请检查 Wide 第2行 field_key 或 Cfg__Fields。"
        )

    def emit(
        entity: str,
        owner_id: Any,
        field_key: str,
        value: Any,
        note: str = "",
        error_reason: str = "",
    ):
        row = [
            entity,
            str(owner_id).strip(),
            field_key,
            "" if value is None else str(value),
            note,
            error_reason,
        ]
        if seen is not None:
            row_key = tuple(row)
            if row_key in seen:
                return
            seen.add(row_key)
        out.append(row)

    for source_row_num, r in enumerate(data_rows, start=3):
        if not r:
            continue

        if len(r) < last_col:
            r = r + [""] * (last_col - len(r))

        row_owner_values: dict[str, str] = {}
        for owner_id_key, col_idx in owner_cols.items():
            row_owner_values[owner_id_key] = (
                str(r[col_idx]).strip() if col_idx < len(r) else ""
            )

        for fc in field_cols:
            v = r[fc["col_idx"]] if fc["col_idx"] < len(r) else ""

            if (not include_empty) and _is_blank(v):
                skipped_empty_value += 1
                continue

            if not _is_blank(v):
                nonblank_source_values += 1

            owner_id_key = fc["owner_id_key"]
            owner_id = row_owner_values.get(owner_id_key, "")

            if _is_blank(owner_id):
                skipped_missing_owner += 1
                print(
                    "⚠️ Missing owner value:"
                    f" source_row={source_row_num}"
                    f" | owner_key={owner_id_key}"
                    f" | entity_type={fc['entity_type']}"
                    f" | field_key={fc['field_key']}"
                )
                continue

            emit(
                fc["entity_type"],
                owner_id,
                fc["field_key"],
                v,
            )

    # A populated Wide sheet is never allowed to silently become an empty Long.
    if data_rows and nonblank_source_values > 0 and len(out) == 0:
        raise ValueError(
            "❌ Wide 有非空目标值，但 Long 结果为 0 行；不允许记录 SUCCESS，"
            "并且不会清空原 Long。\n"
            f"data_rows={len(data_rows)}"
            f" | planned_fields={planned_fields}"
            f" | nonblank_source_values={nonblank_source_values}"
            f" | skipped_missing_owner={skipped_missing_owner}"
            f" | skipped_empty_value={skipped_empty_value}"
        )

    meta = {
        "mode": "OWNER_HEADER_AUTHORITATIVE",
        "last_col": last_col,
        "owner_cols": {k: v + 1 for k, v in owner_cols.items()},
        "single_owner_mode": single_owner_mode,
        "forced_entity": forced_entity,
        "forced_owner_id_key": forced_owner_id_key,
        "entity_override_count": len(entity_overrides),
        "entity_overrides": entity_overrides[:100],
        "planned_fields": planned_fields,
        "nonblank_source_values": nonblank_source_values,
        "skipped_missing_owner": skipped_missing_owner,
        "skipped_empty_value": skipped_empty_value,
        "skipped_excluded_field": skipped_excluded_field,
        "skipped_blank_key": skipped_blank_key,
        "rows_to_write": len(out),
    }

    return out, meta


# ============================================================
# Main entry
# ============================================================

def run(
    *,
    SITE_CODE: Optional[str] = None,
    JOB_NAME: str = "wide_to_long",
    CONSOLE_CORE_URL: Optional[str] = None,
    BOOTSTRAP_GSHEET_SA_B64_SECRET: Optional[str] = None,

    # Lowercase aliases kept for compatibility.
    site_code: Optional[str] = None,
    job_name: Optional[str] = None,
    console_core_url: Optional[str] = None,
    bootstrap_gsheet_sa_b64_secret: Optional[str] = None,

    # Console Core tabs
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,

    # Business sheet labels / worksheet titles
    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Wide",
    output_sheet_label: str = "pre_edit",
    output_worksheet_title: str = "Edit__ValuesLong",
    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = "Cfg__Fields",
    runlog_sheet_label: str = "runlog_sheet",
    runlog_worksheet_title: str = "Ops__RunLog",

    # Runtime options
    run_id: Optional[str] = None,
    include_empty: bool = False,
    clear_output_first: bool = True,
    do_write_wide_keys: bool = True,
    do_build_long: bool = True,
    write_runlog_enabled: bool = True,
    dedup_output: bool = True,
    exclude_display_only: bool = True,
    out_chunk_rows: int = 20000,
    wide_header_row: int = WIDE_HEADER_ROW_DEFAULT,
    wide_write_row: int = WIDE_WRITE_ROW_DEFAULT,

    # Cfg__Fields column candidates
    cfg_match_column_candidates: Optional[list[str]] = None,
    cfg_key_column_candidates: Optional[list[str]] = None,
    cfg_entity_column_candidates: Optional[list[str]] = None,

    # Write control
    target_writes_per_min: int = TARGET_WRITES_PER_MIN_DEFAULT,
    max_retries: int = MAX_RETRIES_DEFAULT,
    base_backoff: float = BASE_BACKOFF_DEFAULT,
    jitter: float = JITTER_DEFAULT,
    secret_home: Optional[str] = None,
) -> dict[str, Any]:
    site_code_final = SITE_CODE or site_code
    job_name_final = job_name or JOB_NAME or "wide_to_long"
    console_core_url_final = CONSOLE_CORE_URL or console_core_url
    bootstrap_secret_final = BOOTSTRAP_GSHEET_SA_B64_SECRET or bootstrap_gsheet_sa_b64_secret

    if _is_blank(site_code_final):
        raise ValueError("❌ 缺少 SITE_CODE")
    if _is_blank(console_core_url_final):
        raise ValueError("❌ 缺少 CONSOLE_CORE_URL")
    if _is_blank(bootstrap_secret_final):
        raise ValueError("❌ 缺少 BOOTSTRAP_GSHEET_SA_B64_SECRET")

    cfg_match_column_candidates = cfg_match_column_candidates or CFG_MATCH_COLUMN_CANDIDATES_DEFAULT
    cfg_key_column_candidates = cfg_key_column_candidates or CFG_KEY_COLUMN_CANDIDATES_DEFAULT
    cfg_entity_column_candidates = cfg_entity_column_candidates or CFG_ENTITY_COLUMN_CANDIDATES_DEFAULT

    actual_run_id = run_id or make_run_id(job_name_final)
    ts_cn = cn_now_str()

    rows_read = 0
    rows_written = 0
    rows_recognized = 0
    rows_planned = 0
    rows_skipped = 0

    wide_key_write_meta: dict[str, Any] = {}
    cfg_meta: dict[str, Any] = {}
    long_meta: dict[str, Any] = {}
    preview: list[dict[str, str]] = []

    ws_runlog = None
    runlog_sheet_url = ""

    try:
        gc, account_cfg = build_runtime_context(
            site_code=str(site_code_final),
            console_core_url=str(console_core_url_final),
            bootstrap_gsheet_sa_b64_secret=str(bootstrap_secret_final),
            cfg_account_tab=cfg_account_tab,
            secret_home=secret_home,
        )

        sh_wide, ws_wide, wide_sheet_url = open_ws_by_label_and_title(
            gc=gc,
            console_core_url=str(console_core_url_final),
            site_code=str(site_code_final),
            label=input_sheet_label,
            worksheet_title=input_worksheet_title,
            cfg_sites_tab=cfg_sites_tab,
        )
        sh_long, ws_long, long_sheet_url = open_ws_by_label_and_title(
            gc=gc,
            console_core_url=str(console_core_url_final),
            site_code=str(site_code_final),
            label=output_sheet_label,
            worksheet_title=output_worksheet_title,
            cfg_sites_tab=cfg_sites_tab,
            create_if_missing=True,
            rows=1000,
            cols=len(LONG_HEADER),
        )
        sh_cfg, ws_cfg, cfg_sheet_url = open_ws_by_label_and_title(
            gc=gc,
            console_core_url=str(console_core_url_final),
            site_code=str(site_code_final),
            label=cfg_sheet_label,
            worksheet_title=cfg_tab_fields,
            cfg_sites_tab=cfg_sites_tab,
        )

        if write_runlog_enabled:
            _, ws_runlog, runlog_sheet_url = open_ws_by_label_and_title(
                gc=gc,
                console_core_url=str(console_core_url_final),
                site_code=str(site_code_final),
                label=runlog_sheet_label,
                worksheet_title=runlog_worksheet_title,
                cfg_sites_tab=cfg_sites_tab,
                create_if_missing=True,
                rows=1000,
                cols=len(RUNLOG_HEADER),
            )

        print("✅ Runtime config ready")
        print("  script_build:", SCRIPT_BUILD)
        print("  site_code :", site_code_final)
        print("  job_name  :", job_name_final)
        print("  config    : Cfg__account_id not used by this job")
        print("✅ Sheets ready")
        print("  wide      :", sh_wide.url, "|", ws_wide.title)
        print("  long      :", sh_long.url, "|", ws_long.title)
        print("  config    :", sh_cfg.url, "|", ws_cfg.title)
        if ws_runlog is not None:
            print("  runlog    :", runlog_sheet_url, "|", ws_runlog.title)

        map_name_to_key: dict[str, str] = {}
        map_key_to_entity: dict[str, str] = {}
        map_key_meta: dict[str, dict[str, str]] = {}

        if do_write_wide_keys or do_build_long:
            map_name_to_key, map_key_to_entity, map_key_meta, cfg_meta = build_cfg_maps(
                ws_cfg,
                cfg_match_column_candidates=cfg_match_column_candidates,
                cfg_key_column_candidates=cfg_key_column_candidates,
                cfg_entity_column_candidates=cfg_entity_column_candidates,
            )

        if do_write_wide_keys:
            wide_key_write_meta = write_wide_keys_from_cfg(
                ws_wide,
                map_name_to_key,
                wide_header_row=wide_header_row,
                wide_write_row=wide_write_row,
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )
            rows_recognized = int(wide_key_write_meta.get("hit", 0) or 0)

        if do_build_long:
            values = _with_sheets_retry(
                lambda: ws_wide.get_all_values(),
                action="wide.read_for_long_build",
            )
            rows_read = max(0, len(values) - 2)

            long_rows, long_meta = build_long_rows(
                values,
                map_key_to_entity,
                map_key_meta,
                include_empty=include_empty,
                dedup_output=dedup_output,
                exclude_display_only=exclude_display_only,
            )
            rows_planned = len(long_rows)
            rows_skipped = int(long_meta.get("skipped_missing_owner", 0) or 0) + int(long_meta.get("skipped_empty_value", 0) or 0)

            print(
                f"✅ Mode={long_meta.get('mode')}"
                f" | owner_cols={long_meta.get('owner_cols')}"
                f" | planned_fields={long_meta.get('planned_fields')}"
                f" | rows_to_write={len(long_rows):,}"
                f" | skipped_missing_owner={long_meta.get('skipped_missing_owner')}"
            )

            if clear_output_first:
                safe_clear(
                    ws_long,
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )

            end_col = gspread.utils.rowcol_to_a1(1, len(LONG_HEADER)).replace("1", "")
            safe_update_range(
                ws_long,
                f"A1:{end_col}1",
                [LONG_HEADER],
                value_input_option="RAW",
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

            est_rows = 1 + len(long_rows)
            est_cells = est_rows * len(LONG_HEADER)
            if est_cells > 9_500_000:
                print("⚠️ 警告：预计接近/超过单表 1000万 cell 上限。建议拆分 Long 表。")

            safe_resize(
                ws_long,
                rows=max(ws_long.row_count, min(est_rows + 50, 2_300_000)),
                cols=len(LONG_HEADER),
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

            out_row_cursor = 2
            written = 0
            t0 = time.time()
            buf: list[list[str]] = []

            for row in long_rows:
                buf.append(row)
                if len(buf) >= out_chunk_rows:
                    write_chunk(
                        ws_long,
                        out_row_cursor,
                        buf,
                        n_cols=len(LONG_HEADER),
                        target_writes_per_min=target_writes_per_min,
                        max_retries=max_retries,
                        base_backoff=base_backoff,
                        jitter=jitter,
                    )
                    out_row_cursor += len(buf)
                    written += len(buf)
                    buf = []
                    print(f"… wrote {written:,} rows")

            if buf:
                write_chunk(
                    ws_long,
                    out_row_cursor,
                    buf,
                    n_cols=len(LONG_HEADER),
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )
                written += len(buf)

            rows_written = written

            if rows_written != rows_planned:
                raise RuntimeError(
                    "❌ Long 写入数量与计划数量不一致："
                    f" rows_planned={rows_planned}"
                    f" | rows_written={rows_written}"
                )

            if rows_read > 0 and rows_written == 0:
                raise RuntimeError(
                    "❌ Wide 有数据但 Long 实际写入 0 行；不允许记录 SUCCESS"
                )

            dt = time.time() - t0
            print(
                f"✅ Done. rows_written={written:,}"
                f" | time={dt:.1f}s"
                f" | INCLUDE_EMPTY={include_empty}"
                f" | DEDUP={dedup_output}"
                f" | EXCLUDE_DISPLAY_ONLY={exclude_display_only}"
            )
            preview = [dict(zip(LONG_HEADER, row)) for row in long_rows[:50]]

        if write_runlog_enabled and ws_runlog is not None:
            write_runlog(
                ws_runlog,
                run_id=actual_run_id,
                ts_cn=ts_cn,
                job_name=job_name_final,
                phase="run",
                log_type="summary",
                status="SUCCESS",
                site_code=str(site_code_final),
                rows_loaded=rows_read,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                message="wide_to_long completed",
                error_reason="",
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

        return {
            "status": "SUCCESS",
            "script_build": SCRIPT_BUILD,
            "run_id": actual_run_id,
            "ts_cn": ts_cn,
            "job_name": job_name_final,
            "summary": {
                "job_name": job_name_final,
                "site_code": site_code_final,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_skipped": rows_skipped,
                "mapped_columns": wide_key_write_meta.get("hit", 0),
                "unmapped_columns": wide_key_write_meta.get("miss", 0),
                "owner_columns": wide_key_write_meta.get("owner_cols", 0),
                "include_empty": include_empty,
                "clear_output_first": clear_output_first,
                "do_write_wide_keys": do_write_wide_keys,
                "do_build_long": do_build_long,
                "dedup_output": dedup_output,
                "exclude_display_only": exclude_display_only,
            },
            "preview": preview,
            "meta": {
                "run_id": actual_run_id,
                "wide_sheet_url": wide_sheet_url,
                "long_sheet_url": long_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "cfg_match_col": cfg_meta.get("match_col", ""),
                "cfg_key_col": cfg_meta.get("key_col", ""),
                "cfg_entity_col": cfg_meta.get("entity_col", ""),
                "wide_key_write_meta": wide_key_write_meta,
                "long_meta": long_meta,
            },
        }

    except Exception as e:
        error_message = trim_text(str(e), max_len=50000)
        error_summary = summarize_error(e, max_len=1000)
        print(f"❌ Run failed: {error_message}")

        if write_runlog_enabled and ws_runlog is not None:
            try:
                write_runlog(
                    ws_runlog,
                    run_id=actual_run_id,
                    ts_cn=ts_cn,
                    job_name=job_name_final,
                    phase="run",
                    log_type="error",
                    status="ERROR",
                    site_code=str(site_code_final or ""),
                    rows_loaded=rows_read,
                    rows_recognized=rows_recognized,
                    rows_planned=rows_planned,
                    rows_written=rows_written,
                    rows_skipped=rows_skipped,
                    message=error_message,
                    error_reason=error_summary,
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )
            except Exception as e2:
                print(f"⚠️ RunLog 写入失败：{e2}")
        raise
