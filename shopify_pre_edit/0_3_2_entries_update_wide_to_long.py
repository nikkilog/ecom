# shopify_pre_edit/0_3_2_entries_update_wide_to_long.py

from __future__ import annotations

import base64
import datetime as dt
import json
import random
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import gspread
import pandas as pd
from google.oauth2 import service_account



MODULE_PATH = "shopify_pre_edit.0_3_2_entries_update_wide_to_long"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
DEFAULT_JOB_NAME = "entries_update_wide_to_long"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# =========================================================
# Constants
# =========================================================

CFG_SITES_TAB_DEFAULT = "Cfg__Sites"

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

OUTPUT_HEADER = [
    "op",
    "entry_type",
    "entry_gid",
    "mode",
    "field_id",
    "value",
    "new_handle",
    "slot",
    "note",
]

OWNER_COLUMN_CANDIDATES = [
    "entry_gid",
    "gid",
    "gid_or_handle",
    "lookup_handle",
    "handle",
]

CONTROL_COLUMN_NAMES = {
    "op",
    "action",
    "entry_type",
    "mode",
    "note",
    "run_id",
    "new_handle",
    *OWNER_COLUMN_CANDIDATES,
}

# Columns such as collection_link__handle and parent_node__handle are
# helper/fallback values for the corresponding reference field. They are not
# standalone Shopify fields and must never be converted into their own field_id.
AUXILIARY_HANDLE_SUFFIX = "__handle"


# =========================================================
# Utils
# =========================================================

def _make_run_id(prefix: str = "entries_update_w2l") -> str:
    return dt.datetime.now(dt.timezone.utc).strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def _norm_header(x: Any) -> str:
    return _norm_str(x).lower()


def _norm_lookup_key(x: Any) -> str:
    s = _norm_str(x).lower()
    s = s.replace("｜", "|").replace("–", "-").replace("—", "-")
    return " ".join(s.split())


def _row_has_any_value(values: list[Any]) -> bool:
    return any(_norm_str(v) != "" for v in values)


def _pad_row(row: list[Any], length: int) -> list[Any]:
    return list(row) + [""] * max(0, length - len(row))


def _safe_cell(row: list[Any], idx: Optional[int]) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return _norm_str(row[idx])


# =========================================================
# Runtime / Secret / Workspace Registry / Google auth
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
    name = _norm_str(secret_name)
    resolved_project_code = _normalize_project_code(project_code)
    if not name:
        raise ValueError("Secret name is blank.")
    if not resolved_project_code:
        raise ValueError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _norm_str(explicit_value):
        return SecretValue(_norm_str(explicit_value), "EXPLICIT_VALUE", "caller")

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
        return SecretValue(str(value).strip(), "COLAB_SECRETS", name)

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
    for suffix in ("_GSHEET", "_SHOPIFY_ACCESS_TOKEN", "_SHOPIFY_TOKEN"):
        if normalized_name.endswith(suffix):
            canonical_name = f"{resolved_project_code}{suffix}"
            if canonical_name != name:
                aliases = (canonical_name,)
            break

    result = resolver.read(name, aliases=aliases)
    return _workspace_secret_result_to_value(result)


def _parse_service_account_text(raw_value: str) -> tuple[dict[str, Any], str]:
    raw = _norm_str(raw_value)
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


def _build_gsheet_client_from_value(raw_value: str) -> gspread.Client:
    sa_info, _secret_format = _parse_service_account_text(raw_value)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


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
    return _build_gsheet_client_from_value(secret.value)


def _sheets_error_status(exc: BaseException) -> Optional[int]:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is None:
        status = getattr(response, "status", None)
    try:
        return int(status) if status is not None else None
    except Exception:
        return None


def _is_retryable_sheets_error(exc: BaseException, *, retry_5xx: bool = True) -> bool:
    status = _sheets_error_status(exc)
    if status == 429:
        return True
    if retry_5xx and status in {500, 502, 503, 504}:
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
    base_sleep: float = 1.5,
    max_delay: float = 20.0,
    retry_5xx: bool = True,
):
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if (
                not _is_retryable_sheets_error(exc, retry_5xx=retry_5xx)
                or attempt >= attempts
            ):
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


def resolve_workspace_project(
    *,
    project_code: str,
    workspace_registry_id: str,
    workspace_gsheet_secret_name: str = "WORKSPACE_GSHEET",
    workspace_registry_tab: str = "Cfg__Projects",
    secret_home: Optional[str] = None,
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
        secret_home=secret_home,
    )
    workspace_gc = _build_gsheet_client_from_value(workspace_secret.value)

    registry_file_id = _extract_spreadsheet_id(workspace_registry_id)
    registry_book = _with_sheets_retry(
        lambda: workspace_gc.open_by_key(registry_file_id),
        action="workspace.open_registry",
    )
    worksheet = _with_sheets_retry(
        lambda: registry_book.worksheet(workspace_registry_tab),
        action="workspace.open_registry_tab",
    )
    values = _with_sheets_retry(
        worksheet.get_all_values,
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
    console_url_col = require_column("console_core_url", "console core url")
    gsheet_secret_col = require_column("gsheet_secret_name", "gsheet secret name")
    account_tab_col = require_column("account_config_tab", "account config tab")
    project_name_col = header_map.get(_normalize_registry_header("project_name"))

    width = len(values[0])
    matches: list[tuple[int, list[Any]]] = []
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
            "Workspace Project Registry has duplicate project rows: "
            f"project_code={resolved_project_code}; "
            f"rows={[row_number for row_number, _ in matches]}."
        )

    source_row, row = matches[0]
    active_text = _norm_str(row[active_col]).lower()
    if active_text not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}; row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": _norm_str(row[project_name_col]) if project_name_col is not None else "",
        "console_core_url": _norm_str(row[console_url_col]),
        "gsheet_secret_name": _norm_str(row[gsheet_secret_col]),
        "account_config_tab": _norm_str(row[account_tab_col]),
        "registry_source_row": str(source_row),
        "workspace_auth_source_type": workspace_secret.source_type,
    }
    missing = [
        key
        for key in ("console_core_url", "gsheet_secret_name", "account_config_tab")
        if not route[key]
    ]
    if missing:
        raise ValueError(
            "Workspace Project Registry route has empty required values: "
            f"project_code={resolved_project_code}; fields={missing}; row={source_row}."
        )

    if print_progress:
        print(
            "[Workspace Registry] resolved | "
            f"project={route['project_code']} | row={source_row} | "
            f"secret={route['gsheet_secret_name']} | "
            f"account_tab={route['account_config_tab']}"
        )
    return route


def resolve_project_gsheet_auth(
    *,
    project_code: str,
    gsheet_secret_name: str,
    secret_home: Optional[str] = None,
) -> dict[str, str]:
    secret = read_secret(
        gsheet_secret_name,
        project_code=project_code,
        secret_home=secret_home,
    )
    return {
        "secret_name": gsheet_secret_name,
        "secret_value": secret.value,
        "source_type": secret.source_type,
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
) -> dict[str, Any]:
    """Check/update exactly one existing Registry row; never append."""
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
        return {"status": "OFF", "target_row": None, "changed_fields": []}

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _norm_str(current_colab_url):
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _norm_str(current_colab_name):
        raise ValueError("UPDATE_URL_AND_NAME requires current_colab_name.")

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
        ws.get_all_values,
        action="registry.read",
    )
    if not values:
        raise ValueError(f"Registry tab {registry_tab!r} is empty.")

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
                lambda rn=row_number, cn=column_number, nv=new_value: ws.update_cell(rn, cn, nv),
                action=f"registry.update_cell:{field_name}",
                retry_5xx=True,
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
        action="cfg_sites.open_tab",
    )
    rows = _with_sheets_retry(
        ws.get_all_records,
        action="cfg_sites.read",
    )
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"{cfg_sites_tab} is empty")

    for col in ["site_code", "label", "sheet_url"]:
        if col not in df.columns:
            raise ValueError(f"{cfg_sites_tab} missing required column: {col}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    matched = df[
        (df["site_code"] == site_code.strip().upper())
        & (df["label"] == label.strip())
    ].copy()
    matched = matched[matched["sheet_url"] != ""]

    if matched.empty:
        raise ValueError(
            f"Cannot find sheet_url for site_code={site_code}, label={label} in {cfg_sites_tab}"
        )

    return matched.iloc[0]["sheet_url"]



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
        action=f"worksheet.open_book:{worksheet_title}",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(worksheet_title),
        action=f"worksheet.open:{worksheet_title}",
    )
    return sh, ws, sheet_url



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
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.flush_every = flush_every
        self._buf: list[list[Any]] = []

        sh = _with_sheets_retry(
            lambda: gc.open_by_url(runlog_sheet_url),
            action="runlog.open_book",
        )
        self.ws = _with_sheets_retry(
            lambda: sh.worksheet(runlog_tab_name),
            action=f"runlog.open_tab:{runlog_tab_name}",
        )
        _with_sheets_retry(
            lambda: self.ws.update(range_name="A1:R1", values=[RUNLOG_HEADER]),
            action="runlog.ensure_header",
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
    ) -> None:
        self._buf.append(
            [
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
                str(message)[:1000],
                str(error_reason)[:250],
            ]
        )
        if len(self._buf) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        pending = list(self._buf)
        _with_sheets_retry(
            lambda: self.ws.append_rows(
                pending,
                value_input_option="RAW",
                table_range="A:R",
            ),
            action="runlog.append_rows",
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
    max_per_reason: int = 2,
) -> None:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        reason = _norm_str(row.get("error_reason")) or "unknown"
        grouped[reason].append(row)

    for reason, items in grouped.items():
        for row in items[:max_per_reason]:
            logger.log_row(
                phase=phase,
                log_type="detail",
                status=status,
                entity_type=_norm_str(row.get("entity_type")) or "METAOBJECT_ENTRY",
                gid=_norm_str(row.get("gid")),
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
# Config: Cfg__Fields
# =========================================================

def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = _with_sheets_retry(
        ws_cfg_fields.get_all_records,
        action="cfg_fields.read",
    )
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    for col in ["field_id", "field_key"]:
        if col not in df.columns:
            raise ValueError(f"Cfg__Fields missing required column: {col}")

    optional_cols = [
        "field_handle",
        "display_name",
        "entity_type",
        "source_type",
        "namespace",
        "key",
        "data_type",
        "field_type",
    ]
    for col in optional_cols:
        if col not in df.columns:
            df[col] = ""

    for col in ["field_id", "field_key", *optional_cols]:
        df[col] = df[col].map(_norm_str)

    df = df[df["field_id"] != ""].copy()
    return df



def build_cfg_fields_lookup(
    cfg_fields_df: pd.DataFrame,
    entity_type: str = "METAOBJECT_ENTRY",
) -> dict[str, str]:
    df = cfg_fields_df.copy()

    if entity_type:
        entity_match = (
            df["entity_type"].map(_norm_str).str.upper()
            == entity_type.strip().upper()
        )
        if entity_match.any():
            df = df[entity_match].copy()

    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    candidate_cols = [
        "field_id",
        "field_key",
        "field_handle",
        "display_name",
        "key",
    ]

    for _, row in df.iterrows():
        field_id = _norm_str(row.get("field_id"))
        if not field_id:
            continue

        for col in candidate_cols:
            key = _norm_lookup_key(row.get(col))
            if not key:
                continue

            if key in lookup and lookup[key] != field_id:
                collisions[key].update([lookup[key], field_id])
            else:
                lookup[key] = field_id

    # Ambiguous short names are not guessed globally.
    for key in collisions:
        lookup.pop(key, None)

    return lookup


def build_field_maps(
    cfg_fields_df: pd.DataFrame,
) -> tuple[dict[str, str], dict[str, str]]:
    field_id_to_key: dict[str, str] = {}
    field_id_to_entity: dict[str, str] = {}

    for row in cfg_fields_df.to_dict("records"):
        field_id = _norm_str(row.get("field_id"))
        field_key = _norm_str(row.get("field_key"))
        entity_type = _norm_str(row.get("entity_type")).upper()

        if field_id:
            field_id_to_key[field_id] = field_key
            field_id_to_entity[field_id] = entity_type

    return field_id_to_key, field_id_to_entity


# =========================================================
# Config: Cfg__MetaobjectDefs
# =========================================================

def load_cfg_metaobject_defs(ws_cfg_metaobject_defs) -> pd.DataFrame:
    rows = _with_sheets_retry(
        ws_cfg_metaobject_defs.get_all_records,
        action="cfg_metaobject_defs.read",
    )
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__MetaobjectDefs is empty")

    for col in ["type", "field_key"]:
        if col not in df.columns:
            raise ValueError(f"Cfg__MetaobjectDefs missing required column: {col}")

    for col in ["type", "type_name", "field_key", "field_name", "field_type", "gid"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(_norm_str)

    df = df[(df["type"] != "") & (df["field_key"] != "")].copy()
    return df



def build_cfg_metaobject_defs_lookup(
    cfg_metaobject_defs_df: pd.DataFrame,
    entity_type: str = "METAOBJECT_ENTRY",
) -> dict[str, str]:
    """
    Key: normalized metaobject type + "|" + normalized field header candidate.
    Value: METAOBJECT_ENTRY|mo.{type}.{field_key}
    """
    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    for _, row in cfg_metaobject_defs_df.iterrows():
        metaobject_type = _norm_str(row.get("type"))
        field_key = _norm_str(row.get("field_key"))
        if not metaobject_type or not field_key:
            continue

        field_id = f"{entity_type}|mo.{metaobject_type}.{field_key}"
        candidates = [
            field_key,
            row.get("field_name"),
            f"mo.{metaobject_type}.{field_key}",
            field_id,
        ]

        for candidate in candidates:
            candidate_key = _norm_lookup_key(candidate)
            if not candidate_key:
                continue

            lookup_key = f"{_norm_lookup_key(metaobject_type)}|{candidate_key}"
            if lookup_key in lookup and lookup[lookup_key] != field_id:
                collisions[lookup_key].update([lookup[lookup_key], field_id])
            else:
                lookup[lookup_key] = field_id

    for key in collisions:
        lookup.pop(key, None)

    return lookup


# =========================================================
# Wide sheet parsing
# =========================================================

def load_wide_sheet(
    ws_input,
) -> tuple[list[list[Any]], list[str], list[str], list[list[Any]]]:
    values = _with_sheets_retry(
        ws_input.get_all_values,
        action="entries_update_wide.read",
    )

    if not values:
        raise ValueError("❌ Wide sheet is empty")
    if len(values) < 2:
        raise ValueError(
            "❌ Wide sheet must have at least 2 rows: header row + field_id row"
        )

    if not _row_has_any_value(values[0]):
        raise ValueError("❌ Wide 第1行为空")

    max_cols = max(len(row) for row in values)
    padded = [_pad_row(row, max_cols) for row in values]

    header_row = [_norm_str(x) for x in padded[0]]
    field_id_row = [_norm_str(x) for x in padded[1]]
    data_rows = padded[2:]

    return padded, header_row, field_id_row, data_rows



def find_effective_data_rows(
    data_rows: list[list[Any]],
) -> list[tuple[int, list[Any]]]:
    result: list[tuple[int, list[Any]]] = []

    for sheet_row, row in enumerate(data_rows, start=3):
        if _row_has_any_value(row):
            result.append((sheet_row, row))

    return result


def _build_header_positions(header_row: list[str]) -> dict[str, list[int]]:
    positions: dict[str, list[int]] = defaultdict(list)
    for idx, header in enumerate(header_row):
        normalized = _norm_header(header)
        if normalized:
            positions[normalized].append(idx)
    return dict(positions)


def detect_layout(header_row: list[str]) -> dict[str, Any]:
    positions = _build_header_positions(header_row)

    duplicated_headers = {
        name: indexes
        for name, indexes in positions.items()
        if len(indexes) > 1
    }
    if duplicated_headers:
        raise ValueError(
            "❌ Wide 第1行存在重复表头: "
            + ", ".join(
                f"{name} at columns {[i + 1 for i in indexes]}"
                for name, indexes in sorted(duplicated_headers.items())
            )
        )

    op_idx = positions.get("op", [None])[0]
    action_idx = positions.get("action", [None])[0]
    if op_idx is None and action_idx is None:
        raise ValueError("❌ 未找到 op/action 列。需要存在 op 或 action 之一")

    owner_columns: list[tuple[str, int]] = []
    for candidate in OWNER_COLUMN_CANDIDATES:
        indexes = positions.get(candidate, [])
        if indexes:
            owner_columns.append((candidate, indexes[0]))

    if not owner_columns:
        raise ValueError(
            "❌ 未找到 owner 列。需要存在 entry_gid / gid / gid_or_handle / lookup_handle / handle 之一"
        )

    fixed_indexes: set[int] = set()
    for name in CONTROL_COLUMN_NAMES:
        indexes = positions.get(name, [])
        fixed_indexes.update(indexes)

    # Detect helper columns such as collection_link__handle. A helper column is
    # excluded from field_id resolution and is used only when its main field is
    # blank on the same row.
    auxiliary_handle_indexes: set[int] = set()
    field_handle_fallbacks: dict[int, int] = {}
    auxiliary_columns: list[str] = []

    for idx, header in enumerate(header_row):
        normalized = _norm_header(header)
        if not normalized.endswith(AUXILIARY_HANDLE_SUFFIX):
            continue

        base_header = normalized[: -len(AUXILIARY_HANDLE_SUFFIX)]
        base_indexes = positions.get(base_header, [])
        if not base_indexes:
            raise ValueError(
                "❌ 辅助 handle 列缺少对应主字段列: "
                f"{header} -> expected main column {base_header}"
            )

        base_idx = base_indexes[0]
        if base_idx in fixed_indexes:
            raise ValueError(
                "❌ 辅助 handle 列不能对应控制列: "
                f"{header} -> {base_header}"
            )

        auxiliary_handle_indexes.add(idx)
        field_handle_fallbacks[base_idx] = idx
        auxiliary_columns.append(_norm_str(header))

    field_col_indexes = [
        idx
        for idx, header in enumerate(header_row)
        if (
            _norm_str(header) != ""
            and idx not in fixed_indexes
            and idx not in auxiliary_handle_indexes
        )
    ]

    if not field_col_indexes:
        raise ValueError(
            "❌ 未找到可转成长表的字段列。控制列和辅助列之外至少需要一个字段列"
        )

    return {
        "op_idx": op_idx,
        "action_idx": action_idx,
        "op_input_columns": [
            name
            for name, idx in [("op", op_idx), ("action", action_idx)]
            if idx is not None
        ],
        "owner_columns": owner_columns,
        "owner_col": owner_columns[0][0],
        "entry_type_idx": positions.get("entry_type", [None])[0],
        "mode_idx": positions.get("mode", [None])[0],
        "note_idx": positions.get("note", [None])[0],
        "run_id_idx": positions.get("run_id", [None])[0],
        "new_handle_idx": positions.get("new_handle", [None])[0],
        "fixed_indexes": fixed_indexes,
        "field_col_indexes": field_col_indexes,
        "auxiliary_handle_indexes": auxiliary_handle_indexes,
        "field_handle_fallbacks": field_handle_fallbacks,
        "auxiliary_columns": auxiliary_columns,
    }


def resolve_input_op(
    row: list[Any],
    *,
    sheet_row: int,
    op_idx: Optional[int],
    action_idx: Optional[int],
) -> tuple[str, Optional[dict[str, Any]]]:
    op_value = _safe_cell(row, op_idx).upper()
    action_value = _safe_cell(row, action_idx).upper()

    if op_value and action_value and op_value != action_value:
        return "", {
            "entity_type": "METAOBJECT_ENTRY",
            "gid": "",
            "field_key": "op",
            "error_reason": "op_action_conflict",
            "message": (
                f"sheet_row={sheet_row} | op={op_value} | action={action_value} | "
                "op and action conflict"
            ),
        }

    return op_value or action_value, None


def resolve_owner_value(
    row: list[Any],
    owner_columns: list[tuple[str, int]],
) -> tuple[str, str]:
    for name, idx in owner_columns:
        value = _safe_cell(row, idx)
        if value:
            return value, name
    return "", ""


# =========================================================
# Field-id resolution
# =========================================================

def resolve_blank_field_id_row_from_cfg_fields(
    *,
    header_row: list[str],
    field_id_row: list[str],
    cfg_fields_df: pd.DataFrame,
    field_col_indexes: list[int],
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[str]:
    max_cols = max(len(header_row), len(field_id_row))
    headers = _pad_row(header_row, max_cols)
    field_ids = _pad_row(field_id_row, max_cols)

    lookup = build_cfg_fields_lookup(
        cfg_fields_df,
        entity_type=entity_type,
    )

    for col_idx in field_col_indexes:
        if _norm_str(field_ids[col_idx]):
            continue

        header = _norm_str(headers[col_idx])
        if not header:
            continue

        matched = lookup.get(_norm_lookup_key(header), "")
        if matched:
            field_ids[col_idx] = matched

    return field_ids


def resolve_field_id_for_cell(
    *,
    header: Any,
    entry_type: Any,
    existing_field_id: Any,
    cfg_metaobject_lookup: dict[str, str],
    entity_type: str = "METAOBJECT_ENTRY",
) -> str:
    existing = _norm_str(existing_field_id)
    if existing:
        return existing

    header_value = _norm_str(header)
    entry_type_value = _norm_str(entry_type)
    if not header_value or not entry_type_value:
        return ""

    lookup_key = (
        f"{_norm_lookup_key(entry_type_value)}|"
        f"{_norm_lookup_key(header_value)}"
    )
    matched = cfg_metaobject_lookup.get(lookup_key, "")
    if matched:
        return matched

    # Same safe deterministic fallback used by Entries_Create.
    simple = _norm_str(header_value)
    if simple and all(ch.isalnum() or ch == "_" for ch in simple):
        return f"{entity_type}|mo.{entry_type_value}.{simple}"

    return ""


def resolve_field_id_row_from_pending_rows(
    *,
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    layout: dict[str, Any],
    cfg_metaobject_lookup: dict[str, str],
    include_empty: bool,
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[str]:
    """
    Fill Row 2 only when all relevant pending rows resolve a column to one field_id.
    Mixed entry types may legitimately require different field_ids, in which case Row 2
    remains blank and each cell is resolved row-by-row during transformation.
    """
    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(row) for _, row in effective_rows), default=0),
    )
    headers = _pad_row(header_row, max_cols)
    field_ids = _pad_row(field_id_row, max_cols)

    pending_rows: list[tuple[int, list[Any]]] = []
    for sheet_row, raw_row in effective_rows:
        row = _pad_row(raw_row, max_cols)
        op_value, conflict = resolve_input_op(
            row,
            sheet_row=sheet_row,
            op_idx=layout["op_idx"],
            action_idx=layout["action_idx"],
        )
        if conflict or op_value in ("", "SKIP"):
            continue
        pending_rows.append((sheet_row, row))

    for col_idx in layout["field_col_indexes"]:
        if _norm_str(field_ids[col_idx]):
            continue

        resolved_ids: set[str] = set()
        for _, row in pending_rows:
            value = _norm_str(row[col_idx])
            if value == "" and not include_empty:
                continue

            entry_type_value = _safe_cell(row, layout["entry_type_idx"])
            resolved = resolve_field_id_for_cell(
                header=headers[col_idx],
                entry_type=entry_type_value,
                existing_field_id="",
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )
            if resolved:
                resolved_ids.add(resolved)

        if len(resolved_ids) == 1:
            field_ids[col_idx] = next(iter(resolved_ids))

    return field_ids


def write_field_id_row_if_changed(
    ws_input,
    *,
    original_field_id_row: list[str],
    resolved_field_id_row: list[str],
) -> int:
    max_cols = max(len(original_field_id_row), len(resolved_field_id_row))
    original = _pad_row(original_field_id_row, max_cols)
    resolved = _pad_row(resolved_field_id_row, max_cols)

    changed = sum(
        1
        for before, after in zip(original, resolved)
        if _norm_str(before) != _norm_str(after)
    )
    if changed == 0:
        return 0

    end_a1 = gspread.utils.rowcol_to_a1(2, max_cols)
    _with_sheets_retry(
        lambda: ws_input.update(
            range_name=f"A2:{end_a1}",
            values=[resolved],
            value_input_option="RAW",
        ),
        action="entries_update_wide.write_field_id_row",
    )
    return changed



# =========================================================
# Validation / transformation
# =========================================================

def validate_and_transform(
    *,
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    layout: dict[str, Any],
    cfg_metaobject_lookup: dict[str, str],
    cfg_field_ids: set[str],
    include_empty: bool,
    default_mode: str = "STRICT",
    entity_type: str = "METAOBJECT_ENTRY",
) -> tuple[pd.DataFrame, dict[str, int], list[dict[str, Any]]]:
    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(row) for _, row in effective_rows), default=0),
    )
    headers = _pad_row(header_row, max_cols)
    field_ids = _pad_row(field_id_row, max_cols)

    output_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    rows_loaded = len(effective_rows)
    rows_pending = 0
    resolved_field_cells = 0
    rows_skipped_input = 0

    for sheet_row, raw_row in effective_rows:
        row = _pad_row(raw_row, max_cols)

        op_value, op_error = resolve_input_op(
            row,
            sheet_row=sheet_row,
            op_idx=layout["op_idx"],
            action_idx=layout["action_idx"],
        )
        if op_error:
            errors.append(op_error)
            continue

        if op_value in ("", "SKIP"):
            rows_skipped_input += 1
            continue

        rows_pending += 1

        if op_value != "UPDATE":
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": "",
                    "field_key": "op",
                    "error_reason": "invalid_op",
                    "message": (
                        f"sheet_row={sheet_row} | op={op_value} | "
                        "Entries_Update only accepts UPDATE, blank, or SKIP"
                    ),
                }
            )
            continue

        owner_value, owner_source = resolve_owner_value(
            row,
            layout["owner_columns"],
        )
        if not owner_value:
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": "",
                    "field_key": "entry_gid",
                    "error_reason": "missing_owner",
                    "message": (
                        f"sheet_row={sheet_row} | no value found in owner columns "
                        f"{[name for name, _ in layout['owner_columns']]}"
                    ),
                }
            )
            continue

        entry_type_value = _safe_cell(row, layout["entry_type_idx"])
        mode_value = _safe_cell(row, layout["mode_idx"]).upper() or default_mode
        note_value = _safe_cell(row, layout["note_idx"])
        new_handle_value = _safe_cell(row, layout["new_handle_idx"])

        if mode_value not in {"STRICT", "LOOSE"}:
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": owner_value,
                    "field_key": "mode",
                    "error_reason": "invalid_mode",
                    "message": (
                        f"sheet_row={sheet_row} | mode={mode_value} | "
                        "mode must be STRICT or LOOSE"
                    ),
                }
            )
            continue

        generated_for_row: list[dict[str, Any]] = []

        for col_idx in layout["field_col_indexes"]:
            # Prefer the actual field value (usually a GID). When it is blank,
            # accept the paired __handle helper as a fallback value. The helper
            # column itself never becomes an output row or field_id.
            main_value = _norm_str(row[col_idx])
            helper_idx = layout["field_handle_fallbacks"].get(col_idx)
            helper_value = _safe_cell(row, helper_idx)
            desired_value = main_value or helper_value

            if desired_value == "" and not include_empty:
                continue

            header = _norm_str(headers[col_idx])
            field_id = resolve_field_id_for_cell(
                header=header,
                entry_type=entry_type_value,
                existing_field_id=field_ids[col_idx],
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )

            if not field_id:
                errors.append(
                    {
                        "entity_type": entity_type,
                        "gid": owner_value,
                        "field_key": header,
                        "error_reason": "unmatched_field_header",
                        "message": (
                            f"sheet_row={sheet_row} | col={col_idx + 1} | "
                            f"entry_type={entry_type_value} | header={header} | "
                            "cannot resolve field_id from Row 2 / Cfg__Fields / Cfg__MetaobjectDefs"
                        ),
                    }
                )
                continue

            if field_id not in cfg_field_ids:
                errors.append(
                    {
                        "entity_type": entity_type,
                        "gid": owner_value,
                        "field_key": header,
                        "error_reason": "field_id_not_in_cfg_fields",
                        "message": (
                            f"sheet_row={sheet_row} | col={col_idx + 1} | "
                            f"header={header} | field_id={field_id} | "
                            "resolved field_id does not exist in Cfg__Fields"
                        ),
                    }
                )
                continue

            match = re.match(r"^METAOBJECT_ENTRY\|mo\.([^.]+)\..+$", field_id)
            field_id_entry_type = match.group(1).strip() if match else ""
            if (
                entry_type_value
                and field_id_entry_type
                and entry_type_value != field_id_entry_type
            ):
                errors.append(
                    {
                        "entity_type": entity_type,
                        "gid": owner_value,
                        "field_key": header,
                        "error_reason": "field_id_entry_type_mismatch",
                        "message": (
                            f"sheet_row={sheet_row} | col={col_idx + 1} | "
                            f"entry_type={entry_type_value} | field_id={field_id} | "
                            f"field_id_entry_type={field_id_entry_type}"
                        ),
                    }
                )
                continue

            resolved_field_cells += 1
            generated_for_row.append(
                {
                    "op": "UPDATE",
                    "entry_type": entry_type_value,
                    "entry_gid": owner_value,
                    "mode": mode_value,
                    "field_id": field_id,
                    "value": desired_value,
                    "new_handle": "",
                    "slot": "",
                    "note": note_value,
                    "_source_sheet_row": sheet_row,
                    "_owner_source": owner_source,
                }
            )

        # Put new_handle on one generated row only. If no field value exists,
        # create a handle-only row accepted by edit_entries_update.py.
        if new_handle_value:
            if generated_for_row:
                generated_for_row[0]["new_handle"] = new_handle_value
            else:
                generated_for_row.append(
                    {
                        "op": "UPDATE",
                        "entry_type": entry_type_value,
                        "entry_gid": owner_value,
                        "mode": mode_value,
                        "field_id": "",
                        "value": "",
                        "new_handle": new_handle_value,
                        "slot": "",
                        "note": note_value,
                        "_source_sheet_row": sheet_row,
                        "_owner_source": owner_source,
                    }
                )

        if not generated_for_row:
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": owner_value,
                    "field_key": "",
                    "error_reason": "nothing_to_update",
                    "message": (
                        f"sheet_row={sheet_row} | UPDATE row has no field value "
                        "and no new_handle"
                    ),
                }
            )
            continue

        output_rows.extend(generated_for_row)

    df_long = pd.DataFrame(output_rows)
    rows_planned_before_dedupe = len(df_long)

    if df_long.empty:
        df_long = pd.DataFrame(columns=OUTPUT_HEADER)
        duplicate_rows = 0
    else:
        dedupe_cols = OUTPUT_HEADER
        before = len(df_long)
        df_long = (
            df_long.drop_duplicates(subset=dedupe_cols, keep="first")
            .reset_index(drop=True)
        )
        duplicate_rows = before - len(df_long)
        df_long = df_long[OUTPUT_HEADER]

    summary = {
        "rows_loaded": rows_loaded,
        "rows_pending": rows_pending,
        "rows_recognized": resolved_field_cells,
        "rows_planned": rows_planned_before_dedupe,
        "rows_written": len(df_long),
        "rows_skipped": rows_skipped_input + duplicate_rows,
        "error_count": len(errors),
    }

    return df_long, summary, errors


# =========================================================
# Sheet writes
# =========================================================

def _ensure_grid_size(ws, *, rows: int, cols: int) -> None:
    target_rows = max(rows, 1)
    target_cols = max(cols, 1)

    current_rows = int(getattr(ws, "row_count", 0) or 0)
    current_cols = int(getattr(ws, "col_count", 0) or 0)

    if current_rows < target_rows or current_cols < target_cols:
        ws.resize(
            rows=max(current_rows, target_rows),
            cols=max(current_cols, target_cols),
        )


def overwrite_long_sheet(
    ws_output,
    df_long: pd.DataFrame,
    *,
    clear_output_first: bool = True,
) -> None:
    values = [OUTPUT_HEADER] + df_long[OUTPUT_HEADER].fillna("").astype(str).values.tolist()

    _ensure_grid_size(
        ws_output,
        rows=len(values),
        cols=len(OUTPUT_HEADER),
    )

    if clear_output_first:
        _with_sheets_retry(
            ws_output.clear,
            action="entries_update_long.clear",
            retry_5xx=False,
        )

    end_a1 = gspread.utils.rowcol_to_a1(len(values), len(OUTPUT_HEADER))
    _with_sheets_retry(
        lambda: ws_output.update(
            range_name=f"A1:{end_a1}",
            values=values,
            value_input_option="RAW",
        ),
        action="entries_update_long.write",
        retry_5xx=True,
    )



# =========================================================
# Main
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = DEFAULT_JOB_NAME,

    gsheet_sa_b64_secret: str,
    console_core_url: str,
    gsheet_sa_value: Optional[str] = None,
    secret_home: Optional[str] = None,

    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Entries_Update-Wide",

    output_sheet_label: Optional[str] = None,
    output_worksheet_title: str = "Entries_Update-Long",

    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = "Cfg__Fields",
    cfg_tab_metaobject_defs: str = "Cfg__MetaobjectDefs",
    cfg_field_match_entity_type: str = "METAOBJECT_ENTRY",

    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,

    include_empty: bool = False,
    clear_output_first: bool = True,
    write_resolved_field_ids: bool = True,
    default_mode: str = "STRICT",
    detail_max_per_reason: int = 2,

    run_id: Optional[str] = None,
) -> dict[str, Any]:
    run_id = run_id or _make_run_id()
    output_sheet_label = output_sheet_label or input_sheet_label
    default_mode = _norm_str(default_mode).upper() or "STRICT"

    if default_mode not in {"STRICT", "LOOSE"}:
        raise ValueError("default_mode must be STRICT or LOOSE")

    gc = build_gsheet_client(
        gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=gsheet_sa_value,
        secret_home=secret_home,
    )

    _, ws_input, input_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_output, output_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=output_sheet_label,
        worksheet_title=output_worksheet_title,
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

    _, ws_cfg_metaobject_defs, cfg_metaobject_defs_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_sheet_label,
        worksheet_title=cfg_tab_metaobject_defs,
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

    rows_loaded = 0
    rows_pending = 0
    rows_recognized = 0
    rows_planned = 0
    rows_written = 0
    rows_skipped = 0

    try:
        cfg_fields_df = load_cfg_fields(ws_cfg_fields)
        cfg_field_ids = set(cfg_fields_df["field_id"].map(_norm_str).tolist())

        cfg_metaobject_defs_df = load_cfg_metaobject_defs(ws_cfg_metaobject_defs)
        cfg_metaobject_lookup = build_cfg_metaobject_defs_lookup(
            cfg_metaobject_defs_df,
            entity_type=cfg_field_match_entity_type,
        )

        _, header_row, original_field_id_row, data_rows = load_wide_sheet(ws_input)
        layout = detect_layout(header_row)
        effective_rows = find_effective_data_rows(data_rows)

        # Auxiliary __handle columns are not real fields. Keep/restore their
        # Row-2 field_id cells as blank even if an older run wrote a synthetic ID.
        cleaned_field_id_row = _pad_row(
            original_field_id_row,
            max(len(header_row), len(original_field_id_row)),
        )
        for helper_idx in layout["auxiliary_handle_indexes"]:
            cleaned_field_id_row[helper_idx] = ""

        resolved_field_id_row = resolve_blank_field_id_row_from_cfg_fields(
            header_row=header_row,
            field_id_row=cleaned_field_id_row,
            cfg_fields_df=cfg_fields_df,
            field_col_indexes=layout["field_col_indexes"],
            entity_type=cfg_field_match_entity_type,
        )
        resolved_field_id_row = resolve_field_id_row_from_pending_rows(
            effective_rows=effective_rows,
            header_row=header_row,
            field_id_row=resolved_field_id_row,
            layout=layout,
            cfg_metaobject_lookup=cfg_metaobject_lookup,
            include_empty=include_empty,
            entity_type=cfg_field_match_entity_type,
        )

        df_long, summary, validation_errors = validate_and_transform(
            effective_rows=effective_rows,
            header_row=header_row,
            field_id_row=resolved_field_id_row,
            layout=layout,
            cfg_metaobject_lookup=cfg_metaobject_lookup,
            cfg_field_ids=cfg_field_ids,
            include_empty=include_empty,
            default_mode=default_mode,
            entity_type=cfg_field_match_entity_type,
        )

        rows_loaded = summary["rows_loaded"]
        rows_pending = summary["rows_pending"]
        rows_recognized = summary["rows_recognized"]
        rows_planned = summary["rows_planned"]
        rows_written = 0
        rows_skipped = summary["rows_skipped"]

        if validation_errors:
            rows_skipped += len(validation_errors)
            summary["rows_written"] = 0
            summary["rows_skipped"] = rows_skipped

            logger.log_row(
                phase="transform",
                log_type="summary",
                status="ERROR",
                rows_loaded=rows_loaded,
                rows_pending=rows_pending,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=0,
                rows_skipped=rows_skipped,
                message=f"Validation failed | errors={len(validation_errors)}",
                error_reason="validation_failed",
            )
            log_grouped_details(
                logger,
                phase="transform",
                status="FAIL",
                rows_loaded=rows_loaded,
                rows_pending=rows_pending,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=0,
                rows_skipped=rows_skipped,
                detail_rows=validation_errors,
                max_per_reason=detail_max_per_reason,
            )
            logger.flush()

            return {
                "status": "ERROR",
                "summary": summary,
                "warnings": [
                    {
                        "type": "validation_failed",
                        "count": len(validation_errors),
                        "examples": validation_errors[: min(20, len(validation_errors))],
                    }
                ],
                "preview": [],
                "meta": {
                    "site_code": site_code,
                    "job_name": job_name,
                    "run_id": run_id,
                    "input_sheet_url": input_sheet_url,
                    "output_sheet_url": output_sheet_url,
                    "cfg_sheet_url": cfg_sheet_url,
                    "cfg_metaobject_defs_sheet_url": cfg_metaobject_defs_sheet_url,
                    "runlog_sheet_url": runlog_sheet_url,
                    "op_input_columns": layout["op_input_columns"],
                    "owner_col": layout["owner_col"],
                    "owner_columns": [name for name, _ in layout["owner_columns"]],
                    "auxiliary_columns": layout["auxiliary_columns"],
                    "field_id_row_written": 0,
                },
            }

        field_id_row_written = 0
        if write_resolved_field_ids:
            field_id_row_written = write_field_id_row_if_changed(
                ws_input,
                original_field_id_row=original_field_id_row,
                resolved_field_id_row=resolved_field_id_row,
            )

        overwrite_long_sheet(
            ws_output,
            df_long,
            clear_output_first=clear_output_first,
        )

        rows_written = len(df_long)
        summary["rows_written"] = rows_written
        summary["field_ids_written_to_row2"] = field_id_row_written

        logger.log_row(
            phase="transform",
            log_type="summary",
            status="SUCCESS",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            message=(
                "Entries_Update wide->long completed | "
                f"op_inputs={layout['op_input_columns']} | "
                f"owner_col={layout['owner_col']} | "
                f"field_ids_written_to_row2={field_id_row_written} | "
                f"input_ws={input_worksheet_title} | "
                f"output_ws={output_worksheet_title}"
            ),
            error_reason="",
        )
        logger.flush()

        preview = df_long.head(50).to_dict("records") if not df_long.empty else []

        return {
            "status": "SUCCESS",
            "summary": summary,
            "warnings": [],
            "preview": preview,
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "cfg_metaobject_defs_sheet_url": cfg_metaobject_defs_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "op_input_columns": layout["op_input_columns"],
                "owner_col": layout["owner_col"],
                "owner_columns": [name for name, _ in layout["owner_columns"]],
                "auxiliary_columns": layout["auxiliary_columns"],
                "field_id_row_written": field_id_row_written,
                "output_columns": OUTPUT_HEADER,
            },
        }

    except Exception as exc:
        logger.log_row(
            phase="transform",
            log_type="summary",
            status="ERROR",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            message=str(exc),
            error_reason=type(exc).__name__,
        )
        logger.flush()
        raise

