# shopify_pre_edit/0_3_1_entries_create_wide_to_long.py

from __future__ import annotations

import base64
import datetime as dt
import json
import random
import re
import sys
import time
from collections import defaultdict
from typing import Any, Optional

import gspread
import pandas as pd
from google.oauth2 import service_account


MODULE_PATH = "shopify_pre_edit.0_3_1_entries_create_wide_to_long"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
DEFAULT_JOB_NAME = "entries_create_wide_to_long"

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


# =========================================================
# Utils
# =========================================================

def _utc_run_id(prefix: str = "wide_to_long") -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}__%Y%m%d_%H%M%S")


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


def _safe_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


# =========================================================
# Secrets / clients
# =========================================================

class SecretValue:
    def __init__(self, value: str, source_type: str, source_detail: str):
        self.value = str(value).strip()
        self.source_type = str(source_type).strip()
        self.source_detail = str(source_detail).strip()


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
    name = _norm_str(secret_name)
    resolved_project_code = _norm_str(project_code).upper()

    if not name:
        raise ValueError("Secret name is blank.")
    if not resolved_project_code:
        raise ValueError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _norm_str(explicit_value):
        return SecretValue(
            _norm_str(explicit_value),
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
    raw = _norm_str(raw_value)
    if not raw:
        raise ValueError("Google service-account Secret is empty.")

    try:
        info = json.loads(raw)
        secret_format = "RAW_JSON"
    except Exception:
        try:
            padded = raw + "=" * ((4 - len(raw) % 4) % 4)
            info = json.loads(
                base64.b64decode(padded).decode("utf-8")
            )
            secret_format = "BASE64_JSON"
        except Exception as exc:
            raise ValueError(
                "Google service-account Secret is neither valid raw JSON nor Base64 JSON."
            ) from exc

    required = {
        "type",
        "project_id",
        "private_key",
        "client_email",
        "token_uri",
    }
    missing = sorted(
        key
        for key in required
        if not info.get(key)
    )

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
    sa_info, _secret_format = _parse_service_account_text(
        secret.value
    )
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


def _build_gsheet_client_from_value(
    raw_value: str,
) -> gspread.Client:
    sa_info, _secret_format = _parse_service_account_text(
        raw_value
    )
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)



def _sheets_error_status(
    exc: BaseException,
) -> Optional[int]:
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

    if retry_5xx and status in {
        500,
        502,
        503,
        504,
    }:
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
    base_sleep: float = 1.5,
    max_delay: float = 20.0,
    retry_5xx: bool = True,
):
    attempts = max(
        1,
        int(max_attempts),
    )

    for attempt in range(
        1,
        attempts + 1,
    ):
        try:
            return operation()
        except Exception as exc:
            retryable = _is_retryable_sheets_error(
                exc,
                retry_5xx=retry_5xx,
            )

            if (
                not retryable
                or attempt >= attempts
            ):
                raise

            delay = min(
                float(max_delay),
                float(base_sleep)
                * (2 ** (attempt - 1)),
            ) + random.random()

            status = _sheets_error_status(exc)
            reason = (
                f"HTTP {status}"
                if status is not None
                else type(exc).__name__
            )

            print(
                "[Sheets retry] "
                f"action={action} | "
                f"attempt={attempt}/{attempts} | "
                f"reason={reason} | "
                f"sleep={delay:.1f}s",
                flush=True,
            )

            time.sleep(delay)

    raise RuntimeError(
        f"Sheets operation exhausted retries: {action}"
    )


def _normalize_registry_header(
    value: Any,
) -> str:
    return re.sub(
        r"[\s_]+",
        " ",
        _norm_str(value).lower(),
    ).strip()


def _extract_spreadsheet_id(
    value: Any,
) -> str:
    raw = _norm_str(value)

    if not raw:
        raise ValueError(
            "Workspace Project Registry ID/URL is empty."
        )

    match = re.search(
        r"/spreadsheets/d/([A-Za-z0-9_-]+)",
        raw,
    )

    if match:
        return match.group(1)

    if re.fullmatch(
        r"[A-Za-z0-9_-]+",
        raw,
    ):
        return raw

    raise ValueError(
        "Workspace Project Registry must be "
        "a Google Sheets ID or URL."
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
    resolved_project_code = _norm_str(
        project_code
    ).upper()

    if not resolved_project_code:
        raise ValueError(
            "project_code is required."
        )

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
        lambda: workspace_gc.open_by_key(
            registry_file_id
        ),
        action="workspace.open_registry",
    )

    worksheet = _with_sheets_retry(
        lambda: registry_book.worksheet(
            workspace_registry_tab
        ),
        action="workspace.open_registry_tab",
    )

    values = _with_sheets_retry(
        lambda: worksheet.get_all_values(),
        action="workspace.read_registry",
    )

    if not values:
        raise ValueError(
            "Workspace Project Registry tab "
            f"{workspace_registry_tab!r} is empty."
        )

    header_map: dict[str, int] = {}
    duplicate_headers: list[str] = []

    for index, raw_header in enumerate(
        values[0]
    ):
        normalized = _normalize_registry_header(
            raw_header
        )

        if not normalized:
            continue

        if normalized in header_map:
            duplicate_headers.append(
                normalized
            )

        header_map[normalized] = index

    if duplicate_headers:
        raise ValueError(
            "Workspace Project Registry has duplicate "
            "normalized headers: "
            + ", ".join(
                sorted(
                    set(duplicate_headers)
                )
            )
        )

    def require_column(
        *aliases: str,
    ) -> int:
        for alias in aliases:
            normalized = _normalize_registry_header(
                alias
            )

            if normalized in header_map:
                return header_map[normalized]

        raise ValueError(
            "Workspace Project Registry is missing "
            "a required column; "
            f"accepted_aliases={aliases}."
        )

    project_col = require_column(
        "project_code",
        "project code",
    )
    active_col = require_column(
        "active",
    )
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
        _normalize_registry_header(
            "project_name"
        )
    )

    width = len(values[0])
    matches: list[
        tuple[
            int,
            list[Any],
        ]
    ] = []

    for row_number, raw_row in enumerate(
        values[1:],
        start=2,
    ):
        row = list(raw_row) + [""] * max(
            0,
            width - len(raw_row),
        )

        if (
            _norm_str(
                row[project_col]
            ).upper()
            == resolved_project_code
        ):
            matches.append(
                (
                    row_number,
                    row,
                )
            )

    if not matches:
        raise ValueError(
            "Workspace Project Registry has no row "
            f"for project_code={resolved_project_code}."
        )

    if len(matches) > 1:
        raise ValueError(
            "Workspace Project Registry has duplicate "
            "project rows: "
            f"project_code={resolved_project_code}; "
            f"rows={[row_number for row_number, _ in matches]}."
        )

    source_row, row = matches[0]

    active_text = _norm_str(
        row[active_col]
    ).lower()

    if active_text not in {
        "true",
        "1",
        "yes",
        "y",
        "是",
    }:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}; "
            f"row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": (
            _norm_str(
                row[project_name_col]
            )
            if project_name_col is not None
            else ""
        ),
        "console_core_url": _norm_str(
            row[console_url_col]
        ),
        "gsheet_secret_name": _norm_str(
            row[gsheet_secret_col]
        ),
        "account_config_tab": _norm_str(
            row[account_tab_col]
        ),
        "registry_source_row": str(
            source_row
        ),
        "workspace_auth_source_type": (
            workspace_secret.source_type
        ),
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
            "Workspace Project Registry route has "
            "empty required values: "
            f"project_code={resolved_project_code}; "
            f"fields={missing}; "
            f"row={source_row}."
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


def resolve_project_gsheet_auth(
    *,
    project_code: str,
    gsheet_secret_name: str,
    secret_home: Optional[str] = None,
) -> dict[str, str]:
    """Resolve the project Sheets credential value once at the auth boundary."""
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
    mode = _norm_str(
        registry_mode
    ).upper() or "OFF"

    allowed = {
        "OFF",
        "CHECK",
        "UPDATE_URL",
        "UPDATE_URL_AND_NAME",
    }

    if mode not in allowed:
        raise ValueError(
            "registry_mode must be one of "
            f"{sorted(allowed)}."
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
        mode
        in {
            "UPDATE_URL",
            "UPDATE_URL_AND_NAME",
        }
        and not _norm_str(
            current_colab_url
        )
    ):
        raise ValueError(
            f"registry_mode={mode} "
            "requires current_colab_url."
        )

    if (
        mode == "UPDATE_URL_AND_NAME"
        and not _norm_str(
            current_colab_name
        )
    ):
        raise ValueError(
            "UPDATE_URL_AND_NAME "
            "requires current_colab_name."
        )

    secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
    )

    gc = _build_gsheet_client_from_value(
        secret.value
    )

    book = _with_sheets_retry(
        lambda: gc.open_by_url(
            console_core_url
        ),
        action="registry.open_console",
    )

    ws = _with_sheets_retry(
        lambda: book.worksheet(
            registry_tab
        ),
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

    for index, raw_header in enumerate(
        values[0]
    ):
        normalized = _normalize_registry_header(
            raw_header
        )

        if not normalized:
            continue

        if normalized in header_map:
            raise ValueError(
                "Registry tab has duplicate "
                "normalized header: "
                f"{normalized}."
            )

        header_map[normalized] = index

    def require_column(
        *aliases: str,
    ) -> int:
        for alias in aliases:
            key = _normalize_registry_header(
                alias
            )

            if key in header_map:
                return header_map[key]

        raise ValueError(
            "Registry tab is missing required "
            "column; "
            f"accepted aliases={aliases}."
        )

    job_col = require_column(
        "job_name",
        "job name",
    )
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
        _norm_str(
            job_name
        ).lower(),
        _norm_str(
            sheet_label
        ).lower(),
        _norm_str(
            tab_name
        ).lower(),
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
            _norm_str(
                padded[job_col]
            ).lower(),
            _norm_str(
                padded[label_col]
            ).lower(),
            _norm_str(
                padded[tab_col]
            ).lower(),
        )

        if logical_key == wanted:
            matches.append(
                row_index
            )

    if not matches:
        raise ValueError(
            "Registry target row was not found. "
            "This function never appends. "
            f"logical_key={wanted}"
        )

    if len(matches) > 1:
        raise ValueError(
            "Registry logical key is duplicated at "
            f"rows={matches}; "
            "no row was changed."
        )

    row_number = matches[0]

    current_row = (
        values[row_number - 1]
        + [""] * max(
            0,
            len(values[0])
            - len(
                values[
                    row_number - 1
                ]
            ),
        )
    )

    changes: list[
        tuple[
            str,
            int,
            str,
            str,
        ]
    ] = []

    provided_url = _norm_str(
        current_colab_url
    )
    provided_name = _norm_str(
        current_colab_name
    )

    if (
        provided_url
        and _norm_str(
            current_row[url_col]
        )
        != provided_url
    ):
        changes.append(
            (
                "colab_url",
                url_col + 1,
                _norm_str(
                    current_row[url_col]
                ),
                provided_url,
            )
        )

    if (
        provided_name
        and _norm_str(
            current_row[name_col]
        )
        != provided_name
    ):
        changes.append(
            (
                "colab_name",
                name_col + 1,
                _norm_str(
                    current_row[name_col]
                ),
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
            else {
                "colab_url",
                "colab_name",
            }
        )

        applied = [
            change
            for change in changes
            if change[0] in permitted
        ]

        for (
            field_name,
            column_number,
            _old_value,
            new_value,
        ) in applied:
            _with_sheets_retry(
                lambda rn=row_number, cn=column_number, nv=new_value: (
                    ws.update_cell(
                        rn,
                        cn,
                        nv,
                    )
                ),
                action=(
                    "registry.update_cell:"
                    f"{field_name}"
                ),
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
            "changed_fields="
            f"{[item[0] for item in changes]}"
        )

    return {
        "status": status,
        "target_row": row_number,
        "changed_fields": [
            item[0]
            for item in changes
        ],
    }


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
        action="cfg_sites.open_tab",
    )
    rows = _with_sheets_retry(
        lambda: ws.get_all_records(),
        action="cfg_sites.read",
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
        action=f"worksheet.open_book:{worksheet_title}",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(worksheet_title),
        action=f"worksheet.open:{worksheet_title}",
    )
    return sh, ws, sheet_url


def open_ws_optional_by_label_and_title(
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
        action=f"config_worksheet.open_book:{worksheet_title}",
    )
    try:
        ws = _with_sheets_retry(
            lambda: sh.worksheet(worksheet_title),
            action=f"config_worksheet.open:{worksheet_title}",
        )
    except Exception as e:
        raise ValueError(f"Cannot open config worksheet: label={label}, title={worksheet_title}") from e
    return sh, ws, sheet_url


def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = _with_sheets_retry(
        lambda: ws_cfg_fields.get_all_records(),
        action="cfg_fields.read",
    )
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    required = ["field_id", "field_key"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Cfg__Fields missing required column: {c}")

    for c in ["field_id", "field_key", "field_handle", "display_name", "entity_type", "source_type", "namespace", "key"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].map(_norm_str)

    df = df[df["field_id"] != ""].copy()
    return df


def _norm_lookup_key(x: Any) -> str:
    s = _norm_str(x).lower()
    s = s.replace("｜", "|").replace("–", "-").replace("—", "-")
    s = " ".join(s.split())
    return s


def build_cfg_fields_lookup(cfg_fields_df: pd.DataFrame, entity_type: str = "METAOBJECT_ENTRY") -> dict[str, str]:
    df = cfg_fields_df.copy()
    if entity_type:
        m = df["entity_type"].map(_norm_str).str.upper() == entity_type.strip().upper()
        if m.any():
            df = df[m].copy()

    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    candidate_cols = ["field_id", "field_key", "field_handle", "display_name", "key"]
    for _, r in df.iterrows():
        fid = _norm_str(r.get("field_id"))
        if not fid:
            continue
        for c in candidate_cols:
            k = _norm_lookup_key(r.get(c))
            if not k:
                continue
            if k in lookup and lookup[k] != fid:
                collisions[k].update([lookup[k], fid])
            else:
                lookup[k] = fid

    for k in collisions:
        lookup.pop(k, None)

    return lookup


def load_cfg_metaobject_defs(ws_cfg_metaobject_defs) -> pd.DataFrame:
    rows = _with_sheets_retry(
        lambda: ws_cfg_metaobject_defs.get_all_records(),
        action="cfg_metaobject_defs.read",
    )
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("Cfg__MetaobjectDefs is empty")

    required = ["type", "field_key"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Cfg__MetaobjectDefs missing required column: {c}")

    for c in ["type", "type_name", "field_key", "field_name", "field_type", "gid"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].map(_norm_str)

    df = df[(df["type"] != "") & (df["field_key"] != "")].copy()
    return df


def build_cfg_metaobject_defs_lookup(
    cfg_metaobject_defs_df: pd.DataFrame,
    entity_type: str = "METAOBJECT_ENTRY",
) -> dict[str, str]:
    """
    Build row-level lookup for metaobject entry fields.

    Key format: normalized metaobject_type + "|" + normalized header candidate.
    Value format: METAOBJECT_ENTRY|mo.{type}.{field_key}

    Example:
      type=card_spec, field_key=icon
      card_spec|icon -> METAOBJECT_ENTRY|mo.card_spec.icon
    """
    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    df = cfg_metaobject_defs_df.copy()
    for _, r in df.iterrows():
        mo_type = _norm_str(r.get("type"))
        field_key = _norm_str(r.get("field_key"))
        if not mo_type or not field_key:
            continue

        fid = f"{entity_type}|mo.{mo_type}.{field_key}"
        candidates = [
            field_key,
            r.get("field_name"),
            f"mo.{mo_type}.{field_key}",
            fid,
        ]

        for cand in candidates:
            ck = _norm_lookup_key(cand)
            if not ck:
                continue
            lk = f"{_norm_lookup_key(mo_type)}|{ck}"
            if lk in lookup and lookup[lk] != fid:
                collisions[lk].update([lookup[lk], fid])
            else:
                lookup[lk] = fid

    for k in collisions:
        lookup.pop(k, None)

    return lookup


def resolve_blank_field_id_row_from_cfg_fields(
    *,
    header_row: list[str],
    field_id_row: list[str],
    cfg_fields_df: pd.DataFrame,
    value_start_col_idx: int = 4,
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[str]:
    """
    Resolve only globally unique/full headers from Cfg__Fields.

    Ambiguous short headers such as icon/label/value are intentionally left blank here.
    They are resolved row-by-row later using entry_type + Cfg__MetaobjectDefs.
    """
    max_cols = max(len(header_row), len(field_id_row))
    headers = header_row + [""] * (max_cols - len(header_row))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    lookup = build_cfg_fields_lookup(cfg_fields_df, entity_type=entity_type)

    for col_idx in range(value_start_col_idx, max_cols):
        existing_fid = _norm_str(field_ids[col_idx])
        if existing_fid:
            continue

        header = _norm_str(headers[col_idx])
        if not header:
            continue

        matched = lookup.get(_norm_lookup_key(header))
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

    h = _norm_str(header)
    et = _norm_str(entry_type)
    if not h or not et:
        return ""

    # First use explicit Cfg__MetaobjectDefs lookup.
    lk = f"{_norm_lookup_key(et)}|{_norm_lookup_key(h)}"
    matched = cfg_metaobject_lookup.get(lk, "")
    if matched:
        return matched

    # Safe deterministic fallback only for simple field keys.
    # This keeps the job usable when Cfg__MetaobjectDefs has not yet synced a newly-created field,
    # while still avoiding display-name guesses with spaces or punctuation.
    simple = _norm_str(h)
    if simple and all(ch.isalnum() or ch == "_" for ch in simple):
        return f"{entity_type}|mo.{et}.{simple}"

    return ""


def validate_resolved_field_ids_for_values(
    *,
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    cfg_metaobject_lookup: dict[str, str],
    value_start_col_idx: int = 4,
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []

    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(r) for _, r in effective_rows), default=0),
    )
    headers = header_row + [""] * (max_cols - len(header_row))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    for sheet_row, row in effective_rows:
        row_pad = row + [""] * (max_cols - len(row))
        entry_type = _norm_str(row_pad[1]) if max_cols > 1 else ""
        for col_idx in range(value_start_col_idx, max_cols):
            value = _norm_str(row_pad[col_idx])
            if value == "":
                continue

            header = _norm_str(headers[col_idx])
            fid = resolve_field_id_for_cell(
                header=header,
                entry_type=entry_type,
                existing_field_id=field_ids[col_idx],
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )
            if not fid:
                errors.append({
                    "entity_type": entity_type,
                    "gid": "",
                    "field_key": header,
                    "error_reason": "unmatched_field_header",
                    "message": (
                        f"sheet_row={sheet_row} | col={col_idx + 1} | "
                        f"entry_type={entry_type} | header={header} | "
                        f"cannot resolve field_id from row2 / Cfg__Fields / Cfg__MetaobjectDefs"
                    ),
                })

    return errors


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
            action="runlog.open_tab",
        )
        _with_sheets_retry(
            lambda: self.ws.update(
                range_name="A1:R1",
                values=[RUNLOG_HEADER],
                value_input_option="RAW",
            ),
            action="runlog.update_header",
            retry_5xx=True,
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

        rows = list(self._buf)

        _with_sheets_retry(
            lambda: self.ws.append_rows(
                rows,
                value_input_option="RAW",
                table_range="A:R",
            ),
            action="runlog.append_rows",
            # Append is not safely repeatable after an ambiguous server 5xx.
            retry_5xx=False,
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
# Core transform
# =========================================================

def _row_has_any_value(values: list[Any]) -> bool:
    return any(_norm_str(v) != "" for v in values)


def _make_header_unique(cols: list[str]) -> list[str]:
    seen = {}
    out = []
    for c in cols:
        base = _norm_str(c)
        if base == "":
            base = "unnamed"
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__dup{seen[base]}")
    return out


def load_wide_sheet(ws_wide) -> tuple[list[list[Any]], list[str], list[str], list[list[Any]]]:
    values = _with_sheets_retry(
        lambda: ws_wide.get_all_values(),
        action="entries_create_wide.read",
    )
    if not values:
        raise ValueError("❌ Wide sheet is empty")
    if len(values) < 2:
        raise ValueError("❌ Wide sheet must have at least 2 rows (header row + optional field_id row)")

    row1 = values[0]
    row2 = values[1]
    if not _row_has_any_value(row1):
        raise ValueError("❌ Wide 第1行为空")

    # Row 2 is allowed to be blank.
    # When blank, run() will resolve field_id from Cfg__Fields using Row 1 headers.
    max_cols = max(len(r) for r in values)
    padded = [r + [""] * (max_cols - len(r)) for r in values]

    header_row = padded[0]
    field_id_row = padded[1]
    data_rows = padded[2:]

    return padded, header_row, field_id_row, data_rows


def find_effective_data_rows(data_rows: list[list[Any]]) -> list[tuple[int, list[Any]]]:
    out = []
    for i, row in enumerate(data_rows, start=3):  # actual sheet row number
        if _row_has_any_value(row):
            out.append((i, row))
    return out


def validate_field_id_row(
    effective_rows: list[tuple[int, list[Any]]],
    field_id_row: list[str],
    value_start_col_idx: int = 4,
) -> list[dict[str, Any]]:
    errors = []

    max_cols = max(len(field_id_row), max((len(r) for _, r in effective_rows), default=0))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    for sheet_row, row in effective_rows:
        row_pad = row + [""] * (max_cols - len(row))
        for col_idx in range(value_start_col_idx, max_cols):
            value = _norm_str(row_pad[col_idx])
            fid = _norm_str(field_ids[col_idx])
            if value != "" and fid == "":
                errors.append({
                    "entity_type": "METAOBJECT_ENTRY",
                    "gid": "",
                    "field_key": "",
                    "error_reason": "missing_field_id",
                    "message": f"sheet_row={sheet_row} | col={col_idx + 1} | value exists but row2 field_id is blank",
                })
    return errors


def transform_wide_to_long(
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    cfg_metaobject_lookup: dict[str, str],
    value_start_col_idx: int = 4,
    entity_type: str = "METAOBJECT_ENTRY",
) -> tuple[pd.DataFrame, int]:
    records = []
    input_value_cells = 0

    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(r) for _, r in effective_rows), default=0),
    )
    headers = header_row + [""] * (max_cols - len(header_row))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    for sheet_row, row in effective_rows:
        row_pad = row + [""] * (max_cols - len(row))

        op = _norm_str(row_pad[0]) if max_cols > 0 else ""
        entry_type = _norm_str(row_pad[1]) if max_cols > 1 else ""
        handle = _norm_str(row_pad[2]) if max_cols > 2 else ""
        mode = _norm_str(row_pad[3]) if max_cols > 3 else ""

        for col_idx in range(value_start_col_idx, max_cols):
            value = _norm_str(row_pad[col_idx])
            if value == "":
                continue

            field_id = resolve_field_id_for_cell(
                header=headers[col_idx],
                entry_type=entry_type,
                existing_field_id=field_ids[col_idx],
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )

            input_value_cells += 1
            records.append({
                "op": op,
                "entry_type": entry_type,
                "handle": handle,
                "mode": mode,
                "field_id": field_id,
                "value": value,
                "slot": "",
                "note": "",
                "_source_sheet_row": sheet_row,
            })

    df_long = pd.DataFrame(records)

    if df_long.empty:
        return pd.DataFrame(columns=["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"]), input_value_cells

    df_long = df_long.drop_duplicates(
        subset=["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"],
        keep="first",
    ).copy()

    df_long = df_long[["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"]].reset_index(drop=True)
    return df_long, input_value_cells


def overwrite_long_sheet(ws_long, df_long: pd.DataFrame):
    header = ["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"]
    values = [header] + df_long.fillna("").astype(str).values.tolist()

    _with_sheets_retry(
        lambda: ws_long.clear(),
        action="entries_create_long.clear",
        retry_5xx=False,
    )

    _with_sheets_retry(
        lambda: ws_long.update(
            range_name="A1:H1",
            values=[header],
            value_input_option="RAW",
        ),
        action="entries_create_long.write_header",
        retry_5xx=True,
    )

    if len(values) > 1:
        _with_sheets_retry(
            lambda: ws_long.update(
                range_name=f"A2:H{len(values)}",
                values=values[1:],
                value_input_option="RAW",
            ),
            action="entries_create_long.write_rows",
            retry_5xx=True,
        )


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = "entries_create_wide_to_long",

    gsheet_sa_b64_secret: str,
    console_core_url: str,

    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Entries_Create-Wide",

    output_sheet_label: str = "pre_edit",
    output_worksheet_title: str = "Entries_Create-Long",

    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",

    cfg_fields_sheet_label: str = "config",
    cfg_fields_worksheet_title: str = "Cfg__Fields",
    cfg_field_match_entity_type: str = "METAOBJECT_ENTRY",

    cfg_metaobject_defs_sheet_label: str = "config",
    cfg_metaobject_defs_worksheet_title: str = "Cfg__MetaobjectDefs",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    run_id: Optional[str] = None,
    detail_max_per_reason: int = 2,
    gsheet_sa_value: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> dict[str, Any]:
    run_id = run_id or _utc_run_id("entries_create_wide_to_long")

    gc = build_gsheet_client(
        gsheet_sa_b64_secret,
        project_code=site_code,
        secret_home=secret_home,
        explicit_value=gsheet_sa_value,
    )

    _, ws_wide, wide_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_long, long_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=output_sheet_label,
        worksheet_title=output_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    runlog_sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=runlog_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_cfg_fields, cfg_fields_sheet_url = open_ws_optional_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_fields_sheet_label,
        worksheet_title=cfg_fields_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )
    cfg_fields_df = load_cfg_fields(ws_cfg_fields)

    _, ws_cfg_metaobject_defs, cfg_metaobject_defs_sheet_url = open_ws_optional_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_metaobject_defs_sheet_label,
        worksheet_title=cfg_metaobject_defs_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )
    cfg_metaobject_defs_df = load_cfg_metaobject_defs(ws_cfg_metaobject_defs)
    cfg_metaobject_lookup = build_cfg_metaobject_defs_lookup(
        cfg_metaobject_defs_df,
        entity_type=cfg_field_match_entity_type,
    )

    logger = RunLogger(
        gc=gc,
        runlog_sheet_url=runlog_sheet_url,
        runlog_tab_name=runlog_tab_name,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
    )

    padded, header_row, field_id_row, data_rows = load_wide_sheet(ws_wide)

    if not _row_has_any_value(field_id_row):
        field_id_row = resolve_blank_field_id_row_from_cfg_fields(
            header_row=header_row,
            field_id_row=field_id_row,
            cfg_fields_df=cfg_fields_df,
            value_start_col_idx=4,
            entity_type=cfg_field_match_entity_type,
        )

    effective_rows = find_effective_data_rows(data_rows)

    rows_loaded = len(effective_rows)
    rows_pending = rows_loaded

    field_id_errors = validate_resolved_field_ids_for_values(
        effective_rows=effective_rows,
        header_row=header_row,
        field_id_row=field_id_row,
        cfg_metaobject_lookup=cfg_metaobject_lookup,
        value_start_col_idx=4,
        entity_type=cfg_field_match_entity_type,
    )

    if field_id_errors:
        rows_recognized = 0
        rows_planned = 0
        rows_written = 0
        rows_skipped = len(field_id_errors)

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
            message=f"Field_id validation failed | errors={len(field_id_errors)}",
            error_reason="unmatched_field_header",
        )
        log_grouped_details(
            logger,
            phase="transform",
            status="FAIL",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            detail_rows=field_id_errors,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "ERROR",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": 0,
                "rows_planned": 0,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
                "error_count": len(field_id_errors),
            },
            "warnings": [
                {
                    "type": "unmatched_field_header",
                    "count": len(field_id_errors),
                    "examples": field_id_errors[: min(10, len(field_id_errors))],
                }
            ],
            "preview": [],
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "wide_sheet_url": wide_sheet_url,
                "long_sheet_url": long_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "cfg_fields_sheet_url": cfg_fields_sheet_url,
                "cfg_metaobject_defs_sheet_url": cfg_metaobject_defs_sheet_url,
            },
        }

    df_long, input_value_cells = transform_wide_to_long(
        effective_rows=effective_rows,
        header_row=header_row,
        field_id_row=field_id_row,
        cfg_metaobject_lookup=cfg_metaobject_lookup,
        value_start_col_idx=4,
        entity_type=cfg_field_match_entity_type,
    )

    rows_recognized = rows_loaded
    rows_planned = len(df_long)
    rows_written = len(df_long)
    rows_skipped = max(0, input_value_cells - len(df_long))

    overwrite_long_sheet(ws_long, df_long)

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
            f"Wide to Long completed | "
            f"rows_loaded={rows_loaded} | "
            f"input_value_cells={input_value_cells} | "
            f"rows_written={rows_written} | "
            f"dedup_skipped={rows_skipped}"
        ),
        error_reason="",
    )
    logger.flush()

    preview = df_long.head(20).to_dict("records") if not df_long.empty else []

    return {
        "status": "SUCCESS",
        "summary": {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_pending,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_written": rows_written,
            "rows_skipped": rows_skipped,
            "error_count": 0,
        },
        "warnings": [],
        "preview": preview,
        "meta": {
            "site_code": site_code,
            "job_name": job_name,
            "run_id": run_id,
            "wide_sheet_url": wide_sheet_url,
            "long_sheet_url": long_sheet_url,
            "runlog_sheet_url": runlog_sheet_url,
        },
    }
