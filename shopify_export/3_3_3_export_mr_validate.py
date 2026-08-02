import base64
import random
import re
import json
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials


MODULE_PATH = "shopify_export.3_3_3_export_mr_validate"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
DEFAULT_JOB_NAME = "export_mr_validate"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_WS_CFG_SITES = "Cfg__Sites"
DEFAULT_WS_CFG_FIELDS = "Cfg__Fields"
DEFAULT_WS_EXPORT = "MR-Validate"
DEFAULT_WS_RUNLOG = "Ops__RunLog"


def _now_cn_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _norm(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _norm_lower(x: Any) -> str:
    return _norm(x).lower()


def _is_blank(x: Any) -> bool:
    return _norm(x) == ""


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {_norm_lower(c): c for c in df.columns}
    for c in candidates:
        if _norm_lower(c) in cols:
            return cols[_norm_lower(c)]
    return None


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_norm(c) for c in out.columns]
    return out



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
    name: str,
    *,
    project_code: str,
    explicit_value: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> SecretValue:
    """Resolve one Secret without printing its value."""
    secret_name = _norm(name)
    resolved_project_code = _normalize_project_code(project_code)

    if not secret_name:
        raise ValueError("Secret name is empty.")
    if not resolved_project_code:
        raise ValueError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _norm(explicit_value):
        return SecretValue(
            value=_norm(explicit_value),
            source_type="EXPLICIT_VALUE",
            source_detail="caller",
        )

    if _runtime_mode() == "COLAB":
        try:
            from google.colab import userdata  # type: ignore
        except Exception as exc:
            raise RuntimeError("Colab Secret adapter is unavailable.") from exc

        value = userdata.get(secret_name)
        if value is None or not str(value).strip():
            raise ValueError(
                f"Colab Secret {secret_name!r} is missing "
                "or not enabled for this notebook."
            )

        return SecretValue(
            value=str(value).strip(),
            source_type="COLAB_SECRETS",
            source_detail=secret_name,
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

    aliases: Tuple[str, ...] = ()
    normalized_secret_name = secret_name.upper()

    for suffix in (
        "_GSHEET",
        "_SHOPIFY_ACCESS_TOKEN",
        "_SHOPIFY_TOKEN",
    ):
        if normalized_secret_name.endswith(suffix):
            canonical_name = f"{resolved_project_code}{suffix}"
            if canonical_name != secret_name:
                aliases = (canonical_name,)
            break

    result = resolver.read(
        secret_name,
        aliases=aliases,
    )
    return _workspace_secret_result_to_value(result)


def _parse_service_account_text(
    raw_value: str,
) -> Tuple[Dict[str, Any], str]:
    raw = _norm(raw_value)
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
                "Google service-account Secret is neither "
                "valid raw JSON nor Base64 JSON."
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


def build_gspread_client_from_value(
    raw_value: str,
) -> gspread.Client:
    info, _secret_format = _parse_service_account_text(raw_value)
    creds = Credentials.from_service_account_info(
        info,
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

    if retry_5xx and status in {500, 502, 503, 504}:
        return True

    err_text = str(exc).lower()
    quota_tokens = (
        "resource_exhausted",
        "ratelimitexceeded",
        "userratelimitexceeded",
        "rate limit exceeded",
        "quota exceeded",
        "read requests per minute",
        "write requests per minute",
        "too many requests",
    )
    return any(token in err_text for token in quota_tokens)


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


def _normalize_registry_header(value: Any) -> str:
    return re.sub(
        r"[\s_]+",
        " ",
        _norm(value).lower(),
    ).strip()


def _extract_spreadsheet_id(value: Any) -> str:
    raw = _norm(value)

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
) -> Dict[str, str]:
    resolved_project_code = _normalize_project_code(project_code)

    if not resolved_project_code:
        raise ValueError("project_code is required.")

    workspace_secret = read_secret(
        workspace_gsheet_secret_name,
        project_code="WORKSPACE",
        secret_home=secret_home,
    )
    workspace_gc = build_gspread_client_from_value(
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
            f"Workspace Project Registry tab "
            f"{workspace_registry_tab!r} is empty."
        )

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

    project_col = require_column(
        "project_code",
        "project code",
    )
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
    timezone_col = require_column(
        "timezone",
        "time zone",
    )

    project_name_col = header_map.get(
        _normalize_registry_header("project_name")
    )

    width = len(values[0])
    matches: List[Tuple[int, List[Any]]] = []

    for row_number, raw_row in enumerate(
        values[1:],
        start=2,
    ):
        row = list(raw_row) + [""] * max(
            0,
            width - len(raw_row),
        )

        if (
            _norm(row[project_col]).upper()
            == resolved_project_code
        ):
            matches.append((row_number, row))

    if not matches:
        raise ValueError(
            "Workspace Project Registry has no row "
            f"for project_code={resolved_project_code}."
        )

    if len(matches) > 1:
        raise ValueError(
            "Workspace Project Registry has duplicate project rows: "
            f"project_code={resolved_project_code}; "
            f"rows={[row_number for row_number, _ in matches]}."
        )

    source_row, row = matches[0]
    active_text = _norm(row[active_col]).lower()

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
            _norm(row[project_name_col])
            if project_name_col is not None
            else ""
        ),
        "console_core_url": _norm(row[console_url_col]),
        "gsheet_secret_name": _norm(row[gsheet_secret_col]),
        "account_config_tab": _norm(row[account_tab_col]),
        "timezone": _norm(row[timezone_col]),
        "registry_source_row": str(source_row),
        "workspace_auth_source_type": workspace_secret.source_type,
    }

    missing = [
        key
        for key in (
            "console_core_url",
            "gsheet_secret_name",
            "account_config_tab",
            "timezone",
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
            f"account_tab={route['account_config_tab']} | "
            f"timezone={route['timezone']}"
        )

    return route


def _read_account_config(
    gc: gspread.Client,
    console_core_url: str,
    account_config_tab: str,
) -> Dict[str, str]:
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="account_config.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(account_config_tab),
        action="account_config.open_tab",
    )
    values = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="account_config.read",
    )

    if not values:
        raise ValueError(f"{account_config_tab} is empty")

    rows = [
        [_norm(cell) for cell in row]
        for row in values
    ]

    first = (rows[0] + ["", ""])[:2]
    first_lower = [value.lower() for value in first]

    header_key_names = {
        "key",
        "config_key",
        "field",
        "name",
        "setting",
    }
    header_val_names = {
        "value",
        "config_value",
        "val",
    }

    has_header = (
        first_lower[0] in header_key_names
        and first_lower[1] in header_val_names
    )

    data_rows = rows[1:] if has_header else rows
    out: Dict[str, str] = {}

    for row in data_rows:
        if not row:
            continue

        key = _norm(row[0]) if len(row) >= 1 else ""
        value = _norm(row[1]) if len(row) >= 2 else ""

        if key:
            out[key] = value

    if not out:
        raise ValueError(
            f"{account_config_tab} has no key/value rows"
        )

    return out


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

    gc = build_gspread_client_from_value(
        project_google_secret.value
    )

    account = _read_account_config(
        gc,
        route["console_core_url"],
        route["account_config_tab"],
    )

    required = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "GSHEET_SA_B64_SECRET",
        "SHOPIFY_TOKEN_SECRET",
    ]

    missing = [
        key
        for key in required
        if not _norm(account.get(key))
    ]

    if missing:
        raise ValueError(
            f"{route['account_config_tab']} missing "
            f"required account config keys: {missing}"
        )

    if (
        _norm(account["GSHEET_SA_B64_SECRET"])
        != route["gsheet_secret_name"]
    ):
        raise ValueError(
            "Workspace Registry Google Secret does not match "
            f"{route['account_config_tab']}.GSHEET_SA_B64_SECRET: "
            f"workspace={route['gsheet_secret_name']}; "
            f"account={account['GSHEET_SA_B64_SECRET']}"
        )

    shopify_secret = read_secret(
        account["SHOPIFY_TOKEN_SECRET"],
        project_code=route["project_code"],
        secret_home=secret_home,
    )

    result = {
        "project_route": route,
        "account": {
            "shop_domain": _norm(account["SHOP_DOMAIN"]),
            "api_version": _norm(account["SHOPIFY_API_VERSION"]),
            "gsheet_secret_name": _norm(
                account["GSHEET_SA_B64_SECRET"]
            ),
            "shopify_token_secret_name": _norm(
                account["SHOPIFY_TOKEN_SECRET"]
            ),
        },
        "credentials": {
            "gsheet_sa_value": project_google_secret.value,
            "shopify_access_token": shopify_secret.value,
        },
        "auth": {
            "runtime_mode": _runtime_mode(),
            "project_google_secret_source_type": (
                project_google_secret.source_type
            ),
            "shopify_secret_source_type": (
                shopify_secret.source_type
            ),
        },
        "gspread_client": gc,
    }

    if print_progress:
        print(
            "[Runtime Auth] ready | "
            f"project={route['project_code']} | "
            "google_source="
            f"{project_google_secret.source_type} | "
            "shopify_source="
            f"{shopify_secret.source_type} | "
            f"shop={result['account']['shop_domain']} | "
            f"api={result['account']['api_version']}"
        )

    return result


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
        and not _norm(current_colab_url)
    ):
        raise ValueError(
            f"registry_mode={mode} requires current_colab_url."
        )

    if (
        mode == "UPDATE_URL_AND_NAME"
        and not _norm(current_colab_name)
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
    gc = build_gspread_client_from_value(
        secret.value
    )

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

    header_map: Dict[str, int] = {}

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
        _norm(job_name).lower(),
        _norm(sheet_label).lower(),
        _norm(tab_name).lower(),
    )

    matches: List[int] = []

    for row_index, row in enumerate(
        values[1:],
        start=2,
    ):
        padded = list(row) + [""] * max(
            0,
            len(values[0]) - len(row),
        )

        logical_key = (
            _norm(padded[job_col]).lower(),
            _norm(padded[label_col]).lower(),
            _norm(padded[tab_col]).lower(),
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
    current_row = (
        values[row_number - 1]
        + [""] * max(
            0,
            len(values[0]) - len(values[row_number - 1]),
        )
    )

    changes: List[Tuple[str, int, str, str]] = []
    provided_url = _norm(current_colab_url)
    provided_name = _norm(current_colab_name)

    if (
        provided_url
        and _norm(current_row[url_col]) != provided_url
    ):
        changes.append(
            (
                "colab_url",
                url_col + 1,
                _norm(current_row[url_col]),
                provided_url,
            )
        )

    if (
        provided_name
        and _norm(current_row[name_col]) != provided_name
    ):
        changes.append(
            (
                "colab_name",
                name_col + 1,
                _norm(current_row[name_col]),
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

        for (
            field_name,
            column_number,
            _old_value,
            new_value,
        ) in applied:
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


def _worksheet_df(ws) -> pd.DataFrame:
    rows = _with_sheets_retry(
        lambda: ws.get_all_records(default_blank=""),
        action=f"worksheet.read_records:{ws.title}",
    )
    return pd.DataFrame(rows)


def _open_ws_by_url_and_title(gc, spreadsheet_url: str, worksheet_title: str):
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(spreadsheet_url),
        action=f"spreadsheet.open:{worksheet_title}",
    )
    return _with_sheets_retry(
        lambda: sh.worksheet(worksheet_title),
        action=f"worksheet.open:{worksheet_title}",
    )


def _open_ss_by_url(gc, spreadsheet_url: str):
    return _with_sheets_retry(
        lambda: gc.open_by_url(spreadsheet_url),
        action="spreadsheet.open_by_url",
    )


def _write_df_to_ws(ws, df: pd.DataFrame, clear_first: bool = True):
    data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()

    if clear_first:
        _with_sheets_retry(
            lambda: ws.clear(),
            action=f"worksheet.clear:{ws.title}",
            retry_5xx=True,
        )
        time.sleep(1)

        if data:
            _with_sheets_retry(
                lambda: ws.update(
                    range_name="A1",
                    values=data,
                    value_input_option="USER_ENTERED",
                ),
                action=f"worksheet.write_full:{ws.title}",
                retry_5xx=True,
            )
        return

    existing = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action=f"worksheet.read_append_start:{ws.title}",
    )
    start_row = len(existing) + 1
    payload = data if start_row == 1 else data[1:]

    if payload:
        _with_sheets_retry(
            lambda: ws.update(
                range_name=f"A{start_row}",
                values=payload,
                value_input_option="USER_ENTERED",
            ),
            action=f"worksheet.append_range:{ws.title}",
            retry_5xx=True,
        )


def _ensure_runlog_headers(ws, headers: List[str]):
    cur = _with_sheets_retry(
        lambda: ws.row_values(1),
        action="runlog.read_header",
    )
    cur_norm = [_norm(x) for x in cur]

    if cur_norm == headers:
        return

    if not cur:
        _with_sheets_retry(
            lambda: ws.update(
                range_name="A1",
                values=[headers],
                value_input_option="USER_ENTERED",
            ),
            action="runlog.write_header",
            retry_5xx=True,
        )


def _append_runlog_rows(ws, rows: List[Dict[str, Any]], headers: List[str]):
    _ensure_runlog_headers(ws, headers)

    values = [
        [_norm(r.get(h, "")) for h in headers]
        for r in rows
    ]

    if not values:
        return

    existing = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="runlog.read_append_start",
    )
    start = len(existing) + 1

    _with_sheets_retry(
        lambda: ws.update(
            range_name=f"A{start}",
            values=values,
            value_input_option="USER_ENTERED",
        ),
        action="runlog.write_rows",
        retry_5xx=True,
    )


def _runlog_headers() -> List[str]:
    return [
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


def _site_targets(df_sites: pd.DataFrame, site_code: str) -> Dict[str, str]:
    df = _normalize_headers(df_sites)

    col_site = _pick_col(df, ["site_code", "site", "code"])
    col_label = _pick_col(df, ["label"])
    col_sheet_url = _pick_col(df, ["sheet_url"])

    if not col_site or not col_label or not col_sheet_url:
        raise RuntimeError("Cfg__Sites 缺少必要字段：site_code / label / sheet_url")

    dfx = df[df[col_site].astype(str).str.strip().str.lower() == site_code.strip().lower()].copy()
    if dfx.empty:
        raise RuntimeError(f"Cfg__Sites 找不到站点：{site_code}")

    targets = {}
    for _, r in dfx.iterrows():
        label = _norm(r[col_label])
        sheet_url = _norm(r[col_sheet_url])
        if label and sheet_url:
            targets[label] = sheet_url

    need = ["config", "export_other", "runlog_sheet"]
    miss = [x for x in need if _is_blank(targets.get(x))]
    if miss:
        raise RuntimeError(f"Cfg__Sites 缺少这些 label：{miss}")

    return {
        "config_url": targets["config"],
        "export_other_url": targets["export_other"],
        "runlog_url": targets["runlog_sheet"],
    }


def _get_shop_domain(df_sites: pd.DataFrame, site_code: str, fallback_shop_domain: str = "") -> str:
    if _norm(fallback_shop_domain):
        return _norm(fallback_shop_domain)

    df = _normalize_headers(df_sites)
    col_site = _pick_col(df, ["site_code", "site", "code"])
    col_domain = _pick_col(df, ["shop_domain", "shopify_domain", "myshopify_domain", "shop", "domain"])

    if not col_site or not col_domain:
        raise RuntimeError("Cfg__Sites 缺少 shop_domain / myshopify_domain 之类字段，且未传入 shop_domain")

    dfx = df[df[col_site].astype(str).str.strip().str.lower() == site_code.strip().lower()].copy()
    if dfx.empty:
        raise RuntimeError(f"Cfg__Sites 找不到站点：{site_code}")

    vals = [_norm(x) for x in dfx[col_domain].tolist() if _norm(x)]
    if not vals:
        raise RuntimeError(f"Cfg__Sites 中站点 {site_code} 没有 shop_domain")

    domain = vals[0]
    if ".myshopify.com" not in domain:
        domain = f"{domain}.myshopify.com"
    return domain


def _parse_metafield_key(metafield_key: str) -> Tuple[str, str]:
    s = _norm(metafield_key)
    if s.startswith("mf."):
        s = s[3:]
    if s.startswith("v_mf."):
        s = s[5:]
    parts = s.split(".", 1)
    if len(parts) != 2:
        raise RuntimeError(f"METAFIELD_KEY 格式不对：{metafield_key}")
    return parts[0], parts[1]


def _get_cfg_field_meta(df_fields: pd.DataFrame, metafield_key: str) -> Dict[str, Any]:
    df = _normalize_headers(df_fields)

    col_entity_type = _pick_col(df, ["entity_type"])
    col_field_key = _pick_col(df, ["field_key"])
    col_field_type = _pick_col(df, ["field_type"])
    col_data_type = _pick_col(df, ["data_type"])
    col_namespace = _pick_col(df, ["namespace"])
    col_key = _pick_col(df, ["key"])

    if not col_entity_type or not col_field_key:
        raise RuntimeError("Cfg__Fields 缺少必要字段：entity_type / field_key")

    target = _norm_lower(metafield_key)
    dfx = df[df[col_field_key].astype(str).str.strip().str.lower() == target].copy()

    if dfx.empty:
        tail = target.replace("mf.", "").replace("v_mf.", "")
        dfx = df[df[col_field_key].astype(str).str.strip().str.lower().str.endswith(tail)].copy()

    if dfx.empty:
        raise RuntimeError(f"Cfg__Fields 找不到 field_key={metafield_key}")

    entity_types = []
    for x in dfx[col_entity_type].astype(str).tolist():
        s = _norm(x).upper()
        if s and s not in entity_types:
            entity_types.append(s)

    field_type = ""
    if col_field_type:
        vals = [_norm(x) for x in dfx[col_field_type].tolist() if _norm(x)]
        if vals:
            field_type = vals[0]

    # Some existing Cfg__Fields rows use field_type=RAW and data_type=metaobject_reference.
    # For MR validation we need the Shopify metafield type, so data_type is a safe fallback
    # when it is the column that actually contains the reference type.
    data_type = ""
    if col_data_type:
        vals = [_norm(x) for x in dfx[col_data_type].tolist() if _norm(x)]
        if vals:
            data_type = vals[0]

    if "reference" not in _norm_lower(field_type) and "reference" in _norm_lower(data_type):
        field_type = data_type

    namespace = ""
    key = ""
    if col_namespace:
        vals = [_norm(x) for x in dfx[col_namespace].tolist() if _norm(x)]
        if vals:
            namespace = vals[0]
    if col_key:
        vals = [_norm(x) for x in dfx[col_key].tolist() if _norm(x)]
        if vals:
            key = vals[0]

    if not namespace or not key:
        parsed = _parse_metafield_key(metafield_key)
        namespace = namespace or parsed[0]
        key = key or parsed[1]

    return {
        "entity_types": entity_types,
        "field_type": field_type,
        "namespace": namespace,
        "key": key,
    }


def _shopify_graphql(
    shop_domain: str,
    api_version: str,
    token: str,
    query: str,
    variables: Optional[dict] = None,
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
        timeout=120,
    )
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Shopify 返回非 JSON：HTTP {resp.status_code} / {resp.text[:500]}")

    if resp.status_code != 200:
        raise RuntimeError(f"Shopify GraphQL HTTP {resp.status_code}：{json.dumps(data, ensure_ascii=False)[:1000]}")

    if data.get("errors"):
        raise RuntimeError(f"Shopify GraphQL errors：{json.dumps(data['errors'], ensure_ascii=False)[:1200]}")

    return data["data"]


PRODUCTS_QUERY = """
query MRValidateProducts($first: Int!, $after: String, $namespace: String!, $key: String!) {
  products(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        title
        handle
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

VARIANTS_QUERY = """
query MRValidateVariants($first: Int!, $after: String, $namespace: String!, $key: String!) {
  productVariants(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        sku
        title
        product {
          id
          legacyResourceId
          title
          handle
        }
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

COLLECTIONS_QUERY = """
query MRValidateCollections($first: Int!, $after: String, $namespace: String!, $key: String!) {
  collections(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        title
        handle
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

PAGES_QUERY = """
query MRValidatePages($first: Int!, $after: String, $namespace: String!, $key: String!) {
  pages(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        title
        handle
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

NODES_QUERY = """
query MRValidateNodes($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Metaobject {
      id
      type
      handle
      displayName
      updatedAt
      capabilities {
        publishable {
          status
        }
      }
      fields {
        key
        type
        value
      }
    }
  }
}
"""


def _iter_connection(
    shop_domain: str,
    api_version: str,
    token: str,
    query: str,
    root_key: str,
    namespace: str,
    key: str,
    page_size: int,
) -> Iterable[Dict[str, Any]]:
    after = None
    while True:
        data = _shopify_graphql(
            shop_domain=shop_domain,
            api_version=api_version,
            token=token,
            query=query,
            variables={
                "first": page_size,
                "after": after,
                "namespace": namespace,
                "key": key,
            },
        )
        blk = data[root_key]
        edges = blk["edges"] or []
        for edge in edges:
            yield edge["node"]

        if not blk["pageInfo"]["hasNextPage"]:
            break
        after = blk["pageInfo"]["endCursor"]


def _parse_reference_value(raw_value: Any) -> List[str]:
    s = _norm(raw_value)
    if not s:
        return []

    ids: List[str] = []

    try:
        j = json.loads(s)
        if isinstance(j, list):
            for x in j:
                v = _norm(x)
                if v.startswith("gid://shopify/Metaobject/"):
                    ids.append(v)
        elif isinstance(j, str) and j.startswith("gid://shopify/Metaobject/"):
            ids.append(j)
    except Exception:
        pass

    if ids:
        return ids

    found = re.findall(r"gid://shopify/Metaobject/\d+", s)
    if found:
        return found

    if s.startswith("gid://shopify/Metaobject/"):
        return [s]

    return []


def _chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _fetch_metaobject_map(
    shop_domain: str,
    api_version: str,
    token: str,
    entry_ids: List[str],
    batch_size: int,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for batch in _chunked(entry_ids, batch_size):
        data = _shopify_graphql(
            shop_domain=shop_domain,
            api_version=api_version,
            token=token,
            query=NODES_QUERY,
            variables={"ids": batch},
        )
        for node in data["nodes"] or []:
            if not node:
                continue
            out[_norm(node.get("id"))] = node
    return out


def _preview_pairs(metaobject_node: Optional[Dict[str, Any]], limit: int = 2) -> List[str]:
    if not metaobject_node:
        return ["", ""]

    vals = []
    for f in metaobject_node.get("fields") or []:
        key = _norm(f.get("key"))
        value = _norm(f.get("value"))
        if key and value:
            vals.append(f"{key}: {value}")

    vals = vals[:limit]
    while len(vals) < limit:
        vals.append("")
    return vals


def _entry_status(node: Optional[Dict[str, Any]]) -> str:
    if not node:
        return "MISSING"
    status = _norm((((node.get("capabilities") or {}).get("publishable") or {}).get("status")))
    return status or "ACTIVE"


def _validate_message(raw_value: Any, raw_ids: List[str], node: Optional[Dict[str, Any]], field_type: str, ref_mode: str) -> str:
    if _is_blank(raw_value):
        return "raw value is empty"
    if "reference" not in _norm_lower(field_type):
        return f"field_type not reference: {field_type}"
    if not raw_ids:
        return "raw value has no resolvable metaobject gid"
    if not node:
        return "target entry not found"
    if ref_mode == "single" and len(raw_ids) > 1:
        return "single reference but raw value contains multiple gids"
    return "OK"


def _base_row_columns() -> List[str]:
    return [
        "from_entity_type",
        "from_gid",
        "from_handle",
        "from_title",
        "from_variant_sku",
        "field_key",
        "field_type",
        "ref_mode",
        "raw_value_count",
        "entry_order",
        "to_entry_gid",
        "to_entry_handle",
        "to_entry_type",
        "to_entry_display",
        "entry_exists",
        "to_entry_status",
        "to_entry_preview_1",
        "to_entry_preview_2",
        "to_entry_updated_at",
        "to_entry_synced_at",
        "raw_value",
        "validate_message",
    ]


def _rows_from_owner(
    owner_entity_type: str,
    node: Dict[str, Any],
    metafield_key: str,
    field_type: str,
    synced_at: str,
) -> List[Dict[str, Any]]:
    mf = node.get("metafield") or {}
    raw_value = _norm(mf.get("value"))
    raw_ids = _parse_reference_value(raw_value)
    ref_mode = "list" if "list." in _norm_lower(field_type) else "single"

    from_gid = _norm(node.get("id"))
    from_handle = _norm(node.get("handle"))
    from_title = _norm(node.get("title"))
    from_variant_sku = ""

    if owner_entity_type == "VARIANT":
        product = node.get("product") or {}
        from_handle = _norm(product.get("handle"))
        from_title = _norm(product.get("title")) or _norm(node.get("title"))
        from_variant_sku = _norm(node.get("sku"))

    if not raw_ids:
        return [{
            "from_entity_type": owner_entity_type,
            "from_gid": from_gid,
            "from_handle": from_handle,
            "from_title": from_title,
            "from_variant_sku": from_variant_sku,
            "field_key": metafield_key,
            "field_type": field_type,
            "ref_mode": ref_mode,
            "raw_value_count": 0,
            "entry_order": "",
            "to_entry_gid": "",
            "to_entry_handle": "",
            "to_entry_type": "",
            "to_entry_display": "",
            "entry_exists": "FALSE",
            "to_entry_status": "EMPTY",
            "to_entry_preview_1": "",
            "to_entry_preview_2": "",
            "to_entry_updated_at": "",
            "to_entry_synced_at": synced_at,
            "raw_value": raw_value,
            "validate_message": _validate_message(raw_value, raw_ids, None, field_type, ref_mode),
        }]

    rows = []
    for idx, entry_gid in enumerate(raw_ids, start=1):
        rows.append({
            "from_entity_type": owner_entity_type,
            "from_gid": from_gid,
            "from_handle": from_handle,
            "from_title": from_title,
            "from_variant_sku": from_variant_sku,
            "field_key": metafield_key,
            "field_type": field_type,
            "ref_mode": ref_mode,
            "raw_value_count": len(raw_ids),
            "entry_order": idx,
            "to_entry_gid": entry_gid,
            "to_entry_handle": "",
            "to_entry_type": "",
            "to_entry_display": "",
            "entry_exists": "",
            "to_entry_status": "",
            "to_entry_preview_1": "",
            "to_entry_preview_2": "",
            "to_entry_updated_at": "",
            "to_entry_synced_at": synced_at,
            "raw_value": raw_value,
            "validate_message": "",
        })
    return rows


def _fill_target_info(rows: List[Dict[str, Any]], meta_map: Dict[str, Dict[str, Any]], field_type: str):
    for row in rows:
        gid = _norm(row.get("to_entry_gid"))
        node = meta_map.get(gid)

        previews = _preview_pairs(node, limit=2)
        row["to_entry_handle"] = _norm(node.get("handle")) if node else ""
        row["to_entry_type"] = _norm(node.get("type")) if node else ""
        row["to_entry_display"] = _norm(node.get("displayName")) if node else ""
        row["entry_exists"] = "TRUE" if node else "FALSE"
        row["to_entry_status"] = _entry_status(node)
        row["to_entry_preview_1"] = previews[0]
        row["to_entry_preview_2"] = previews[1]
        row["to_entry_updated_at"] = _norm(node.get("updatedAt")) if node else ""

        raw_ids = _parse_reference_value(row.get("raw_value"))
        row["validate_message"] = _validate_message(
            row.get("raw_value"),
            raw_ids,
            node,
            field_type,
            row.get("ref_mode"),
        )


def run(
    *,
    gc,
    shopify_token: str,
    site_code: str,
    console_core_url: str,
    shop_domain: str = "",
    api_version: str = "2026-01",
    metafield_key: str,
    owner_entity_types: Optional[List[str]] = None,
    ws_cfg_sites: str = DEFAULT_WS_CFG_SITES,
    ws_cfg_fields: str = DEFAULT_WS_CFG_FIELDS,
    ws_export: str = DEFAULT_WS_EXPORT,
    ws_runlog: str = DEFAULT_WS_RUNLOG,
    overwrite_export_sheet: bool = True,
    product_page_size: int = 100,
    variant_page_size: int = 100,
    collection_page_size: int = 100,
    page_page_size: int = 100,
    metaobject_batch_size: int = 100,
    preview_rows: int = 30,
    job_name: str = "export_mr_validate",
) -> Dict[str, Any]:
    started_at = _now_cn_str()
    synced_at = started_at
    status = "SUCCESS"
    error_reason = ""
    message = ""
    warnings: List[str] = []

    rows_loaded = 0
    rows_recognized = 0
    rows_planned = 0
    rows_written = 0
    rows_skipped = 0

    all_rows: List[Dict[str, Any]] = []
    df_out = pd.DataFrame(columns=_base_row_columns())
    effective_entity_types: List[str] = []
    targets: Dict[str, str] = {}

    try:
        ws_sites = _open_ws_by_url_and_title(gc, console_core_url, ws_cfg_sites)
        df_sites = _worksheet_df(ws_sites)
        if df_sites.empty:
            raise RuntimeError("Cfg__Sites 为空")

        targets = _site_targets(df_sites, site_code)

        ws_fields = _open_ws_by_url_and_title(gc, targets["config_url"], ws_cfg_fields)
        df_fields = _worksheet_df(ws_fields)
        if df_fields.empty:
            raise RuntimeError("Cfg__Fields 为空")

        cfg = _get_cfg_field_meta(df_fields, metafield_key=metafield_key)
        namespace = cfg["namespace"]
        key = cfg["key"]
        field_type = cfg["field_type"] or "unknown"

        effective_entity_types = [x.upper() for x in (owner_entity_types or cfg["entity_types"])]
        effective_entity_types = [x for x in effective_entity_types if x in {"PRODUCT", "VARIANT", "COLLECTION", "PAGE"}]
        if not effective_entity_types:
            raise RuntimeError(f"Cfg__Fields 无法识别 {metafield_key} 的 owner entity type")

        real_shop_domain = _get_shop_domain(df_sites, site_code=site_code, fallback_shop_domain=shop_domain)

        scans = {
            "PRODUCT": (PRODUCTS_QUERY, "products", product_page_size),
            "VARIANT": (VARIANTS_QUERY, "productVariants", variant_page_size),
            "COLLECTION": (COLLECTIONS_QUERY, "collections", collection_page_size),
            "PAGE": (PAGES_QUERY, "pages", page_page_size),
        }

        for entity_type in effective_entity_types:
            query, root_key, page_size = scans[entity_type]
            for node in _iter_connection(
                shop_domain=real_shop_domain,
                api_version=api_version,
                token=shopify_token,
                query=query,
                root_key=root_key,
                namespace=namespace,
                key=key,
                page_size=page_size,
            ):
                rows_loaded += 1
                mf = node.get("metafield")
                if not mf:
                    continue

                rows = _rows_from_owner(
                    owner_entity_type=entity_type,
                    node=node,
                    metafield_key=metafield_key,
                    field_type=field_type,
                    synced_at=synced_at,
                )
                if rows:
                    rows_recognized += 1
                    all_rows.extend(rows)

        rows_planned = len(all_rows)

        target_ids = []
        seen = set()
        for r in all_rows:
            gid = _norm(r.get("to_entry_gid"))
            if gid and gid not in seen:
                seen.add(gid)
                target_ids.append(gid)

        meta_map = {}
        if target_ids:
            meta_map = _fetch_metaobject_map(
                shop_domain=real_shop_domain,
                api_version=api_version,
                token=shopify_token,
                entry_ids=target_ids,
                batch_size=metaobject_batch_size,
            )

        _fill_target_info(all_rows, meta_map=meta_map, field_type=field_type)

        df_out = pd.DataFrame(all_rows, columns=_base_row_columns()).fillna("")

        ws_export_obj = _open_ws_by_url_and_title(gc, targets["export_other_url"], ws_export)
        _write_df_to_ws(ws_export_obj, df_out, clear_first=overwrite_export_sheet)
        rows_written = len(df_out)

        bad_count = int((df_out["validate_message"] != "OK").sum()) if not df_out.empty else 0
        if bad_count:
            warnings.append(f"发现 {bad_count} 行 validate_message != OK")

        if "reference" not in _norm_lower(field_type):
            warnings.append(f"Cfg__Fields.field_type={field_type}，不像 reference 字段，请检查")

        if not df_out.empty:
            vc = df_out["validate_message"].value_counts(dropna=False).to_dict()
            for k, v in vc.items():
                ks = _norm(k)
                if ks and ks != "OK":
                    warnings.append(f"{ks}: {v}")

        message = f"导出完成：owners_scanned={rows_loaded}, owners_hit={rows_recognized}, rows={rows_written}"

    except Exception as e:
        status = "FAILED"
        error_reason = type(e).__name__
        message = f"{type(e).__name__}: {e}"
        warnings.append(message)
        traceback.print_exc()

    try:
        if targets:
            ws_runlog_obj = _open_ws_by_url_and_title(gc, targets["runlog_url"], ws_runlog)
            run_id = f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            log_rows = [{
                "run_id": run_id,
                "ts_cn": _now_cn_str(),
                "job_name": job_name,
                "phase": "preview",
                "log_type": "summary",
                "status": status,
                "site_code": site_code,
                "entity_type": ",".join(effective_entity_types),
                "gid": "",
                "field_key": metafield_key,
                "rows_loaded": rows_loaded,
                "rows_pending": 0,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_written": rows_written,
                "rows_skipped": rows_skipped,
                "message": message,
                "error_reason": error_reason,
            }]

            detail_counts: Dict[str, int] = {}
            if not df_out.empty and "validate_message" in df_out.columns:
                bad = df_out[df_out["validate_message"].astype(str) != "OK"].copy()
                for _, r in bad.iterrows():
                    reason = _norm(r["validate_message"])
                    if not reason:
                        continue

                    cnt = detail_counts.get(reason, 0)
                    if cnt >= 2:
                        continue
                    detail_counts[reason] = cnt + 1

                    log_rows.append({
                        "run_id": run_id,
                        "ts_cn": _now_cn_str(),
                        "job_name": job_name,
                        "phase": "preview",
                        "log_type": "detail",
                        "status": "WARN" if status == "SUCCESS" else status,
                        "site_code": site_code,
                        "entity_type": _norm(r.get("from_entity_type")),
                        "gid": _norm(r.get("from_gid")),
                        "field_key": metafield_key,
                        "rows_loaded": rows_loaded,
                        "rows_pending": 0,
                        "rows_recognized": rows_recognized,
                        "rows_planned": rows_planned,
                        "rows_written": rows_written,
                        "rows_skipped": rows_skipped,
                        "message": f"to_entry_gid={_norm(r.get('to_entry_gid'))}",
                        "error_reason": reason,
                    })

            _append_runlog_rows(ws_runlog_obj, log_rows, _runlog_headers())

    except Exception as e:
        warnings.append(f"RunLog 写入失败：{type(e).__name__}: {e}")

    return {
        "status": status,
        "site_code": site_code,
        "job_name": job_name,
        "metafield_key": metafield_key,
        "rows_exported": rows_written,
        "summary": {
            "status": status,
            "site_code": site_code,
            "job_name": job_name,
            "rows_loaded": rows_loaded,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_written": rows_written,
            "message": message,
        },
        "preview": df_out.head(preview_rows).copy(),
        "warnings": warnings,
        "dataframe": df_out,
    }
