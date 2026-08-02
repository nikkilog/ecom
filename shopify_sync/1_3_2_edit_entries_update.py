# shopify_sync/edit_entries_update.py
# Progress build v2: batched Shopify preflight + visible stage output

from __future__ import annotations

import base64
import json
import os
import random
import re
import sys
import time
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials


MODULE_PATH = "shopify_sync.1_3_2_edit_entries_update"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
DEFAULT_JOB_NAME = "edit_entries_update"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


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


M_METAOBJECT_UPDATE = """
mutation($id: ID!, $metaobject: MetaobjectUpdateInput!) {
  metaobjectUpdate(id: $id, metaobject: $metaobject) {
    metaobject { id handle type }
    userErrors { field message code }
  }
}
"""

Q_METAOBJECT_BY_ID = """
query($id: ID!) {
  node(id: $id) {
    ... on Metaobject {
      id
      handle
      type
    }
  }
}
"""


Q_METAOBJECTS_BY_IDS = """
query($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Metaobject {
      id
      handle
      type
    }
  }
}
"""

Q_METAOBJECT_BY_HANDLE_EXACT = """
query($handle: MetaobjectHandleInput!) {
  metaobjectByHandle(handle: $handle) {
    id
    handle
    type
  }
}
"""

Q_COLLECTION_BY_HANDLE = """
query($h: String!) {
  collectionByHandle(handle: $h) {
    id
    handle
  }
}
"""


@dataclass
class Context:
    site_code: str
    job_name: str
    tz_name: str
    dry_run: bool
    confirmed: bool
    default_mode: str
    empty_means_clear: bool
    job_chunk_size: int
    preview_limit: int
    detail_limit_per_error: int
    cfg_sites_url: str
    cfg_sites_tab: str
    label_edit: str
    label_config: str
    label_runlog: str
    tab_edit_update: str
    tab_cfg_fields: str
    tab_runlog: str
    shop_domain: str
    api_version: str
    gsheet_sa_b64: str
    shopify_token: str
    run_id: Optional[str] = None


class RunLogger:
    def __init__(self, ws_log, ctx: Context):
        self.ws_log = ws_log
        self.ctx = ctx
        self.buf: List[List[Any]] = []
        self.flush_every = 200
        self.detail_seen_count: Dict[Tuple[str, str], int] = {}

    def ensure_header(self) -> None:
        _with_sheets_retry(
            lambda: self.ws_log.update(
                range_name="A1:R1",
                values=[RUNLOG_HEADER_18],
                value_input_option="RAW",
            ),
            action="runlog.update_header",
            retry_5xx=True,
        )

    def add(
        self,
        *,
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
        self.buf.append([
            self.ctx.run_id,
            now_cn_str(self.ctx.tz_name),
            self.ctx.job_name,
            phase,
            log_type,
            status,
            self.ctx.site_code,
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
        ])
        if len(self.buf) >= self.flush_every:
            self.flush()

    def add_detail_limited(
        self,
        *,
        phase: str,
        status: str,
        entity_type: str,
        gid: str,
        field_key: str,
        message: str,
        error_reason: str,
    ) -> None:
        k = (phase, error_reason or "UNKNOWN")
        n = self.detail_seen_count.get(k, 0)
        if n >= self.ctx.detail_limit_per_error:
            return
        self.detail_seen_count[k] = n + 1
        self.add(
            phase=phase,
            log_type="detail",
            status=status,
            entity_type=entity_type,
            gid=gid,
            field_key=field_key,
            message=message,
            error_reason=error_reason,
        )

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
            action="runlog.append_rows",
            retry_5xx=False,
        )
        self.buf = []


def progress(message: str) -> None:
    """Print progress immediately in Colab instead of waiting for buffered output."""
    print(f"[edit_entries_update] {message}", flush=True)


def now_cn_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def gen_run_id(job_name: str, tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime(f"{job_name}_%Y%m%d_%H%M%S")


def _norm(x: Any) -> str:
    return str(x).strip()


def _norm_lower(x: Any) -> str:
    return _norm(x).lower()


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for x in candidates:
        if x.lower() in cols:
            return cols[x.lower()]
    return None


def split_multi(raw: str) -> List[str]:
    return [x.strip() for x in re.split(r"[,\n]+", str(raw)) if x.strip()]


def slot_to_int(x: Any) -> Optional[int]:
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def chunk(lst: List[Any], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def err_signature(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"sheet_row=\d+", "sheet_row=?", s)
    s = re.sub(r"gid://shopify/[A-Za-z]+/\d+", "gid://shopify/X/?", s)
    s = re.sub(r"handle=[A-Za-z0-9\-_:/]+", "handle=?", s)
    return s[:240]


def reason_from_error(s: str) -> str:
    t = str(s).lower()
    if "field_id not found" in t:
        return "FIELD_ID_NOT_FOUND"
    if "missing key in cfg__fields" in t:
        return "CFG_KEY_MISSING"
    if "entry not found by gid" in t:
        return "ENTRY_GID_NOT_FOUND"
    if "type mismatch" in t or "actual metaobject type" in t:
        return "TYPE_MISMATCH"
    if "collection_reference parse failed" in t:
        return "COLLECTION_REF_PARSE_FAILED"
    if "metaobject handle not found" in t:
        return "METAOBJECT_REF_NOT_FOUND"
    if "graphql" in t or "usererrors" in t:
        return "SHOPIFY_API_ERROR"
    if "multiple new_handle" in t:
        return "MULTIPLE_NEW_HANDLE"
    if "duplicate handle target" in t:
        return "DUPLICATE_TARGET_HANDLE"
    return "OTHER"


def format_user_errors(ues: List[Dict[str, Any]]) -> str:
    parts = []
    for u in (ues or []):
        code = u.get("code") or ""
        msg = u.get("message") or ""
        fld = u.get("field") or []
        parts.append(f"{code} | {msg} | field={fld}")
    return " ; ".join(parts)[:1000]



@dataclass(frozen=True)
class SecretValue:
    value: str
    source_type: str
    source_detail: str


def _clean_str(value: Any) -> str:
    return "" if value is None else str(value).strip()


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
    name: str,
    *,
    project_code: str,
    explicit_value: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> SecretValue:
    secret_name = _clean_str(name)
    resolved_project_code = _clean_str(project_code).upper()
    if not secret_name:
        raise ValueError("Secret name is empty.")
    if not resolved_project_code:
        raise ValueError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _clean_str(explicit_value):
        return SecretValue(
            value=_clean_str(explicit_value),
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
                f"Colab Secret {secret_name!r} is missing or not enabled for this notebook."
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

    result = resolver.read(secret_name, aliases=aliases)
    return _workspace_secret_result_to_value(result)


def _parse_service_account_text(raw_value: str) -> Tuple[Dict[str, Any], str]:
    raw = _clean_str(raw_value)
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

    text = str(exc).lower()
    return any(
        token in text
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
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if (
                not _is_retryable_sheets_error(
                    exc,
                    retry_5xx=retry_5xx,
                )
                or attempt >= attempts
            ):
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
    return re.sub(r"[\s_]+", " ", _clean_str(value).lower()).strip()


def _extract_spreadsheet_id(value: Any) -> str:
    raw = _clean_str(value)
    if not raw:
        raise ValueError("Workspace Project Registry ID/URL is empty.")

    match = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", raw)
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
    resolved_project_code = _clean_str(project_code).upper()
    if not resolved_project_code:
        raise ValueError("project_code is required.")

    workspace_secret = read_secret(
        workspace_gsheet_secret_name,
        project_code="WORKSPACE",
        secret_home=secret_home,
    )
    workspace_gc = build_gc(workspace_secret.value)

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
        lambda: worksheet.get_all_values(),
        action="workspace.read_registry",
    )
    if not values:
        raise ValueError(
            f"Workspace Project Registry tab {workspace_registry_tab!r} is empty."
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

    project_col = require_column("project_code", "project code")
    active_col = require_column("active")
    console_url_col = require_column("console_core_url", "console core url")
    gsheet_secret_col = require_column("gsheet_secret_name", "gsheet secret name")
    account_tab_col = require_column("account_config_tab", "account config tab")
    timezone_col = require_column("timezone", "time zone")
    project_name_col = header_map.get(_normalize_registry_header("project_name"))

    width = len(values[0])
    matches: List[Tuple[int, List[Any]]] = []
    for row_number, raw_row in enumerate(values[1:], start=2):
        row = list(raw_row) + [""] * max(0, width - len(raw_row))
        if _clean_str(row[project_col]).upper() == resolved_project_code:
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
    active_text = _clean_str(row[active_col]).lower()
    if active_text not in {"true", "1", "yes", "y", "是"}:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}; row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": (
            _clean_str(row[project_name_col])
            if project_name_col is not None
            else ""
        ),
        "console_core_url": _clean_str(row[console_url_col]),
        "gsheet_secret_name": _clean_str(row[gsheet_secret_col]),
        "account_config_tab": _clean_str(row[account_tab_col]),
        "timezone": _clean_str(row[timezone_col]),
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
            f"project_code={resolved_project_code}; fields={missing}; row={source_row}."
        )

    if print_progress:
        print(
            "[Workspace Registry] resolved | "
            f"project={route['project_code']} | row={source_row} | "
            f"secret={route['gsheet_secret_name']} | "
            f"account_tab={route['account_config_tab']} | "
            f"timezone={route['timezone']}"
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

    project_gsheet_secret = read_secret(
        route["gsheet_secret_name"],
        project_code=project_code,
        secret_home=secret_home,
    )

    account_cfg = load_account_config_from_console(
        console_core_url=route["console_core_url"],
        console_gsheet_sa_b64=project_gsheet_secret.value,
        cfg_account_tab=route["account_config_tab"],
    )

    required = require_account_cfg(
        account_cfg,
        [
            "SHOP_DOMAIN",
            "SHOPIFY_API_VERSION",
            "GSHEET_SA_B64_SECRET",
            "SHOPIFY_TOKEN_SECRET",
        ],
    )

    if required["GSHEET_SA_B64_SECRET"] != route["gsheet_secret_name"]:
        raise ValueError(
            "Workspace Registry gsheet_secret_name does not match "
            f"{route['account_config_tab']}.GSHEET_SA_B64_SECRET: "
            f"workspace={route['gsheet_secret_name']}, "
            f"account={required['GSHEET_SA_B64_SECRET']}"
        )

    shopify_secret = read_secret(
        required["SHOPIFY_TOKEN_SECRET"],
        project_code=project_code,
        secret_home=secret_home,
    )

    result = {
        "project_route": route,
        "console_core_url": route["console_core_url"],
        "account_config_tab": route["account_config_tab"],
        "workspace_timezone": route["timezone"],
        "account_config": account_cfg,
        "shop_domain": required["SHOP_DOMAIN"],
        "api_version": required["SHOPIFY_API_VERSION"],
        "gsheet_secret_name": required["GSHEET_SA_B64_SECRET"],
        "shopify_token_secret_name": required["SHOPIFY_TOKEN_SECRET"],
        "gsheet_sa_value": project_gsheet_secret.value,
        "shopify_access_token": shopify_secret.value,
        "gsheet_auth_source_type": project_gsheet_secret.source_type,
        "shopify_auth_source_type": shopify_secret.source_type,
    }

    if print_progress:
        print(
            "[Runtime Context] "
            f"project={project_code} | shop_domain={result['shop_domain']} | "
            f"api_version={result['api_version']} | "
            f"gsheet_source={result['gsheet_auth_source_type']} | "
            f"shopify_source={result['shopify_auth_source_type']}"
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
        return {
            "status": "OFF",
            "target_row": None,
            "changed_fields": [],
        }

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _clean_str(
        current_colab_url
    ):
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _clean_str(current_colab_name):
        raise ValueError("UPDATE_URL_AND_NAME requires current_colab_name.")

    secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
    )
    gc = build_gc(secret.value)

    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="registry.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(registry_tab),
        action="registry.open_tab",
    )
    values = _with_sheets_retry(
        lambda: ws.get_all_values(),
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
            raise ValueError(
                f"Registry tab has duplicate normalized header: {normalized}."
            )
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
        changes.append(
            ("colab_url", url_col + 1, _clean_str(current_row[url_col]), provided_url)
        )
    if provided_name and _clean_str(current_row[name_col]) != provided_name:
        changes.append(
            ("colab_name", name_col + 1, _clean_str(current_row[name_col]), provided_name)
        )

    if mode == "CHECK":
        status = "CHANGE_DETECTED" if changes else "NO_CHANGE"
    else:
        permitted = (
            {"colab_url"}
            if mode == "UPDATE_URL"
            else {"colab_url", "colab_name"}
        )
        applied = [change for change in changes if change[0] in permitted]

        for field_name, column_number, _old_value, new_value in applied:
            _with_sheets_retry(
                lambda rn=row_number, cn=column_number, nv=new_value: (
                    ws.update_cell(rn, cn, nv)
                ),
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


def build_gc(sa_b64: str):
    sa_info, _secret_format = _parse_service_account_text(sa_b64)
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)




def load_account_config_from_console(
    *,
    console_core_url: str,
    console_gsheet_sa_b64: str,
    cfg_account_tab: str = "Cfg__account_id",
) -> Dict[str, str]:
    """
    Load account/site runtime config from Console Core / Cfg__account_id.

    Expected sheet layout:
      Column A = config key
      Column B = config value

    Example:
      SHOP_DOMAIN                aeqjdw-r1.myshopify.com
      SHOPIFY_API_VERSION         2026-01
      GSHEET_SA_B64_SECRET        NRP_GSHEET
      SHOPIFY_TOKEN_SECRET        NRP_SHOPIFY_ACCESS_TOKEN
    """
    gc = build_gc(console_gsheet_sa_b64)
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(console_core_url),
        action="account_config.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(cfg_account_tab),
        action="account_config.open_tab",
    )
    values = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="account_config.read",
    )

    cfg: Dict[str, str] = {}
    for row in values:
        if not row:
            continue
        key = str(row[0]).strip() if len(row) >= 1 else ""
        val = str(row[1]).strip() if len(row) >= 2 else ""
        if not key:
            continue
        if key.lower() in {"key", "config_key", "field", "name"}:
            continue
        cfg[key] = val
    return cfg


def require_account_cfg(account_cfg: Dict[str, str], keys: List[str]) -> Dict[str, str]:
    missing = [k for k in keys if not str(account_cfg.get(k, "")).strip()]
    if missing:
        raise ValueError(f"Cfg__account_id missing required config values: {missing}")
    return {k: str(account_cfg.get(k, "")).strip() for k in keys}


def resolve_secret_value(
    secret_name: str,
    *,
    project_code: str,
    secret_home: Optional[str] = None,
) -> str:
    """Backward-compatible value-only adapter over the formal Secret boundary."""
    return read_secret(
        secret_name,
        project_code=project_code,
        secret_home=secret_home,
    ).value


def run_from_console_core(
    *,
    site_code: str,
    console_core_url: str,
    console_gsheet_sa_b64: str,
    cfg_account_tab: str = "Cfg__account_id",
    cfg_sites_tab: str = "Cfg__Sites",
    label_edit: str = "edit",
    label_config: str = "config",
    label_runlog: str = "runlog_sheet",
    tab_edit_update: str = "Edit__Entries_Update",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_runlog: str = "Ops__RunLog",
    dry_run: bool = True,
    confirmed: bool = False,
    default_mode: str = "LOOSE",
    empty_means_clear: bool = True,
    tz_name: str = "Asia/Shanghai",
    job_name: str = "edit_entries_update",
    run_id: Optional[str] = None,
    job_chunk_size: int = 25,
    preview_limit: int = 20,
    detail_limit_per_error: int = 2,
    secret_home: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Console-Core routed entrypoint.

    Cell 1 should only provide the Console Core URL and bootstrap Google Sheets secret value.
    Site-level config is loaded from Cfg__account_id.
    Sheet routing is loaded from Cfg__Sites.
    """
    account_cfg = load_account_config_from_console(
        console_core_url=console_core_url,
        console_gsheet_sa_b64=console_gsheet_sa_b64,
        cfg_account_tab=cfg_account_tab,
    )

    required = require_account_cfg(
        account_cfg,
        [
            "SHOP_DOMAIN",
            "GSHEET_SA_B64_SECRET",
            "SHOPIFY_TOKEN_SECRET",
        ],
    )

    shop_domain = required["SHOP_DOMAIN"]
    api_version = str(
        account_cfg.get("SHOPIFY_API_VERSION")
        or account_cfg.get("API_VERSION")
        or "2026-04"
    ).strip()

    gsheet_secret_name = required["GSHEET_SA_B64_SECRET"]
    shopify_token_secret_name = required["SHOPIFY_TOKEN_SECRET"]

    gsheet_sa_b64 = resolve_secret_value(
        gsheet_secret_name,
        project_code=site_code,
        secret_home=secret_home,
    )
    shopify_token = resolve_secret_value(
        shopify_token_secret_name,
        project_code=site_code,
        secret_home=secret_home,
    )

    if not gsheet_sa_b64:
        raise ValueError(f"Missing secret value: {gsheet_secret_name}")
    if not shopify_token:
        raise ValueError(f"Missing secret value: {shopify_token_secret_name}")

    result = run(
        site_code=site_code,
        gsheet_sa_b64=gsheet_sa_b64,
        shopify_token=shopify_token,
        shop_domain=shop_domain,
        api_version=api_version,
        cfg_sites_url=console_core_url,
        cfg_sites_tab=cfg_sites_tab,
        label_edit=label_edit,
        label_config=label_config,
        label_runlog=label_runlog,
        tab_edit_update=tab_edit_update,
        tab_cfg_fields=tab_cfg_fields,
        tab_runlog=tab_runlog,
        dry_run=dry_run,
        confirmed=confirmed,
        default_mode=default_mode,
        empty_means_clear=empty_means_clear,
        tz_name=tz_name,
        job_name=job_name,
        run_id=run_id,
        job_chunk_size=job_chunk_size,
        preview_limit=preview_limit,
        detail_limit_per_error=detail_limit_per_error,
    )

    result.setdefault("runtime_config", {})
    result["runtime_config"].update({
        "config_source": f"{cfg_account_tab} + {cfg_sites_tab}",
        "shop_domain": shop_domain,
        "api_version": api_version,
        "gsheet_sa_b64_secret": gsheet_secret_name,
        "shopify_token_secret": shopify_token_secret_name,
        "console_core_url": console_core_url,
    })
    return result


class ShopifyClient:
    def __init__(self, shop_domain: str, api_version: str, token: str):
        self.graphql_url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        }

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None, retry: int = 6) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        for i in range(retry):
            r = requests.post(self.graphql_url, headers=self.headers, json=payload, timeout=90)
            if r.status_code in (429, 502, 503, 504):
                wait_s = min(2 ** i, 20) + random.random()
                progress(
                    f"Shopify API retry {i + 1}/{retry}: "
                    f"HTTP {r.status_code}; waiting {wait_s:.1f}s"
                )
                time.sleep(wait_s)
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("errors"):
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        raise RuntimeError("GraphQL failed after retries")


def resolve_sheet_url_from_cfg_sites(gc, cfg_sites_url: str, cfg_sites_tab: str, site_code: str, label: str) -> str:
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(cfg_sites_url),
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
    df = pd.DataFrame(rows).fillna("")
    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    c_site = pick_col(df, ["site_code"])
    c_label = pick_col(df, ["label"])
    c_url = pick_col(df, ["sheet_url"])
    if not c_site or not c_label or not c_url:
        raise ValueError(f"Cfg__Sites must contain site_code/label/sheet_url. Got={df.columns.tolist()}")

    hit = df[
        (df[c_site].astype(str).str.strip().str.lower() == _norm_lower(site_code)) &
        (df[c_label].astype(str).str.strip().str.lower() == _norm_lower(label))
    ].copy()

    if hit.empty:
        raise ValueError(f"Cfg__Sites no match for site_code={site_code}, label={label}")

    url = str(hit.iloc[0][c_url]).strip()
    if not url:
        raise ValueError(f"Cfg__Sites matched but sheet_url empty for site_code={site_code}, label={label}")
    return url


def load_cfg_fields(ws_fields) -> Dict[str, Dict[str, str]]:
    rows = _with_sheets_retry(
        lambda: ws_fields.get_all_records(),
        action="cfg_fields.read",
    )
    df = pd.DataFrame(rows).fillna("")

    c_field_id = pick_col(df, ["field_id"])
    c_entity = pick_col(df, ["entity_type"])
    c_field_key = pick_col(df, ["field_key"])
    c_namespace = pick_col(df, ["namespace"])
    c_key = pick_col(df, ["key"])
    c_data_type = pick_col(df, ["data_type", "field_type"])

    need = [c_field_id, c_entity, c_field_key, c_namespace, c_key, c_data_type]
    if not all(need):
        raise ValueError(f"Cfg__Fields required columns missing. Got={df.columns.tolist()}")

    out: Dict[str, Dict[str, str]] = {}
    for _, r in df.iterrows():
        fid = str(r[c_field_id]).strip()
        if not fid:
            continue
        out[fid] = {
            "field_id": fid,
            "entity_type": str(r[c_entity]).strip(),
            "field_key": str(r[c_field_key]).strip(),
            "namespace": str(r[c_namespace]).strip(),
            "key": str(r[c_key]).strip(),
            "data_type": str(r[c_data_type]).strip(),
        }
    return out


def load_edit_rows(ws_edit, required_cols: List[str]) -> pd.DataFrame:
    vals = _with_sheets_retry(
        lambda: ws_edit.get_all_values(),
        action="edit_entries_update.read",
    )
    if len(vals) < 2:
        raise ValueError("Edit__Entries_Update is empty")

    hdr = [str(x).strip() for x in vals[0]]
    data = vals[1:]
    rows = []
    max_len = len(hdr)

    for i, r in enumerate(data, start=2):
        rr = [str(x).strip() for x in r]
        rr += [""] * (max_len - len(rr))
        rec = dict(zip(hdr, rr))
        rec["_sheet_row"] = i
        rows.append(rec)

    df = pd.DataFrame(rows).fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Edit__Entries_Update missing required columns: {missing}")

    for opt in ["slot", "note"]:
        if opt not in df.columns:
            df[opt] = ""

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    df["op"] = df["op"].astype(str).str.strip().str.upper()
    df["mode"] = df["mode"].astype(str).str.strip().str.upper()
    return df


def extract_type_from_field_id(field_id: str) -> str:
    s = str(field_id).strip()
    m = re.match(r"^METAOBJECT_ENTRY\|mo\.([^.]+)\..+$", s)
    if not m:
        return ""
    return m.group(1).strip()


class Resolver:
    def __init__(self, sc: ShopifyClient):
        self.sc = sc
        self.collection_cache: Dict[str, str] = {}
        self.mo_info_cache: Dict[str, Dict[str, str]] = {}
        self.mo_gid_by_handle_cache: Dict[Tuple[str, str], str] = {}

    def get_metaobject_info_by_gid(self, entry_gid: str) -> Dict[str, str]:
        gid = str(entry_gid).strip()
        if gid in self.mo_info_cache:
            return self.mo_info_cache[gid]
        data = self.sc.gql(Q_METAOBJECT_BY_ID, {"id": gid})
        node = data.get("node") or {}
        info = {
            "id": node.get("id", "") or "",
            "handle": node.get("handle", "") or "",
            "type": node.get("type", "") or "",
        }
        self.mo_info_cache[gid] = info
        return info

    def get_metaobject_infos_by_gids(
        self,
        entry_gids: List[str],
        batch_size: int = 50,
    ) -> Dict[str, Dict[str, str]]:
        """
        Resolve many metaobject GIDs in batches.

        The old implementation made one Shopify request per entry_gid.
        This method reduces preflight requests substantially and prints
        visible progress for every batch.
        """
        gids = list(dict.fromkeys(
            str(x).strip() for x in entry_gids if str(x).strip()
        ))
        missing = [gid for gid in gids if gid not in self.mo_info_cache]

        if not missing:
            progress(f"Shopify preflight cache hit: {len(gids)} entry_gid(s)")
            return {gid: self.mo_info_cache[gid] for gid in gids}

        batch_size = max(1, int(batch_size))
        total = len(missing)
        total_batches = (total + batch_size - 1) // batch_size

        progress(
            f"Shopify preflight: checking {total} uncached entry_gid(s) "
            f"in {total_batches} batch(es), batch_size={batch_size}"
        )

        checked = 0
        for batch_no, gid_batch in enumerate(chunk(missing, batch_size), start=1):
            start_no = checked + 1
            end_no = checked + len(gid_batch)
            progress(
                f"Shopify preflight batch {batch_no}/{total_batches}: "
                f"entry_gid {start_no}-{end_no}/{total}"
            )

            data = self.sc.gql(Q_METAOBJECTS_BY_IDS, {"ids": gid_batch})
            nodes = data.get("nodes") or []

            for idx, gid in enumerate(gid_batch):
                node = nodes[idx] if idx < len(nodes) and nodes[idx] else {}
                self.mo_info_cache[gid] = {
                    "id": node.get("id", "") or "",
                    "handle": node.get("handle", "") or "",
                    "type": node.get("type", "") or "",
                }

            checked = end_no
            progress(
                f"Shopify preflight batch {batch_no}/{total_batches} complete: "
                f"{checked}/{total}"
            )

        return {gid: self.mo_info_cache[gid] for gid in gids}

    def to_gid_collection(self, val: str) -> str:
        v = str(val).strip()
        if not v:
            return ""
        if v.startswith("gid://"):
            return v
        if v in self.collection_cache:
            return self.collection_cache[v]
        if re.fullmatch(r"\d+", v):
            gid = f"gid://shopify/Collection/{v}"
            self.collection_cache[v] = gid
            return gid

        data = self.sc.gql(Q_COLLECTION_BY_HANDLE, {"h": v})
        node = data.get("collectionByHandle") or {}
        gid = node.get("id", "") or ""
        self.collection_cache[v] = gid
        return gid

    def parse_type_and_handle(self, raw: str, fallback_type: str) -> Tuple[str, str]:
        s = str(raw).strip()
        if "/" in s:
            t, h = s.split("/", 1)
            return t.strip(), h.strip()
        if ":" in s:
            t, h = s.split(":", 1)
            return t.strip(), h.strip()
        return fallback_type, s

    def gid_from_metaobject_handle(self, mo_type: str, handle_str: str, strict: bool = True) -> str:
        k = (mo_type, handle_str)
        if k in self.mo_gid_by_handle_cache:
            gid = self.mo_gid_by_handle_cache[k]
            if strict and not gid:
                raise ValueError(f"metaobject handle not found (cached): type={mo_type} handle={handle_str}")
            return gid

        data = self.sc.gql(Q_METAOBJECT_BY_HANDLE_EXACT, {"handle": {"type": mo_type, "handle": handle_str}})
        node = data.get("metaobjectByHandle") or {}
        gid = node.get("id", "") or ""
        self.mo_gid_by_handle_cache[k] = gid

        if strict and not gid:
            raise ValueError(f"metaobject handle not found: type={mo_type} handle={handle_str}")
        return gid

    def to_gid_metaobject(self, raw: str, current_entry_type: str, strict: bool = True) -> str:
        v = str(raw).strip()
        if not v:
            return ""
        if v.startswith("gid://"):
            return v
        ref_type, ref_handle = self.parse_type_and_handle(v, fallback_type=current_entry_type)
        return self.gid_from_metaobject_handle(ref_type, ref_handle, strict=strict)


def preflight(
    df: pd.DataFrame,
    cfg_by_field_id: Dict[str, Dict[str, str]],
    resolver: Resolver,
    ctx: Context,
) -> Tuple[pd.DataFrame, List[str], Dict[str, int], List[Dict[str, Any]]]:
    warnings: List[str] = []

    df = df[~(
        (df["op"] == "") &
        (df["entry_gid"] == "") &
        (df["field_id"] == "") &
        (df["value"] == "") &
        (df["new_handle"] == "")
    )].copy()

    df["mode"] = df["mode"].replace("", ctx.default_mode)

    bad_op = df[df["op"] != "UPDATE"]
    if not bad_op.empty:
        raise ValueError(f"Only op=UPDATE allowed. Bad rows={bad_op[['op','_sheet_row']].head(20).to_dict('records')}")

    bad_mode = df[~df["mode"].isin(["STRICT", "LOOSE"])]
    if not bad_mode.empty:
        raise ValueError(f"Invalid mode. Bad rows={bad_mode[['mode','_sheet_row']].head(20).to_dict('records')}")

    bad_gid = df[df["entry_gid"] == ""]
    if not bad_gid.empty:
        raise ValueError(f"Update rows require entry_gid. Bad rows={bad_gid[['_sheet_row']].head(20).to_dict('records')}")

    bad_empty_action = df[(df["field_id"] == "") & (df["new_handle"] == "")]
    if not bad_empty_action.empty:
        raise ValueError(
            f"Each row must have field_id or new_handle. Bad rows={bad_empty_action[['_sheet_row']].head(20).to_dict('records')}"
        )

    df_with_field = df[df["field_id"] != ""].copy()
    bad_field = df_with_field[~df_with_field["field_id"].isin(cfg_by_field_id.keys())]
    if not bad_field.empty:
        raise ValueError(f"field_id not found in Cfg__Fields. Bad rows={bad_field[['field_id','_sheet_row']].head(20).to_dict('records')}")

    bad_entity = []
    for _, r in df_with_field.iterrows():
        meta = cfg_by_field_id.get(r["field_id"], {})
        if meta.get("entity_type") != "METAOBJECT_ENTRY":
            bad_entity.append({
                "field_id": r["field_id"],
                "_sheet_row": r["_sheet_row"],
                "entity_type": meta.get("entity_type", ""),
            })
    if bad_entity:
        raise ValueError(f"Only METAOBJECT_ENTRY field_id allowed. Bad rows={bad_entity[:20]}")

    bad_type_match = []
    gid_type_cache: Dict[str, str] = {}
    gids = list(dict.fromkeys(df["entry_gid"].astype(str).tolist()))

    progress(f"Preflight: {len(gids)} distinct entry_gid(s) found")
    gid_infos = resolver.get_metaobject_infos_by_gids(gids, batch_size=50)
    for gid in gids:
        gid_type_cache[gid] = gid_infos.get(gid, {}).get("type", "") or ""

    for _, r in df_with_field.iterrows():
        fid_type = extract_type_from_field_id(r["field_id"])
        actual_type = gid_type_cache.get(r["entry_gid"], "")
        if fid_type and actual_type and fid_type != actual_type:
            bad_type_match.append({
                "entry_gid": r["entry_gid"],
                "field_id": r["field_id"],
                "field_id_type": fid_type,
                "actual_type": actual_type,
                "_sheet_row": r["_sheet_row"],
            })
    if bad_type_match:
        raise ValueError(f"field_id type mismatch with entry_gid actual type. Bad rows={bad_type_match[:20]}")

    # 同一 entry_gid 多个不同 new_handle：直接报错
    handle_conflicts = []
    for entry_gid, g in df.groupby("entry_gid", dropna=False):
        hs = [str(x).strip() for x in g["new_handle"].tolist() if str(x).strip()]
        uniq = sorted(set(hs))
        if len(uniq) > 1:
            handle_conflicts.append({
                "entry_gid": entry_gid,
                "new_handles": uniq,
                "sheet_rows": g["_sheet_row"].tolist()[:20],
            })
    if handle_conflicts:
        raise ValueError(f"multiple new_handle found for same entry_gid. Bad groups={handle_conflicts[:20]}")

    # 多个 entry 目标 handle 重复：给 warning
    handle_target_groups = (
        df[df["new_handle"] != ""]
        .groupby("new_handle")["entry_gid"]
        .nunique()
        .reset_index(name="gid_cnt")
    )
    dup_targets = handle_target_groups[handle_target_groups["gid_cnt"] > 1]
    if not dup_targets.empty:
        warnings.append(
            "duplicate handle target detected: "
            + "; ".join([f"{r['new_handle']} -> {r['gid_cnt']} gids" for _, r in dup_targets.head(20).iterrows()])
        )

    counters = {
        "rows_loaded": int(len(df)),
        "rows_pending": int(len(df)),
        "rows_recognized": int(len(df_with_field)),
        "distinct_gid_count": int(len(gids)),
    }

    preview_rows: List[Dict[str, Any]] = []
    return df, warnings, counters, preview_rows


def build_fields_for_entry(
    rows_for_entry: List[Dict[str, Any]],
    entry_type: str,
    strict: bool,
    cfg_by_field_id: Dict[str, Dict[str, str]],
    resolver: Resolver,
    empty_means_clear: bool,
) -> Tuple[List[Dict[str, str]], List[str]]:
    by_field: Dict[str, List[Dict[str, Any]]] = {}
    warnings: List[str] = []

    for r in rows_for_entry:
        fid = str(r.get("field_id", "")).strip()
        if not fid:
            continue
        by_field.setdefault(fid, []).append(r)

    out: List[Dict[str, str]] = []

    for field_id, items in by_field.items():
        meta = cfg_by_field_id.get(field_id)
        if not meta:
            if strict:
                sr = items[0].get("_sheet_row")
                raise ValueError(f"sheet_row={sr} field_id not found: {field_id}")
            warnings.append(f"skip unknown field_id: {field_id}")
            continue

        data_type = str(meta.get("data_type", "")).strip().lower()
        key = str(meta.get("key", "")).strip()

        if not key:
            if strict:
                sr = items[0].get("_sheet_row")
                raise ValueError(f"sheet_row={sr} missing key in Cfg__Fields for field_id={field_id}")
            warnings.append(f"skip field with empty key: {field_id}")
            continue

        if data_type.startswith("list."):
            def _sort_key(r):
                si = slot_to_int(r.get("slot", ""))
                return (999999 if si is None else si, int(r.get("_sheet_row", 10 ** 9)))

            items_sorted = sorted(items, key=_sort_key)

            vals: List[str] = []
            for r in items_sorted:
                raw = str(r.get("value", "")).strip()
                if raw == "":
                    continue
                vals.extend(split_multi(raw))

            if not vals:
                if empty_means_clear:
                    out.append({"key": key, "value": "[]"})
                continue

            if "collection_reference" in data_type:
                gids = []
                for it in vals:
                    gid = resolver.to_gid_collection(it)
                    if not gid:
                        raise ValueError(f"collection_reference parse failed: {it}")
                    gids.append(gid)
                out.append({"key": key, "value": json.dumps(gids)})
                continue

            if "metaobject_reference" in data_type:
                gids = []
                for it in vals:
                    gids.append(resolver.to_gid_metaobject(it, current_entry_type=entry_type, strict=strict))
                out.append({"key": key, "value": json.dumps(gids)})
                continue

            out.append({"key": key, "value": json.dumps(vals, ensure_ascii=False)})
            continue

        # scalar: the last sheet row is authoritative.
        # When EMPTY_MEANS_CLEAR=True, a blank value explicitly clears the field,
        # including single collection_reference and metaobject_reference fields.
        items_sorted = sorted(items, key=lambda x: int(x.get("_sheet_row", 10 ** 9)))
        final_raw = str(items_sorted[-1].get("value", "")).strip()

        if final_raw == "":
            if not empty_means_clear:
                continue
            out.append({"key": key, "value": ""})
            continue

        if "collection_reference" in data_type:
            gid = resolver.to_gid_collection(final_raw)
            if not gid:
                raise ValueError(f"collection_reference parse failed: {final_raw}")
            out.append({"key": key, "value": gid})
            continue

        if "metaobject_reference" in data_type:
            gid = resolver.to_gid_metaobject(final_raw, current_entry_type=entry_type, strict=strict)
            out.append({"key": key, "value": gid})
            continue

        out.append({"key": key, "value": final_raw})

    return out, warnings


def build_jobs(
    plan_df: pd.DataFrame,
    cfg_by_field_id: Dict[str, Dict[str, str]],
    resolver: Resolver,
    ctx: Context,
) -> Tuple[List[Dict[str, Any]], List[str], Dict[str, int], List[Dict[str, Any]]]:
    records = plan_df.to_dict("records")
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        entry_gid = str(r.get("entry_gid", "")).strip()
        groups.setdefault(entry_gid, []).append(r)

    jobs: List[Dict[str, Any]] = []
    warnings: List[str] = []
    preview: List[Dict[str, Any]] = []
    rows_skipped = 0

    total_groups = len(groups)
    progress(f"Build jobs: processing {total_groups} entry group(s)")

    for group_no, (entry_gid, items) in enumerate(groups.items(), start=1):
        if group_no == 1 or group_no % 10 == 0 or group_no == total_groups:
            progress(
                f"Build jobs: group {group_no}/{total_groups} "
                f"entry_gid={entry_gid}"
            )

        modes = [str(x.get("mode", "")).strip().upper() or ctx.default_mode for x in items]
        strict = ("STRICT" in modes)
        min_row = min(int(x.get("_sheet_row", 10 ** 9)) for x in items)

        info = resolver.get_metaobject_info_by_gid(entry_gid)
        entry_type = info.get("type", "") or ""
        current_handle = info.get("handle", "") or ""

        if not entry_type:
            if strict:
                raise ValueError(f"sheet_row={min_row} entry not found by gid: {entry_gid}")
            rows_skipped += len(items)
            warnings.append(f"skip missing entry_gid in Shopify: {entry_gid}")
            continue

        new_handles = []
        for r in sorted(items, key=lambda x: int(x.get("_sheet_row", 10 ** 9))):
            nh = str(r.get("new_handle", "")).strip()
            if nh:
                new_handles.append(nh)
        target_handle = new_handles[-1] if new_handles else ""

        fields_input, field_warnings = build_fields_for_entry(
            rows_for_entry=items,
            entry_type=entry_type,
            strict=strict,
            cfg_by_field_id=cfg_by_field_id,
            resolver=resolver,
            empty_means_clear=ctx.empty_means_clear,
        )
        warnings.extend([f"{entry_gid}: {w}" for w in field_warnings])

        metaobject_input: Dict[str, Any] = {}
        if target_handle:
            metaobject_input["handle"] = target_handle
        if fields_input:
            metaobject_input["fields"] = fields_input

        if not metaobject_input:
            rows_skipped += len(items)
            warnings.append(f"{entry_gid}: nothing to update")
            continue

        jobs.append({
            "_min_row": min_row,
            "entry_gid": entry_gid,
            "entry_type": entry_type,
            "strict": strict,
            "current_handle": current_handle,
            "target_handle": target_handle,
            "items": items,
            "metaobject_input": metaobject_input,
            "fields_count": len(fields_input),
        })

        if len(preview) < ctx.preview_limit:
            preview.append({
                "sheet_row": min_row,
                "entry_gid": entry_gid,
                "entry_type": entry_type,
                "current_handle": current_handle,
                "target_handle": target_handle,
                "fields_count": len(fields_input),
                "field_keys": [x["key"] for x in fields_input[:12]],
                "has_handle_change": bool(target_handle and target_handle != current_handle),
                "fields_preview": fields_input[:5],
            })

    jobs.sort(key=lambda x: int(x["_min_row"]))

    counters = {
        "rows_planned": int(sum(len(x["items"]) for x in jobs)),
        "jobs_planned": int(len(jobs)),
        "rows_skipped_build": int(rows_skipped),
    }
    return jobs, warnings, counters, preview


def execute_jobs(
    jobs: List[Dict[str, Any]],
    resolver: Resolver,
    sc: ShopifyClient,
    logger: RunLogger,
    ctx: Context,
) -> Dict[str, Any]:
    ok = fail = skip = written = 0
    seen_fail_signatures = set()

    for batch_idx, batch in enumerate(chunk(jobs, ctx.job_chunk_size), start=1):
        progress(f"Execute batch {batch_idx}: {len(batch)} job(s)")

        for job in batch:
            entry_gid = job["entry_gid"]
            entry_type = job["entry_type"]
            strict = job["strict"]
            payload = {
                "id": entry_gid,
                "metaobject": job["metaobject_input"],
            }

            try:
                if not job["metaobject_input"]:
                    skip += 1
                    continue

                if ctx.dry_run:
                    ok += 1
                    continue

                if not ctx.confirmed:
                    raise RuntimeError("Apply blocked: CONFIRMED must be True when DRY_RUN=False")

                data = sc.gql(M_METAOBJECT_UPDATE, payload)
                mu = data.get("metaobjectUpdate") or {}
                ues = mu.get("userErrors") or []
                if ues:
                    raise RuntimeError(format_user_errors(ues))

                ok += 1
                written += 1

            except Exception as e:
                if strict:
                    fail += 1
                else:
                    skip += 1

                msg = f"sheet_row={job['_min_row']} gid={entry_gid} | {str(e)}"
                reason = reason_from_error(str(e))
                sig = err_signature(msg)
                if sig not in seen_fail_signatures:
                    seen_fail_signatures.add(sig)
                    logger.add_detail_limited(
                        phase="apply" if not ctx.dry_run else "preview",
                        status="FAIL" if strict else "SKIP",
                        entity_type=entry_type or "METAOBJECT_ENTRY",
                        gid=entry_gid,
                        field_key="update",
                        message=msg,
                        error_reason=reason,
                    )

    return {
        "ok": ok,
        "fail": fail,
        "skip": skip,
        "written": written,
    }


def run(
    *,
    site_code: str,
    gsheet_sa_b64: str,
    shopify_token: str,
    shop_domain: str,
    api_version: str,
    cfg_sites_url: str,
    cfg_sites_tab: str = "Cfg__Sites",
    label_edit: str = "edit",
    label_config: str = "config",
    label_runlog: str = "runlog_sheet",
    tab_edit_update: str = "Edit__Entries_Update",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_runlog: str = "Ops__RunLog",
    dry_run: bool = True,
    confirmed: bool = False,
    default_mode: str = "LOOSE",
    empty_means_clear: bool = True,
    tz_name: str = "Asia/Shanghai",
    job_name: str = "edit_entries_update",
    run_id: Optional[str] = None,
    job_chunk_size: int = 25,
    preview_limit: int = 20,
    detail_limit_per_error: int = 2,
) -> Dict[str, Any]:
    ctx = Context(
        site_code=site_code.strip().upper(),
        job_name=job_name,
        tz_name=tz_name,
        dry_run=dry_run,
        confirmed=confirmed,
        default_mode=default_mode,
        empty_means_clear=empty_means_clear,
        job_chunk_size=job_chunk_size,
        preview_limit=preview_limit,
        detail_limit_per_error=detail_limit_per_error,
        cfg_sites_url=cfg_sites_url,
        cfg_sites_tab=cfg_sites_tab,
        label_edit=label_edit,
        label_config=label_config,
        label_runlog=label_runlog,
        tab_edit_update=tab_edit_update,
        tab_cfg_fields=tab_cfg_fields,
        tab_runlog=tab_runlog,
        shop_domain=shop_domain,
        api_version=api_version,
        gsheet_sa_b64=gsheet_sa_b64,
        shopify_token=shopify_token,
        run_id=run_id or gen_run_id(job_name, tz_name),
    )

    progress(
        f"START run_id={ctx.run_id} site={ctx.site_code} "
        f"dry_run={ctx.dry_run} confirmed={ctx.confirmed}"
    )
    if ctx.dry_run:
        progress(
            "DRY_RUN=True: Shopify write mutations are skipped, "
            "but sheet reads and Shopify validation still run."
        )

    if (not ctx.dry_run) and (not ctx.confirmed):
        raise ValueError("Apply blocked: set DRY_RUN=False and CONFIRMED=True together.")

    progress("Authorizing Google Sheets")
    gc = build_gc(ctx.gsheet_sa_b64)
    progress("Google Sheets authorization complete")

    sc = ShopifyClient(ctx.shop_domain, ctx.api_version, ctx.shopify_token)
    resolver = Resolver(sc)

    progress("Resolving edit/config/runlog sheet URLs from Cfg__Sites")
    url_edit = resolve_sheet_url_from_cfg_sites(gc, ctx.cfg_sites_url, ctx.cfg_sites_tab, ctx.site_code, ctx.label_edit)
    progress("Resolved edit sheet URL")
    url_config = resolve_sheet_url_from_cfg_sites(gc, ctx.cfg_sites_url, ctx.cfg_sites_tab, ctx.site_code, ctx.label_config)
    progress("Resolved config sheet URL")
    url_runlog = resolve_sheet_url_from_cfg_sites(gc, ctx.cfg_sites_url, ctx.cfg_sites_tab, ctx.site_code, ctx.label_runlog)
    progress("Resolved runlog sheet URL")

    progress("Opening Google Sheets workbooks")
    sh_edit = _with_sheets_retry(
        lambda: gc.open_by_url(url_edit),
        action="run.open_edit_sheet",
    )
    sh_cfg = _with_sheets_retry(
        lambda: gc.open_by_url(url_config),
        action="run.open_config_sheet",
    )
    sh_runlog = _with_sheets_retry(
        lambda: gc.open_by_url(url_runlog),
        action="run.open_runlog_sheet",
    )
    progress("Google Sheets workbooks opened")

    progress("Opening worksheets")
    ws_edit = _with_sheets_retry(
        lambda: sh_edit.worksheet(ctx.tab_edit_update),
        action="run.open_edit_tab",
    )
    ws_fields = _with_sheets_retry(
        lambda: sh_cfg.worksheet(ctx.tab_cfg_fields),
        action="run.open_cfg_fields_tab",
    )
    ws_log = _with_sheets_retry(
        lambda: sh_runlog.worksheet(ctx.tab_runlog),
        action="run.open_runlog_tab",
    )
    progress("Worksheets opened")

    logger = RunLogger(ws_log, ctx)
    progress("Checking Ops__RunLog header")
    logger.ensure_header()
    progress("Ops__RunLog header ready")

    progress(f"Loading {ctx.tab_cfg_fields}")
    cfg_by_field_id = load_cfg_fields(ws_fields)
    progress(f"Loaded {len(cfg_by_field_id)} field definition(s)")

    progress(f"Loading {ctx.tab_edit_update}")
    df = load_edit_rows(
        ws_edit,
        required_cols=["op", "entry_gid", "mode", "field_id", "value", "new_handle"],
    )
    progress(f"Loaded {len(df)} raw edit row(s)")

    progress("Starting preflight validation")
    plan_df, warnings_preflight, c1, _ = preflight(df, cfg_by_field_id, resolver, ctx)
    progress(
        f"Preflight complete: rows_loaded={c1['rows_loaded']} "
        f"distinct_gid_count={c1['distinct_gid_count']}"
    )

    progress("Starting job construction")
    jobs, warnings_build, c2, preview = build_jobs(plan_df, cfg_by_field_id, resolver, ctx)
    progress(
        f"Job construction complete: jobs_planned={c2['jobs_planned']} "
        f"rows_planned={c2['rows_planned']}"
    )

    warnings_all = warnings_preflight + warnings_build

    summary = {
        "run_id": ctx.run_id,
        "site_code": ctx.site_code,
        "job_name": ctx.job_name,
        "phase": "preview" if ctx.dry_run else "apply",
        "dry_run": ctx.dry_run,
        "confirmed": ctx.confirmed,
        "rows_loaded": c1["rows_loaded"],
        "rows_pending": c1["rows_pending"],
        "rows_recognized": c1["rows_recognized"],
        "rows_planned": c2["rows_planned"],
        "jobs_planned": c2["jobs_planned"],
        "rows_written": 0,
        "rows_skipped": c2["rows_skipped_build"],
        "warning_count": len(warnings_all),
        "edit_url": sh_edit.url,
        "config_url": sh_cfg.url,
        "runlog_url": sh_runlog.url,
        "worksheet_edit": ctx.tab_edit_update,
        "worksheet_runlog": ctx.tab_runlog,
    }

    logger.add(
        phase=summary["phase"],
        log_type="summary",
        status="OK",
        entity_type="METAOBJECT_ENTRY",
        gid="",
        field_key="summary",
        rows_loaded=summary["rows_loaded"],
        rows_pending=summary["rows_pending"],
        rows_recognized=summary["rows_recognized"],
        rows_planned=summary["rows_planned"],
        rows_written=summary["rows_written"],
        rows_skipped=summary["rows_skipped"],
        message=f"pre-execute summary; jobs_planned={summary['jobs_planned']}; warnings={summary['warning_count']}",
        error_reason="",
    )

    if warnings_all:
        for w in warnings_all[:20]:
            logger.add_detail_limited(
                phase=summary["phase"],
                status="WARN",
                entity_type="METAOBJECT_ENTRY",
                gid="",
                field_key="warning",
                message=w,
                error_reason="WARNING",
            )

    progress(
        f"Starting {'preview' if ctx.dry_run else 'apply'} execution "
        f"for {len(jobs)} job(s)"
    )
    exec_result = execute_jobs(jobs, resolver, sc, logger, ctx)
    progress(
        f"Execution complete: ok={exec_result['ok']} "
        f"fail={exec_result['fail']} skip={exec_result['skip']} "
        f"written={exec_result['written']}"
    )
    summary["rows_written"] = exec_result["written"]
    summary["rows_skipped"] = int(summary["rows_skipped"]) + int(exec_result["skip"])
    summary["ok"] = exec_result["ok"]
    summary["fail"] = exec_result["fail"]
    summary["skip"] = exec_result["skip"]

    logger.add(
        phase=summary["phase"],
        log_type="summary",
        status="OK" if exec_result["fail"] == 0 else "PARTIAL_FAIL",
        entity_type="METAOBJECT_ENTRY",
        gid="",
        field_key="summary",
        rows_loaded=summary["rows_loaded"],
        rows_pending=summary["rows_pending"],
        rows_recognized=summary["rows_recognized"],
        rows_planned=summary["rows_planned"],
        rows_written=summary["rows_written"],
        rows_skipped=summary["rows_skipped"],
        message=f"done; ok={summary['ok']} fail={summary['fail']} skip={summary['skip']} jobs={summary['jobs_planned']}",
        error_reason="",
    )
    progress("Flushing run log")
    logger.flush()
    progress(
        f"DONE run_id={ctx.run_id} rows_loaded={summary['rows_loaded']} "
        f"jobs_planned={summary['jobs_planned']} "
        f"rows_written={summary['rows_written']} "
        f"warning_count={summary['warning_count']}"
    )

    return {
        "summary": summary,
        "preview": preview,
        "warnings": warnings_all[:100],
        "jobs_planned": jobs[:ctx.preview_limit],
    }
