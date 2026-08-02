# shopify_export/3_3_1_export_metaobject_entries.py

from __future__ import annotations

import base64
import datetime as dt
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo


# =========================================================
# Defaults
# =========================================================
DEFAULT_TZ_NAME = "Asia/Shanghai"
DEFAULT_API_VERSION = "2026-01"

MODULE_PATH = "shopify_export.3_3_1_export_metaobject_entries"
MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
DEFAULT_JOB_NAME = "export_metaobject_entries"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

TAB_CFG_SITES = "Cfg__Sites"
TAB_METAOBJECT_DEFS = "Cfg__MetaobjectDefs"
TAB_EXPORT = "MetaobjectEntries"
TAB_RUNLOG = "Ops__RunLog"

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


# =========================================================
# GraphQL
# =========================================================
Q_METAOBJECTS_RICH = """
query ($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      updatedAt
      fields {
        key
        type
        value
        references(first: 50) {
          nodes {
            __typename
            ... on Metaobject { id handle }
            ... on Product { id handle }
            ... on ProductVariant { id sku }
            ... on Collection { id handle }
            ... on Page { id handle }
          }
        }
        reference {
          __typename
          ... on Metaobject { id handle }
          ... on Product { id handle }
          ... on ProductVariant { id sku }
          ... on Collection { id handle }
          ... on Page { id handle }
        }
      }
    }
  }
}
"""

Q_METAOBJECTS_NO_UPDATED = """
query ($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      fields {
        key
        type
        value
        references(first: 50) {
          nodes {
            __typename
            ... on Metaobject { id handle }
            ... on Product { id handle }
            ... on ProductVariant { id sku }
            ... on Collection { id handle }
            ... on Page { id handle }
          }
        }
        reference {
          __typename
          ... on Metaobject { id handle }
          ... on Product { id handle }
          ... on ProductVariant { id sku }
          ... on Collection { id handle }
          ... on Page { id handle }
        }
      }
    }
  }
}
"""

Q_METAOBJECTS_MIN = """
query ($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      fields {
        key
        type
        value
      }
    }
  }
}
"""


# =========================================================
# Models
# =========================================================
@dataclass
class ShopifyClient:
    shop_domain: str
    access_token: str
    api_version: str = DEFAULT_API_VERSION
    timeout: int = 60
    min_sleep: float = 0.10
    max_retries: int = 5

    def __post_init__(self) -> None:
        self.url = f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
            }
        )

    def gql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(
                    self.url,
                    json={"query": query, "variables": variables},
                    timeout=self.timeout,
                )
                text = resp.text
                if resp.status_code >= 500 or resp.status_code == 429:
                    raise RuntimeError(f"HTTP {resp.status_code}: {text[:500]}")

                data = resp.json()
                if data.get("errors"):
                    raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False)[:1000])

                # soft throttle sleep
                try:
                    throttle = (
                        data.get("extensions", {})
                        .get("cost", {})
                        .get("throttleStatus", {})
                    )
                    currently_available = throttle.get("currentlyAvailable")
                    restore_rate = throttle.get("restoreRate")
                    if currently_available is not None and restore_rate:
                        if currently_available < 100:
                            time.sleep(max(0.2, 60.0 / max(restore_rate, 1)))
                        else:
                            time.sleep(self.min_sleep)
                    else:
                        time.sleep(self.min_sleep)
                except Exception:
                    time.sleep(self.min_sleep)

                return data
            except Exception as e:
                last_err = e
                if attempt == self.max_retries:
                    raise
                time.sleep(min(8, 0.7 * attempt + random.random()))
        raise RuntimeError(f"GraphQL failed: {last_err}")



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
    return _clean_str(value).upper()


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
    """Resolve one Secret without exposing its value."""
    secret_name = _clean_str(name)
    resolved_project_code = _normalize_project_code(project_code)

    if not secret_name:
        raise ValueError("Secret name is empty.")
    if not resolved_project_code:
        raise ValueError("PROJECT_CODE is required for Secret resolution.")

    if explicit_value is not None and _clean_str(explicit_value):
        return SecretValue(
            _clean_str(explicit_value),
            "EXPLICIT_VALUE",
            "caller",
        )

    if _runtime_mode() == "COLAB":
        try:
            from google.colab import userdata  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "Colab Secret adapter is unavailable."
            ) from exc

        value = userdata.get(secret_name)
        if value is None or not str(value).strip():
            raise ValueError(
                f"Colab Secret {secret_name!r} is missing "
                "or not enabled for this notebook."
            )

        return SecretValue(
            str(value).strip(),
            "COLAB_SECRETS",
            secret_name,
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
    raw = _clean_str(raw_value)
    if not raw:
        raise ValueError(
            "Google service-account Secret is empty."
        )

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
            "Google Secret is not a complete service-account "
            f"credential; missing={missing}."
        )

    return info, secret_format


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
    return any(
        token in err_text
        for token in quota_tokens
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
        _clean_str(value).lower(),
    ).strip()


def _extract_spreadsheet_id(
    value: Any,
) -> str:
    raw = _clean_str(value)

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


def _build_gc_from_secret_value(
    raw_value: str,
) -> gspread.Client:
    info, _secret_format = _parse_service_account_text(
        raw_value
    )
    creds = Credentials.from_service_account_info(
        info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


def resolve_workspace_project(
    *,
    project_code: str,
    workspace_registry_id: str,
    workspace_gsheet_secret_name: str = "WORKSPACE_GSHEET",
    workspace_registry_tab: str = "Cfg__Projects",
    secret_home: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, str]:
    resolved_project_code = _normalize_project_code(
        project_code
    )

    if not resolved_project_code:
        raise ValueError(
            "project_code is required."
        )

    workspace_secret = read_secret(
        workspace_gsheet_secret_name,
        project_code="WORKSPACE",
        secret_home=secret_home,
    )

    workspace_gc = _build_gc_from_secret_value(
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

    header_map: Dict[str, int] = {}
    duplicate_headers: List[str] = []

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
                sorted(set(duplicate_headers))
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
    timezone_col = require_column(
        "timezone",
        "time zone",
    )

    project_name_col = header_map.get(
        _normalize_registry_header(
            "project_name"
        )
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
            _clean_str(
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

    active_text = _clean_str(
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
            _clean_str(
                row[project_name_col]
            )
            if project_name_col is not None
            else ""
        ),
        "console_core_url": _clean_str(
            row[console_url_col]
        ),
        "gsheet_secret_name": _clean_str(
            row[gsheet_secret_col]
        ),
        "account_config_tab": _clean_str(
            row[account_tab_col]
        ),
        "timezone": _clean_str(
            row[timezone_col]
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
            "timezone",
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
        lambda: gc.open_by_url(
            console_core_url
        ),
        action="account_config.open_console",
    )
    ws = _with_sheets_retry(
        lambda: sh.worksheet(
            account_config_tab
        ),
        action="account_config.open_tab",
    )
    values = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="account_config.read",
    )

    if not values:
        raise ValueError(
            f"{account_config_tab} is empty"
        )

    rows = [
        [
            _clean_str(cell)
            for cell in row
        ]
        for row in values
    ]

    first = (
        rows[0] + ["", ""]
    )[:2]
    first_lower = [
        value.lower()
        for value in first
    ]

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

    data_rows = (
        rows[1:]
        if has_header
        else rows
    )

    out: Dict[str, str] = {}

    for row in data_rows:
        if not row:
            continue

        key = (
            _clean_str(row[0])
            if len(row) >= 1
            else ""
        )
        value = (
            _clean_str(row[1])
            if len(row) >= 2
            else ""
        )

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

    gc = _build_gc_from_secret_value(
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
        if not _clean_str(
            account.get(key)
        )
    ]

    if missing:
        raise ValueError(
            f"{route['account_config_tab']} missing "
            f"required account config keys: {missing}"
        )

    if (
        _clean_str(
            account["GSHEET_SA_B64_SECRET"]
        )
        != route["gsheet_secret_name"]
    ):
        raise ValueError(
            "Workspace Registry Google Secret does not "
            "match Cfg__account_id. "
            f"registry={route['gsheet_secret_name']}; "
            "cfg="
            f"{account['GSHEET_SA_B64_SECRET']}"
        )

    shopify_secret = read_secret(
        account["SHOPIFY_TOKEN_SECRET"],
        project_code=route["project_code"],
        secret_home=secret_home,
    )

    result = {
        "project_route": route,
        "account": {
            "shop_domain": _clean_str(
                account["SHOP_DOMAIN"]
            ),
            "api_version": _clean_str(
                account["SHOPIFY_API_VERSION"]
            ),
            "gsheet_secret_name": _clean_str(
                account["GSHEET_SA_B64_SECRET"]
            ),
            "shopify_token_secret_name": _clean_str(
                account["SHOPIFY_TOKEN_SECRET"]
            ),
        },
        "credentials": {
            "gsheet_sa_value": (
                project_google_secret.value
            ),
            "shopify_access_token": (
                shopify_secret.value
            ),
        },
        "auth": {
            "runtime_mode": _runtime_mode(),
            "workspace_secret_source_type": (
                route["workspace_auth_source_type"]
            ),
            "project_google_secret_source_type": (
                project_google_secret.source_type
            ),
            "shopify_secret_source_type": (
                shopify_secret.source_type
            ),
        },
    }

    if print_progress:
        print(
            "[Runtime Auth] ready | "
            f"project={route['project_code']} | "
            "google_source="
            f"{project_google_secret.source_type} | "
            "shopify_source="
            f"{shopify_secret.source_type} | "
            "shop="
            f"{result['account']['shop_domain']} | "
            "api="
            f"{result['account']['api_version']}"
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
    mode = _clean_str(
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
        mode in {
            "UPDATE_URL",
            "UPDATE_URL_AND_NAME",
        }
        and not _clean_str(
            current_colab_url
        )
    ):
        raise ValueError(
            f"registry_mode={mode} "
            "requires current_colab_url."
        )

    if (
        mode == "UPDATE_URL_AND_NAME"
        and not _clean_str(
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

    gc = _build_gc_from_secret_value(
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

    header_map: Dict[str, int] = {}

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
                "Registry tab has duplicate normalized "
                f"header: {normalized}."
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
        _clean_str(job_name).lower(),
        _clean_str(sheet_label).lower(),
        _clean_str(tab_name).lower(),
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
            _clean_str(
                padded[job_col]
            ).lower(),
            _clean_str(
                padded[label_col]
            ).lower(),
            _clean_str(
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
                values[row_number - 1]
            ),
        )
    )

    changes: List[
        Tuple[
            str,
            int,
            str,
            str,
        ]
    ] = []

    provided_url = _clean_str(
        current_colab_url
    )
    provided_name = _clean_str(
        current_colab_name
    )

    if (
        provided_url
        and _clean_str(
            current_row[url_col]
        )
        != provided_url
    ):
        changes.append(
            (
                "colab_url",
                url_col + 1,
                _clean_str(
                    current_row[url_col]
                ),
                provided_url,
            )
        )

    if (
        provided_name
        and _clean_str(
            current_row[name_col]
        )
        != provided_name
    ):
        changes.append(
            (
                "colab_name",
                name_col + 1,
                _clean_str(
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
# General helpers
# =========================================================
def _now_cn_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def _gen_run_id(job_name: str, tz_name: str) -> str:
    return f"{job_name}_{dt.datetime.now(ZoneInfo(tz_name)).strftime('%Y%m%d_%H%M%S')}"


def _build_gc_from_sa_b64(sa_b64: str) -> gspread.Client:
    info, _secret_format = _parse_service_account_text(sa_b64)
    creds = Credentials.from_service_account_info(
        info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


def _open_ws_by_url(gc: gspread.Client, url: str, title: str):
    sh = _with_sheets_retry(
        lambda: gc.open_by_url(url),
        action=f"spreadsheet.open:{title}",
    )
    return _with_sheets_retry(
        lambda: sh.worksheet(title),
        action=f"worksheet.open:{title}",
    )


def _ws_to_df(ws) -> pd.DataFrame:
    values = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action=f"worksheet.read:{ws.title}",
    )
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    width = len(header)
    norm_rows = []
    for row in rows:
        row = list(row[:width]) + [""] * max(0, width - len(row))
        norm_rows.append(row)
    return pd.DataFrame(norm_rows, columns=header)


def _ensure_runlog_header(ws) -> None:
    cur = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="runlog.read_header",
    )
    if not cur or cur[0] != RUNLOG_HEADER_18:
        _with_sheets_retry(
            lambda: ws.clear(),
            action="runlog.clear_for_header",
            retry_5xx=True,
        )
        _with_sheets_retry(
            lambda: ws.update(
                range_name="A1:R1",
                values=[RUNLOG_HEADER_18],
                value_input_option="RAW",
            ),
            action="runlog.write_header",
            retry_5xx=True,
        )


def _append_rows_safe(ws, rows: List[List[Any]]) -> None:
    if rows:
        _with_sheets_retry(
            lambda: ws.append_rows(
                rows,
                value_input_option="RAW",
            ),
            action="runlog.append_rows",
            # Append is not safely repeatable after an ambiguous 5xx.
            retry_5xx=False,
        )


def _clean_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _is_true(x: Any) -> bool:
    return _clean_str(x).upper() in {"TRUE", "1", "YES", "Y"}


def _unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _is_list_field_type(field_type: str) -> bool:
    return _clean_str(field_type).startswith("list.")


def _safe_json_loads(s: Any) -> Optional[Any]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _extract_handle_or_sku(node: Dict[str, Any]) -> str:
    if not isinstance(node, dict):
        return ""
    return _clean_str(node.get("handle") or node.get("sku") or "")


# =========================================================
# Cfg__Sites routing
# =========================================================
def _resolve_site_urls(gc: gspread.Client, console_core_url: str, site_code: str) -> Dict[str, str]:
    ws = _open_ws_by_url(gc, console_core_url, TAB_CFG_SITES)
    df = _ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    df.columns = [str(c).strip() for c in df.columns]
    need = {"site_code", "sheet_url", "label"}
    if not need.issubset(set(df.columns)):
        raise ValueError(f"Cfg__Sites missing columns: {sorted(list(need - set(df.columns)))}")

    dfx = df[df["site_code"].astype(str).str.strip().str.upper() == site_code.strip().upper()].copy()
    if dfx.empty:
        raise ValueError(f"Cfg__Sites: site_code not found: {site_code}")

    out: Dict[str, str] = {}
    for _, r in dfx.iterrows():
        label = _clean_str(r.get("label"))
        sheet_url = _clean_str(r.get("sheet_url"))
        if label and sheet_url:
            out[label] = sheet_url

    required_labels = ["config", "export_other", "runlog_sheet"]
    missing = [x for x in required_labels if x not in out]
    if missing:
        raise ValueError(f"Cfg__Sites missing labels for {site_code}: {missing}")

    return out


# =========================================================
# Defs
# =========================================================
def load_defs_df(gc: gspread.Client, config_url: str) -> pd.DataFrame:
    ws = _open_ws_by_url(gc, config_url, TAB_METAOBJECT_DEFS)
    df = _ws_to_df(ws)
    if df.empty:
        return pd.DataFrame(columns=["type", "type_name", "field_key", "field_type"])

    df.columns = [str(c).strip() for c in df.columns]
    for c in ["type", "field_key", "field_type", "type_name"]:
        if c not in df.columns:
            df[c] = ""
    df["type"] = df["type"].astype(str).str.strip()
    df["field_key"] = df["field_key"].astype(str).str.strip()
    df["field_type"] = df["field_type"].astype(str).str.strip()
    df["type_name"] = df["type_name"].astype(str).str.strip()
    df = df[(df["type"] != "") & (df["field_key"] != "")].copy()
    return df


def resolve_types_to_export(defs_df: pd.DataFrame, only_types: Optional[List[str]] = None) -> List[str]:
    if only_types:
        return [str(x).strip() for x in only_types if str(x).strip()]
    if defs_df.empty:
        return []
    return _unique_keep_order(defs_df["type"].astype(str).tolist())


# =========================================================
# Fetch
# =========================================================
def _pick_query_variant(client: ShopifyClient, mo_type: str) -> Tuple[str, str]:
    probes = [
        ("RICH", Q_METAOBJECTS_RICH),
        ("NO_UPDATED", Q_METAOBJECTS_NO_UPDATED),
        ("MIN", Q_METAOBJECTS_MIN),
    ]
    last_err = None
    for name, q in probes:
        try:
            client.gql(q, {"type": mo_type, "first": 1, "after": None})
            return name, q
        except Exception as e:
            last_err = e
    raise RuntimeError(f"All query variants failed for type={mo_type}: {last_err}")


def fetch_metaobjects_for_type(
    client: ShopifyClient,
    mo_type: str,
) -> Tuple[List[Dict[str, Any]], str]:
    variant_name, query = _pick_query_variant(client, mo_type)

    rows: List[Dict[str, Any]] = []
    after = None
    while True:
        data = client.gql(query, {"type": mo_type, "first": 100, "after": after})
        root = data["data"]["metaobjects"]
        nodes = root.get("nodes", []) or []
        rows.extend(nodes)
        page = root.get("pageInfo", {}) or {}
        if not page.get("hasNextPage"):
            break
        after = page.get("endCursor")
    return rows, variant_name


# =========================================================
# Flatten
# =========================================================
def _flatten_field_record(field: Dict[str, Any]) -> Dict[str, Any]:
    """
    return:
      {
        "key": "...",
        "type": "...",
        "value": "...",
        "single_ref_handle_or_sku": "...",
        "list_handles_or_skus": [...],
      }
    """
    out = {
        "key": _clean_str(field.get("key")),
        "type": _clean_str(field.get("type")),
        "value": field.get("value"),
        "single_ref_handle_or_sku": "",
        "list_handles_or_skus": [],
    }

    ref = field.get("reference")
    if isinstance(ref, dict):
        out["single_ref_handle_or_sku"] = _extract_handle_or_sku(ref)

    refs = (((field.get("references") or {}).get("nodes")) if isinstance(field.get("references"), dict) else None)
    if isinstance(refs, list):
        out["list_handles_or_skus"] = [_extract_handle_or_sku(x) for x in refs if _extract_handle_or_sku(x)]

    return out


def _normalize_scalar_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def _parse_list_value(raw_value: Any) -> List[str]:
    """
    Shopify list values often come as JSON array strings.
    """
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(x) for x in raw_value]
    sv = str(raw_value).strip()
    if not sv:
        return []
    parsed = _safe_json_loads(sv)
    if isinstance(parsed, list):
        return ["" if x is None else str(x) for x in parsed]
    # fallback: treat as single item
    return [sv]


def build_export_rows(
    raw_by_type: Dict[str, List[Dict[str, Any]]],
    defs_df: pd.DataFrame,
    list_join_sep: str = " | ",
    split_hard_cap: int = 50,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    """
    returns:
      df_entries,
      split_widths: {"type.field_key": max_cols},
      extras: ["type.field_key", ...]
    """
    defs_order: Dict[str, List[Tuple[str, str]]] = {}
    defs_ftype_map: Dict[str, str] = {}

    if not defs_df.empty:
        for mo_type, g in defs_df.groupby("type", sort=False):
            items = []
            for _, r in g.iterrows():
                fk = _clean_str(r["field_key"])
                ft = _clean_str(r["field_type"])
                items.append((fk, ft))
                defs_ftype_map[f"{mo_type}.{fk}"] = ft
            defs_order[mo_type] = items

    system_cols = [
        "_sys.type",
        "_sys.entry_gid",
        "_sys.handle",
        "_sys.updated_at",
        "_sys.synced_at",
    ]

    rows_out: List[Dict[str, Any]] = []
    split_widths: Dict[str, int] = {}
    extras_seen: List[str] = []

    for mo_type, entries in raw_by_type.items():
        for node in entries:
            row: Dict[str, Any] = {
                "_sys.type": mo_type,
                "_sys.entry_gid": _clean_str(node.get("id")),
                "_sys.handle": _clean_str(node.get("handle")),
                "_sys.updated_at": _clean_str(node.get("updatedAt")),
                "_sys.synced_at": "",
            }

            fields = node.get("fields") or []
            if not isinstance(fields, list):
                fields = []

            actual_field_keys: List[str] = []

            for field in fields:
                ff = _flatten_field_record(field)
                fk = ff["key"]
                if not fk:
                    continue

                actual_field_keys.append(fk)
                full_key = f"{mo_type}.{fk}"
                field_type = defs_ftype_map.get(full_key, _clean_str(ff["type"]))

                # list.*
                if _is_list_field_type(field_type):
                    items = _parse_list_value(ff["value"])
                    handles = ff["list_handles_or_skus"]

                    row[full_key] = list_join_sep.join(items)

                    width = min(max(len(items), 1), split_hard_cap)
                    split_widths[full_key] = max(split_widths.get(full_key, 1), width)

                    for i in range(width):
                        col = full_key if i == 0 else f"{full_key}_{i}"
                        row[col] = items[i] if i < len(items) else ""

                    if handles:
                        hwidth = min(max(len(handles), 1), split_hard_cap)
                        split_widths[f"{full_key}__handle"] = max(
                            split_widths.get(f"{full_key}__handle", 1), hwidth
                        )
                        row[f"{full_key}__handle"] = handles[0] if len(handles) >= 1 else ""
                        for i in range(hwidth):
                            col = f"{full_key}__handle" if i == 0 else f"{full_key}__handle_{i}"
                            row[col] = handles[i] if i < len(handles) else ""
                else:
                    scalar = _normalize_scalar_value(ff["value"])
                    row[full_key] = scalar

                    # single reference extra readable col
                    ref_h = ff["single_ref_handle_or_sku"]
                    if ref_h:
                        row[f"{full_key}__handle"] = ref_h

            # extras from actual fields not covered by defs
            defs_keys_this_type = {fk for fk, _ in defs_order.get(mo_type, [])}
            for fk in actual_field_keys:
                if fk not in defs_keys_this_type:
                    extras_seen.append(f"{mo_type}.{fk}")

            rows_out.append(row)

    df = pd.DataFrame(rows_out) if rows_out else pd.DataFrame(columns=system_cols)
    if df.empty:
        return pd.DataFrame(columns=system_cols), split_widths, _unique_keep_order(extras_seen)

    # final ordered columns
    final_cols: List[str] = list(system_cols)

    # defs main order
    for mo_type, pairs in defs_order.items():
        for fk, ft in pairs:
            base = f"{mo_type}.{fk}"
            if _is_list_field_type(ft):
                width = max(split_widths.get(base, 1), 1)
                for i in range(width):
                    final_cols.append(base if i == 0 else f"{base}_{i}")

                hbase = f"{base}__handle"
                if hbase in df.columns or split_widths.get(hbase):
                    hwidth = max(split_widths.get(hbase, 1), 1)
                    for i in range(hwidth):
                        final_cols.append(hbase if i == 0 else f"{hbase}_{i}")
            else:
                final_cols.append(base)
                hbase = f"{base}__handle"
                if hbase in df.columns:
                    final_cols.append(hbase)

    # extras tail
    extras_unique = _unique_keep_order(extras_seen)
    for base in extras_unique:
        if base in final_cols:
            continue
        if base in df.columns:
            final_cols.append(base)
        # append split siblings if exist
        i = 1
        while f"{base}_{i}" in df.columns:
            final_cols.append(f"{base}_{i}")
            i += 1
        if f"{base}__handle" in df.columns:
            final_cols.append(f"{base}__handle")
            j = 1
            while f"{base}__handle_{j}" in df.columns:
                final_cols.append(f"{base}__handle_{j}")
                j += 1

    # any remaining columns not yet included
    for c in df.columns.tolist():
        if c not in final_cols:
            final_cols.append(c)

    for c in final_cols:
        if c not in df.columns:
            df[c] = ""

    df = df[final_cols].copy()
    df["_sys.synced_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df.fillna(""), split_widths, extras_unique


# =========================================================
# 2-row header
# =========================================================
def build_two_row_header(cols: List[str]) -> List[List[str]]:
    row1: List[str] = []
    row2: List[str] = []

    for c in cols:
        if c.startswith("_sys."):
            row1.append("_sys")
            row2.append(c.replace("_sys.", "", 1))
            continue

        base = c
        suffix = ""

        m = re.match(r"^(.*?)(__handle(?:_\d+)?|_\d+)$", c)
        if m:
            base = m.group(1)
            suffix = m.group(2)

        parts = base.split(".", 1)
        if len(parts) == 2:
            mo_type, field_key = parts
        else:
            mo_type, field_key = "", base

        row1.append(mo_type)
        row2.append(field_key + suffix)

    return [row1, row2]


# =========================================================
# Write export sheet
# =========================================================
def _read_existing_header_2rows(ws) -> List[List[str]]:
    vals = _with_sheets_retry(
        lambda: ws.get_all_values(),
        action="export.read_existing_header",
    )
    if len(vals) >= 2:
        return [vals[0], vals[1]]
    return []


def write_export_sheet(
    ws,
    df_entries: pd.DataFrame,
    write_mode: str = "OVERWRITE",
) -> Dict[str, Any]:
    cols = df_entries.columns.tolist()
    header2 = build_two_row_header(cols)
    body = df_entries.astype(str).fillna("").values.tolist()
    values = header2 + body

    write_mode = _clean_str(write_mode).upper() or "OVERWRITE"

    if write_mode == "APPEND":
        existing = _read_existing_header_2rows(ws)
        if existing != header2:
            raise ValueError("APPEND aborted: target header != current generated header")
        _with_sheets_retry(
            lambda: ws.append_rows(
                body,
                value_input_option="RAW",
            ),
            action="export.append_rows",
            retry_5xx=False,
        )
        return {
            "rows_written": len(body),
            "cols_written": len(cols),
            "write_mode": "APPEND",
        }

    # default overwrite
    _with_sheets_retry(
        lambda: ws.clear(),
        action="export.clear",
        retry_5xx=True,
    )
    end_col = max(len(cols), 1)
    # simple full update
    _with_sheets_retry(
        lambda: ws.update(
            values=values,
            range_name=(
                f"A1:{_col_num_to_a1(end_col)}{len(values)}"
            ),
            value_input_option="RAW",
        ),
        action="export.write_full_range",
        retry_5xx=True,
    )
    return {
        "rows_written": len(body),
        "cols_written": len(cols),
        "write_mode": "OVERWRITE",
    }


def _col_num_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


# =========================================================
# Runlog
# =========================================================
def _make_log_row(
    *,
    run_id: str,
    ts_cn: str,
    job_name: str,
    phase: str,
    log_type: str,
    status: str,
    site_code: str,
    entity_type: str = "METAOBJECT_ENTRY",
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
) -> List[Any]:
    return [
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
        message,
        error_reason,
    ]


def write_runlog(
    ws_log,
    summary_row: List[Any],
    detail_rows: List[List[Any]],
) -> None:
    _ensure_runlog_header(ws_log)
    rows = [summary_row] + detail_rows
    _append_rows_safe(ws_log, rows)


# =========================================================
# Summary text
# =========================================================
def _format_split_summary(split_widths: Dict[str, int], limit: int = 20) -> str:
    if not split_widths:
        return "No dynamic split columns."
    items = sorted(split_widths.items(), key=lambda x: (-x[1], x[0]))
    lines = []
    for k, v in items[:limit]:
        lines.append(f"{k} -> {v} col(s)")
    if len(items) > limit:
        lines.append(f"... and {len(items) - limit} more")
    return "\n".join(lines)


# =========================================================
# Public run
# =========================================================
def run(
    *,
    site_code: str,
    console_core_url: str,
    shop_domain: str,
    shopify_access_token: str,
    gsheet_sa_b64: str,
    api_version: str = DEFAULT_API_VERSION,
    job_name: str = "export_metaobject_entries",
    tz_name: str = DEFAULT_TZ_NAME,
    only_types: Optional[List[str]] = None,
    write_mode: str = "OVERWRITE",   # OVERWRITE / APPEND
    dry_run: bool = False,
    export_tab_name: str = TAB_EXPORT,
    runlog_tab_name: str = TAB_RUNLOG,
    list_join_sep: str = " | ",
    split_hard_cap: int = 50,
    print_preview_rows: int = 5,
) -> Dict[str, Any]:
    """
    Pure export job:
    - route by Cfg__Sites
    - defs from config/Cfg__MetaobjectDefs
    - output to export_other/MetaobjectEntries
    - 2-row header
    - APPEND only if header exactly matches
    """
    ts_cn = _now_cn_str(tz_name)
    run_id = _gen_run_id(job_name, tz_name)

    gc = _build_gc_from_sa_b64(gsheet_sa_b64)
    urls = _resolve_site_urls(gc, console_core_url, site_code)

    config_url = urls["config"]
    export_url = urls["export_other"]
    runlog_url = urls["runlog_sheet"]

    defs_df = load_defs_df(gc, config_url)
    types_to_export = resolve_types_to_export(defs_df, only_types=only_types)
    if not types_to_export:
        raise ValueError("No metaobject types resolved from Cfg__MetaobjectDefs")

    client = ShopifyClient(
        shop_domain=shop_domain,
        access_token=shopify_access_token,
        api_version=api_version,
    )

    raw_by_type: Dict[str, List[Dict[str, Any]]] = {}
    query_variants: Dict[str, str] = {}
    detail_rows: List[List[Any]] = []
    detail_error_counter: Dict[str, int] = {}

    total_loaded = 0
    for mo_type in types_to_export:
        try:
            nodes, qv = fetch_metaobjects_for_type(client, mo_type)
            raw_by_type[mo_type] = nodes
            query_variants[mo_type] = qv
            total_loaded += len(nodes)
        except Exception as e:
            reason = "FETCH_TYPE_FAILED"
            if detail_error_counter.get(reason, 0) < 2:
                detail_rows.append(
                    _make_log_row(
                        run_id=run_id,
                        ts_cn=ts_cn,
                        job_name=job_name,
                        phase="preview" if dry_run else "apply",
                        log_type="detail",
                        status="ERROR",
                        site_code=site_code,
                        field_key=mo_type,
                        message=str(e)[:500],
                        error_reason=reason,
                    )
                )
                detail_error_counter[reason] = detail_error_counter.get(reason, 0) + 1

    df_entries, split_widths, extras = build_export_rows(
        raw_by_type=raw_by_type,
        defs_df=defs_df,
        list_join_sep=list_join_sep,
        split_hard_cap=split_hard_cap,
    )

    rows_planned = len(df_entries)
    rows_written = 0
    rows_skipped = 0
    status = "OK"
    write_result: Dict[str, Any] = {}

    split_text = _format_split_summary(split_widths)
    extras_text = ", ".join(extras[:30]) if extras else ""

    if not dry_run:
        try:
            ws_export = _open_ws_by_url(gc, export_url, export_tab_name)
            write_result = write_export_sheet(ws_export, df_entries, write_mode=write_mode)
            rows_written = int(write_result["rows_written"])
        except Exception as e:
            status = "ERROR"
            reason = "WRITE_EXPORT_FAILED"
            if detail_error_counter.get(reason, 0) < 2:
                detail_rows.append(
                    _make_log_row(
                        run_id=run_id,
                        ts_cn=ts_cn,
                        job_name=job_name,
                        phase="apply",
                        log_type="detail",
                        status="ERROR",
                        site_code=site_code,
                        message=str(e)[:500],
                        error_reason=reason,
                    )
                )
                detail_error_counter[reason] = detail_error_counter.get(reason, 0) + 1
    else:
        rows_skipped = rows_planned

    summary_msg_parts = [
        f"types={len(types_to_export)}",
        f"rows_loaded={total_loaded}",
        f"rows_planned={rows_planned}",
        f"rows_written={rows_written}",
        f"write_mode={_clean_str(write_mode).upper()}",
        f"query_variants={json.dumps(query_variants, ensure_ascii=False)}",
        f"dynamic_splits={len(split_widths)}",
    ]
    if extras:
        summary_msg_parts.append(f"defs_missing_fields={extras_text}")

    summary_row = _make_log_row(
        run_id=run_id,
        ts_cn=ts_cn,
        job_name=job_name,
        phase="preview" if dry_run else "apply",
        log_type="summary",
        status=status,
        site_code=site_code,
        rows_loaded=total_loaded,
        rows_pending=rows_planned if dry_run else "",
        rows_recognized=rows_planned,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message=" | ".join(summary_msg_parts),
        error_reason="",
    )

    try:
        ws_log = _open_ws_by_url(gc, runlog_url, runlog_tab_name)
        write_runlog(ws_log, summary_row, detail_rows)
    except Exception as e:
        print(f"[WARN] runlog write failed: {e}")

    preview_df = df_entries.head(print_preview_rows).copy()

    # console output
    print("=" * 80)
    print(f"run_id      : {run_id}")
    print(f"job_name    : {job_name}")
    print(f"site_code   : {site_code}")
    print(f"shop_domain : {shop_domain}")
    print(f"types       : {types_to_export}")
    print(f"rows_loaded : {total_loaded}")
    print(f"rows_planned: {rows_planned}")
    print(f"rows_written: {rows_written}")
    print(f"write_mode  : {_clean_str(write_mode).upper()}")
    print(f"dry_run     : {dry_run}")
    print("-" * 80)
    print("Query variants:")
    for k, v in query_variants.items():
        print(f"  - {k}: {v}")
    print("-" * 80)
    print("Dynamic split summary:")
    print(split_text)
    print("-" * 80)
    if extras:
        print("Defs missing fields found in live data:")
        for x in extras[:50]:
            print(f"  - {x}")
    else:
        print("Defs coverage: no extra live fields found.")
    print("=" * 80)

    return {
        "ok": status == "OK",
        "run_id": run_id,
        "job_name": job_name,
        "site_code": site_code,
        "types_to_export": types_to_export,
        "query_variants": query_variants,
        "rows_loaded": total_loaded,
        "rows_planned": rows_planned,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "write_mode": _clean_str(write_mode).upper(),
        "split_widths": split_widths,
        "defs_missing_fields": extras,
        "preview": preview_df,
        "df_entries": df_entries,
        "write_result": write_result,
    }
