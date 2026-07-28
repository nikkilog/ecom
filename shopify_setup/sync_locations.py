# -*- coding: utf-8 -*-
"""Central Shopify Location registry synchronization.

GitHub target: ``ecom/shopify_setup/sync_locations.py``
Import path: ``shopify_setup.sync_locations``

The module synchronizes Shopify Admin GraphQL locations into
Console Core / ``Cfg__Locations`` while preserving human-governed fields.
It supports both Colab and local Jupyter/CLI execution without OAuth popups.

Secret resolution order:
- explicit value passed by the caller;
- exact Colab Secret name when running in Colab;
- the shared ``workspace_secret_resolver`` contract when running locally.

The active ``project_code`` selects the local Secret files. The logical Google
Secret name remains independently configurable, so a Colab Secret such as
``Apollo_GSHEET`` can map locally to ``Google/APOLLO_GSHEET_keys-*.json``.

Real writes require ``dry_run=False`` and ``confirmed=True``.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import math
import platform
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo


MODULE_VERSION = "2.3.0"
MODULE_PATH = "shopify_setup.sync_locations"
DEFAULT_JOB_NAME = "config_locations"
LOCATION_HEADERS = [
    "site_code",
    "location_code",
    "location_name",
    "location_gid",
    "province_code",
    "active",
    "is_default",
    "notes",
    "synced_at",
]

SYSTEM_MANAGED_FIELDS = {
    "site_code",
    "location_name",
    "location_gid",
    "province_code",
    "active",
    "synced_at",
}

HUMAN_MANAGED_FIELDS = {
    "location_code",
    "is_default",
    "notes",
}

LEGACY_HEADER_ALIASES = {
    "库存列名": "location_code",
    "shopify location 名": "location_name",
    "shopify location id": "location_gid",
    "isactive": "active",
    "provincecode": "province_code",
}

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

Q_LOCATIONS = """
query Locations(
  $first: Int!,
  $after: String,
  $includeInactive: Boolean!,
  $includeLegacy: Boolean!
) {
  locations(
    first: $first,
    after: $after,
    includeInactive: $includeInactive,
    includeLegacy: $includeLegacy
  ) {
    nodes {
      id
      name
      isActive
      address {
        provinceCode
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


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


class ShopifyGraphQLClient:
    def __init__(
        self,
        shop_domain: str,
        api_version: str,
        access_token: str,
        *,
        timeout: int = 90,
        print_progress: bool = True,
    ) -> None:
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        self.timeout = int(timeout)
        self.print_progress = bool(print_progress)
        self.session = requests.Session()

    def gql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        retry: int = 6,
    ) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        last_error: Optional[BaseException] = None

        for attempt in range(1, max(1, int(retry)) + 1):
            try:
                response = self.session.post(
                    self.url,
                    headers=self.headers,
                    json=payload,
                    timeout=self.timeout,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    delay = min(2 ** (attempt - 1), 20) + random.random()
                    if self.print_progress:
                        print(
                            "[Shopify retry] "
                            f"attempt={attempt}/{retry} "
                            f"http={response.status_code} "
                            f"sleep={delay:.1f}s"
                        )
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                body = response.json()
                if body.get("errors"):
                    raise RuntimeError(
                        "Shopify GraphQL errors: "
                        + json.dumps(body["errors"], ensure_ascii=False)
                    )
                data = body.get("data")
                if not isinstance(data, dict):
                    raise RuntimeError("Shopify GraphQL response has no data object.")
                return data
            except BaseException as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= retry:
                    break
                delay = min(2 ** (attempt - 1), 20) + random.random()
                if self.print_progress:
                    print(
                        "[Shopify retry] "
                        f"attempt={attempt}/{retry} "
                        f"error={type(exc).__name__} "
                        f"sleep={delay:.1f}s"
                    )
                time.sleep(delay)

        raise RuntimeError(
            f"Shopify GraphQL failed after {retry} attempts: {last_error}"
        ) from last_error


class RunLogger18:
    def __init__(
        self,
        worksheet: gspread.Worksheet,
        run_id: str,
        job_name: str,
        site_code: str,
        tz_name: str,
    ) -> None:
        self.worksheet = worksheet
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.tz_name = tz_name
        self.buffer: List[List[Any]] = []
        _ensure_runlog_header(worksheet)

    def log(
        self,
        *,
        phase: str,
        log_type: str,
        status: str,
        entity_type: str = "LOCATION",
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
        self.buffer.append(
            [
                self.run_id,
                _now_str(self.tz_name),
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
            ]
        )

    def flush(self) -> None:
        if not self.buffer:
            return
        self.worksheet.append_rows(self.buffer, value_input_option="RAW")
        self.buffer.clear()


def _runtime_mode() -> str:
    try:
        import google.colab  # type: ignore  # noqa: F401

        return "COLAB"
    except Exception:
        return "LOCAL"


def _now_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def _make_run_id(job_name: str, tz_name: str) -> str:
    stamp = dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d_%H%M%S")
    suffix = f"{random.randint(0, 999999):06d}"
    return f"{job_name}_{stamp}_{suffix}"


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _normalize_site_code(value: Any) -> str:
    return _safe_str(value).upper()


def _normalize_header(value: Any) -> str:
    text = _safe_str(value)
    return LEGACY_HEADER_ALIASES.get(text.lower(), text)


def _normalize_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = _safe_str(value).lower()
    if not text:
        return default
    if text in {"true", "1", "yes", "y", "是"}:
        return True
    if text in {"false", "0", "no", "n", "否"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _bool_cell(value: Any, *, default: bool = False) -> str:
    return "TRUE" if _normalize_bool(value, default=default) else "FALSE"


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
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
) -> SecretValue:
    """Read one Secret without printing its value.

    Colab keeps the caller-provided logical name exactly as written. Local
    execution delegates to the independent Workspace Secret Resolver. For a
    Google Sheets Service Account name, the normalized ``PROJECT_CODE_GSHEET``
    name is added as a compatibility alias.

    ``local_secret_aliases`` remains in the signature for caller compatibility
    but local routing is now owned by Workspace Secret Resolver.
    """
    secret_name = _safe_str(name)
    resolved_project_code = _normalize_site_code(project_code)
    if not secret_name:
        raise RuntimeError("Secret name is empty.")
    if not resolved_project_code:
        raise RuntimeError(
            "PROJECT_CODE is required for Local Secret resolution. "
            "Pass the active site/project code explicitly."
        )

    if explicit_value is not None and _safe_str(explicit_value):
        return SecretValue(str(explicit_value).strip(), "EXPLICIT_VALUE", "caller")

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
            "/Users/nikki/Documents/AI_Workspace/Projects/"
            "Workspace_Secret_Resolver"
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

def _parse_service_account(secret: SecretValue) -> Dict[str, Any]:
    raw = secret.value.strip()
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
    info["__secret_format"] = secret_format
    return info


def _build_gspread_client(secret: SecretValue) -> Tuple[gspread.Client, Dict[str, str]]:
    info = _parse_service_account(secret)
    secret_format = str(info.pop("__secret_format"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials), {
        "source_type": secret.source_type,
        "source_detail": secret.source_detail,
        "secret_format": secret_format,
        "service_account_email": _safe_str(info.get("client_email")),
    }


def _load_account_config(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_account_id: str,
) -> AccountConfig:
    values = gc.open_by_url(console_core_url).worksheet(tab_cfg_account_id).get_all_values()
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

    required = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "GSHEET_SA_B64_SECRET",
        "SHOPIFY_TOKEN_SECRET",
    ]
    missing = [key for key in required if not config.get(key)]
    if missing:
        raise ValueError(f"{tab_cfg_account_id} missing required values: {missing}")

    return AccountConfig(
        shop_domain=config["SHOP_DOMAIN"],
        api_version=config["SHOPIFY_API_VERSION"],
        gsheet_sa_b64_secret=config["GSHEET_SA_B64_SECRET"],
        shopify_token_secret=config["SHOPIFY_TOKEN_SECRET"],
    )


def _resolve_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_sites: str,
    site_code: str,
    label: str,
) -> str:
    records = gc.open_by_url(console_core_url).worksheet(tab_cfg_sites).get_all_records()
    matches = [
        row
        for row in records
        if _normalize_site_code(row.get("site_code")) == _normalize_site_code(site_code)
        and _safe_str(row.get("label")) == _safe_str(label)
    ]
    if not matches:
        raise ValueError(
            f"No route in {tab_cfg_sites} for site_code={site_code}, label={label}."
        )
    if len(matches) > 1:
        raise ValueError(
            f"Duplicated route in {tab_cfg_sites} for site_code={site_code}, label={label}."
        )
    url = _safe_str(matches[0].get("sheet_url"))
    if not url:
        raise ValueError(
            f"Empty sheet_url in {tab_cfg_sites} for site_code={site_code}, label={label}."
        )
    return url


def _ensure_runlog_header(worksheet: gspread.Worksheet) -> None:
    values = worksheet.get_all_values()
    if not values:
        worksheet.update(range_name="A1", values=[RUNLOG_HEADER_18])
        return
    current = [_safe_str(x) for x in values[0][: len(RUNLOG_HEADER_18)]]
    if current != RUNLOG_HEADER_18:
        raise ValueError(
            f"RunLog header mismatch in {worksheet.title}. "
            f"Expected={RUNLOG_HEADER_18}; actual={current}"
        )


def _get_optional_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
) -> Optional[gspread.Worksheet]:
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return None


def _records_from_matrix(
    values: Sequence[Sequence[Any]],
) -> Tuple[List[str], List[Dict[str, str]]]:
    if not values:
        return LOCATION_HEADERS.copy(), []

    raw_headers = [_normalize_header(value) for value in values[0]]
    headers: List[str] = []
    for index, header in enumerate(raw_headers, start=1):
        header = header or f"extra_col_{index}"
        if header in headers:
            raise ValueError(f"Duplicate column in Cfg__Locations: {header}")
        headers.append(header)

    records: List[Dict[str, str]] = []
    for row in values[1:]:
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        record = {headers[i]: _safe_str(padded[i]) for i in range(len(headers))}
        if any(record.values()):
            records.append(record)
    return headers, records


def _load_existing_locations(
    worksheet: Optional[gspread.Worksheet],
) -> Tuple[List[str], List[Dict[str, str]]]:
    if worksheet is None:
        return LOCATION_HEADERS.copy(), []
    return _records_from_matrix(worksheet.get_all_values())


def _fetch_all_locations(
    client: ShopifyGraphQLClient,
    *,
    page_size: int,
    include_inactive: bool,
    include_legacy: bool,
    print_progress: bool,
) -> List[Dict[str, Any]]:
    if page_size < 1 or page_size > 250:
        raise ValueError("page_size must be between 1 and 250.")

    rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 0
    while True:
        page += 1
        data = client.gql(
            Q_LOCATIONS,
            {
                "first": int(page_size),
                "after": cursor,
                "includeInactive": bool(include_inactive),
                "includeLegacy": bool(include_legacy),
            },
        )
        connection = data.get("locations") or {}
        nodes = connection.get("nodes") or []
        rows.extend(nodes)
        if print_progress:
            print(
                "[Location fetch] "
                f"page={page} fetched={len(nodes)} total={len(rows)}"
            )

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = _safe_str(page_info.get("endCursor")) or None
        if not cursor:
            raise RuntimeError("locations.hasNextPage=true but endCursor is empty.")
    return rows


def _clean_location_code(value: Any) -> str:
    code = _safe_str(value).upper()
    if code.startswith("在仓-"):
        code = code[3:]
    code = re.sub(r"[^A-Z0-9_-]+", "-", code)
    return re.sub(r"-+", "-", code).strip("-_")


def _base_location_code(location_name: str, province_code: str) -> str:
    province = _clean_location_code(province_code)
    if province:
        return province
    tokens = re.findall(r"[A-Za-z0-9]+", location_name.upper())
    ignored = {"WAREHOUSE", "LOCATION", "STORE", "FULFILLMENT", "CENTER"}
    usable = [token for token in tokens if token not in ignored]
    return _clean_location_code(usable[0] if usable else "LOC")[:16] or "LOC"


def _next_unique_code(base: str, used: set[str]) -> str:
    base = _clean_location_code(base) or "LOC"
    if base not in used:
        used.add(base)
        return base
    index = 2
    while f"{base}-{index}" in used:
        index += 1
    value = f"{base}-{index}"
    used.add(value)
    return value


def _canonical_existing_record(
    record: Mapping[str, Any],
    current_site_code: str,
    headers: Sequence[str],
) -> Dict[str, str]:
    output = {header: _safe_str(record.get(header)) for header in headers}
    output["site_code"] = _normalize_site_code(
        output.get("site_code") or current_site_code
    )
    output["location_code"] = _clean_location_code(output.get("location_code"))
    output["active"] = _bool_cell(output.get("active"), default=False)
    output["is_default"] = _bool_cell(output.get("is_default"), default=False)
    return output


def _build_sync_plan(
    *,
    site_code: str,
    existing_headers: Sequence[str],
    existing_records: Sequence[Mapping[str, Any]],
    shopify_nodes: Sequence[Mapping[str, Any]],
    synced_at: str,
) -> Dict[str, Any]:
    site_code = _normalize_site_code(site_code)
    headers = list(LOCATION_HEADERS)
    for header in existing_headers:
        normalized = _normalize_header(header)
        if normalized and normalized not in headers:
            headers.append(normalized)

    canonical_existing = [
        _canonical_existing_record(record, site_code, headers)
        for record in existing_records
    ]
    other_site_records = [
        record for record in canonical_existing if record["site_code"] != site_code
    ]
    current_records = [
        record for record in canonical_existing if record["site_code"] == site_code
    ]

    by_gid: Dict[str, Dict[str, str]] = {}
    duplicate_gids: List[str] = []
    for record in current_records:
        gid = _safe_str(record.get("location_gid"))
        if not gid:
            continue
        if gid in by_gid:
            duplicate_gids.append(gid)
        by_gid[gid] = record
    if duplicate_gids:
        raise ValueError(
            "Cfg__Locations contains duplicate location_gid values for this site: "
            + ", ".join(sorted(set(duplicate_gids)))
        )

    used_codes = {
        _clean_location_code(record.get("location_code"))
        for record in current_records
        if _clean_location_code(record.get("location_code"))
    }

    normalized_nodes: List[Dict[str, str]] = []
    node_gids: set[str] = set()
    for node in shopify_nodes:
        gid = _safe_str(node.get("id"))
        if not gid:
            raise ValueError("Shopify returned a Location without id.")
        if gid in node_gids:
            raise ValueError(f"Shopify returned duplicate Location GID: {gid}")
        node_gids.add(gid)
        address = node.get("address") or {}
        normalized_nodes.append(
            {
                "location_gid": gid,
                "location_name": _safe_str(node.get("name")),
                "province_code": _safe_str(address.get("provinceCode")).upper(),
                "active": _bool_cell(node.get("isActive"), default=False),
            }
        )

    final_current: List[Dict[str, str]] = []
    preview: List[Dict[str, str]] = []
    stats = {
        "shopify_locations": len(normalized_nodes),
        "existing_site_rows": len(current_records),
        "new": 0,
        "updated": 0,
        "unchanged": 0,
        "missing_preserved": 0,
    }
    warnings: List[str] = []

    for node in normalized_nodes:
        existing = by_gid.get(node["location_gid"])
        if existing is None:
            code = _next_unique_code(
                _base_location_code(node["location_name"], node["province_code"]),
                used_codes,
            )
            record = {header: "" for header in headers}
            record.update(
                {
                    "site_code": site_code,
                    "location_code": code,
                    "location_name": node["location_name"],
                    "location_gid": node["location_gid"],
                    "province_code": node["province_code"],
                    "active": node["active"],
                    "is_default": "FALSE",
                    "notes": "",
                    "synced_at": synced_at,
                }
            )
            final_current.append(record)
            stats["new"] += 1
            preview.append(
                {
                    "change_type": "NEW",
                    "changed_fields": ",".join(SYSTEM_MANAGED_FIELDS | HUMAN_MANAGED_FIELDS),
                    **{key: record.get(key, "") for key in LOCATION_HEADERS},
                }
            )
            continue

        record = dict(existing)
        if not record.get("location_code"):
            record["location_code"] = _next_unique_code(
                _base_location_code(node["location_name"], node["province_code"]),
                used_codes,
            )

        changed_fields: List[str] = []
        desired_system_values = {
            "site_code": site_code,
            "location_name": node["location_name"],
            "location_gid": node["location_gid"],
            "province_code": node["province_code"],
            "active": node["active"],
        }
        for field, desired in desired_system_values.items():
            if _safe_str(record.get(field)) != _safe_str(desired):
                record[field] = _safe_str(desired)
                changed_fields.append(field)
        if not existing.get("location_code") and record.get("location_code"):
            changed_fields.append("location_code")

        if changed_fields:
            record["synced_at"] = synced_at
            stats["updated"] += 1
            change_type = "UPDATED"
        else:
            stats["unchanged"] += 1
            change_type = "UNCHANGED"

        final_current.append(record)
        preview.append(
            {
                "change_type": change_type,
                "changed_fields": ",".join(changed_fields),
                **{key: record.get(key, "") for key in LOCATION_HEADERS},
            }
        )

    for record in current_records:
        gid = _safe_str(record.get("location_gid"))
        if gid and gid in node_gids:
            continue
        final_current.append(record)
        stats["missing_preserved"] += 1
        warning = (
            "Existing Cfg__Locations row was not returned by Shopify and was preserved: "
            f"location_code={record.get('location_code')}, gid={gid or '(blank)'}"
        )
        warnings.append(warning)
        preview.append(
            {
                "change_type": "MISSING_PRESERVED",
                "changed_fields": "",
                **{key: record.get(key, "") for key in LOCATION_HEADERS},
            }
        )

    active_defaults = [
        record
        for record in final_current
        if _normalize_bool(record.get("active"), default=False)
        and _normalize_bool(record.get("is_default"), default=False)
    ]
    if len(active_defaults) == 0:
        warnings.append(
            f"site_code={site_code} has no active is_default=TRUE Location."
        )
    elif len(active_defaults) > 1:
        warnings.append(
            f"site_code={site_code} has {len(active_defaults)} active default Locations; expected one."
        )

    final_records = other_site_records + final_current
    return {
        "headers": headers,
        "records": final_records,
        "preview_records": preview,
        "stats": stats,
        "warnings": warnings,
    }


def _a1_col(number: int) -> str:
    output = ""
    n = int(number)
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        output = chr(65 + remainder) + output
    return output


def _matrix_from_records(
    headers: Sequence[str],
    records: Sequence[Mapping[str, Any]],
) -> List[List[str]]:
    return [list(headers)] + [
        [_safe_str(record.get(header)) for header in headers]
        for record in records
    ]


def _write_matrix(
    spreadsheet: gspread.Spreadsheet,
    worksheet: Optional[gspread.Worksheet],
    title: str,
    matrix: Sequence[Sequence[Any]],
) -> gspread.Worksheet:
    rows_needed = max(2, len(matrix) + 20)
    cols_needed = max(2, len(matrix[0]) if matrix else len(LOCATION_HEADERS))
    if worksheet is None:
        worksheet = spreadsheet.add_worksheet(
            title=title,
            rows=rows_needed,
            cols=cols_needed,
        )
    elif worksheet.row_count < rows_needed or worksheet.col_count < cols_needed:
        worksheet.resize(
            rows=max(worksheet.row_count, rows_needed),
            cols=max(worksheet.col_count, cols_needed),
        )

    worksheet.batch_clear(
        [f"A1:{_a1_col(max(worksheet.col_count, cols_needed))}{worksheet.row_count}"]
    )
    worksheet.update(range_name="A1", values=list(matrix), value_input_option="RAW")
    return worksheet


def _normalize_registry_header(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", _safe_str(value).lower()).strip()


def _extract_spreadsheet_id(value: Any) -> str:
    """Accept a Google Sheets file ID or a normal spreadsheet URL."""
    text = _safe_str(value)
    if not text:
        raise ValueError("Workspace Project Registry ID/URL is empty.")

    match = re.search(
        r"/spreadsheets/d/([A-Za-z0-9_-]+)",
        text,
    )
    if match:
        return match.group(1)

    if re.fullmatch(r"[A-Za-z0-9_-]+", text):
        return text

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
    explicit_workspace_sa_value: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, str]:
    """Resolve one active project route from the Workspace Project Registry.

    Authentication is intentionally two-stage:

    1. ``WORKSPACE_GSHEET`` reads only the Workspace Project Registry.
    2. The selected row returns the project-specific Google Secret and
       Console Core route used by the actual Location synchronization.
    """
    resolved_project_code = _normalize_site_code(project_code)
    if not resolved_project_code:
        raise ValueError("project_code is required.")

    registry_tab = _safe_str(workspace_registry_tab)
    if not registry_tab:
        raise ValueError("workspace_registry_tab is required.")

    if print_progress:
        print(
            "[Workspace Registry] resolve bootstrap Secret | "
            f"project={resolved_project_code} | "
            f"secret={workspace_gsheet_secret_name}"
        )

    workspace_secret = read_secret(
        workspace_gsheet_secret_name,
        project_code="WORKSPACE",
        explicit_value=explicit_workspace_sa_value,
        secret_home=secret_home,
    )
    workspace_gc, auth_meta = _build_gspread_client(workspace_secret)

    registry_file_id = _extract_spreadsheet_id(workspace_registry_id)
    registry_book = workspace_gc.open_by_key(registry_file_id)
    try:
        worksheet = registry_book.worksheet(registry_tab)
    except gspread.WorksheetNotFound as exc:
        raise ValueError(
            f"Workspace Project Registry tab {registry_tab!r} does not exist "
            f"in {registry_book.title!r}."
        ) from exc

    values = worksheet.get_all_values()
    if not values:
        raise ValueError(
            f"Workspace Project Registry tab {registry_tab!r} is empty."
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
    timezone_col = require_column("timezone", "time zone")

    project_name_col = header_map.get(
        _normalize_registry_header("project_name")
    )
    notes_col = header_map.get(_normalize_registry_header("notes"))

    matches: List[Tuple[int, List[Any]]] = []
    width = len(values[0])
    for row_number, raw_row in enumerate(values[1:], start=2):
        row = list(raw_row) + [""] * max(0, width - len(raw_row))
        if _normalize_site_code(row[project_col]) == resolved_project_code:
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
    try:
        is_active = _normalize_bool(row[active_col], default=False)
    except ValueError as exc:
        raise ValueError(
            f"Workspace Project Registry row {source_row}: {exc}"
        ) from exc

    if not is_active:
        raise ValueError(
            "Workspace Project Registry project is inactive: "
            f"project_code={resolved_project_code}, row={source_row}."
        )

    route = {
        "project_code": resolved_project_code,
        "project_name": (
            _safe_str(row[project_name_col])
            if project_name_col is not None
            else ""
        ),
        "console_core_url": _safe_str(row[console_url_col]),
        "gsheet_secret_name": _safe_str(row[gsheet_secret_col]),
        "account_config_tab": _safe_str(row[account_tab_col]),
        "timezone": _safe_str(row[timezone_col]),
        "notes": (
            _safe_str(row[notes_col])
            if notes_col is not None
            else ""
        ),
        "registry_id": registry_file_id,
        "registry_tab": registry_tab,
        "registry_source_row": str(source_row),
        "workspace_gsheet_secret_name": _safe_str(
            workspace_gsheet_secret_name
        ),
        "workspace_auth_source_type": _safe_str(
            auth_meta.get("source_type")
        ),
        "workspace_service_account_email": _safe_str(
            auth_meta.get("service_account_email")
        ),
    }

    empty_required = [
        key
        for key in (
            "console_core_url",
            "gsheet_secret_name",
            "account_config_tab",
            "timezone",
        )
        if not route[key]
    ]
    if empty_required:
        raise ValueError(
            "Workspace Project Registry route has empty required values: "
            f"project_code={resolved_project_code}; "
            f"fields={empty_required}; row={source_row}."
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
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
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
            print("[Registry] mode=OFF; no registry read or write")
        return {"status": "OFF", "changed_fields": [], "target_row": None}

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _safe_str(
        current_colab_url
    ):
        raise ValueError(f"registry_mode={mode} requires current_colab_url.")
    if mode == "UPDATE_URL_AND_NAME" and not _safe_str(current_colab_name):
        raise ValueError("UPDATE_URL_AND_NAME requires current_colab_name.")

    if print_progress:
        print(
            "[Registry] resolving target | "
            f"job_name={job_name} | sheet_label={sheet_label} | tab_name={tab_name}"
        )

    sa_secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, auth_meta = _build_gspread_client(sa_secret)
    worksheet = gc.open_by_url(console_core_url).worksheet(registry_tab)
    values = worksheet.get_all_values()
    if not values:
        raise ValueError(f"Registry tab {registry_tab!r} is empty.")

    header_map: Dict[str, int] = {}
    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if normalized:
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
        for _, column_number, _, new_value in applied:
            worksheet.update_cell(row_number, column_number, new_value)
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


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    tab_cfg_locations: str = "Cfg__Locations",
    tab_runlog: str = "Ops__RunLog",
    runlog_sheet_label: str = "runlog_sheet",
    include_inactive: bool = True,
    include_legacy: bool = False,
    page_size: int = 100,
    preview_rows: int = 50,
    dry_run: bool = True,
    confirmed: bool = False,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
) -> Dict[str, Any]:
    """Preview or apply the Shopify Location registry synchronization."""
    site_code = _normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not _safe_str(console_core_url):
        raise ValueError("console_core_url is required.")
    if not _safe_str(bootstrap_gsheet_sa_b64_secret):
        raise ValueError("bootstrap_gsheet_sa_b64_secret is required.")
    if not dry_run and not confirmed:
        raise ValueError("Real write blocked: set dry_run=False and confirmed=True.")

    run_id = run_id or _make_run_id(job_name, tz_name)
    phase = "preview" if dry_run else "apply"
    started = time.monotonic()

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 8, f"Resolve bootstrap Google Secret | site={site_code} | phase={phase}")
    bootstrap_secret = read_secret(
        bootstrap_gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, google_auth_meta = _build_gspread_client(bootstrap_secret)
    console = gc.open_by_url(console_core_url)
    progress(
        2,
        8,
        "Google Console access ready | "
        f"source={google_auth_meta['source_type']} | "
        f"format={google_auth_meta['secret_format']}",
    )

    account = _load_account_config(gc, console_core_url, tab_cfg_account_id)
    if account.gsheet_sa_b64_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "Bootstrap Google Secret name does not match Cfg__account_id. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}; "
            f"cfg={account.gsheet_sa_b64_secret}"
        )
    progress(
        3,
        8,
        f"Account config ready | shop={account.shop_domain} | api={account.api_version}",
    )

    shopify_secret = read_secret(
        account.shopify_token_secret,
        project_code=site_code,
        explicit_value=shopify_token_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    progress(
        4,
        8,
        "Shopify token ready | "
        f"secret_name={account.shopify_token_secret} | "
        f"source={shopify_secret.source_type}",
    )

    runlog_url = _resolve_sheet_url_by_label(
        gc,
        console_core_url,
        tab_cfg_sites,
        site_code,
        runlog_sheet_label,
    )
    runlog_ws = gc.open_by_url(runlog_url).worksheet(tab_runlog)
    logger = RunLogger18(
        worksheet=runlog_ws,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )
    target_ws = _get_optional_worksheet(console, tab_cfg_locations)

    try:
        progress(5, 8, "Fetch Shopify Locations with pagination")
        client = ShopifyGraphQLClient(
            shop_domain=account.shop_domain,
            api_version=account.api_version,
            access_token=shopify_secret.value,
            print_progress=print_progress,
        )
        shopify_nodes = _fetch_all_locations(
            client,
            page_size=page_size,
            include_inactive=include_inactive,
            include_legacy=include_legacy,
            print_progress=print_progress,
        )
        if not shopify_nodes:
            raise ValueError(
                "Shopify returned zero Locations. Check token scope and include filters."
            )

        progress(6, 8, f"Build sync plan | target={tab_cfg_locations}")
        existing_headers, existing_records = _load_existing_locations(target_ws)
        plan = _build_sync_plan(
            site_code=site_code,
            existing_headers=existing_headers,
            existing_records=existing_records,
            shopify_nodes=shopify_nodes,
            synced_at=_now_str(tz_name),
        )
        stats = plan["stats"]
        warnings = plan["warnings"]
        changed_rows = stats["new"] + stats["updated"]
        print(
            "[Plan] "
            f"shopify={stats['shopify_locations']} "
            f"existing={stats['existing_site_rows']} "
            f"new={stats['new']} updated={stats['updated']} "
            f"unchanged={stats['unchanged']} "
            f"missing_preserved={stats['missing_preserved']} "
            f"warnings={len(warnings)}"
        )

        sheet_rows_written = 0
        if dry_run:
            progress(7, 8, "Preview only | Cfg__Locations not changed")
        else:
            progress(
                7,
                8,
                f"Write complete Cfg__Locations matrix | changed_rows={changed_rows}",
            )
            matrix = _matrix_from_records(plan["headers"], plan["records"])
            _write_matrix(console, target_ws, tab_cfg_locations, matrix)
            sheet_rows_written = len(plan["records"])

        status = "SUCCESS_WITH_WARNINGS" if warnings else "SUCCESS"
        logger.log(
            phase=phase,
            log_type="summary",
            status=status,
            rows_loaded=stats["shopify_locations"],
            rows_pending=changed_rows,
            rows_recognized=stats["shopify_locations"],
            rows_planned=changed_rows,
            rows_written=sheet_rows_written,
            rows_skipped=stats["unchanged"],
            message=(
                f"{'preview' if dry_run else 'applied'} | "
                f"new={stats['new']} | updated={stats['updated']} | "
                f"unchanged={stats['unchanged']} | "
                f"missing_preserved={stats['missing_preserved']} | "
                f"warnings={len(warnings)}"
            ),
        )
        for warning in warnings[:5]:
            logger.log(
                phase=phase,
                log_type="detail",
                status="WARN",
                message=warning,
                error_reason="LOCATION_SYNC_WARNING",
            )
        logger.flush()

        elapsed = round(time.monotonic() - started, 2)
        progress(
            8,
            8,
            f"Completed | status={status} | elapsed={elapsed}s | errors=0",
        )
        preview_df = pd.DataFrame(plan["preview_records"])
        if preview_rows >= 0:
            preview_df = preview_df.head(int(preview_rows))

        return {
            "ok": True,
            "status": status,
            "phase": phase,
            "run_id": run_id,
            "job_name": job_name,
            "site_code": site_code,
            "summary": {
                **stats,
                "changed_rows": changed_rows,
                "sheet_rows_written": sheet_rows_written,
                "warning_count": len(warnings),
                "error_count": 0,
                "elapsed_seconds": elapsed,
            },
            "preview": preview_df,
            "warnings": warnings,
            "targets": {
                "console_core_url": console_core_url,
                "target_tab": tab_cfg_locations,
                "runlog_sheet_url": runlog_url,
                "runlog_tab": tab_runlog,
                "shop_domain": account.shop_domain,
                "api_version": account.api_version,
                "module_path": MODULE_PATH,
                "module_version": MODULE_VERSION,
            },
            "runtime": {
                "runtime_mode": _runtime_mode(),
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "google_secret_source": google_auth_meta["source_type"],
                "google_secret_format": google_auth_meta["secret_format"],
                "shopify_secret_source": shopify_secret.source_type,
                "service_account_email": google_auth_meta["service_account_email"],
            },
        }

    except BaseException as exc:  # noqa: BLE001
        try:
            logger.log(
                phase=phase,
                log_type="summary",
                status="FAILED",
                message=str(exc),
                error_reason=type(exc).__name__,
            )
            logger.flush()
        except Exception as log_exc:  # noqa: BLE001
            if print_progress:
                print(f"[RunLog warning] failed to write failure log: {log_exc}")
        if print_progress:
            print(f"[FAILED] {type(exc).__name__}: {exc}")
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync Shopify Locations into Console Core / Cfg__Locations."
    )
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument("--bootstrap-gsheet-secret", required=True)
    parser.add_argument("--secret-home", default=None)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--preview-rows", type=int, default=50)
    parser.add_argument("--include-legacy", action="store_true")
    parser.add_argument("--exclude-inactive", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirmed", action="store_true")
    parser.add_argument("--tz-name", default="America/New_York")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=args.bootstrap_gsheet_secret,
        include_inactive=not args.exclude_inactive,
        include_legacy=args.include_legacy,
        page_size=args.page_size,
        preview_rows=args.preview_rows,
        dry_run=not args.apply,
        confirmed=args.confirmed,
        tz_name=args.tz_name,
        secret_home=args.secret_home,
    )
    print(json.dumps({"status": result["status"], "summary": result["summary"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
