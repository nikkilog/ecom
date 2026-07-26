# -*- coding: utf-8 -*-
"""Shopify Location setup sync for the Commerce Operations System.

This setup module synchronizes Shopify Admin GraphQL Location records into
Console Core / Cfg__Locations while preserving human-governed fields.

It belongs to the site bootstrap/setup layer rather than the field-schema
configuration layer. The same module is intended for every Shopify project.

Designed for both:
- Colab: secrets from ``google.colab.userdata``.
- Local Python: explicit values, environment variables, or files in SECRET_HOME.

Local preview example::

    python -m shopify_setup.sync_locations \
      --site-code PBS \
      --console-core-url "https://docs.google.com/spreadsheets/d/.../edit" \
      --bootstrap-gsheet-secret PBS_GSHEET

Local apply example::

    python -m shopify_setup.sync_locations \
      --site-code PBS \
      --console-core-url "https://docs.google.com/spreadsheets/d/.../edit" \
      --bootstrap-gsheet-secret PBS_GSHEET \
      --apply --confirmed
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import math
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo


MODULE_VERSION = "1.1.0"
DEFAULT_JOB_NAME = "sync_locations"
MODULE_PATH = "shopify_setup.sync_locations"

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


class ShopifyGraphQLClient:
    def __init__(
        self,
        shop_domain: str,
        api_version: str,
        access_token: str,
        timeout: int = 90,
    ) -> None:
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        self.timeout = int(timeout)
        self.session = requests.Session()

    def gql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        retry: int = 6,
    ) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        last_error: Optional[Exception] = None

        for attempt in range(max(1, int(retry))):
            try:
                response = self.session.post(
                    self.url,
                    headers=self.headers,
                    json=payload,
                    timeout=self.timeout,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    delay = min(2**attempt, 20) + random.random()
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                body = response.json()
                if body.get("errors"):
                    raise RuntimeError(json.dumps(body["errors"], ensure_ascii=False))
                data = body.get("data")
                if not isinstance(data, dict):
                    raise RuntimeError("Shopify GraphQL response has no data object.")
                return data
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= retry - 1:
                    break
                delay = min(2**attempt, 20) + random.random()
                time.sleep(delay)

        raise RuntimeError(f"Shopify GraphQL failed after retries: {last_error}")


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


def _normalize_header(value: Any) -> str:
    text = _safe_str(value)
    alias = LEGACY_HEADER_ALIASES.get(text.lower())
    return alias or text


def _normalize_site_code(value: Any) -> str:
    return _safe_str(value).upper()


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


def _read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def _secret_file_candidates(secret_home: Path, secret_name: str) -> Iterable[Path]:
    yield secret_home / secret_name
    yield secret_home / f"{secret_name}.txt"
    yield secret_home / f"{secret_name}.secret"
    yield secret_home / f"{secret_name}.json"


def _load_secret(
    secret_name: str,
    *,
    explicit_value: Optional[str] = None,
    secret_home: Optional[str] = None,
) -> str:
    """Load a secret in Colab or local Python without changing job logic."""
    if explicit_value is not None and _safe_str(explicit_value):
        return str(explicit_value).strip()

    env_value = os.environ.get(secret_name)
    if env_value and env_value.strip():
        return env_value.strip()

    try:
        from google.colab import userdata  # type: ignore

        colab_value = userdata.get(secret_name)
        if colab_value and str(colab_value).strip():
            return str(colab_value).strip()
    except Exception:
        pass

    homes: List[Path] = []
    if secret_home:
        homes.append(Path(secret_home).expanduser())
    for env_key in ("SECRET_HOME", "SHOPIFY_SECRET_HOME"):
        if os.environ.get(env_key):
            homes.append(Path(os.environ[env_key]).expanduser())
    homes.append(Path.home() / "Documents" / "Projects" / "_Secrets")

    seen: set[str] = set()
    for home in homes:
        key = str(home)
        if key in seen:
            continue
        seen.add(key)
        for candidate in _secret_file_candidates(home, secret_name):
            if candidate.is_file():
                value = _read_text_file(candidate)
                if value:
                    return value

    raise RuntimeError(
        f"Cannot resolve secret '{secret_name}'. Use Colab userdata, an environment "
        "variable with the same name, an explicit value, or a file under SECRET_HOME."
    )


def _parse_service_account(secret_value: str) -> Dict[str, Any]:
    text = secret_value.strip()
    if text.startswith("{"):
        data = json.loads(text)
    else:
        padded = text + "=" * (-len(text) % 4)
        try:
            decoded = base64.b64decode(padded).decode("utf-8")
            data = json.loads(decoded)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                "Google service-account secret must be base64(JSON) or raw JSON."
            ) from exc
    if not isinstance(data, dict) or not data.get("client_email"):
        raise ValueError("Invalid Google service-account JSON.")
    return data


def _build_gspread_client(sa_secret_value: str) -> gspread.Client:
    info = _parse_service_account(sa_secret_value)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)


def _load_account_config(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_account_id: str,
) -> AccountConfig:
    worksheet = gc.open_by_url(console_core_url).worksheet(tab_cfg_account_id)
    values = worksheet.get_all_values()
    if not values:
        raise ValueError(f"{tab_cfg_account_id} is empty.")

    config: Dict[str, str] = {}
    duplicates: List[str] = []
    for row_number, row in enumerate(values, start=1):
        if not row:
            continue
        key = _safe_str(row[0]).upper()
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
    worksheet = gc.open_by_url(console_core_url).worksheet(tab_cfg_sites)
    records = worksheet.get_all_records()
    if not records:
        raise ValueError(f"{tab_cfg_sites} is empty.")

    matches = []
    for row in records:
        if _normalize_site_code(row.get("site_code")) != _normalize_site_code(site_code):
            continue
        if _safe_str(row.get("label")) != label:
            continue
        matches.append(row)

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


def _get_or_none_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
) -> Optional[gspread.Worksheet]:
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return None


def _records_from_matrix(values: Sequence[Sequence[Any]]) -> Tuple[List[str], List[Dict[str, str]]]:
    if not values:
        return LOCATION_HEADERS.copy(), []

    raw_headers = [_normalize_header(x) for x in values[0]]
    headers: List[str] = []
    for header in raw_headers:
        if not header:
            header = f"extra_col_{len(headers) + 1}"
        if header in headers:
            raise ValueError(f"Duplicate column in Cfg__Locations: {header}")
        headers.append(header)

    records: List[Dict[str, str]] = []
    for row in values[1:]:
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        record = {headers[i]: _safe_str(padded[i]) for i in range(len(headers))}
        if any(_safe_str(v) for v in record.values()):
            records.append(record)

    return headers, records


def _load_existing_locations(
    worksheet: Optional[gspread.Worksheet],
) -> Tuple[List[str], List[Dict[str, str]]]:
    if worksheet is None:
        return LOCATION_HEADERS.copy(), []
    headers, records = _records_from_matrix(worksheet.get_all_values())

    # Legacy Ref-Location rows have no site_code. They are accepted only when the
    # caller later supplies the current site_code during normalization.
    return headers, records


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
            print(f"      Shopify page={page} fetched={len(nodes)} total={len(rows)}")

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = _safe_str(page_info.get("endCursor")) or None
        if not cursor:
            raise RuntimeError("locations.hasNextPage is true but endCursor is empty.")

    return rows


def _clean_location_code(value: Any) -> str:
    code = _safe_str(value).upper()
    if code.startswith("在仓-"):
        code = code[3:]
    code = re.sub(r"[^A-Z0-9_-]+", "-", code)
    code = re.sub(r"-+", "-", code).strip("-_")
    return code


def _base_code(location_name: str, province_code: str) -> str:
    province = _clean_location_code(province_code)
    if province:
        return province

    tokens = re.findall(r"[A-Za-z0-9]+", location_name.upper())
    ignored = {"WAREHOUSE", "LOCATION", "STORE", "FULFILLMENT", "CENTER"}
    tokens = [token for token in tokens if token not in ignored]
    if tokens:
        return _clean_location_code(tokens[0])[:16]
    return "LOC"


def _next_unique_code(base: str, used: set[str]) -> str:
    base = _clean_location_code(base) or "LOC"
    if base not in used:
        used.add(base)
        return base
    index = 2
    while f"{base}-{index}" in used:
        index += 1
    code = f"{base}-{index}"
    used.add(code)
    return code


def _canonical_existing_record(record: Dict[str, str], current_site_code: str) -> Dict[str, str]:
    out = dict(record)
    out["site_code"] = _normalize_site_code(out.get("site_code") or current_site_code)
    out["location_code"] = _clean_location_code(out.get("location_code"))
    out["location_name"] = _safe_str(out.get("location_name"))
    out["location_gid"] = _safe_str(out.get("location_gid"))
    out["province_code"] = _safe_str(out.get("province_code")).upper()
    out["active"] = _bool_cell(out.get("active"), default=False)
    out["is_default"] = _bool_cell(out.get("is_default"), default=False)
    out["notes"] = _safe_str(out.get("notes"))
    out["synced_at"] = _safe_str(out.get("synced_at"))
    return out


def _shopify_location_record(node: Dict[str, Any]) -> Dict[str, str]:
    address = node.get("address") or {}
    return {
        "location_name": _safe_str(node.get("name")),
        "location_gid": _safe_str(node.get("id")),
        "province_code": _safe_str(address.get("provinceCode")).upper(),
        "active": "TRUE" if bool(node.get("isActive")) else "FALSE",
    }


def _compare_managed_fields(before: Dict[str, str], after: Dict[str, str]) -> bool:
    # synced_at is refreshed on every successful fetch, but it should not make every
    # Location look like a business-field update in the run summary.
    compare_fields = SYSTEM_MANAGED_FIELDS - {"synced_at"}
    return any(
        _safe_str(before.get(key)) != _safe_str(after.get(key))
        for key in compare_fields
    )


def _build_sync_plan(
    *,
    site_code: str,
    existing_headers: List[str],
    existing_records: List[Dict[str, str]],
    shopify_nodes: List[Dict[str, Any]],
    synced_at: str,
) -> Dict[str, Any]:
    site_code = _normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is empty.")

    all_headers = LOCATION_HEADERS.copy()
    for header in existing_headers:
        if header and header not in all_headers:
            all_headers.append(header)

    normalized_existing = [
        _canonical_existing_record(record, site_code) for record in existing_records
    ]

    by_gid: Dict[Tuple[str, str], Dict[str, str]] = {}
    for index, record in enumerate(normalized_existing, start=2):
        record_site = _normalize_site_code(record.get("site_code"))
        gid = _safe_str(record.get("location_gid"))
        if not gid:
            raise ValueError(f"Cfg__Locations row {index} has empty location_gid.")
        key = (record_site, gid)
        if key in by_gid:
            raise ValueError(f"Duplicate site_code + location_gid in Cfg__Locations: {key}")
        by_gid[key] = record

    current_site_records = [
        row for row in normalized_existing if row["site_code"] == site_code
    ]
    other_site_records = [
        row for row in normalized_existing if row["site_code"] != site_code
    ]

    used_codes = {
        _clean_location_code(row.get("location_code"))
        for row in current_site_records
        if _clean_location_code(row.get("location_code"))
    }

    warnings: List[str] = []
    result_site_records: List[Dict[str, str]] = []
    fetched_gids: set[str] = set()
    new_count = 0
    updated_count = 0
    unchanged_count = 0

    sorted_nodes = sorted(
        shopify_nodes,
        key=lambda node: (
            _safe_str((node.get("address") or {}).get("provinceCode")),
            _safe_str(node.get("name")).lower(),
            _safe_str(node.get("id")),
        ),
    )

    for node in sorted_nodes:
        system_row = _shopify_location_record(node)
        gid = system_row["location_gid"]
        if not gid:
            raise ValueError("Shopify returned a Location without id.")
        if gid in fetched_gids:
            raise ValueError(f"Shopify returned duplicate Location GID: {gid}")
        fetched_gids.add(gid)

        existing = by_gid.get((site_code, gid))
        if existing:
            merged = dict(existing)
            before = dict(existing)
            merged.update(system_row)
            merged["site_code"] = site_code
            merged["synced_at"] = synced_at
            if not _clean_location_code(merged.get("location_code")):
                merged["location_code"] = _next_unique_code(
                    _base_code(merged["location_name"], merged["province_code"]),
                    used_codes,
                )
                warnings.append(
                    f"Generated location_code={merged['location_code']} for existing gid={gid}."
                )
            else:
                merged["location_code"] = _clean_location_code(merged["location_code"])
            if _compare_managed_fields(before, merged):
                updated_count += 1
            else:
                unchanged_count += 1
            result_site_records.append(merged)
        else:
            code = _next_unique_code(
                _base_code(system_row["location_name"], system_row["province_code"]),
                used_codes,
            )
            row = {header: "" for header in all_headers}
            row.update(system_row)
            row.update(
                {
                    "site_code": site_code,
                    "location_code": code,
                    "is_default": "FALSE",
                    "notes": "",
                    "synced_at": synced_at,
                }
            )
            result_site_records.append(row)
            new_count += 1

    missing_records = [
        row for row in current_site_records if row["location_gid"] not in fetched_gids
    ]
    for row in missing_records:
        result_site_records.append(dict(row))
        warnings.append(
            "Existing row was not returned by Shopify and was preserved unchanged: "
            f"location_code={row.get('location_code')}, gid={row.get('location_gid')}"
        )

    merged_records = other_site_records + result_site_records
    for row in merged_records:
        for header in all_headers:
            row.setdefault(header, "")

    # Governance validation for every site represented in the registry.
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in merged_records:
        grouped.setdefault(_normalize_site_code(row.get("site_code")), []).append(row)

    for group_site, rows in grouped.items():
        seen_codes: Dict[str, str] = {}
        defaults: List[Dict[str, str]] = []
        for row in rows:
            code = _clean_location_code(row.get("location_code"))
            if not code:
                raise ValueError(
                    f"Cfg__Locations has empty location_code for site={group_site}, "
                    f"gid={row.get('location_gid')}"
                )
            if code in seen_codes:
                raise ValueError(
                    f"Duplicate location_code for site={group_site}: {code}; "
                    f"gids={seen_codes[code]}, {row.get('location_gid')}"
                )
            seen_codes[code] = _safe_str(row.get("location_gid"))
            row["location_code"] = code

            if _normalize_bool(row.get("is_default"), default=False):
                defaults.append(row)
                if not _normalize_bool(row.get("active"), default=False):
                    raise ValueError(
                        f"Default location must be active: site={group_site}, code={code}"
                    )

        if len(defaults) > 1:
            raise ValueError(
                f"Only one is_default=TRUE is allowed per site. site={group_site}; "
                f"codes={[row['location_code'] for row in defaults]}"
            )
        if group_site == site_code and not defaults:
            warnings.append(
                f"No default location is configured for site={site_code}. "
                "Create Product must provide a location_code until one row is marked is_default=TRUE."
            )

    merged_records.sort(
        key=lambda row: (
            _normalize_site_code(row.get("site_code")),
            _clean_location_code(row.get("location_code")),
            _safe_str(row.get("location_name")).lower(),
        )
    )

    preview_rows = []
    for row in result_site_records:
        preview_rows.append({header: row.get(header, "") for header in all_headers})

    return {
        "headers": all_headers,
        "records": merged_records,
        "preview_records": preview_rows,
        "stats": {
            "shopify_locations": len(shopify_nodes),
            "existing_site_rows": len(current_site_records),
            "new": new_count,
            "updated": updated_count,
            "unchanged": unchanged_count,
            "missing_preserved": len(missing_records),
            "table_rows_after": len(merged_records),
        },
        "warnings": warnings,
    }


def _matrix_from_records(headers: Sequence[str], records: Sequence[Dict[str, str]]) -> List[List[str]]:
    matrix = [list(headers)]
    for record in records:
        matrix.append([_safe_str(record.get(header, "")) for header in headers])
    return matrix


def _write_matrix(
    spreadsheet: gspread.Spreadsheet,
    worksheet: Optional[gspread.Worksheet],
    title: str,
    matrix: List[List[str]],
) -> gspread.Worksheet:
    rows_needed = max(2, len(matrix) + 5)
    cols_needed = max(2, len(matrix[0]) + 2 if matrix else 2)

    if worksheet is None:
        worksheet = spreadsheet.add_worksheet(
            title=title,
            rows=rows_needed,
            cols=cols_needed,
        )
    else:
        if worksheet.row_count < rows_needed or worksheet.col_count < cols_needed:
            worksheet.resize(
                rows=max(worksheet.row_count, rows_needed),
                cols=max(worksheet.col_count, cols_needed),
            )

    existing_rows = max(1, worksheet.row_count)
    existing_cols = max(1, worksheet.col_count)
    worksheet.batch_clear([f"A1:{_a1_col(existing_cols)}{existing_rows}"])
    worksheet.update(range_name="A1", values=matrix, value_input_option="RAW")
    return worksheet


def _a1_col(number: int) -> str:
    output = ""
    n = int(number)
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        output = chr(65 + remainder) + output
    return output


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
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
    shop_domain: Optional[str] = None,
    api_version: Optional[str] = None,
    gsheet_sa_b64_secret: Optional[str] = None,
    shopify_token_secret: Optional[str] = None,
) -> Dict[str, Any]:
    """Preview or apply a Shopify Location registry synchronization."""
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

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 7, f"Load bootstrap Google credential | site={site_code} | phase={phase}")
    bootstrap_secret_value = _load_secret(
        bootstrap_gsheet_sa_b64_secret,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
    )
    gc = _build_gspread_client(bootstrap_secret_value)
    console = gc.open_by_url(console_core_url)

    progress(2, 7, f"Read {tab_cfg_account_id} and resolve site account")
    account = _load_account_config(gc, console_core_url, tab_cfg_account_id)
    resolved_shop_domain = _safe_str(shop_domain or account.shop_domain)
    resolved_api_version = _safe_str(api_version or account.api_version)
    resolved_gsheet_secret = _safe_str(
        gsheet_sa_b64_secret or account.gsheet_sa_b64_secret
    )
    resolved_shopify_secret = _safe_str(
        shopify_token_secret or account.shopify_token_secret
    )

    if resolved_gsheet_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "BOOTSTRAP_GSHEET_SA_B64_SECRET does not match "
            "Cfg__account_id.GSHEET_SA_B64_SECRET. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}, cfg={resolved_gsheet_secret}"
        )

    token = _load_secret(
        resolved_shopify_secret,
        explicit_value=shopify_token_value,
        secret_home=secret_home,
    )

    progress(3, 7, f"Resolve RunLog route | label={runlog_sheet_label}")
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

    target_ws = _get_or_none_worksheet(console, tab_cfg_locations)
    try:
        progress(4, 7, "Fetch Shopify Locations with pagination")
        client = ShopifyGraphQLClient(
            shop_domain=resolved_shop_domain,
            api_version=resolved_api_version,
            access_token=token,
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
                "Shopify returned zero Locations. Check token scopes and include filters."
            )

        progress(5, 7, f"Compare Shopify with Console Core / {tab_cfg_locations}")
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

        if print_progress:
            print(
                "      "
                f"shopify={stats['shopify_locations']} "
                f"existing={stats['existing_site_rows']} "
                f"new={stats['new']} updated={stats['updated']} "
                f"unchanged={stats['unchanged']} "
                f"missing_preserved={stats['missing_preserved']}"
            )

        planned_changes = stats["new"] + stats["updated"]
        rows_written = 0
        if dry_run:
            progress(6, 7, "Preview only; no Cfg__Locations values changed")
        else:
            progress(6, 7, f"Write Cfg__Locations | changed={planned_changes}")
            matrix = _matrix_from_records(plan["headers"], plan["records"])
            _write_matrix(console, target_ws, tab_cfg_locations, matrix)
            rows_written = planned_changes

        status = "OK" if not warnings else "WARN"
        logger.log(
            phase=phase,
            log_type="summary",
            status=status,
            rows_loaded=stats["shopify_locations"],
            rows_pending=planned_changes,
            rows_recognized=stats["shopify_locations"],
            rows_planned=planned_changes,
            rows_written=rows_written,
            rows_skipped=planned_changes if dry_run else stats["unchanged"],
            message=(
                f"{'preview' if dry_run else 'applied'} | "
                f"new={stats['new']} | updated={stats['updated']} | "
                f"unchanged={stats['unchanged']} | "
                f"missing_preserved={stats['missing_preserved']} | "
                f"warnings={len(warnings)}"
            ),
        )
        for warning in warnings[:2]:
            logger.log(
                phase=phase,
                log_type="detail",
                status="WARN",
                message=warning,
                error_reason="LOCATION_SYNC_WARNING",
            )
        logger.flush()

        progress(
            7,
            7,
            f"Completed | status={status} written={rows_written} warnings={len(warnings)} errors=0",
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
                "rows_planned": planned_changes,
                "rows_written": rows_written,
                "warnings_count": len(warnings),
                "error_count": 0,
            },
            "preview": preview_df,
            "warnings": warnings,
            "targets": {
                "console_core_url": console_core_url,
                "target_tab": tab_cfg_locations,
                "runlog_sheet_url": runlog_url,
                "runlog_tab": tab_runlog,
                "shop_domain": resolved_shop_domain,
                "api_version": resolved_api_version,
                "include_inactive": bool(include_inactive),
                "include_legacy": bool(include_legacy),
                "module_version": MODULE_VERSION,
            },
        }

    except Exception as exc:  # noqa: BLE001
        logger.log(
            phase=phase,
            log_type="summary",
            status="FAIL",
            message=str(exc),
            error_reason="JOB_FAILED",
        )
        logger.flush()
        if print_progress:
            print(f"[FAILED] {exc}")
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Shopify Locations into Console Core / Cfg__Locations.")
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument("--bootstrap-gsheet-secret", required=True)
    parser.add_argument("--secret-home", default=None)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--preview-rows", type=int, default=50)
    parser.add_argument("--include-legacy", action="store_true")
    parser.add_argument("--exclude-inactive", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Write Cfg__Locations.")
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
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
