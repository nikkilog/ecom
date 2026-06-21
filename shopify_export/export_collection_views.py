# -*- coding: utf-8 -*-
"""
job_name: export_collection_views
module_path: shopify_export/export_collection_views_config.py

Config-driven Shopify Collection multi-view export.

Configuration sources:
- Console Core / Cfg__Sites
- Console Core / Cfg__account_id
- config / Cfg__Fields
- config / Cfg__ExportTabs
- config / Cfg__ExportTabFields

Design rules:
- Every enabled row in Cfg__ExportTabs with base_entity_type=COLLECTION is a view.
- base_sheet is intentionally blank. No IDX__COLLECTIONS worksheet is created.
- Shopify Collections are fetched once into memory.
- Required metafields are derived from Cfg__Fields and the enabled views/filters.
- Output columns, order, aliases and filters are controlled by config.
- No Collection metafield namespace/key whitelist is maintained in this module.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1


JOB_NAME = "export_collection_views"
CFG_SITES_TAB = "Cfg__Sites"
CFG_ACCOUNT_TAB = "Cfg__account_id"
CFG_FIELDS_TAB = "Cfg__Fields"
CFG_EXPORT_TABS_TAB = "Cfg__ExportTabs"
CFG_EXPORT_TAB_FIELDS_TAB = "Cfg__ExportTabFields"
RUNLOG_TAB_NAME = "Ops__RunLog"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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

FIELD_DEF_COLUMNS = [
    "field_id",
    "display_name",
    "entity_type",
    "field_key",
    "expr",
    "field_type",
    "data_type",
    "source_type",
    "namespace",
    "key",
]

VIEW_FIELD_COLUMNS = [
    "view_id",
    "field_id",
    "seq",
    "alias",
    "entity_type",
    "field_key",
    "expr",
    "field_type",
    "data_type",
    "source_type",
    "namespace",
    "key",
    "join_key",
    "agg",
    "notes",
]

# These are GraphQL source fields understood by the generic collection-path evaluator.
# They are fetched as a compact base payload; config still controls which fields are
# exposed, filtered and written.
COLLECTION_BASE_SELECTION = """
        id
        legacyResourceId
        handle
        title
        description
        descriptionHtml
        updatedAt
        templateSuffix
        image { url }
        ruleSet {
          appliedDisjunctively
          rules {
            column
            relation
            condition
          }
        }
""".strip()

TOKEN_RE = re.compile(r"\{([^{}]+)\}")
MF_VALUE_RE = re.compile(
    r'^MF_VALUE\(\s*["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']\s*\)$',
    re.IGNORECASE,
)
RULE_TEXT_RE = re.compile(
    r"^COLLECTION_RULE_TEXT\(\s*collection\.ruleSet\.rules\[(\d+)\]\s*\)$",
    re.IGNORECASE,
)
URL_FORMULA_RE = re.compile(
    r'^=IF\(LEN\(\{([^{}]+)\}&""\)=0,"","([^"]*)"&\{([^{}]+)\}\)$',
    re.IGNORECASE,
)


class CollectionViewExportError(RuntimeError):
    pass


@dataclass(frozen=True)
class FieldDef:
    field_id: str
    display_name: str
    entity_type: str
    field_key: str
    expr: str
    field_type: str
    data_type: str
    source_type: str
    namespace: str
    key: str


@dataclass(frozen=True)
class ViewField:
    view_id: str
    field_id: str
    seq: float
    alias: str
    field_def: FieldDef


@dataclass(frozen=True)
class ViewDef:
    view_id: str
    target_sheet: str
    target_sheet_label: str
    layout: str
    base_entity_type: str
    base_sheet: str
    base_key_field_id: str
    fixed_filter_mode: str
    fixed_filters: Dict[str, Any]
    fields: Tuple[ViewField, ...]


@dataclass
class RuntimeConfig:
    site_code: str
    console_core_url: str
    shop_domain: str
    api_version: str
    storefront_base_url: str
    admin_base_url: str
    runlog_label: str = "runlog_sheet"
    default_target_sheet_label: str = "export_other"
    write_mode: str = "REPLACE"
    page_size: int = 250
    write_chunk_rows: int = 2000
    retry: int = 6
    request_timeout: int = 60
    sleep_every_n_calls: int = 20
    sleep_seconds: float = 1.0
    strict_config: bool = True
    online_store_publication_id: str = ""
    auto_find_online_store_publication_id: bool = True


class ShopifyClient:
    def __init__(
        self,
        shop_domain: str,
        api_version: str,
        access_token: str,
        retry: int = 6,
        timeout: int = 60,
    ):
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": str(access_token or "").strip(),
            "Content-Type": "application/json",
        }
        self.retry = max(int(retry), 1)
        self.timeout = max(int(timeout), 1)
        self.call_count = 0

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        last_error: Optional[Exception] = None

        for attempt in range(1, self.retry + 1):
            try:
                response = requests.post(
                    self.url,
                    headers=self.headers,
                    json=payload,
                    timeout=self.timeout,
                )

                if response.status_code == 429:
                    delay = min(30.0, 1.2 * attempt + random.random())
                    print(f"⚠️ Shopify 429 | retry={attempt}/{self.retry} | sleep={delay:.2f}s")
                    time.sleep(delay)
                    continue

                response.raise_for_status()
                data = response.json()
                if data.get("errors"):
                    raise CollectionViewExportError(f"Shopify GraphQL errors: {data['errors']}")
                if "data" not in data:
                    raise CollectionViewExportError(f"Shopify GraphQL response missing data: {data}")

                self.call_count += 1
                return data["data"]

            except Exception as exc:  # retry transport, HTTP and GraphQL errors
                last_error = exc
                if attempt >= self.retry:
                    break
                delay = min(30.0, 1.0 * attempt + random.random())
                print(
                    f"⚠️ Shopify request failed | retry={attempt}/{self.retry} "
                    f"| sleep={delay:.2f}s | error={exc}"
                )
                time.sleep(delay)

        raise CollectionViewExportError(
            f"Shopify GraphQL failed after {self.retry} attempts. Last error: {last_error}"
        )


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value).strip()


def _now_cn() -> str:
    return (dt.datetime.utcnow() + dt.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")


def _run_id() -> str:
    stamp = (dt.datetime.utcnow() + dt.timedelta(hours=8)).strftime("%Y%m%d_%H%M%S")
    return f"export_collection_views_{stamp}"


def _normalise_url_root(value: str) -> str:
    return _safe_str(value).rstrip("/")


def _make_unique_headers(headers: Sequence[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for raw in headers:
        name = _safe_str(raw) or "Unnamed"
        seen[name] = seen.get(name, 0) + 1
        out.append(name if seen[name] == 1 else f"{name}-{seen[name]}")
    return out


def _parse_seq(value: Any, fallback: int) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return float(fallback)


def _strict_json_object(value: Any, context: str) -> Dict[str, Any]:
    text = _safe_str(value)
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception as exc:
        raise CollectionViewExportError(f"{context} is not valid JSON: {text}") from exc
    if not isinstance(obj, dict):
        raise CollectionViewExportError(f"{context} must be a JSON object: {text}")
    return obj


def _records_from_values(values: List[List[Any]], worksheet_name: str) -> List[Dict[str, str]]:
    if not values:
        return []

    headers = [_safe_str(x) for x in values[0]]
    if not any(headers):
        return []

    duplicates = sorted({h for h in headers if h and headers.count(h) > 1})
    if duplicates:
        raise CollectionViewExportError(
            f"Worksheet {worksheet_name} has duplicate headers: {duplicates}"
        )

    rows: List[Dict[str, str]] = []
    for raw in values[1:]:
        padded = list(raw) + [""] * max(0, len(headers) - len(raw))
        record = {
            headers[i]: _safe_str(padded[i])
            for i in range(len(headers))
            if headers[i]
        }
        if any(v != "" for v in record.values()):
            rows.append(record)
    return rows


def _read_records(spreadsheet, tab_name: str) -> List[Dict[str, str]]:
    try:
        ws = spreadsheet.worksheet(tab_name)
    except Exception as exc:
        raise CollectionViewExportError(
            f"Worksheet not found: {tab_name} in {getattr(spreadsheet, 'url', '')}"
        ) from exc
    return _records_from_values(ws.get_all_values(), tab_name)


def _read_account_config(console_spreadsheet, site_code: str) -> Dict[str, str]:
    try:
        ws = console_spreadsheet.worksheet(CFG_ACCOUNT_TAB)
    except Exception as exc:
        raise CollectionViewExportError(f"Worksheet not found: {CFG_ACCOUNT_TAB}") from exc

    values = ws.get_all_values()
    if not values:
        raise CollectionViewExportError(f"{CFG_ACCOUNT_TAB} is empty")

    rows = [[_safe_str(c) for c in r] for r in values if any(_safe_str(c) for c in r)]
    header = [x.lower() for x in rows[0]] if rows else []
    config: Dict[str, str] = {}

    key_names = ["key", "config_key", "field_key", "name", "setting", "account_key"]
    value_names = ["value", "config_value", "field_value", "setting_value", "account_value"]

    key_idx = next((header.index(x) for x in key_names if x in header), None)
    value_idx = next((header.index(x) for x in value_names if x in header), None)

    if key_idx is not None and value_idx is not None:
        site_idx = next(
            (header.index(x) for x in ["site_code", "account_id", "site"] if x in header),
            None,
        )
        for raw in rows[1:]:
            padded = raw + [""] * max(0, len(header) - len(raw))
            if site_idx is not None:
                row_site = _safe_str(padded[site_idx]).upper()
                if row_site and row_site != site_code.upper():
                    continue
            key = _safe_str(padded[key_idx])
            value = _safe_str(padded[value_idx])
            if key:
                config[key] = value
    else:
        for raw in rows:
            if len(raw) < 2:
                continue
            key = _safe_str(raw[0])
            value = _safe_str(raw[1])
            if key and key.lower() not in {"key", "site_code"}:
                config[key] = value

    required = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "STOREFRONT_BASE_URL",
        "ADMIN_BASE_URL",
    ]
    missing = [key for key in required if not _safe_str(config.get(key))]
    if missing:
        raise CollectionViewExportError(f"{CFG_ACCOUNT_TAB} missing required keys: {missing}")
    return config


def make_gspread_client(service_account_b64: str):
    try:
        info = json.loads(base64.b64decode(service_account_b64).decode("utf-8"))
    except Exception as exc:
        raise CollectionViewExportError("Failed to decode Google service-account base64 JSON") from exc
    credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(credentials)


def get_label_sheet_url(
    console_spreadsheet,
    site_code: str,
    label: str,
) -> str:
    rows = _read_records(console_spreadsheet, CFG_SITES_TAB)
    matches = [
        row
        for row in rows
        if _safe_str(row.get("site_code")).upper() == _safe_str(site_code).upper()
        and _safe_str(row.get("label")) == _safe_str(label)
    ]
    if not matches:
        raise CollectionViewExportError(
            f"{CFG_SITES_TAB} cannot find site_code={site_code}, label={label}"
        )
    if len(matches) > 1:
        raise CollectionViewExportError(
            f"{CFG_SITES_TAB} has duplicate routes: site_code={site_code}, label={label}"
        )
    url = _safe_str(matches[0].get("sheet_url"))
    if not url:
        raise CollectionViewExportError(
            f"{CFG_SITES_TAB} route has empty sheet_url: site_code={site_code}, label={label}"
        )
    return url


def _field_def_from_row(row: Mapping[str, Any]) -> FieldDef:
    return FieldDef(
        field_id=_safe_str(row.get("field_id")),
        display_name=_safe_str(row.get("display_name")),
        entity_type=_safe_str(row.get("entity_type")).upper(),
        field_key=_safe_str(row.get("field_key")),
        expr=_safe_str(row.get("expr")),
        field_type=_safe_str(row.get("field_type")).upper() or "RAW",
        data_type=_safe_str(row.get("data_type")),
        source_type=_safe_str(row.get("source_type")).upper(),
        namespace=_safe_str(row.get("namespace")),
        key=_safe_str(row.get("key")),
    )


def _load_field_definitions(rows: Sequence[Mapping[str, Any]]) -> Dict[str, FieldDef]:
    by_id: Dict[str, FieldDef] = {}
    duplicates: List[str] = []

    for row in rows:
        definition = _field_def_from_row(row)
        if not definition.field_id:
            continue
        if definition.field_id in by_id:
            duplicates.append(definition.field_id)
            continue
        by_id[definition.field_id] = definition

    if duplicates:
        raise CollectionViewExportError(
            f"{CFG_FIELDS_TAB} has duplicate field_id values: {sorted(set(duplicates))[:20]}"
        )
    return by_id


def _merge_view_field_definition(
    view_row: Mapping[str, Any],
    base_definition: Optional[FieldDef],
) -> FieldDef:
    """Cfg__ExportTabFields non-empty values override Cfg__Fields."""

    def pick(column: str, default: str = "") -> str:
        value = _safe_str(view_row.get(column))
        if value:
            return value
        if base_definition is not None:
            return _safe_str(getattr(base_definition, column, default))
        return default

    field_id = _safe_str(view_row.get("field_id"))
    definition = FieldDef(
        field_id=field_id,
        display_name=(base_definition.display_name if base_definition else field_id),
        entity_type=pick("entity_type").upper(),
        field_key=pick("field_key"),
        expr=pick("expr"),
        field_type=pick("field_type", "RAW").upper() or "RAW",
        data_type=pick("data_type"),
        source_type=pick("source_type").upper(),
        namespace=pick("namespace"),
        key=pick("key"),
    )
    return definition


def _select_collection_tab_rows(
    tab_rows: Sequence[Mapping[str, Any]],
    view_toggles: Optional[Mapping[str, bool]],
) -> List[Dict[str, str]]:
    collection_rows = [
        dict(row)
        for row in tab_rows
        if _safe_str(row.get("base_entity_type")).upper() == "COLLECTION"
    ]
    if not collection_rows:
        raise CollectionViewExportError(
            f"{CFG_EXPORT_TABS_TAB} has no base_entity_type=COLLECTION rows"
        )

    seen: set[str] = set()
    duplicates: List[str] = []
    for row in collection_rows:
        view_id = _safe_str(row.get("view_id"))
        if view_id in seen:
            duplicates.append(view_id)
        seen.add(view_id)
    if duplicates:
        raise CollectionViewExportError(
            f"{CFG_EXPORT_TABS_TAB} has duplicate Collection view_id values: {sorted(set(duplicates))}"
        )

    if view_toggles is None:
        return collection_rows

    enabled = {_safe_str(k) for k, value in view_toggles.items() if bool(value)}
    if not enabled:
        raise CollectionViewExportError(
            "VIEW_TOGGLES was supplied but no Collection view is enabled"
        )

    available = {_safe_str(row.get("view_id")) for row in collection_rows}
    unknown = sorted(enabled - available)
    if unknown:
        raise CollectionViewExportError(
            f"VIEW_TOGGLES contains unknown/non-Collection views: {unknown}"
        )
    return [row for row in collection_rows if _safe_str(row.get("view_id")) in enabled]


def _load_views(
    tab_rows: Sequence[Mapping[str, Any]],
    view_field_rows: Sequence[Mapping[str, Any]],
    field_defs: Mapping[str, FieldDef],
    view_toggles: Optional[Mapping[str, bool]],
    default_target_sheet_label: str,
    strict_config: bool,
) -> List[ViewDef]:
    selected_tabs = _select_collection_tab_rows(tab_rows, view_toggles)
    fields_by_view: Dict[str, List[Mapping[str, Any]]] = {}
    for row in view_field_rows:
        fields_by_view.setdefault(_safe_str(row.get("view_id")), []).append(row)

    views: List[ViewDef] = []
    for tab in selected_tabs:
        view_id = _safe_str(tab.get("view_id"))
        if not view_id:
            raise CollectionViewExportError(f"{CFG_EXPORT_TABS_TAB} has blank view_id")

        layout = _safe_str(tab.get("layout")).upper() or "WIDE"
        if layout != "WIDE":
            raise CollectionViewExportError(
                f"Collection view currently supports layout=WIDE only: {view_id}={layout}"
            )

        base_sheet = _safe_str(tab.get("base_sheet"))
        if base_sheet and strict_config:
            raise CollectionViewExportError(
                f"Collection view base_sheet must be blank (no IDX__COLLECTIONS): "
                f"view_id={view_id}, base_sheet={base_sheet}"
            )

        base_key = _safe_str(tab.get("base_key_field_id")) or "COLLECTION|core.gid"
        if base_key != "COLLECTION|core.gid" and strict_config:
            raise CollectionViewExportError(
                f"Collection view base_key_field_id must be COLLECTION|core.gid: "
                f"view_id={view_id}, value={base_key}"
            )

        raw_fields = fields_by_view.get(view_id, [])
        if not raw_fields:
            raise CollectionViewExportError(
                f"{CFG_EXPORT_TAB_FIELDS_TAB} has no fields for view_id={view_id}"
            )

        view_fields: List[ViewField] = []
        for position, row in enumerate(raw_fields, start=1):
            field_id = _safe_str(row.get("field_id"))
            if not field_id:
                raise CollectionViewExportError(
                    f"{CFG_EXPORT_TAB_FIELDS_TAB} has blank field_id: view_id={view_id}, row={position}"
                )

            base_definition = field_defs.get(field_id)
            if base_definition is None and strict_config:
                raise CollectionViewExportError(
                    f"field_id is not registered in {CFG_FIELDS_TAB}: {field_id} "
                    f"(view_id={view_id})"
                )

            definition = _merge_view_field_definition(row, base_definition)
            if definition.entity_type != "COLLECTION":
                raise CollectionViewExportError(
                    f"Collection view can only use COLLECTION fields: "
                    f"view_id={view_id}, field_id={field_id}, entity_type={definition.entity_type}"
                )
            if not definition.expr and definition.source_type != "METAFIELD":
                raise CollectionViewExportError(
                    f"Collection field has no expr: view_id={view_id}, field_id={field_id}"
                )

            alias = (
                _safe_str(row.get("alias"))
                or definition.display_name
                or definition.field_id
            )
            view_fields.append(
                ViewField(
                    view_id=view_id,
                    field_id=field_id,
                    seq=_parse_seq(row.get("seq"), position),
                    alias=alias,
                    field_def=definition,
                )
            )

        view_fields.sort(key=lambda x: (x.seq, x.field_id))

        filter_mode = _safe_str(tab.get("fixed_filter_mode")).upper()
        if filter_mode and filter_mode not in {"AND", "OR"}:
            raise CollectionViewExportError(
                f"fixed_filter_mode must be AND or OR: view_id={view_id}, value={filter_mode}"
            )
        fixed_filters = _strict_json_object(
            tab.get("fixed_filters_json"),
            f"{CFG_EXPORT_TABS_TAB}.{view_id}.fixed_filters_json",
        )

        for filter_field_id in fixed_filters:
            definition = field_defs.get(_safe_str(filter_field_id))
            if definition is None:
                raise CollectionViewExportError(
                    f"Filter field is not registered in {CFG_FIELDS_TAB}: "
                    f"view_id={view_id}, field_id={filter_field_id}"
                )
            if definition.entity_type != "COLLECTION":
                raise CollectionViewExportError(
                    f"Collection view filter must use COLLECTION field: "
                    f"view_id={view_id}, field_id={filter_field_id}"
                )

        views.append(
            ViewDef(
                view_id=view_id,
                target_sheet=_safe_str(tab.get("target_sheet")) or view_id,
                target_sheet_label=(
                    _safe_str(tab.get("target_sheet_label"))
                    or default_target_sheet_label
                ),
                layout=layout,
                base_entity_type="COLLECTION",
                base_sheet=base_sheet,
                base_key_field_id=base_key,
                fixed_filter_mode=filter_mode,
                fixed_filters=fixed_filters,
                fields=tuple(view_fields),
            )
        )

    return views


def _merge_filters(
    view: ViewDef,
    global_filters: Optional[Mapping[str, Any]],
    view_filter_overrides: Optional[Mapping[str, Mapping[str, Any]]],
    field_defs: Mapping[str, FieldDef],
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    merged.update(dict(global_filters or {}))
    merged.update(view.fixed_filters)
    merged.update(dict((view_filter_overrides or {}).get(view.view_id, {})))

    for field_id in merged:
        definition = field_defs.get(_safe_str(field_id))
        if definition is None:
            raise CollectionViewExportError(
                f"Filter field is not registered in {CFG_FIELDS_TAB}: "
                f"view_id={view.view_id}, field_id={field_id}"
            )
        if definition.entity_type != "COLLECTION":
            raise CollectionViewExportError(
                f"Collection filter must use COLLECTION field: "
                f"view_id={view.view_id}, field_id={field_id}"
            )
    return merged




def _build_effective_field_definitions(
    views: Sequence[ViewDef],
    field_defs: Mapping[str, FieldDef],
) -> Dict[str, FieldDef]:
    """
    Start from Cfg__Fields, then apply non-empty semantic overrides already merged
    from Cfg__ExportTabFields. The same field_id cannot have conflicting
    semantics across Collection views because records are materialised once.
    """
    effective = dict(field_defs)
    view_override_seen: Dict[str, FieldDef] = {}

    for view in views:
        for view_field in view.fields:
            field_id = view_field.field_id
            definition = view_field.field_def
            previous_override = view_override_seen.get(field_id)
            if previous_override is not None and previous_override != definition:
                raise CollectionViewExportError(
                    f"Conflicting Cfg__ExportTabFields definitions for field_id={field_id}. "
                    f"Collection fields are fetched once, so semantic definitions must match across views."
                )
            view_override_seen[field_id] = definition
            effective[field_id] = definition

    return effective

def _collect_required_field_ids(
    views: Sequence[ViewDef],
    global_filters: Optional[Mapping[str, Any]],
    view_filter_overrides: Optional[Mapping[str, Mapping[str, Any]]],
) -> List[str]:
    required: set[str] = {"COLLECTION|core.gid"}
    for view in views:
        required.update(field.field_id for field in view.fields)
        required.update(_safe_str(k) for k in view.fixed_filters.keys())
    required.update(_safe_str(k) for k in (global_filters or {}).keys())
    for override in (view_filter_overrides or {}).values():
        required.update(_safe_str(k) for k in override.keys())
    return sorted(x for x in required if x)


def _extract_token_dependencies(expr: str) -> List[str]:
    return [_safe_str(x) for x in TOKEN_RE.findall(_safe_str(expr)) if _safe_str(x)]


def _expand_required_definitions(
    required_field_ids: Iterable[str],
    field_defs: Mapping[str, FieldDef],
) -> Dict[str, FieldDef]:
    """Include token dependencies referenced by calculated expressions."""
    expanded: Dict[str, FieldDef] = {}
    pending = list(required_field_ids)

    while pending:
        field_id = _safe_str(pending.pop())
        if not field_id or field_id in expanded:
            continue
        definition = field_defs.get(field_id)
        if definition is None:
            raise CollectionViewExportError(
                f"Required field is not registered in {CFG_FIELDS_TAB}: {field_id}"
            )
        if definition.entity_type != "COLLECTION":
            raise CollectionViewExportError(
                f"Required Collection field has wrong entity_type: "
                f"{field_id}={definition.entity_type}"
            )
        expanded[field_id] = definition
        for dependency in _extract_token_dependencies(definition.expr):
            if dependency.startswith("COLLECTION|") and dependency not in expanded:
                pending.append(dependency)

    return expanded


def _metafield_identity(definition: FieldDef) -> Optional[Tuple[str, str]]:
    if definition.source_type == "METAFIELD":
        namespace = definition.namespace
        key = definition.key
        if namespace and key:
            return namespace, key

    match = MF_VALUE_RE.match(definition.expr)
    if match:
        return match.group(1), match.group(2)

    field_key = definition.field_key
    if field_key.startswith("mf."):
        parts = field_key.split(".", 2)
        if len(parts) == 3:
            return parts[1], parts[2]
    return None


def _metafield_alias(namespace: str, key: str) -> str:
    digest = hashlib.sha1(f"{namespace}\0{key}".encode("utf-8")).hexdigest()[:12]
    return f"mf_{digest}"


def _collect_metafields(
    definitions: Mapping[str, FieldDef],
) -> Dict[Tuple[str, str], str]:
    mapping: Dict[Tuple[str, str], str] = {}
    for definition in definitions.values():
        identity = _metafield_identity(definition)
        if identity is not None:
            mapping.setdefault(identity, _metafield_alias(*identity))
    return mapping


def _needs_publication_status(definitions: Mapping[str, FieldDef]) -> bool:
    for definition in definitions.values():
        expr = definition.expr.lower()
        if "publishedonpublication" in expr:
            return True
    return False


def resolve_online_store_publication_id(
    client: ShopifyClient,
    configured_id: str = "",
    auto_find: bool = True,
) -> Tuple[str, List[Tuple[str, str]]]:
    query = """
    query($first:Int!){
      publications(first:$first){
        nodes { id name }
      }
    }
    """
    data = client.gql(query, {"first": 100})
    publications = [
        (_safe_str(node.get("id")), _safe_str(node.get("name")))
        for node in data.get("publications", {}).get("nodes", [])
    ]
    valid_ids = {pid for pid, _ in publications if pid}

    configured = _safe_str(configured_id)
    if configured:
        if configured not in valid_ids:
            raise CollectionViewExportError(
                f"Configured ONLINE_STORE_PUBLICATION_ID is not available: {configured}"
            )
        return configured, publications

    if auto_find:
        for publication_id, name in publications:
            if name.lower() == "online store":
                return publication_id, publications

    return "", publications


def build_collections_query(
    metafields: Mapping[Tuple[str, str], str],
    include_publication_status: bool,
) -> str:
    metafield_blocks = []
    for (namespace, key), alias in sorted(metafields.items()):
        metafield_blocks.append(
            f'{alias}: metafield(namespace: {json.dumps(namespace)}, key: {json.dumps(key)}) '
            "{ type value }"
        )

    publication_block = ""
    variable_defs = ["$first:Int!", "$after:String"]
    if include_publication_status:
        variable_defs.append("$pubId:ID!")
        publication_block = "publishedOnPublication(publicationId: $pubId)"

    fields = "\n        ".join(
        [COLLECTION_BASE_SELECTION]
        + metafield_blocks
        + ([publication_block] if publication_block else [])
    )

    return f"""
    query({', '.join(variable_defs)}) {{
      collections(first:$first, after:$after) {{
        nodes {{
        {fields}
        }}
        pageInfo {{ hasNextPage endCursor }}
      }}
    }}
    """


def fetch_all_collections(
    client: ShopifyClient,
    metafields: Mapping[Tuple[str, str], str],
    publication_id: str,
    page_size: int,
    sleep_every_n_calls: int,
    sleep_seconds: float,
) -> Tuple[List[Dict[str, Any]], int]:
    include_status = bool(publication_id)
    query = build_collections_query(metafields, include_publication_status=include_status)

    nodes: List[Dict[str, Any]] = []
    after: Optional[str] = None
    page_count = 0

    print(
        f"🚀 Fetch Collections | page_size={page_size} "
        f"| metafields={len(metafields)} | publication_status={include_status}"
    )

    while True:
        variables: Dict[str, Any] = {
            "first": int(page_size),
            "after": after,
        }
        if include_status:
            variables["pubId"] = publication_id

        data = client.gql(query, variables)
        connection = data.get("collections") or {}
        page_nodes = connection.get("nodes") or []
        nodes.extend(page_nodes)
        page_count += 1

        print(
            f"📦 Collections page {page_count} | page_rows={len(page_nodes)} "
            f"| total_rows={len(nodes)}"
        )

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")

        if sleep_every_n_calls > 0 and page_count % sleep_every_n_calls == 0:
            time.sleep(max(float(sleep_seconds), 0.0))

    return nodes, page_count


def _get_path_value(root: Any, path: str) -> Any:
    """Resolve collection.a.b and optional list indexes such as rules[0]."""
    text = _safe_str(path)
    if text.startswith("collection."):
        text = text[len("collection."):]
    elif text == "collection":
        return root

    current = root
    for segment in text.split(".") if text else []:
        match = re.fullmatch(r"([^\[\]]+)(?:\[(\d+)\])?", segment)
        if not match:
            return None
        key = match.group(1)
        index_text = match.group(2)

        if not isinstance(current, Mapping):
            return None
        current = current.get(key)

        if index_text is not None:
            if not isinstance(current, list):
                return None
            index = int(index_text)
            if index < 0 or index >= len(current):
                return None
            current = current[index]

    return current


def _friendly_rule(rule: Any) -> str:
    if not isinstance(rule, Mapping):
        return ""
    column = _safe_str(rule.get("column")).lower()
    relation = _safe_str(rule.get("relation")).lower()
    condition = _safe_str(rule.get("condition"))
    if not column or not relation:
        return ""
    return " ".join(x for x in [column, relation, condition] if x).strip()


def _normalise_metafield_value(value: Any, data_type: str) -> Any:
    if value is None:
        return ""
    text = str(value)
    type_name = _safe_str(data_type).lower()

    # Shopify returns list/reference/json values as JSON strings. Keep a stable,
    # compact JSON representation without resolving specific field names.
    if type_name.startswith("list.") or type_name in {"json", "list.json"}:
        try:
            parsed = json.loads(text)
            return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return text

    if type_name == "boolean":
        return "TRUE" if text.strip().lower() == "true" else "FALSE"
    return text


def _formula_url_value(expr: str, resolver) -> Optional[str]:
    compact = re.sub(r"\s+", "", _safe_str(expr))
    match = URL_FORMULA_RE.match(compact)
    if not match:
        return None
    first_token = _safe_str(match.group(1))
    prefix = match.group(2)
    second_token = _safe_str(match.group(3))
    if first_token != second_token:
        return None
    source_value = _safe_str(resolver(first_token))
    return "" if not source_value else f"{prefix}{source_value}"


def evaluate_field(
    definition: FieldDef,
    node: Mapping[str, Any],
    all_definitions: Mapping[str, FieldDef],
    metafield_aliases: Mapping[Tuple[str, str], str],
    cache: Dict[str, Any],
    stack: Optional[List[str]] = None,
) -> Any:
    field_id = definition.field_id
    if field_id in cache:
        return cache[field_id]

    stack = list(stack or [])
    if field_id in stack:
        raise CollectionViewExportError(
            f"Circular Collection field expression: {' -> '.join(stack + [field_id])}"
        )
    stack.append(field_id)

    def resolve(other_field_id: str) -> Any:
        other = all_definitions.get(other_field_id)
        if other is None:
            raise CollectionViewExportError(
                f"Expression references unknown field_id: {field_id} -> {other_field_id}"
            )
        return evaluate_field(
            other,
            node,
            all_definitions,
            metafield_aliases,
            cache,
            stack,
        )

    metafield_identity = _metafield_identity(definition)
    if metafield_identity is not None:
        alias = metafield_aliases.get(metafield_identity)
        payload = node.get(alias) if alias else None
        raw_value = payload.get("value") if isinstance(payload, Mapping) else ""
        value = _normalise_metafield_value(raw_value, definition.data_type)
        cache[field_id] = value
        return value

    expr = definition.expr.strip()
    expr_lower = expr.lower()

    if expr.startswith("collection."):
        raw = _get_path_value(node, expr)
        if isinstance(raw, (dict, list)):
            value = json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
        elif isinstance(raw, bool):
            value = "TRUE" if raw else "FALSE"
        else:
            value = "" if raw is None else raw
        cache[field_id] = value
        return value

    if expr_lower == 'if_not_null(collection.ruleset,"smart","manual")':
        value = "SMART" if node.get("ruleSet") is not None else "MANUAL"
        cache[field_id] = value
        return value

    if expr_lower == 'if(collection.ruleset.applieddisjunctively,"any","all")':
        rule_set = node.get("ruleSet")
        if not isinstance(rule_set, Mapping):
            value = ""
        else:
            value = "any" if bool(rule_set.get("appliedDisjunctively")) else "all"
        cache[field_id] = value
        return value

    rule_match = RULE_TEXT_RE.match(expr)
    if rule_match:
        index = int(rule_match.group(1))
        rules = (node.get("ruleSet") or {}).get("rules") or []
        value = _friendly_rule(rules[index]) if index < len(rules) else ""
        cache[field_id] = value
        return value

    if expr.startswith("="):
        value = _formula_url_value(expr, resolve)
        if value is not None:
            cache[field_id] = value
            return value

    raise CollectionViewExportError(
        f"Unsupported Collection field expression: field_id={field_id}, expr={expr}"
    )


def build_collection_records(
    nodes: Sequence[Mapping[str, Any]],
    required_definitions: Mapping[str, FieldDef],
    all_definitions: Mapping[str, FieldDef],
    metafield_aliases: Mapping[Tuple[str, str], str],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for node in nodes:
        cache: Dict[str, Any] = {}
        record: Dict[str, Any] = {}
        for field_id, definition in required_definitions.items():
            record[field_id] = evaluate_field(
                definition,
                node,
                all_definitions,
                metafield_aliases,
                cache,
            )
        records.append(record)
    return records


def _normalise_compare_value(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return _safe_str(value)


def _matches_filter(actual: Any, wanted: Any) -> bool:
    actual_text = _normalise_compare_value(actual)
    if isinstance(wanted, list):
        return actual_text in {_normalise_compare_value(x) for x in wanted}
    return actual_text == _normalise_compare_value(wanted)


def apply_filters(
    records: Sequence[Dict[str, Any]],
    filters: Mapping[str, Any],
    mode: str,
    view_id: str,
) -> List[Dict[str, Any]]:
    if not filters:
        return list(records)

    mode = _safe_str(mode).upper() or "AND"
    if mode not in {"AND", "OR"}:
        raise CollectionViewExportError(
            f"Filter mode must be AND or OR: view_id={view_id}, value={mode}"
        )

    output: List[Dict[str, Any]] = []
    for record in records:
        checks: List[bool] = []
        for field_id, wanted in filters.items():
            if field_id not in record:
                raise CollectionViewExportError(
                    f"Filter field was not materialised: view_id={view_id}, field_id={field_id}"
                )
            checks.append(_matches_filter(record.get(field_id), wanted))

        keep = all(checks) if mode == "AND" else any(checks)
        if keep:
            output.append(record)
    return output


def ensure_worksheet(spreadsheet, title: str, rows: int, cols: int):
    try:
        ws = spreadsheet.worksheet(title)
    except Exception:
        ws = spreadsheet.add_worksheet(
            title=title,
            rows=max(rows, 1000),
            cols=max(cols, 26),
        )
    return ws


def _resize_if_needed(ws, required_rows: int, required_cols: int) -> None:
    rows = max(required_rows, 1)
    cols = max(required_cols, 1)
    if ws.row_count < rows or ws.col_count < cols:
        ws.resize(rows=max(rows, ws.row_count), cols=max(cols, ws.col_count))


def _retry_sheet_call(label: str, func, retry: int = 6):
    last_error = None
    for attempt in range(1, max(retry, 1) + 1):
        try:
            return func()
        except Exception as exc:
            last_error = exc
            if attempt >= retry:
                break
            delay = min(20.0, 1.0 * attempt + random.random())
            print(f"⚠️ Sheets {label} failed | retry={attempt}/{retry} | sleep={delay:.2f}s")
            time.sleep(delay)
    raise CollectionViewExportError(f"Sheets {label} failed: {last_error}")


def write_view_table(
    spreadsheet,
    view: ViewDef,
    records: Sequence[Dict[str, Any]],
    write_mode: str,
    chunk_rows: int,
    retry: int,
) -> Dict[str, Any]:
    headers = _make_unique_headers([field.alias for field in view.fields])
    body = [
        [record.get(field.field_id, "") for field in view.fields]
        for record in records
    ]

    ws = ensure_worksheet(
        spreadsheet,
        view.target_sheet,
        rows=len(body) + 20,
        cols=len(headers) + 10,
    )
    _resize_if_needed(ws, len(body) + 20, len(headers) + 10)

    mode = _safe_str(write_mode).upper() or "REPLACE"
    if mode == "REPLACE":
        _retry_sheet_call("clear", ws.clear, retry=retry)
        _retry_sheet_call(
            "write header",
            lambda: ws.update(
                range_name="A1",
                values=[headers],
                value_input_option="RAW",
            ),
            retry=retry,
        )

        start_row = 2
        for offset in range(0, len(body), max(int(chunk_rows), 1)):
            block = body[offset:offset + max(int(chunk_rows), 1)]
            if not block:
                continue
            first_row = start_row + offset
            last_row = first_row + len(block) - 1
            last_col = rowcol_to_a1(1, len(headers)).replace("1", "")
            target_range = f"A{first_row}:{last_col}{last_row}"
            _retry_sheet_call(
                f"write {target_range}",
                lambda target_range=target_range, block=block: ws.update(
                    range_name=target_range,
                    values=block,
                    value_input_option="RAW",
                ),
                retry=retry,
            )

    elif mode == "APPEND":
        existing_header = [_safe_str(x) for x in ws.row_values(1)]
        if existing_header and existing_header != headers:
            raise CollectionViewExportError(
                f"APPEND header mismatch: view_id={view.view_id}, tab={view.target_sheet}"
            )
        if not existing_header:
            _retry_sheet_call(
                "write header",
                lambda: ws.update(
                    range_name="A1",
                    values=[headers],
                    value_input_option="RAW",
                ),
                retry=retry,
            )
        for offset in range(0, len(body), max(int(chunk_rows), 1)):
            block = body[offset:offset + max(int(chunk_rows), 1)]
            if block:
                _retry_sheet_call(
                    "append rows",
                    lambda block=block: ws.append_rows(block, value_input_option="RAW"),
                    retry=retry,
                )
    else:
        raise CollectionViewExportError(f"Unsupported WRITE_MODE: {write_mode}")

    return {
        "view_id": view.view_id,
        "target_sheet": view.target_sheet,
        "target_sheet_label": view.target_sheet_label,
        "rows_written": len(body),
        "cols_written": len(headers),
        "fixed_filter_mode": view.fixed_filter_mode,
        "fixed_filters": view.fixed_filters,
    }


def _ensure_runlog_tab(spreadsheet):
    try:
        ws = spreadsheet.worksheet(RUNLOG_TAB_NAME)
    except Exception:
        ws = spreadsheet.add_worksheet(
            title=RUNLOG_TAB_NAME,
            rows=1000,
            cols=len(RUNLOG_HEADER) + 5,
        )

    current = [_safe_str(x) for x in ws.row_values(1)]
    if current != RUNLOG_HEADER:
        ws.clear()
        ws.update(range_name="A1", values=[RUNLOG_HEADER], value_input_option="RAW")
    return ws


def _runlog_row(
    run_id: str,
    ts_cn: str,
    site_code: str,
    log_type: str,
    status: str,
    field_key: str,
    rows_loaded: int,
    rows_planned: int,
    rows_written: int,
    rows_skipped: int,
    message: str,
    error_reason: str = "",
) -> List[Any]:
    return [
        run_id,
        ts_cn,
        JOB_NAME,
        "apply",
        log_type,
        status,
        site_code,
        "COLLECTION",
        "",
        field_key,
        rows_loaded,
        rows_loaded,
        rows_loaded,
        rows_planned,
        rows_written,
        rows_skipped,
        message,
        error_reason,
    ]


def write_runlog(
    gc,
    console_spreadsheet,
    site_code: str,
    runlog_label: str,
    rows: Sequence[Sequence[Any]],
) -> None:
    if not runlog_label or not rows:
        return
    try:
        url = get_label_sheet_url(console_spreadsheet, site_code, runlog_label)
        spreadsheet = gc.open_by_url(url)
        ws = _ensure_runlog_tab(spreadsheet)
        current_rows = max(len(ws.get_all_values()), 1)
        required_rows = current_rows + len(rows) + 10
        if ws.row_count < required_rows:
            ws.resize(rows=required_rows, cols=max(ws.col_count, len(RUNLOG_HEADER)))
        ws.append_rows(list(rows), value_input_option="RAW")
    except Exception as exc:
        print(f"⚠️ RunLog write failed: {exc}")


def run(
    *,
    site_code: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    shopify_token: str,
    view_toggles: Optional[Dict[str, bool]] = None,
    global_filters: Optional[Dict[str, Any]] = None,
    filter_mode: str = "AND",
    view_filter_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    runlog_label: str = "runlog_sheet",
    default_target_sheet_label: str = "export_other",
    write_mode: str = "REPLACE",
    online_store_publication_id: str = "",
    auto_find_online_store_publication_id: bool = True,
    page_size: int = 250,
    write_chunk_rows: int = 2000,
    retry: int = 6,
    request_timeout: int = 60,
    sleep_every_n_calls: int = 20,
    sleep_seconds: float = 1.0,
    strict_config: bool = True,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Run all configured Collection views, or only views enabled in view_toggles.

    view_toggles:
      None -> run every Cfg__ExportTabs row where base_entity_type=COLLECTION.
      {"V_COLLECTION_ALL": True, ...} -> run only True entries.
    """
    run_id = _run_id()
    ts_cn = _now_cn()
    site_code_norm = _safe_str(site_code).upper()

    gc = make_gspread_client(gsheet_sa_b64)
    console_spreadsheet = gc.open_by_url(console_core_url)
    account_cfg = _read_account_config(console_spreadsheet, site_code_norm)

    runtime = RuntimeConfig(
        site_code=site_code_norm,
        console_core_url=_safe_str(console_core_url),
        shop_domain=_safe_str(account_cfg.get("SHOP_DOMAIN")),
        api_version=_safe_str(account_cfg.get("SHOPIFY_API_VERSION")),
        storefront_base_url=_normalise_url_root(account_cfg.get("STOREFRONT_BASE_URL", "")),
        admin_base_url=_normalise_url_root(account_cfg.get("ADMIN_BASE_URL", "")),
        runlog_label=_safe_str(runlog_label),
        default_target_sheet_label=_safe_str(default_target_sheet_label) or "export_other",
        write_mode=_safe_str(write_mode).upper() or "REPLACE",
        page_size=int(page_size),
        write_chunk_rows=int(write_chunk_rows),
        retry=int(retry),
        request_timeout=int(request_timeout),
        sleep_every_n_calls=int(sleep_every_n_calls),
        sleep_seconds=float(sleep_seconds),
        strict_config=bool(strict_config),
        online_store_publication_id=_safe_str(online_store_publication_id),
        auto_find_online_store_publication_id=bool(auto_find_online_store_publication_id),
    )

    if verbose:
        print(f"========== {JOB_NAME} | start ==========")
        print(f"run_id={run_id}")
        print(f"site_code={runtime.site_code}")
        print(f"shop_domain={runtime.shop_domain}")
        print(f"api_version={runtime.api_version}")
        print(f"write_mode={runtime.write_mode}")
        print("base_sheet policy=blank / in-memory Collections")

    config_url = get_label_sheet_url(console_spreadsheet, runtime.site_code, "config")
    config_spreadsheet = gc.open_by_url(config_url)

    field_rows = _read_records(config_spreadsheet, CFG_FIELDS_TAB)
    tab_rows = _read_records(config_spreadsheet, CFG_EXPORT_TABS_TAB)
    view_field_rows = _read_records(config_spreadsheet, CFG_EXPORT_TAB_FIELDS_TAB)

    field_defs = _load_field_definitions(field_rows)
    views = _load_views(
        tab_rows=tab_rows,
        view_field_rows=view_field_rows,
        field_defs=field_defs,
        view_toggles=view_toggles,
        default_target_sheet_label=runtime.default_target_sheet_label,
        strict_config=runtime.strict_config,
    )

    effective_filter_mode = _safe_str(filter_mode).upper() or "AND"
    if effective_filter_mode not in {"AND", "OR"}:
        raise CollectionViewExportError(f"filter_mode must be AND or OR: {filter_mode}")

    # Validate and materialise global/override filter definitions too.
    merged_filter_by_view: Dict[str, Dict[str, Any]] = {}
    for view in views:
        merged_filter_by_view[view.view_id] = _merge_filters(
            view,
            global_filters,
            view_filter_overrides,
            field_defs,
        )

    effective_field_defs = _build_effective_field_definitions(views, field_defs)
    required_ids = _collect_required_field_ids(
        views,
        global_filters,
        view_filter_overrides,
    )
    required_definitions = _expand_required_definitions(required_ids, effective_field_defs)
    metafield_aliases = _collect_metafields(required_definitions)

    client = ShopifyClient(
        shop_domain=runtime.shop_domain,
        api_version=runtime.api_version,
        access_token=shopify_token,
        retry=runtime.retry,
        timeout=runtime.request_timeout,
    )

    publication_required = _needs_publication_status(required_definitions)
    publication_id = ""
    publications: List[Tuple[str, str]] = []
    if publication_required:
        publication_id, publications = resolve_online_store_publication_id(
            client,
            configured_id=runtime.online_store_publication_id,
            auto_find=runtime.auto_find_online_store_publication_id,
        )
        if not publication_id:
            raise CollectionViewExportError(
                "A configured Collection view/filter requires online-store publication status, "
                "but the Online Store publication ID could not be resolved"
            )

    if verbose:
        print(f"config_url={config_url}")
        print(f"collection_views={len(views)}")
        print("view_ids=" + ", ".join(view.view_id for view in views))
        print(f"required_fields={len(required_definitions)}")
        print(f"required_metafields={len(metafield_aliases)}")
        print(f"publication_required={publication_required}")
        if publication_required:
            print(f"publication_id={publication_id}")

    nodes: List[Dict[str, Any]] = []
    records: List[Dict[str, Any]] = []
    page_count = 0
    output_results: List[Dict[str, Any]] = []
    runlog_rows: List[List[Any]] = []

    try:
        nodes, page_count = fetch_all_collections(
            client=client,
            metafields=metafield_aliases,
            publication_id=publication_id,
            page_size=runtime.page_size,
            sleep_every_n_calls=runtime.sleep_every_n_calls,
            sleep_seconds=runtime.sleep_seconds,
        )
        records = build_collection_records(
            nodes=nodes,
            required_definitions=required_definitions,
            all_definitions=effective_field_defs,
            metafield_aliases=metafield_aliases,
        )

        output_sheet_cache: Dict[str, Any] = {}
        output_url_cache: Dict[str, str] = {}

        for view in views:
            filters = merged_filter_by_view[view.view_id]
            # Fixed mode matches Product View behavior and applies to the merged filter set.
            mode = view.fixed_filter_mode or effective_filter_mode
            filtered = apply_filters(records, filters, mode, view.view_id)

            label = view.target_sheet_label
            if label not in output_sheet_cache:
                target_url = get_label_sheet_url(
                    console_spreadsheet,
                    runtime.site_code,
                    label,
                )
                output_sheet_cache[label] = gc.open_by_url(target_url)
                output_url_cache[label] = target_url

            result = write_view_table(
                spreadsheet=output_sheet_cache[label],
                view=view,
                records=filtered,
                write_mode=runtime.write_mode,
                chunk_rows=runtime.write_chunk_rows,
                retry=runtime.retry,
            )
            result["target_sheet_url"] = output_url_cache[label]
            result["rows_before_filter"] = len(records)
            result["rows_skipped"] = len(records) - len(filtered)
            result["effective_filters"] = filters
            output_results.append(result)

            runlog_rows.append(
                _runlog_row(
                    run_id=run_id,
                    ts_cn=ts_cn,
                    site_code=runtime.site_code,
                    log_type="view",
                    status="SUCCESS",
                    field_key=view.view_id,
                    rows_loaded=len(records),
                    rows_planned=len(filtered),
                    rows_written=len(filtered),
                    rows_skipped=len(records) - len(filtered),
                    message=(
                        f"view ok | target={label}/{view.target_sheet} "
                        f"| rows={len(filtered)} | cols={len(view.fields)}"
                    ),
                )
            )

            if verbose:
                print(
                    f"✅ view={view.view_id} | target={label}/{view.target_sheet} "
                    f"| rows={len(filtered)} | cols={len(view.fields)}"
                )

        total_written = sum(int(x.get("rows_written", 0)) for x in output_results)
        total_skipped = sum(int(x.get("rows_skipped", 0)) for x in output_results)
        summary_message = (
            f"export ok | pages={page_count} | collections={len(records)} "
            f"| views={len(output_results)} | total_view_rows={total_written}"
        )
        runlog_rows.append(
            _runlog_row(
                run_id=run_id,
                ts_cn=ts_cn,
                site_code=runtime.site_code,
                log_type="summary",
                status="SUCCESS",
                field_key="",
                rows_loaded=len(records),
                rows_planned=total_written,
                rows_written=total_written,
                rows_skipped=total_skipped,
                message=summary_message,
            )
        )
        write_runlog(
            gc,
            console_spreadsheet,
            runtime.site_code,
            runtime.runlog_label,
            runlog_rows,
        )

        result = {
            "ok": True,
            "run_id": run_id,
            "job_name": JOB_NAME,
            "site_code": runtime.site_code,
            "config_sheet_url": config_url,
            "collection_count": len(records),
            "page_count": page_count,
            "shopify_call_count": client.call_count,
            "required_field_count": len(required_definitions),
            "required_metafield_count": len(metafield_aliases),
            "publication_id_used": publication_id,
            "view_count": len(output_results),
            "results": output_results,
            "message": summary_message,
            "finished_at_cn": _now_cn(),
        }

        if verbose:
            print("✅", summary_message)
            print(f"========== {JOB_NAME} | end ==========")
        return result

    except Exception as exc:
        failure_message = f"export failed | {type(exc).__name__}: {exc}"
        failure_row = _runlog_row(
            run_id=run_id,
            ts_cn=ts_cn,
            site_code=runtime.site_code,
            log_type="summary",
            status="ERROR",
            field_key="",
            rows_loaded=len(records) or len(nodes),
            rows_planned=0,
            rows_written=0,
            rows_skipped=0,
            message=failure_message,
            error_reason=str(exc),
        )
        write_runlog(
            gc,
            console_spreadsheet,
            runtime.site_code,
            runtime.runlog_label,
            [failure_row],
        )
        raise
