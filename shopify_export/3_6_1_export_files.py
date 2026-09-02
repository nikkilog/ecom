# -*- coding: utf-8 -*-
"""
job_name: export_files
canonical_module_path: shopify_export/3_5_1_export_files.py

Config-driven Shopify Files export.

Configuration authority:
- Config / Cfg__ExportTabs controls the view and output route.
- Config / Cfg__ExportTabFields controls output fields, aliases and order.
- Cfg__ExportTabs.target_sheet_label is required.

Business date semantics:
- START_DATE and END_DATE are inclusive project-local calendar dates.
- The Shopify query uses [start midnight, day-after-end midnight) in UTC.
- Shopify API/query failures happen before the output tab is cleared.
"""

from __future__ import annotations

import csv
import datetime as dt
import importlib
import json
import random
import re
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests
from gspread.utils import rowcol_to_a1


JOB_NAME = "export_files"
MODULE_PATH = "shopify_export.3_5_1_export_files"
MODULE_VERSION = "2026-09-02-config-route-created-at-v1"
ENTITY_TYPE = "FILE"
DEFAULT_VIEW_ID = "V_FILE"
DEFAULT_TARGET_SHEET_LABEL = "export_file"
BASE_KEY_FIELD_ID = "FILE|core.gid"

CFG_EXPORT_TABS_TAB = "Cfg__ExportTabs"
CFG_EXPORT_TAB_FIELDS_TAB = "Cfg__ExportTabFields"
RUNLOG_TAB_NAME = "Ops__RunLog"

RUNLOG_HEADER = [
    "run_id", "ts_cn", "job_name", "phase", "log_type", "status",
    "site_code", "entity_type", "gid", "field_key", "rows_loaded",
    "rows_pending", "rows_recognized", "rows_planned", "rows_written",
    "rows_skipped", "message", "error_reason",
]

FILES_QUERY = """
query ExportFiles($first: Int!, $after: String, $query: String!) {
  files(
    first: $first
    after: $after
    query: $query
    sortKey: CREATED_AT
    reverse: true
  ) {
    nodes {
      __typename
      id
      createdAt
      ... on GenericFile {
        url
        mimeType
        originalFileSize
      }
      ... on MediaImage {
        mimeType
        image { url }
        originalSource { fileSize }
      }
      ... on Video {
        filename
        originalSource { url mimeType fileSize }
      }
      ... on Model3d {
        filename
        originalSource { url mimeType filesize }
      }
      ... on ExternalVideo {
        originUrl
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


class FileExportError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExportField:
    field_id: str
    seq: float
    alias: str
    expr: str
    field_type: str
    data_type: str


@dataclass(frozen=True)
class ExportView:
    view_id: str
    target_sheet: str
    target_sheet_label: str
    layout: str
    base_entity_type: str
    base_sheet: str
    base_key_field_id: str
    fixed_filter_mode: str
    fixed_filters: Dict[str, Any]
    fields: Tuple[ExportField, ...]


def _runtime_core():
    """Reuse the established Console Core auth/registry boundary."""
    return importlib.import_module("shopify_export.3_4_1_export_customers")


def resolve_runtime_context(**kwargs):
    return _runtime_core().resolve_runtime_context(**kwargs)


def update_existing_notebook_registry_row(**kwargs):
    return _runtime_core().update_existing_notebook_registry_row(**kwargs)


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none"} else text


def _is_enabled(value: Any, default: bool = True) -> bool:
    text = _safe_str(value).lower()
    if not text:
        return default
    return text in {"true", "1", "yes", "y", "on", "是"}


def _parse_seq(value: Any, fallback: int) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return float(fallback)


def _make_unique_headers(headers: Sequence[str]) -> List[str]:
    seen: Dict[str, int] = {}
    result: List[str] = []
    for raw in headers:
        name = _safe_str(raw) or "Unnamed"
        seen[name] = seen.get(name, 0) + 1
        result.append(name if seen[name] == 1 else f"{name}-{seen[name]}")
    return result


def _parse_json_object(value: Any, context: str) -> Dict[str, Any]:
    text = _safe_str(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise FileExportError(f"Invalid JSON in {context}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise FileExportError(f"{context} must be a JSON object.")
    return parsed


def _read_records(spreadsheet, tab_name: str, retry: int = 6) -> List[Dict[str, str]]:
    core = _runtime_core()
    try:
        worksheet = core._retry_sheet_call(
            f"open worksheet {tab_name}", lambda: spreadsheet.worksheet(tab_name), retry=retry
        )
        values = core._retry_sheet_call(
            f"read worksheet {tab_name}", worksheet.get_all_values, retry=retry
        )
    except Exception as exc:
        raise FileExportError(f"Cannot read {tab_name}: {exc}") from exc
    if not values:
        return []
    headers = [_safe_str(value) for value in values[0]]
    if not any(headers):
        return []
    records: List[Dict[str, str]] = []
    for row_number, raw_row in enumerate(values[1:], start=2):
        row = list(raw_row) + [""] * max(0, len(headers) - len(raw_row))
        if not any(_safe_str(value) for value in row):
            continue
        record = {headers[index]: _safe_str(row[index]) for index in range(len(headers)) if headers[index]}
        record["__row_number__"] = str(row_number)
        records.append(record)
    return records


def _select_view(
    tab_rows: Sequence[Mapping[str, Any]],
    field_rows: Sequence[Mapping[str, Any]],
    view_id: str,
) -> ExportView:
    wanted = _safe_str(view_id) or DEFAULT_VIEW_ID
    matches = [row for row in tab_rows if _safe_str(row.get("view_id")) == wanted]
    if len(matches) != 1:
        raise FileExportError(
            f"{CFG_EXPORT_TABS_TAB} must contain exactly one view_id={wanted}; found={len(matches)}"
        )
    row = matches[0]
    if "enabled" in row and not _is_enabled(row.get("enabled"), default=True):
        raise FileExportError(f"view_id={wanted} is disabled.")

    target_sheet = _safe_str(row.get("target_sheet"))
    target_sheet_label = _safe_str(row.get("target_sheet_label"))
    layout = (_safe_str(row.get("layout")) or "WIDE").upper()
    base_entity_type = _safe_str(
        row.get("base_entity_type") or row.get("base_entity")
    ).upper()
    base_sheet = _safe_str(row.get("base_sheet"))
    base_key = _safe_str(row.get("base_key_field_id"))
    filter_mode = (_safe_str(row.get("fixed_filter_mode")) or "AND").upper()
    fixed_filters = _parse_json_object(
        row.get("fixed_filters_json"), f"{CFG_EXPORT_TABS_TAB}!fixed_filters_json ({wanted})"
    )

    if not target_sheet:
        raise FileExportError(f"view_id={wanted} is missing target_sheet.")
    if not target_sheet_label:
        raise FileExportError(
            f"view_id={wanted} is missing required target_sheet_label. "
            f"Set it to {DEFAULT_TARGET_SHEET_LABEL!r}."
        )
    if layout != "WIDE":
        raise FileExportError(f"view_id={wanted} requires layout=WIDE; got={layout}")
    if base_entity_type != ENTITY_TYPE:
        raise FileExportError(
            f"view_id={wanted} requires base_entity_type=FILE; got={base_entity_type}"
        )
    if base_sheet:
        raise FileExportError(
            f"view_id={wanted} base_sheet must be blank; Files are fetched directly from Shopify."
        )
    if base_key != BASE_KEY_FIELD_ID:
        raise FileExportError(
            f"view_id={wanted} requires base_key_field_id={BASE_KEY_FIELD_ID}; got={base_key}"
        )
    if filter_mode not in {"AND", "OR"}:
        raise FileExportError(f"fixed_filter_mode must be AND or OR; got={filter_mode}")

    selected_rows = [
        item for item in field_rows
        if _safe_str(item.get("view_id")) == wanted
        and ("enabled" not in item or _is_enabled(item.get("enabled"), default=True))
    ]
    if not selected_rows:
        raise FileExportError(f"{CFG_EXPORT_TAB_FIELDS_TAB} has no fields for view_id={wanted}.")

    fields: List[ExportField] = []
    seen: set[str] = set()
    for index, item in enumerate(selected_rows, start=1):
        field_id = _safe_str(item.get("field_id"))
        if not field_id:
            raise FileExportError(
                f"{CFG_EXPORT_TAB_FIELDS_TAB} row {item.get('__row_number__')} is missing field_id."
            )
        if field_id in seen:
            raise FileExportError(f"Duplicate field_id for {wanted}: {field_id}")
        seen.add(field_id)
        alias = _safe_str(item.get("alias")) or field_id
        fields.append(
            ExportField(
                field_id=field_id,
                seq=_parse_seq(item.get("seq"), index),
                alias=alias,
                expr=_safe_str(item.get("expr")),
                field_type=(_safe_str(item.get("field_type")) or "RAW").upper(),
                data_type=(_safe_str(item.get("data_type")) or "string").lower(),
            )
        )
    fields.sort(key=lambda item: (item.seq, item.field_id))

    supported = set(_canonical_file_values({}).keys())
    unsupported = [field.field_id for field in fields if field.field_id not in supported]
    if unsupported:
        raise FileExportError(
            "Unsupported FILE field_id(s) in Cfg__ExportTabFields: " + ", ".join(unsupported)
        )

    return ExportView(
        view_id=wanted,
        target_sheet=target_sheet,
        target_sheet_label=target_sheet_label,
        layout=layout,
        base_entity_type=base_entity_type,
        base_sheet=base_sheet,
        base_key_field_id=base_key,
        fixed_filter_mode=filter_mode,
        fixed_filters=fixed_filters,
        fields=tuple(fields),
    )


def normalize_date_window(
    start_date: str,
    end_date: str,
    timezone_name: str,
) -> Dict[str, str]:
    try:
        start_day = dt.date.fromisoformat(_safe_str(start_date))
        end_day = dt.date.fromisoformat(_safe_str(end_date))
    except Exception as exc:
        raise FileExportError("START_DATE and END_DATE must use YYYY-MM-DD.") from exc
    if start_day > end_day:
        raise FileExportError("START_DATE cannot be after END_DATE.")
    try:
        timezone = ZoneInfo(_safe_str(timezone_name) or "UTC")
    except Exception as exc:
        raise FileExportError(f"Unknown project timezone: {timezone_name}") from exc

    start_local = dt.datetime.combine(start_day, dt.time.min, tzinfo=timezone)
    end_local_exclusive = dt.datetime.combine(
        end_day + dt.timedelta(days=1), dt.time.min, tzinfo=timezone
    )
    start_utc = start_local.astimezone(dt.timezone.utc)
    end_utc_exclusive = end_local_exclusive.astimezone(dt.timezone.utc)

    def iso_z(value: dt.datetime) -> str:
        return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    start_iso = iso_z(start_utc)
    end_iso = iso_z(end_utc_exclusive)
    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "timezone": str(timezone),
        "start_local": start_local.isoformat(),
        "end_local_exclusive": end_local_exclusive.isoformat(),
        "start_utc": start_iso,
        "end_utc_exclusive": end_iso,
        "shopify_query": f"created_at:>={start_iso} created_at:<{end_iso}",
    }


class ShopifyClient:
    def __init__(
        self,
        shop_domain: str,
        api_version: str,
        access_token: str,
        retry: int = 6,
        timeout: int = 60,
    ) -> None:
        self.url = f"https://{_safe_str(shop_domain)}/admin/api/{_safe_str(api_version)}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": _safe_str(access_token),
            "Content-Type": "application/json",
        }
        self.retry = max(int(retry), 1)
        self.timeout = max(int(timeout), 1)
        self.call_count = 0

    @staticmethod
    def _retryable_graphql(errors: Any) -> bool:
        retryable = {"THROTTLED", "INTERNAL_SERVER_ERROR", "SERVICE_UNAVAILABLE"}
        if not isinstance(errors, list):
            return False
        for item in errors:
            if isinstance(item, Mapping):
                code = _safe_str((item.get("extensions") or {}).get("code")).upper()
                if code in retryable:
                    return True
        return False

    def gql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retry + 1):
            try:
                response = requests.post(
                    self.url,
                    headers=self.headers,
                    json={"query": query, "variables": variables},
                    timeout=self.timeout,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise FileExportError(
                        f"Shopify HTTP {response.status_code}: {response.text[:500]}"
                    )
                response.raise_for_status()
                payload = response.json()
                errors = payload.get("errors")
                if errors:
                    error = FileExportError(f"Shopify GraphQL errors: {errors}")
                    if not self._retryable_graphql(errors):
                        raise error
                    raise error
                if "data" not in payload:
                    raise FileExportError("Shopify GraphQL response is missing data.")
                self.call_count += 1
                return payload["data"]
            except Exception as exc:
                last_error = exc
                retryable_http = isinstance(exc, requests.RequestException)
                retryable_text = any(
                    token in str(exc).upper()
                    for token in ("HTTP 429", "HTTP 500", "HTTP 502", "HTTP 503", "HTTP 504", "THROTTLED")
                )
                if attempt >= self.retry or not (retryable_http or retryable_text):
                    raise
                delay = min(30.0, 1.5 * (2 ** (attempt - 1))) + random.random()
                print(
                    f"[Shopify retry] attempt={attempt}/{self.retry} | sleep={delay:.1f}s | {type(exc).__name__}",
                    flush=True,
                )
                time.sleep(delay)
        raise FileExportError(f"Shopify request failed: {last_error}")


def fetch_files(
    client: ShopifyClient,
    search_query: str,
    page_size: int = 250,
    sleep_every_n_calls: int = 20,
    sleep_seconds: float = 1.0,
) -> Tuple[List[Dict[str, Any]], int]:
    page_size = max(1, min(int(page_size), 250))
    nodes: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 0
    while True:
        data = client.gql(
            FILES_QUERY,
            {"first": page_size, "after": cursor, "query": search_query},
        )
        connection = data.get("files") or {}
        page_nodes = connection.get("nodes") or []
        page_info = connection.get("pageInfo") or {}
        page += 1
        nodes.extend(page_nodes)
        print(
            f"[Shopify Files] page={page} | page_rows={len(page_nodes)} | total={len(nodes)}",
            flush=True,
        )
        if not page_info.get("hasNextPage"):
            break
        cursor = _safe_str(page_info.get("endCursor"))
        if not cursor:
            raise FileExportError("Shopify pagination says hasNextPage but endCursor is blank.")
        if sleep_every_n_calls > 0 and page % int(sleep_every_n_calls) == 0:
            time.sleep(max(float(sleep_seconds), 0.0))
    return nodes, page


def _source(node: Mapping[str, Any]) -> Mapping[str, Any]:
    source = node.get("originalSource")
    return source if isinstance(source, Mapping) else {}


def _primary_url(node: Mapping[str, Any]) -> str:
    typename = _safe_str(node.get("__typename"))
    if typename == "GenericFile":
        return _safe_str(node.get("url"))
    if typename == "MediaImage":
        image = node.get("image") or {}
        return _safe_str(image.get("url")) if isinstance(image, Mapping) else ""
    if typename in {"Video", "Model3d"}:
        return _safe_str(_source(node).get("url"))
    if typename == "ExternalVideo":
        return _safe_str(node.get("originUrl"))
    return ""


def _mime_type(node: Mapping[str, Any]) -> str:
    direct = _safe_str(node.get("mimeType"))
    return direct or _safe_str(_source(node).get("mimeType"))


def _file_size(node: Mapping[str, Any]) -> Any:
    typename = _safe_str(node.get("__typename"))
    if typename == "GenericFile":
        return node.get("originalFileSize") if node.get("originalFileSize") is not None else ""
    source = _source(node)
    value = source.get("fileSize")
    if value is None:
        value = source.get("filesize")
    return value if value is not None else ""


def _url_basename(url: str) -> str:
    if not _safe_str(url):
        return ""
    parsed = urllib.parse.urlparse(_safe_str(url))
    return urllib.parse.unquote(Path(parsed.path).name)


def _filename(node: Mapping[str, Any], primary_url: str) -> str:
    return _safe_str(node.get("filename")) or _url_basename(primary_url)


def _canonical_file_values(node: Mapping[str, Any]) -> Dict[str, Any]:
    url = _primary_url(node)
    return {
        "FILE|core.gid": _safe_str(node.get("id")),
        "FILE|derived.filename": _filename(node, url),
        "FILE|core.file_type": _safe_str(node.get("__typename")),
        "FILE|core.file_url": url,
        "FILE|core.created_at": _safe_str(node.get("createdAt")),
        "FILE|core.mime_type": _mime_type(node),
        "FILE|core.file_size": _file_size(node),
    }


def _matches_file_type(record: Mapping[str, Any], file_type: str) -> bool:
    wanted = (_safe_str(file_type) or "ALL").upper().replace("-", "_")
    if wanted == "ALL":
        return True
    typename = _safe_str(record.get("FILE|core.file_type")).upper()
    mime = _safe_str(record.get("FILE|core.mime_type")).lower()
    filename = _safe_str(record.get("FILE|derived.filename")).lower()
    if wanted == "PDF":
        return mime == "application/pdf" or filename.endswith(".pdf")
    if wanted in {"IMAGE", "MEDIA_IMAGE", "MEDIAIMAGE"}:
        return typename == "MEDIAIMAGE" or mime.startswith("image/")
    aliases = {
        "GENERIC_FILE": "GENERICFILE",
        "VIDEO": "VIDEO",
        "MODEL_3D": "MODEL3D",
        "MODEL3D": "MODEL3D",
        "EXTERNAL_VIDEO": "EXTERNALVIDEO",
        "EXTERNALVIDEO": "EXTERNALVIDEO",
    }
    if wanted not in aliases:
        raise FileExportError(
            "FILE_TYPE must be ALL, PDF, IMAGE, GENERIC_FILE, VIDEO, MODEL_3D, or EXTERNAL_VIDEO."
        )
    return typename == aliases[wanted]


def _validate_file_type(file_type: str) -> str:
    wanted = (_safe_str(file_type) or "ALL").upper().replace("-", "_")
    allowed = {
        "ALL", "PDF", "IMAGE", "MEDIA_IMAGE", "MEDIAIMAGE",
        "GENERIC_FILE", "VIDEO", "MODEL_3D", "MODEL3D",
        "EXTERNAL_VIDEO", "EXTERNALVIDEO",
    }
    if wanted not in allowed:
        raise FileExportError(
            "FILE_TYPE must be ALL, PDF, IMAGE, GENERIC_FILE, VIDEO, MODEL_3D, or EXTERNAL_VIDEO."
        )
    return wanted


def _normalise_compare(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return _safe_str(value)


def _matches_fixed_filter(actual: Any, wanted: Any) -> bool:
    actual_text = _normalise_compare(actual)
    if isinstance(wanted, list):
        return actual_text in {_normalise_compare(item) for item in wanted}
    return actual_text == _normalise_compare(wanted)


def _apply_fixed_filters(
    records: Sequence[Dict[str, Any]],
    filters: Mapping[str, Any],
    mode: str,
) -> List[Dict[str, Any]]:
    if not filters:
        return list(records)
    unknown = sorted(set(filters) - set(_canonical_file_values({})))
    if unknown:
        raise FileExportError("Unsupported fixed filter field_id(s): " + ", ".join(unknown))
    result: List[Dict[str, Any]] = []
    for record in records:
        checks = [_matches_fixed_filter(record.get(key), value) for key, value in filters.items()]
        if (all(checks) if mode == "AND" else any(checks)):
            result.append(record)
    return result


def build_output(
    nodes: Sequence[Mapping[str, Any]],
    view: ExportView,
    file_type: str,
) -> Tuple[List[str], List[List[Any]], Dict[str, int]]:
    _validate_file_type(file_type)
    canonical = [_canonical_file_values(node) for node in nodes]
    type_filtered = [record for record in canonical if _matches_file_type(record, file_type)]
    final_records = _apply_fixed_filters(
        type_filtered, view.fixed_filters, view.fixed_filter_mode
    )
    headers = _make_unique_headers([field.alias for field in view.fields])
    body = [
        [record.get(field.field_id, "") for field in view.fields]
        for record in final_records
    ]
    return headers, body, {
        "shopify_rows": len(nodes),
        "rows_after_type_filter": len(type_filtered),
        "rows_after_fixed_filters": len(final_records),
        "rows_skipped": len(nodes) - len(final_records),
    }


def _prepare_snapshot(path: str) -> Path:
    snapshot = Path(path).expanduser().resolve()
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    # Required run-start rule: clear an existing snapshot; otherwise create it.
    snapshot.write_text("", encoding="utf-8")
    return snapshot


def _write_snapshot(path: Path, headers: Sequence[str], body: Sequence[Sequence[Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(list(headers))
        writer.writerows(body)


def _read_snapshot(path: Path, fields: Sequence[ExportField]) -> Tuple[List[str], List[List[Any]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))
    if not rows:
        raise FileExportError("Local snapshot is empty after materialisation.")
    headers = rows[0]
    body: List[List[Any]] = []
    for raw in rows[1:]:
        row = list(raw) + [""] * max(0, len(fields) - len(raw))
        converted: List[Any] = []
        for index, field in enumerate(fields):
            value: Any = row[index]
            if value != "" and field.data_type in {"number", "integer", "number_integer", "int"}:
                try:
                    value = int(value)
                except Exception:
                    pass
            converted.append(value)
        body.append(converted)
    return headers, body


def _column_letters(column_count: int) -> str:
    return re.sub(r"\d", "", rowcol_to_a1(1, max(int(column_count), 1)))


def _ensure_worksheet(spreadsheet, title: str, rows: int, cols: int, retry: int):
    core = _runtime_core()
    try:
        return core._retry_sheet_call(
            f"open worksheet {title}", lambda: spreadsheet.worksheet(title), retry=retry
        )
    except Exception:
        return core._retry_sheet_call(
            f"add worksheet {title}",
            lambda: spreadsheet.add_worksheet(
                title=title, rows=max(rows, 1000), cols=max(cols, 26)
            ),
            retry=retry,
        )


def write_output(
    spreadsheet,
    view: ExportView,
    headers: Sequence[str],
    body: Sequence[Sequence[Any]],
    chunk_rows: int,
    retry: int,
) -> Dict[str, Any]:
    if not headers:
        raise FileExportError("No output headers were configured.")
    core = _runtime_core()
    worksheet = _ensure_worksheet(
        spreadsheet,
        view.target_sheet,
        rows=len(body) + 20,
        cols=len(headers) + 5,
        retry=retry,
    )
    required_rows = max(len(body) + 20, 1000)
    required_cols = max(len(headers) + 5, 26)
    if worksheet.row_count < required_rows or worksheet.col_count < required_cols:
        core._retry_sheet_call(
            f"resize {view.target_sheet}",
            lambda: worksheet.resize(
                rows=max(worksheet.row_count, required_rows),
                cols=max(worksheet.col_count, required_cols),
            ),
            retry=retry,
        )

    # Output is touched only after Shopify fetch, config materialisation and snapshot read-back succeed.
    core._retry_sheet_call(f"clear {view.target_sheet}", worksheet.clear, retry=retry)
    core._retry_sheet_call(
        f"write header {view.target_sheet}",
        lambda: worksheet.update(
            range_name="A1", values=[list(headers)], value_input_option="RAW"
        ),
        retry=retry,
    )

    chunk_size = max(int(chunk_rows), 1)
    last_col = _column_letters(len(headers))
    written = 0
    total = len(body)
    for offset in range(0, total, chunk_size):
        block = [list(row) for row in body[offset:offset + chunk_size]]
        first_row = 2 + offset
        last_row = first_row + len(block) - 1
        target_range = f"A{first_row}:{last_col}{last_row}"
        core._retry_sheet_call(
            f"write {view.target_sheet}!{target_range}",
            lambda target_range=target_range, block=block: worksheet.update(
                range_name=target_range,
                values=block,
                value_input_option="RAW",
            ),
            retry=retry,
        )
        written += len(block)
        print(
            f"[Google Sheets] tab={view.target_sheet} | written={written}/{total}",
            flush=True,
        )
    return {
        "target_sheet": view.target_sheet,
        "target_sheet_label": view.target_sheet_label,
        "rows_written": written,
        "cols_written": len(headers),
    }


def _run_id() -> str:
    return f"files_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"


def _timestamp(timezone_name: str) -> str:
    try:
        timezone = ZoneInfo(_safe_str(timezone_name) or "UTC")
    except Exception:
        timezone = dt.timezone.utc
    return dt.datetime.now(timezone).isoformat(timespec="seconds")


def _runlog_row(
    run_id: str,
    timestamp: str,
    site_code: str,
    status: str,
    rows_loaded: int,
    rows_planned: int,
    rows_written: int,
    rows_skipped: int,
    message: str,
    error_reason: str = "",
) -> List[Any]:
    return [
        run_id, timestamp, JOB_NAME, "export", "summary", status,
        site_code, ENTITY_TYPE, "", "", rows_loaded, rows_loaded,
        rows_loaded, rows_planned, rows_written, rows_skipped,
        message, error_reason,
    ]


def _write_runlog(
    gc,
    console_spreadsheet,
    site_code: str,
    runlog_label: str,
    row: Sequence[Any],
    retry: int,
) -> None:
    if not _safe_str(runlog_label):
        return
    core = _runtime_core()
    try:
        runlog_url = core.get_label_sheet_url(
            console_spreadsheet, _safe_str(site_code).upper(), runlog_label
        )
        spreadsheet = core._retry_sheet_call(
            "open runlog spreadsheet", lambda: gc.open_by_url(runlog_url), retry=retry
        )
        worksheet = _ensure_worksheet(
            spreadsheet, RUNLOG_TAB_NAME, rows=1000, cols=len(RUNLOG_HEADER) + 2, retry=retry
        )
        current = core._retry_sheet_call(
            "read runlog header", lambda: worksheet.row_values(1), retry=retry
        )
        current = [_safe_str(value) for value in current]
        if not current:
            core._retry_sheet_call(
                "write runlog header",
                lambda: worksheet.update(
                    range_name="A1", values=[RUNLOG_HEADER], value_input_option="RAW"
                ),
                retry=retry,
            )
        elif current != RUNLOG_HEADER:
            raise FileExportError("Ops__RunLog header does not match the governed 18-column schema.")
        core._retry_sheet_call(
            "append runlog row",
            lambda: worksheet.append_row(list(row), value_input_option="RAW"),
            retry=retry,
        )
    except Exception as exc:
        print(f"[RunLog warning] {type(exc).__name__}: {exc}", flush=True)


def run(
    *,
    site_code: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    shopify_token: str,
    start_date: str,
    end_date: str,
    timezone_name: str,
    view_id: str = DEFAULT_VIEW_ID,
    file_type: str = "ALL",
    runlog_label: str = "runlog_sheet",
    local_snapshot_path: str = "export_files_snapshot.csv",
    page_size: int = 250,
    write_chunk_rows: int = 2000,
    retry: int = 6,
    request_timeout: int = 60,
    sleep_every_n_calls: int = 20,
    sleep_seconds: float = 1.0,
    preview_only: bool = False,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Fetch Shopify Files by Created At and write the configured V_FILE view."""
    started = time.time()
    run_id = _run_id()
    site = _safe_str(site_code).upper()
    retry = max(int(retry), 1)
    snapshot = _prepare_snapshot(local_snapshot_path)
    date_window = normalize_date_window(start_date, end_date, timezone_name)
    core = _runtime_core()

    gc = core.make_gspread_client(gsheet_sa_b64)
    console = core._retry_sheet_call(
        "open console core", lambda: gc.open_by_url(console_core_url), retry=retry
    )
    account_cfg = core._read_account_config(console, site)
    shop_domain = _safe_str(account_cfg.get("SHOP_DOMAIN"))
    api_version = _safe_str(account_cfg.get("SHOPIFY_API_VERSION"))

    if verbose:
        print(f"========== {JOB_NAME} | start ==========")
        print("run_id:", run_id)
        print("site_code:", site)
        print("project_timezone:", date_window["timezone"])
        print("date_range_inclusive:", date_window["start_date"], "to", date_window["end_date"])
        print("utc_window:", date_window["start_utc"], "to", date_window["end_utc_exclusive"], "(exclusive)")
        print("file_type:", (_safe_str(file_type) or "ALL").upper())
        print("local_snapshot:", snapshot)

    config_url = core.get_label_sheet_url(console, site, "config")
    config_spreadsheet = core._retry_sheet_call(
        "open config spreadsheet", lambda: gc.open_by_url(config_url), retry=retry
    )
    tab_rows = _read_records(config_spreadsheet, CFG_EXPORT_TABS_TAB, retry=retry)
    field_rows = _read_records(config_spreadsheet, CFG_EXPORT_TAB_FIELDS_TAB, retry=retry)
    view = _select_view(tab_rows, field_rows, view_id)

    if verbose:
        print("config_view:", view.view_id)
        print("output_route:", f"{view.target_sheet_label}/{view.target_sheet}")
        print("output_fields:", [field.field_id for field in view.fields])

    client = ShopifyClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=shopify_token,
        retry=retry,
        timeout=request_timeout,
    )
    nodes: List[Dict[str, Any]] = []
    page_count = 0
    counts = {
        "shopify_rows": 0,
        "rows_after_type_filter": 0,
        "rows_after_fixed_filters": 0,
        "rows_skipped": 0,
    }
    rows_written = 0
    try:
        nodes, page_count = fetch_files(
            client,
            date_window["shopify_query"],
            page_size=page_size,
            sleep_every_n_calls=sleep_every_n_calls,
            sleep_seconds=sleep_seconds,
        )
        headers, body, counts = build_output(nodes, view, file_type)

        # The local CSV is the exact write source for Google Sheets.
        _write_snapshot(snapshot, headers, body)
        snapshot_headers, snapshot_body = _read_snapshot(snapshot, view.fields)
        if snapshot_headers != headers or len(snapshot_body) != len(body):
            raise FileExportError("Local snapshot verification failed.")

        target_url = core.get_label_sheet_url(console, site, view.target_sheet_label)
        output_result = {
            "target_sheet": view.target_sheet,
            "target_sheet_label": view.target_sheet_label,
            "rows_written": 0,
            "cols_written": len(headers),
        }
        if not preview_only:
            target_spreadsheet = core._retry_sheet_call(
                f"open output spreadsheet {view.target_sheet_label}",
                lambda: gc.open_by_url(target_url),
                retry=retry,
            )
            output_result = write_output(
                target_spreadsheet,
                view,
                snapshot_headers,
                snapshot_body,
                chunk_rows=write_chunk_rows,
                retry=retry,
            )
            rows_written = int(output_result["rows_written"])

        message = (
            f"export ok | pages={page_count} | matched={counts['rows_after_fixed_filters']} "
            f"| written={rows_written} | preview_only={bool(preview_only)}"
        )
        _write_runlog(
            gc,
            console,
            site,
            runlog_label,
            _runlog_row(
                run_id, _timestamp(timezone_name), site, "SUCCESS",
                counts["shopify_rows"], counts["rows_after_fixed_filters"],
                rows_written, counts["rows_skipped"], message,
            ),
            retry,
        )
        result = {
            "ok": True,
            "run_id": run_id,
            "job_name": JOB_NAME,
            "module_version": MODULE_VERSION,
            "site_code": site,
            "shop_domain": shop_domain,
            "api_version": api_version,
            "view_id": view.view_id,
            "target_sheet_label": view.target_sheet_label,
            "target_sheet": view.target_sheet,
            "target_sheet_url": target_url,
            "date_window": date_window,
            "file_type": (_safe_str(file_type) or "ALL").upper(),
            "page_count": page_count,
            "shopify_call_count": client.call_count,
            "shopify_rows": counts["shopify_rows"],
            "rows_after_type_filter": counts["rows_after_type_filter"],
            "rows_after_fixed_filters": counts["rows_after_fixed_filters"],
            "rows_skipped": counts["rows_skipped"],
            "rows_written": rows_written,
            "cols_written": len(headers),
            "headers": headers,
            "local_snapshot_path": str(snapshot),
            "preview_only": bool(preview_only),
            "elapsed_seconds": round(time.time() - started, 3),
            "finished_at": _timestamp(timezone_name),
            "message": message,
        }
        if verbose:
            print("✅", message)
            print(f"========== {JOB_NAME} | end ==========")
        return result
    except Exception as exc:
        message = f"export failed | {type(exc).__name__}: {exc}"
        _write_runlog(
            gc,
            console,
            site,
            runlog_label,
            _runlog_row(
                run_id, _timestamp(timezone_name), site, "ERROR",
                counts["shopify_rows"] or len(nodes),
                counts["rows_after_fixed_filters"], rows_written,
                counts["rows_skipped"], message, str(exc),
            ),
            retry,
        )
        raise


__all__ = [
    "JOB_NAME",
    "MODULE_PATH",
    "MODULE_VERSION",
    "DEFAULT_VIEW_ID",
    "resolve_runtime_context",
    "update_existing_notebook_registry_row",
    "normalize_date_window",
    "build_output",
    "run",
]
