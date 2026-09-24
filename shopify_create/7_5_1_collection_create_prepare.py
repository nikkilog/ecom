# -*- coding: utf-8 -*-
"""Build Collection Create ``Input`` from the wide ``Prepare`` tab.

GitHub target: ``ecom/shopify_create/7_5_1_collection_create_prepare.py``
Import path: ``shopify_create.7_5_1_collection_create_prepare``

Scope
-----
- Resolve the ``create_collection`` workbook and ``config`` workbook via Console Core.
- Read ``Prepare`` where column A is ``Title`` and every remaining condition is a
  dynamic pair: ``<CONDITION_TYPE>-N`` + ``Value-N``.
- Convert each wide Collection row to the long ``Input`` contract used by
  Collection Create Prepare/Apply.
- For ``PRODUCT_METAFIELD-N``, resolve the cell value under that header against
  ``Cfg__Fields.display_name`` and output product metafield ``namespace`` + ``key``.
- Default ``match_type=ALL`` and ``relation=AUTO``.
- If a title contains no uppercase letters, apply Title Case; otherwise preserve it.
- Generate a URL-safe handle from the normalized title.
- Overwrite ``Input`` and write RunLog evidence.

This module does not call Shopify and does not touch Preview/Defaults/Result.

Prepare contract
----------------
Example headers::

    Title | PRODUCT_METAFIELD-1 | Value-1 | PRODUCT_TYPE-2 | Value-2 | ...

The sequence is global across a row and may contain mixed condition types. Header
pairs must be consecutive and numbered 1..N with no gaps. A row may leave a whole
pair blank when that Collection uses fewer conditions than the widest schema.

For ``PRODUCT_METAFIELD-N``:
- the left cell is a Cfg__Fields ``display_name`` (for example ``Product Type-1``)
- ``Value-N`` is the desired Collection condition value.

For non-metafield types, the left cell is optional/ignored and ``Value-N`` is the
condition value.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd


gp = importlib.import_module("shopify_create.7_1_1_generic_product_prepare")

MODULE_VERSION = "2026-09-23-collection-create-prepare-v2"
MODULE_PATH = "shopify_create.7_5_1_collection_create_prepare"
DEFAULT_JOB_NAME = "collection_create_prepare"

# Thin-runner compatibility boundary.
read_secret = gp.read_secret
_build_gspread_client = gp._build_gspread_client
_sheets_retry = gp._sheets_retry
update_existing_notebook_registry_row = gp.update_existing_notebook_registry_row

INPUT_HEADERS = [
    "title",
    "handle",
    "match_type",
    "condition_type",
    "namespace",
    "key",
    "relation",
    "value",
]

SUPPORTED_CONDITION_TYPES = {
    "PRODUCT_VENDOR",
    "PRODUCT_TYPE",
    "PRODUCT_STATUS",
    "PRODUCT_TAG",
    "PRODUCT_METAFIELD",
}

_CONDITION_HEADER_RE = re.compile(
    r"^(PRODUCT_VENDOR|PRODUCT_TYPE|PRODUCT_STATUS|PRODUCT_TAG|PRODUCT_METAFIELD)-(\d+)$",
    re.IGNORECASE,
)
_VALUE_HEADER_RE = re.compile(r"^VALUE-(\d+)$", re.IGNORECASE)


@dataclass(frozen=True)
class ConditionColumn:
    seq: int
    condition_type: str
    condition_col: int
    value_col: int


@dataclass(frozen=True)
class PrepareCollection:
    source_row: int
    title: str
    handle: str
    input_rows: Tuple[Tuple[str, ...], ...]


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _normalize_space(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_str(value)).strip()


def _normalize_display_lookup(value: Any) -> str:
    # Match the existing Console Core display-name lookup semantics closely:
    # case-insensitive, whitespace-insensitive around hyphens/underscores.
    text = _normalize_space(value).casefold()
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"[\s_]+", " ", text)
    text = re.sub(r"\s*-\s*", "-", text)
    return text.strip()


def _normalize_title(value: Any) -> str:
    title = _normalize_space(value)
    if not title:
        return ""
    # User contract: only titles with no uppercase letters receive PROPER/Title Case.
    if not any(ch.isupper() for ch in title if ch.isalpha()):
        return title.title()
    return title


def _slugify_title(value: Any) -> str:
    title = _normalize_space(value)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", title).strip("-").lower()
    slug = re.sub(r"-+", "-", slug)
    if not slug:
        raise ValueError(f"Cannot generate handle from title={title!r}.")
    return slug


def _parse_prepare_schema(headers: Sequence[Any]) -> List[ConditionColumn]:
    normalized = [_normalize_space(v) for v in headers]
    if not normalized or normalized[0].casefold() != "title":
        raise ValueError(
            "Prepare column A must be Title. "
            f"got={normalized[0] if normalized else '<missing>'!r}."
        )

    specs: List[ConditionColumn] = []
    seen_seq = set()
    i = 1
    while i < len(normalized):
        header = normalized[i]
        if not header:
            # get_all_values can preserve internal blank headers; do not silently
            # jump over them because that would make sequence pairing ambiguous.
            trailing = normalized[i:]
            if any(trailing):
                raise ValueError(f"Prepare has a blank header before column {i + 1} data.")
            break

        match = _CONDITION_HEADER_RE.fullmatch(header)
        if not match:
            if _VALUE_HEADER_RE.fullmatch(header):
                raise ValueError(
                    f"Prepare column {i + 1} has orphan {header!r}; expected a condition column first."
                )
            raise ValueError(
                f"Prepare unsupported condition header={header!r} at column {i + 1}. "
                f"Supported condition types={sorted(SUPPORTED_CONDITION_TYPES)}."
            )

        condition_type = match.group(1).upper()
        seq = int(match.group(2))
        if seq < 1:
            raise ValueError(f"Prepare sequence must be >= 1; got {seq}.")
        if seq in seen_seq:
            raise ValueError(f"Prepare has duplicate condition sequence={seq}.")
        if i + 1 >= len(normalized):
            raise ValueError(f"Prepare {header!r} is missing paired Value-{seq} column.")

        expected_value_header = f"Value-{seq}"
        actual_value_header = normalized[i + 1]
        value_match = _VALUE_HEADER_RE.fullmatch(actual_value_header)
        if not value_match or int(value_match.group(1)) != seq:
            raise ValueError(
                f"Prepare {header!r} must be immediately followed by {expected_value_header!r}; "
                f"got={actual_value_header!r}."
            )

        specs.append(
            ConditionColumn(
                seq=seq,
                condition_type=condition_type,
                condition_col=i,
                value_col=i + 1,
            )
        )
        seen_seq.add(seq)
        i += 2

    if not specs:
        raise ValueError("Prepare has no condition/value column pairs.")

    seqs = [spec.seq for spec in specs]
    expected = list(range(1, max(seqs) + 1))
    if seqs != expected:
        raise ValueError(
            "Prepare condition sequences must be consecutive 1..N in column order; "
            f"got={seqs}, expected={expected}."
        )
    return specs


def _read_cfg_field_records(values: Sequence[Sequence[Any]]) -> Dict[str, List[Dict[str, str]]]:
    if not values:
        raise ValueError("Cfg__Fields is empty.")
    headers = [_safe_str(v) for v in values[0]]
    positions = {name: idx for idx, name in enumerate(headers) if name}
    required = {"display_name", "entity_type", "field_key"}
    missing = sorted(required - set(positions))
    if missing:
        raise ValueError(f"Cfg__Fields missing required columns: {missing}")

    index: Dict[str, List[Dict[str, str]]] = {}
    for source_row, raw in enumerate(values[1:], start=2):
        padded = list(raw) + [""] * max(0, len(headers) - len(raw))
        record = {
            name: _safe_str(padded[idx])
            for name, idx in positions.items()
        }
        display_name = record.get("display_name", "")
        if not display_name:
            continue
        record["_source_row"] = str(source_row)
        index.setdefault(_normalize_display_lookup(display_name), []).append(record)
    return index


def _strip_metafield_prefix(field_key: str) -> Tuple[str, str]:
    value = _safe_str(field_key)
    for prefix in ("mf.", "v_mf.", "vmf."):
        if value.startswith(prefix):
            remainder = value[len(prefix):]
            parts = remainder.split(".", 1)
            if len(parts) == 2 and all(parts):
                return parts[0], parts[1]
    return "", ""


def _resolve_product_metafield(
    display_name: str,
    cfg_index: Mapping[str, Sequence[Mapping[str, str]]],
    *,
    source_row: int,
    seq: int,
) -> Tuple[str, str, str]:
    lookup = _normalize_display_lookup(display_name)
    candidates = [dict(r) for r in cfg_index.get(lookup, [])]
    if not candidates:
        raise ValueError(
            f"Prepare row {source_row} condition {seq}: Cfg__Fields.display_name "
            f"not found: {display_name!r}."
        )

    product_candidates = [
        r
        for r in candidates
        if _safe_str(r.get("entity_type")).upper() == "PRODUCT"
        and _safe_str(r.get("source_type")).upper() in {"", "METAFIELD"}
        and _safe_str(r.get("field_key")).startswith("mf.")
    ]
    if not product_candidates:
        owners = sorted(
            {
                f"{_safe_str(r.get('entity_type')).upper()}:{_safe_str(r.get('field_key'))}"
                for r in candidates
            }
        )
        raise ValueError(
            f"Prepare row {source_row} condition {seq}: display_name={display_name!r} "
            "does not resolve to a PRODUCT metafield in Cfg__Fields. "
            f"candidates={owners[:10]}"
        )
    if len(product_candidates) != 1:
        details = [
            {
                "row": r.get("_source_row"),
                "field_key": r.get("field_key"),
                "namespace": r.get("namespace"),
                "key": r.get("key"),
            }
            for r in product_candidates[:10]
        ]
        raise ValueError(
            f"Prepare row {source_row} condition {seq}: display_name={display_name!r} "
            f"is ambiguous in Cfg__Fields: {details}"
        )

    record = product_candidates[0]
    field_key = _safe_str(record.get("field_key"))
    namespace = _safe_str(record.get("namespace"))
    key = _safe_str(record.get("key"))
    parsed_namespace, parsed_key = _strip_metafield_prefix(field_key)
    if not namespace:
        namespace = parsed_namespace
    if not key:
        key = parsed_key

    if not namespace or not key:
        raise ValueError(
            f"Prepare row {source_row} condition {seq}: cannot derive namespace/key "
            f"from Cfg__Fields field_key={field_key!r}."
        )
    # Guard against malformed Config where explicit namespace/key disagree with field_key.
    if parsed_namespace and parsed_key and (namespace, key) != (parsed_namespace, parsed_key):
        raise ValueError(
            f"Prepare row {source_row} condition {seq}: Cfg__Fields identity mismatch for "
            f"display_name={display_name!r}: field_key={field_key!r}, "
            f"namespace={namespace!r}, key={key!r}."
        )
    return namespace, key, field_key


def build_input_matrix(
    prepare_values: Sequence[Sequence[Any]],
    cfg_values: Sequence[Sequence[Any]],
) -> Tuple[List[List[str]], List[PrepareCollection]]:
    if not prepare_values:
        raise ValueError("Prepare is empty.")

    specs = _parse_prepare_schema(prepare_values[0])
    cfg_index = _read_cfg_field_records(cfg_values)
    width = max(len(prepare_values[0]), max(spec.value_col for spec in specs) + 1)

    output: List[List[str]] = [list(INPUT_HEADERS)]
    collections: List[PrepareCollection] = []
    seen_handles: Dict[str, int] = {}

    for source_row, raw in enumerate(prepare_values[1:], start=2):
        padded = list(raw) + [""] * max(0, width - len(raw))
        title_raw = _normalize_space(padded[0] if padded else "")
        pair_values = [
            _safe_str(padded[spec.condition_col]) or _safe_str(padded[spec.value_col])
            for spec in specs
        ]
        if not title_raw and not any(pair_values):
            continue
        if not title_raw:
            raise ValueError(f"Prepare row {source_row}: Title is required.")

        title = _normalize_title(title_raw)
        handle = _slugify_title(title)
        if handle in seen_handles:
            raise ValueError(
                f"Prepare rows {seen_handles[handle]} and {source_row} generate duplicate "
                f"handle={handle!r}. Use one Prepare row per Collection."
            )
        seen_handles[handle] = source_row

        row_outputs: List[Tuple[str, ...]] = []
        for spec in specs:
            descriptor = _normalize_space(padded[spec.condition_col])
            value = _safe_str(padded[spec.value_col])
            if not descriptor and not value:
                continue
            if not value:
                raise ValueError(
                    f"Prepare row {source_row} condition {spec.seq}: Value-{spec.seq} is required."
                )

            namespace = ""
            key = ""
            if spec.condition_type == "PRODUCT_METAFIELD":
                if not descriptor:
                    raise ValueError(
                        f"Prepare row {source_row} condition {spec.seq}: "
                        "PRODUCT_METAFIELD requires a Cfg__Fields display_name in the left cell."
                    )
                namespace, key, _ = _resolve_product_metafield(
                    descriptor,
                    cfg_index,
                    source_row=source_row,
                    seq=spec.seq,
                )

            input_row = (
                title,
                handle,
                "ALL",
                spec.condition_type,
                namespace,
                key,
                "AUTO",
                value,
            )
            row_outputs.append(input_row)
            output.append(list(input_row))

        if not row_outputs:
            raise ValueError(f"Prepare row {source_row}: Collection has no conditions.")
        collections.append(
            PrepareCollection(
                source_row=source_row,
                title=title,
                handle=handle,
                input_rows=tuple(row_outputs),
            )
        )

    if not collections:
        raise ValueError("Prepare contains no Collection rows.")
    return output, collections


def _require_worksheet(book: Any, tab_name: str) -> Any:
    return gp._require_worksheet(book, tab_name)


def _write_input_sheet(book: Any, tab_input: str, matrix: Sequence[Sequence[Any]]) -> int:
    if not matrix or len(matrix) < 2:
        raise ValueError("Refusing to write Input with no data rows.")
    rows = len(matrix)
    cols = len(INPUT_HEADERS)
    ws = gp._get_or_create_preview_worksheet(book, tab_input, max(100, rows + 50), cols + 3)
    if ws.row_count < rows or ws.col_count < cols:
        gp._sheets_retry(
            f"resize {tab_input}",
            lambda: ws.resize(rows=max(ws.row_count, rows + 50), cols=max(ws.col_count, cols + 3)),
        )
    gp._sheets_retry(f"clear {tab_input}", ws.clear)
    gp._sheets_retry(
        f"write {tab_input}",
        lambda: ws.update(
            range_name=f"A1:{gp._a1_col(cols)}{rows}",
            values=[list(row) for row in matrix],
            value_input_option="RAW",
        ),
    )
    try:
        gp._sheets_retry(f"freeze {tab_input}", lambda: ws.freeze(rows=1))
    except Exception:
        pass
    return rows - 1


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    config_sheet_label: str = "config",
    create_sheet_label: str = "create_collection",
    runlog_sheet_label: str = "runlog_sheet",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_prepare: str = "Prepare",
    tab_input: str = "Input",
    tab_runlog: str = "Ops__RunLog",
    write_input: bool = True,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    sa_b64_value: Optional[str] = None,
) -> Dict[str, Any]:
    site_code = gp._normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not _safe_str(console_core_url):
        raise ValueError("console_core_url is required.")
    if not _safe_str(bootstrap_gsheet_sa_b64_secret):
        raise ValueError("bootstrap_gsheet_sa_b64_secret is required.")

    run_id = run_id or gp._make_run_id(job_name, tz_name)
    started = time.monotonic()
    logger = None

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 7, f"Resolve Google access | site={site_code}")
    google_secret = gp.read_secret(
        bootstrap_gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, google_auth = gp._build_gspread_client(google_secret)
    console = gp._sheets_retry("open Console Core", lambda: gc.open_by_url(console_core_url))

    account = gp._load_account_values(console, tab_cfg_account_id)
    configured_secret = _safe_str(account.get("GSHEET_SA_B64_SECRET"))
    if configured_secret and configured_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "Bootstrap Google Secret does not match Cfg__account_id. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}; cfg={configured_secret}"
        )

    progress(2, 7, "Resolve routed workbooks | create_collection + config + runlog")
    create_url = gp._resolve_sheet_url_by_label(console, tab_cfg_sites, site_code, create_sheet_label)
    config_url = gp._resolve_sheet_url_by_label(console, tab_cfg_sites, site_code, config_sheet_label)
    runlog_url = gp._resolve_sheet_url_by_label(console, tab_cfg_sites, site_code, runlog_sheet_label)
    create_book = gp._sheets_retry("open create_collection workbook", lambda: gc.open_by_url(create_url))
    config_book = gp._sheets_retry("open config workbook", lambda: gc.open_by_url(config_url))
    runlog_ws = gp._sheets_retry(
        f"open runlog {tab_runlog}",
        lambda: gc.open_by_url(runlog_url).worksheet(tab_runlog),
    )
    logger = gp.RunLogger18(
        worksheet=runlog_ws,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )

    try:
        progress(3, 7, f"Read source | tab={tab_prepare}")
        prepare_ws = _require_worksheet(create_book, tab_prepare)
        prepare_values = gp._sheets_retry(f"read {tab_prepare}", prepare_ws.get_all_values)
        specs = _parse_prepare_schema(prepare_values[0] if prepare_values else [])
        print(
            "[Prepare schema] "
            f"condition_pairs={len(specs)} | "
            f"types={[spec.condition_type for spec in specs]}"
        )

        progress(4, 7, f"Read Config field registry | tab={tab_cfg_fields}")
        cfg_ws = _require_worksheet(config_book, tab_cfg_fields)
        cfg_values = gp._sheets_retry(f"read {tab_cfg_fields}", cfg_ws.get_all_values)

        progress(5, 7, "Resolve fields and build long Input")
        matrix, collections = build_input_matrix(prepare_values, cfg_values)
        data_rows = matrix[1:]
        metafield_rows = sum(1 for row in data_rows if row[3] == "PRODUCT_METAFIELD")
        print(
            "[Build] "
            f"collections={len(collections)} | condition_rows={len(data_rows)} | "
            f"metafield_rows={metafield_rows}"
        )

        rows_written = 0
        progress(6, 7, f"Overwrite Input | enabled={write_input}")
        if write_input:
            rows_written = _write_input_sheet(create_book, tab_input, matrix)

        elapsed = round(time.monotonic() - started, 2)
        logger.log(
            phase="input",
            log_type="summary",
            status="SUCCESS",
            entity_type="COLLECTION_CREATE",
            rows_loaded=len(collections),
            rows_pending=len(data_rows),
            rows_recognized=len(data_rows),
            rows_planned=len(data_rows),
            rows_written=rows_written,
            rows_skipped=0,
            message=(
                f"prepare_collections={len(collections)} | condition_rows={len(data_rows)} | "
                f"metafield_rows={metafield_rows} | write_input={write_input}"
            ),
            error_reason="",
        )
        gp._sheets_retry("write final RunLog", logger.flush)
        progress(7, 7, f"Completed | SUCCESS | elapsed={elapsed}s")

        df = pd.DataFrame(data_rows, columns=INPUT_HEADERS)
        return {
            "ok": True,
            "status": "SUCCESS",
            "run_id": run_id,
            "job_name": job_name,
            "summary": {
                "prepare_collections": len(collections),
                "condition_rows": len(data_rows),
                "metafield_rows": metafield_rows,
                "input_rows_written": rows_written,
                "elapsed_seconds": elapsed,
            },
            "input_preview": df,
            "runtime": {
                "runtime_mode": gp._runtime_mode(),
                "google_secret_source": google_auth["source_type"],
                "python": sys.version.split()[0],
            },
            "targets": {
                "config_sheet_label": config_sheet_label,
                "create_sheet_label": create_sheet_label,
                "prepare_tab": tab_prepare,
                "input_tab": tab_input,
                "cfg_fields_tab": tab_cfg_fields,
                "runlog_sheet_label": runlog_sheet_label,
                "runlog_tab": tab_runlog,
            },
        }
    except Exception as exc:
        if logger is not None:
            try:
                logger.log(
                    phase="input",
                    log_type="summary",
                    status="FAILED",
                    entity_type="COLLECTION_CREATE",
                    rows_loaded=0,
                    rows_pending=0,
                    rows_recognized=0,
                    rows_planned=0,
                    rows_written=0,
                    rows_skipped=0,
                    message=f"{type(exc).__name__}: {exc}",
                    error_reason="INPUT_BUILD_FAILED",
                )
                gp._sheets_retry("write failed RunLog", logger.flush)
            except Exception:
                pass
        raise


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Collection Create Input from Prepare")
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument("--bootstrap-gsheet-sa-b64-secret", required=True)
    parser.add_argument("--config-sheet-label", default="config")
    parser.add_argument("--create-sheet-label", default="create_collection")
    parser.add_argument("--runlog-sheet-label", default="runlog_sheet")
    parser.add_argument("--no-write-input", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=args.bootstrap_gsheet_sa_b64_secret,
        config_sheet_label=args.config_sheet_label,
        create_sheet_label=args.create_sheet_label,
        runlog_sheet_label=args.runlog_sheet_label,
        write_input=not args.no_write_input,
    )
    print(
        {
            "status": result["status"],
            "run_id": result["run_id"],
            "summary": result["summary"],
            "targets": result["targets"],
        }
    )


if __name__ == "__main__":
    main()
