"""Edit Shopify file-to-product references and product media order.

Formal repository destination:
    shopify_sync/1_5_1_edit_files.py

Input contract (Google Sheet tab ``Edit_File``):
    product_gid_or_handle, file_gid, action, media_position,
    filename, note, run_id

The module deliberately keeps runtime/auth/Google Sheets routing aligned with
the existing Console_Core Edit modules by reusing their common runtime helpers.
Business behavior lives in this file.
"""

from __future__ import annotations

import csv
import datetime as dt
import importlib
import json
import os
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional


MODULE_PATH = "shopify_sync.1_5_1_edit_files"
MODULE_VERSION = "2026-09-02-edit-files-v1"
DEFAULT_JOB_NAME = "edit_files"
DEFAULT_INPUT_SHEET_LABEL = "edit"
DEFAULT_INPUT_TAB = "Edit_File"

INPUT_COLUMNS = [
    "product_gid_or_handle",
    "file_gid",
    "action",
    "media_position",
    "filename",
    "note",
    "run_id",
]
ALLOWED_ACTIONS = {"ADD", "REMOVE", "REORDER"}
FILE_TYPES = {"MediaImage", "GenericFile", "Video", "Model3d"}
ORDERABLE_FILE_TYPES = {"MediaImage", "Video", "Model3d"}


def _runtime_module():
    """Load the established Console_Core runtime without duplicating secrets logic."""
    return importlib.import_module("shopify_sync.1_1_1_edit_metafields")


def resolve_runtime_context(**kwargs):
    return _runtime_module().resolve_runtime_context(**kwargs)


def update_existing_notebook_registry_row(**kwargs):
    return _runtime_module().update_existing_notebook_registry_row(**kwargs)


def _norm(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _run_id() -> str:
    return dt.datetime.utcnow().strftime("edit_files_%Y%m%d_%H%M%S")


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    size = max(1, int(size))
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _product_numeric_id(product_gid: str) -> str:
    match = re.fullmatch(r"gid://shopify/Product/(\d+)", _norm(product_gid))
    return match.group(1) if match else ""


def _parse_position(raw: Any) -> tuple[Optional[int], str]:
    text = _norm(raw)
    if not text:
        return None, ""
    if not re.fullmatch(r"[1-9]\d*", text):
        return None, "media_position_must_be_positive_integer"
    return int(text), ""


def _row_result(row: dict[str, Any], **updates: Any) -> dict[str, Any]:
    result = {
        "sheet_row": row.get("_sheet_row", ""),
        "product_input": row.get("product_gid_or_handle", ""),
        "product_gid": row.get("_product_gid", ""),
        "file_gid": row.get("file_gid", ""),
        "filename": row.get("filename", ""),
        "action": row.get("action", ""),
        "media_position": row.get("_media_position", ""),
        "current_position": row.get("_current_position", ""),
        "decision": row.get("_decision", ""),
        "result": row.get("_result", ""),
        "reason": row.get("_reason", ""),
        "note": row.get("note", ""),
    }
    result.update(updates)
    return result


def validate_input_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate the fixed seven-column contract without calling Shopify."""
    normalized: list[dict[str, Any]] = []
    for source in rows:
        row = {column: _norm(source.get(column)) for column in INPUT_COLUMNS}
        row["_sheet_row"] = source.get("_sheet_row", "")
        row["action"] = row["action"].upper()
        row["_media_position"], position_error = _parse_position(row["media_position"])
        reasons: list[str] = []
        if not row["product_gid_or_handle"]:
            reasons.append("missing_product_gid_or_handle")
        if not row["file_gid"]:
            reasons.append("missing_file_gid")
        elif not re.fullmatch(
            r"gid://shopify/(MediaImage|GenericFile|Video|Model3d)/\d+",
            row["file_gid"],
        ):
            reasons.append("file_gid_must_be_complete_shopify_file_gid")
        if row["action"] not in ALLOWED_ACTIONS:
            reasons.append("action_must_be_ADD_REMOVE_or_REORDER")
        if position_error:
            reasons.append(position_error)
        if row["action"] == "REORDER" and row["_media_position"] is None:
            reasons.append("REORDER_requires_media_position")
        if row["action"] == "REMOVE" and row["_media_position"] is not None:
            reasons.append("REMOVE_forbids_media_position")
        row["_reason"] = ";".join(reasons)
        row["_decision"] = "BLOCK" if reasons else "PENDING"
        normalized.append(row)

    pair_counts = Counter(
        (row["product_gid_or_handle"].lower(), row["file_gid"])
        for row in normalized
        if row["product_gid_or_handle"] and row["file_gid"]
    )
    position_counts = Counter(
        (row["product_gid_or_handle"].lower(), row["_media_position"])
        for row in normalized
        if row["product_gid_or_handle"] and row["_media_position"] is not None
    )
    for row in normalized:
        reasons = [item for item in row["_reason"].split(";") if item]
        if pair_counts[(row["product_gid_or_handle"].lower(), row["file_gid"])] > 1:
            reasons.append("duplicate_product_file_pair")
        position_key = (row["product_gid_or_handle"].lower(), row["_media_position"])
        if row["_media_position"] is not None and position_counts[position_key] > 1:
            reasons.append("duplicate_media_position_for_product")
        row["_reason"] = ";".join(dict.fromkeys(reasons))
        row["_decision"] = "BLOCK" if reasons else "PENDING"
    return normalized


def build_target_media_order(
    current_ids: list[str], desired_positions: dict[str, int]
) -> tuple[list[str], list[dict[str, str]]]:
    """Build a deterministic final order and minimal sequential Shopify moves.

    ``desired_positions`` uses the Sheet's one-based positions. Returned
    ``newPosition`` values use Shopify's zero-based string format.
    """
    if len(set(desired_positions.values())) != len(desired_positions):
        raise ValueError("duplicate_media_position_for_product")
    missing = [file_gid for file_gid in desired_positions if file_gid not in current_ids]
    if missing:
        raise ValueError("ordered_file_not_attached:" + ",".join(missing))
    for position in desired_positions.values():
        if position < 1 or position > len(current_ids):
            raise ValueError(
                f"media_position_out_of_range:{position};media_count={len(current_ids)}"
            )

    specified = set(desired_positions)
    target = [item for item in current_ids if item not in specified]
    for file_gid, position in sorted(
        desired_positions.items(), key=lambda item: (item[1], item[0])
    ):
        target.insert(position - 1, file_gid)

    simulated = list(current_ids)
    moves: list[dict[str, str]] = []
    for file_gid, position in sorted(
        desired_positions.items(), key=lambda item: (item[1], item[0])
    ):
        wanted_index = position - 1
        current_index = simulated.index(file_gid)
        if current_index == wanted_index:
            continue
        simulated.pop(current_index)
        simulated.insert(wanted_index, file_gid)
        moves.append({"id": file_gid, "newPosition": str(wanted_index)})

    if simulated != target:
        raise AssertionError("internal_media_order_plan_mismatch")
    return target, moves


def _load_input_snapshot(ws, snapshot_path: str) -> tuple[list[dict[str, Any]], int]:
    runtime = _runtime_module()
    values = runtime._with_sheets_retry(
        lambda: ws.get_all_values(value_render_option="FORMATTED_VALUE"),
        action=f"edit_files.read:{ws.title}",
    )
    if not values:
        raise ValueError(f"Input tab {ws.title!r} is empty.")
    headers = [_norm(value) for value in values[0]]
    if headers != INPUT_COLUMNS:
        raise ValueError(
            "Edit_File header must exactly equal: " + ", ".join(INPUT_COLUMNS)
            + f". Actual: {headers}"
        )

    snapshot = Path(snapshot_path).expanduser().resolve()
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    snapshot.write_text("", encoding="utf-8")
    with snapshot.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["_sheet_row", *INPUT_COLUMNS])
        writer.writeheader()
        for sheet_row, raw in enumerate(values[1:], start=2):
            padded = list(raw) + [""] * max(0, len(INPUT_COLUMNS) - len(raw))
            record = {column: _norm(padded[index]) for index, column in enumerate(INPUT_COLUMNS)}
            record["_sheet_row"] = sheet_row
            if any(record[column] for column in INPUT_COLUMNS):
                writer.writerow(record)

    rows: list[dict[str, Any]] = []
    with snapshot.open("r", encoding="utf-8", newline="") as handle:
        for record in csv.DictReader(handle):
            record["_sheet_row"] = int(record["_sheet_row"])
            rows.append(record)
    return rows, len(values) - 1


def _resolve_product_gid(client, product_input: str) -> str:
    runtime = _runtime_module()
    value = _norm(product_input)
    if re.fullmatch(r"gid://shopify/Product/\d+", value):
        return value
    if value.isdigit():
        return f"gid://shopify/Product/{value}"
    return _norm(runtime.resolve_product_by_handle(client, value))


def _fetch_file_info(client, file_ids: list[str]) -> dict[str, dict[str, str]]:
    runtime = _runtime_module()
    query = """
    query EditFilesNodes($ids: [ID!]!) {
      nodes(ids: $ids) {
        id
        __typename
        ... on MediaImage { fileStatus }
        ... on GenericFile { fileStatus }
        ... on Video { fileStatus }
        ... on Model3d { fileStatus }
      }
    }
    """
    result: dict[str, dict[str, str]] = {}
    for part in _chunks(sorted(set(file_ids)), 80):
        data = runtime.gql(client, query, {"ids": part})
        for node in data.get("nodes") or []:
            if not node:
                continue
            result[_norm(node.get("id"))] = {
                "type": _norm(node.get("__typename")),
                "status": _norm(node.get("fileStatus")).upper(),
            }
    return result


def _fetch_product_media(client, product_gid: str) -> Optional[list[str]]:
    runtime = _runtime_module()
    query = """
    query EditFilesProductMedia($id: ID!, $after: String) {
      product(id: $id) {
        id
        media(first: 250, after: $after) {
          nodes { id }
          pageInfo { hasNextPage endCursor }
        }
      }
    }
    """
    media_ids: list[str] = []
    after = None
    while True:
        data = runtime.gql(client, query, {"id": product_gid, "after": after})
        product = data.get("product")
        if product is None:
            return None
        connection = product.get("media") or {}
        media_ids.extend(_norm(node.get("id")) for node in connection.get("nodes") or [])
        page = connection.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            return [item for item in media_ids if item]
        after = page.get("endCursor")


def _fetch_product_file_refs(client, product_gid: str) -> set[str]:
    runtime = _runtime_module()
    numeric_id = _product_numeric_id(product_gid)
    if not numeric_id:
        raise ValueError(f"Invalid Product GID: {product_gid}")
    query = """
    query EditFilesProductReferences($first: Int!, $after: String, $query: String!) {
      files(first: $first, after: $after, query: $query) {
        nodes { id }
        pageInfo { hasNextPage endCursor }
      }
    }
    """
    result: set[str] = set()
    after = None
    while True:
        data = runtime.gql(
            client,
            query,
            {"first": 250, "after": after, "query": f"product_id:{numeric_id}"},
        )
        connection = data.get("files") or {}
        result.update(_norm(node.get("id")) for node in connection.get("nodes") or [])
        page = connection.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            return {item for item in result if item}
        after = page.get("endCursor")


def _file_update(client, *, file_gid: str, product_gid: str, add: bool) -> None:
    runtime = _runtime_module()
    mutation = """
    mutation EditFilesReference($files: [FileUpdateInput!]!) {
      fileUpdate(files: $files) {
        files { id }
        userErrors { field message code }
      }
    }
    """
    field = "referencesToAdd" if add else "referencesToRemove"
    data = runtime.gql(
        client,
        mutation,
        {"files": [{"id": file_gid, field: [product_gid]}]},
    )
    payload = data.get("fileUpdate") or {}
    errors = payload.get("userErrors") or []
    if errors:
        raise RuntimeError("fileUpdate userErrors: " + json.dumps(errors, ensure_ascii=False))


def _start_reorder(client, product_gid: str, moves: list[dict[str, str]]) -> str:
    runtime = _runtime_module()
    mutation = """
    mutation EditFilesReorder($id: ID!, $moves: [MoveInput!]!) {
      productReorderMedia(id: $id, moves: $moves) {
        job { id }
        mediaUserErrors { field message code }
      }
    }
    """
    data = runtime.gql(client, mutation, {"id": product_gid, "moves": moves})
    payload = data.get("productReorderMedia") or {}
    errors = payload.get("mediaUserErrors") or []
    if errors:
        raise RuntimeError(
            "productReorderMedia mediaUserErrors: "
            + json.dumps(errors, ensure_ascii=False)
        )
    job_id = _norm((payload.get("job") or {}).get("id"))
    if not job_id:
        raise RuntimeError("productReorderMedia returned no Job ID")
    return job_id


def _wait_for_job(client, job_id: str, timeout_seconds: int, poll_seconds: float) -> None:
    runtime = _runtime_module()
    query = "query EditFilesJob($id: ID!) { job(id: $id) { id done } }"
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    while True:
        data = runtime.gql(client, query, {"id": job_id})
        job = data.get("job")
        if job and job.get("done"):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Shopify Job did not finish in time: {job_id}")
        print(f"[Reorder Job] waiting | job={job_id}", flush=True)
        time.sleep(max(0.2, float(poll_seconds)))


def _wait_for_media_membership(
    client,
    product_gid: str,
    required_ids: set[str],
    forbidden_ids: set[str],
    timeout_seconds: int,
    poll_seconds: float,
) -> list[str]:
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    while True:
        media_ids = _fetch_product_media(client, product_gid)
        if media_ids is None:
            raise RuntimeError(f"Product disappeared during apply: {product_gid}")
        current = set(media_ids)
        if required_ids.issubset(current) and current.isdisjoint(forbidden_ids):
            return media_ids
        if time.monotonic() >= deadline:
            missing = sorted(required_ids - current)
            remaining = sorted(forbidden_ids.intersection(current))
            raise TimeoutError(
                "Product media membership did not settle in time | missing="
                + ",".join(missing)
                + " | still_present="
                + ",".join(remaining)
            )
        time.sleep(max(0.2, float(poll_seconds)))


def _write_run_ids(ws, sheet_rows: list[int], run_id: str, chunk_size: int = 50) -> int:
    runtime = _runtime_module()
    unique_rows = sorted(set(int(value) for value in sheet_rows))
    written = 0
    for part in _chunks(unique_rows, chunk_size):
        updates = [{"range": f"G{row}", "values": [[run_id]]} for row in part]
        runtime._with_sheets_retry(
            lambda payload=updates: ws.batch_update(payload, value_input_option="RAW"),
            action=f"edit_files.write_run_id:{part[0]}-{part[-1]}",
            max_attempts=6,
            max_delay=20.0,
        )
        written += len(part)
        print(f"[Sheet writeback] run_id | {written}/{len(unique_rows)}", flush=True)
    return written


def _log_summary(logger, *, phase: str, status: str, summary: dict[str, int], message: str) -> None:
    logger.log_row(
        phase=phase,
        log_type="summary",
        status=status,
        entity_type="FILE",
        rows_loaded=summary.get("rows_loaded", 0),
        rows_pending=summary.get("rows_pending", 0),
        rows_recognized=summary.get("rows_recognized", 0),
        rows_planned=summary.get("rows_planned", 0),
        rows_written=summary.get("rows_written", 0),
        rows_skipped=summary.get("rows_skipped", 0),
        message=message,
        error_reason="" if status in {"SUCCESS", "NEEDS_CONFIRMATION"} else status.lower(),
    )
    logger.flush()


def run(
    *,
    site_code: str,
    gsheet_sa_value: str,
    shopify_access_token: str,
    shop_domain: str,
    console_core_url: str,
    job_name: str = DEFAULT_JOB_NAME,
    api_version: str = "2026-07",
    input_sheet_label: str = DEFAULT_INPUT_SHEET_LABEL,
    worksheet_title: str = DEFAULT_INPUT_TAB,
    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",
    cfg_sites_tab: str = "Cfg__Sites",
    run_id: Optional[str] = None,
    dry_run: bool = True,
    confirmed: bool = False,
    preview_limit: int = 100,
    snapshot_path: str = "edit_files_input_snapshot.csv",
    http_timeout: int = 60,
    async_timeout_seconds: int = 120,
    poll_seconds: float = 2.0,
    writeback_chunk_size: int = 50,
) -> dict[str, Any]:
    """Preview or apply Edit_File rows.

    Shopify writes occur only when ``dry_run is False`` AND
    ``confirmed is True``. Blank ``run_id`` rows are pending. Successful and
    already-satisfied rows are stamped only after a real Apply run.
    """
    runtime = _runtime_module()
    run_id = _norm(run_id) or _run_id()
    apply_enabled = (not bool(dry_run)) and bool(confirmed)

    print(f"[Start] {dt.datetime.now().astimezone().isoformat(timespec='seconds')}")
    print(f"[Module] {MODULE_PATH} | {MODULE_VERSION}")
    print(f"[Safety] DRY_RUN={dry_run} | CONFIRMED={confirmed} | APPLY={apply_enabled}")

    gc = runtime.build_gsheet_client(gsheet_sa_value)
    client = runtime.build_shopify_client(
        shopify_access_token=shopify_access_token,
        shop_domain=shop_domain,
        api_version=api_version,
        http_timeout=http_timeout,
    )
    _, ws, input_sheet_url = runtime.open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )
    runlog_url = runtime.get_sheet_url_by_label(
        gc, console_core_url, site_code, runlog_sheet_label, cfg_sites_tab
    )
    logger = runtime.RunLogger(
        gc=gc,
        runlog_sheet_url=runlog_url,
        runlog_tab_name=runlog_tab_name,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
    )

    print("[Phase 1/5] Snapshot input")
    all_rows, physical_rows = _load_input_snapshot(ws, snapshot_path)
    pending_source = [row for row in all_rows if not _norm(row.get("run_id"))]
    rows = validate_input_rows(pending_source)
    summary = {
        "rows_loaded": len(all_rows),
        "rows_pending": len(rows),
        "rows_recognized": 0,
        "rows_planned": 0,
        "rows_written": 0,
        "rows_no_change": 0,
        "rows_blocked": 0,
        "rows_failed": 0,
        "rows_skipped": 0,
    }
    print(
        f"[Input] physical_rows={physical_rows} | loaded={len(all_rows)} | "
        f"pending={len(rows)} | snapshot={Path(snapshot_path).resolve()}"
    )
    if not rows:
        _log_summary(
            logger,
            phase="preview",
            status="SUCCESS",
            summary=summary,
            message="No pending rows in Edit_File",
        )
        return {
            "status": "no_pending_rows",
            "summary": summary,
            "preview": [],
            "warnings": [],
            "meta": {"run_id": run_id, "input_sheet_url": input_sheet_url},
        }

    print("[Phase 2/5] Resolve products and validate files")
    product_cache: dict[str, str] = {}
    for index, row in enumerate(rows, start=1):
        if row["_decision"] == "BLOCK":
            continue
        product_input = row["product_gid_or_handle"]
        if product_input not in product_cache:
            product_cache[product_input] = _resolve_product_gid(client, product_input)
        row["_product_gid"] = product_cache[product_input]
        if not row["_product_gid"]:
            row["_decision"] = "BLOCK"
            row["_reason"] = "product_not_found_or_handle_not_exact"
        if index == 1 or index % 25 == 0 or index == len(rows):
            print(f"[Resolve] {index}/{len(rows)} | product={product_input}", flush=True)

    # Catch equivalent inputs that resolve to the same Product (for example a
    # numeric ID in one row and the full GID in another).
    resolved_pair_counts = Counter(
        (row.get("_product_gid", ""), row["file_gid"])
        for row in rows
        if row["_decision"] != "BLOCK" and row.get("_product_gid")
    )
    resolved_position_counts = Counter(
        (row.get("_product_gid", ""), row["_media_position"])
        for row in rows
        if row["_decision"] != "BLOCK"
        and row.get("_product_gid")
        and row["_media_position"] is not None
    )
    for row in rows:
        if row["_decision"] == "BLOCK" or not row.get("_product_gid"):
            continue
        reasons = []
        if resolved_pair_counts[(row["_product_gid"], row["file_gid"])] > 1:
            reasons.append("duplicate_product_file_pair_after_resolution")
        if (
            row["_media_position"] is not None
            and resolved_position_counts[(row["_product_gid"], row["_media_position"])] > 1
        ):
            reasons.append("duplicate_media_position_after_resolution")
        if reasons:
            row["_decision"] = "BLOCK"
            row["_reason"] = ";".join(reasons)

    file_info = _fetch_file_info(
        client,
        [row["file_gid"] for row in rows if row["_decision"] != "BLOCK"],
    )
    for row in rows:
        if row["_decision"] == "BLOCK":
            continue
        info = file_info.get(row["file_gid"])
        if not info or info.get("type") not in FILE_TYPES:
            row["_decision"] = "BLOCK"
            row["_reason"] = "file_not_found_or_unsupported_file_type"
            continue
        row["_file_type"] = info["type"]
        if info.get("status") != "READY":
            row["_decision"] = "BLOCK"
            row["_reason"] = f"file_not_READY:{info.get('status') or 'UNKNOWN'}"
        elif row["_media_position"] is not None and info["type"] not in ORDERABLE_FILE_TYPES:
            row["_decision"] = "BLOCK"
            row["_reason"] = "GenericFile_cannot_have_media_position_or_REORDER"

    print("[Phase 3/5] Read current references and build plan")
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["_decision"] != "BLOCK":
            groups[row["_product_gid"]].append(row)

    product_state: dict[str, dict[str, Any]] = {}
    for index, (product_gid, group) in enumerate(groups.items(), start=1):
        media = _fetch_product_media(client, product_gid)
        if media is None:
            for row in group:
                row["_decision"] = "BLOCK"
                row["_reason"] = "product_not_found"
            continue
        refs = _fetch_product_file_refs(client, product_gid)
        product_state[product_gid] = {"media": media, "refs": refs}
        media_position = {file_gid: pos for pos, file_gid in enumerate(media, start=1)}
        projected_count = len(media)
        for row in group:
            attached = row["file_gid"] in refs
            row["_attached"] = attached
            row["_current_position"] = media_position.get(row["file_gid"], "")
            action = row["action"]
            if action == "ADD":
                if attached and row["_media_position"] is None:
                    row["_decision"] = "NO_CHANGE"
                    row["_reason"] = "already_associated"
                else:
                    row["_decision"] = "APPLY"
                    row["_association_op"] = "" if attached else "ADD"
                    if not attached and row.get("_file_type") in ORDERABLE_FILE_TYPES:
                        projected_count += 1
            elif action == "REMOVE":
                if not attached:
                    row["_decision"] = "NO_CHANGE"
                    row["_reason"] = "not_associated"
                else:
                    row["_decision"] = "APPLY"
                    row["_association_op"] = "REMOVE"
                    if row["file_gid"] in media:
                        projected_count -= 1
            elif action == "REORDER":
                if row["file_gid"] not in media:
                    row["_decision"] = "BLOCK"
                    row["_reason"] = "REORDER_requires_existing_product_media"
                else:
                    # Keep every position directive in the same final-order
                    # constraint set. A different move could otherwise displace
                    # a row that happened to start at its requested position.
                    row["_decision"] = "APPLY"
                    row["_association_op"] = ""

        for row in group:
            position = row["_media_position"]
            if (
                row["_decision"] == "APPLY"
                and position is not None
                and position > projected_count
            ):
                row["_decision"] = "BLOCK"
                row["_reason"] = (
                    f"media_position_out_of_range:{position};"
                    f"projected_media_count={projected_count}"
                )
        print(
            f"[Plan] product {index}/{len(groups)} | {product_gid} | "
            f"rows={len(group)} | refs={len(refs)} | media={len(media)}",
            flush=True,
        )

    summary["rows_recognized"] = sum(row["_decision"] != "BLOCK" for row in rows)
    summary["rows_planned"] = sum(row["_decision"] == "APPLY" for row in rows)
    summary["rows_no_change"] = sum(row["_decision"] == "NO_CHANGE" for row in rows)
    summary["rows_blocked"] = sum(row["_decision"] == "BLOCK" for row in rows)
    summary["rows_skipped"] = summary["rows_no_change"] + summary["rows_blocked"]
    preview = [_row_result(row) for row in rows[: max(0, int(preview_limit))]]
    warnings = []
    blocked = [item for item in preview if item["decision"] == "BLOCK"]
    if blocked:
        warnings.append({"type": "blocked_rows", "count": summary["rows_blocked"], "examples": blocked[:5]})
    if any(row["action"] == "REMOVE" and row["_decision"] == "APPLY" for row in rows):
        warnings.append({
            "type": "REMOVE_is_destructive",
            "count": sum(row["action"] == "REMOVE" and row["_decision"] == "APPLY" for row in rows),
            "examples": [{"message": "Removing a file-product reference removes it from the product media gallery and can clear variant image usage."}],
        })

    if not apply_enabled:
        status = "needs_confirmation" if not confirmed else "dry_run_confirmed_no_apply"
        _log_summary(
            logger,
            phase="preview",
            status="NEEDS_CONFIRMATION",
            summary=summary,
            message=(
                f"Preview only | planned={summary['rows_planned']} | "
                f"no_change={summary['rows_no_change']} | blocked={summary['rows_blocked']}"
            ),
        )
        return {
            "status": status,
            "summary": summary,
            "preview": preview,
            "warnings": warnings,
            "meta": {
                "run_id": run_id,
                "input_sheet_url": input_sheet_url,
                "snapshot_path": str(Path(snapshot_path).resolve()),
                "apply_enabled": False,
            },
        }

    print("[Phase 4/5] Apply associations and media order")
    completed_rows: set[int] = {
        int(row["_sheet_row"]) for row in rows if row["_decision"] == "NO_CHANGE"
    }
    failed_rows: set[int] = set()

    for product_index, (product_gid, group) in enumerate(groups.items(), start=1):
        active = [row for row in group if row["_decision"] == "APPLY"]
        if not active:
            continue
        print(
            f"[Apply] product {product_index}/{len(groups)} | {product_gid} | rows={len(active)}",
            flush=True,
        )
        for row in active:
            op = row.get("_association_op", "")
            if not op:
                continue
            try:
                _file_update(
                    client,
                    file_gid=row["file_gid"],
                    product_gid=product_gid,
                    add=(op == "ADD"),
                )
                row["_result"] = f"{op}_OK"
                if row["_media_position"] is None:
                    completed_rows.add(int(row["_sheet_row"]))
            except Exception as exc:
                row["_result"] = "FAILED"
                row["_reason"] = f"{type(exc).__name__}:{exc}"
                failed_rows.add(int(row["_sheet_row"]))

        ordered_rows = [
            row for row in active
            if row["_media_position"] is not None
            and int(row["_sheet_row"]) not in failed_rows
        ]
        if not ordered_rows:
            continue
        try:
            required = {row["file_gid"] for row in ordered_rows}
            forbidden = {
                row["file_gid"]
                for row in active
                if row.get("_association_op") == "REMOVE"
                and int(row["_sheet_row"]) not in failed_rows
                and row["file_gid"] in product_state[product_gid]["media"]
            }
            current_media = _wait_for_media_membership(
                client,
                product_gid,
                required,
                forbidden,
                timeout_seconds=async_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            desired = {row["file_gid"]: int(row["_media_position"]) for row in ordered_rows}
            target, moves = build_target_media_order(current_media, desired)
            if moves:
                job_id = _start_reorder(client, product_gid, moves)
                _wait_for_job(
                    client,
                    job_id,
                    timeout_seconds=async_timeout_seconds,
                    poll_seconds=poll_seconds,
                )
                verified = _fetch_product_media(client, product_gid)
                if verified is None or any(
                    verified[position - 1] != file_gid
                    for file_gid, position in desired.items()
                ):
                    raise RuntimeError("post_reorder_verification_failed")
                if verified != target:
                    print(
                        "[Warning] specified positions verified; unrelated media ordering "
                        "differs from local target projection.",
                        flush=True,
                    )
            for row in ordered_rows:
                row["_result"] = "REORDER_OK" if moves else "NO_CHANGE_AFTER_REFRESH"
                completed_rows.add(int(row["_sheet_row"]))
        except Exception as exc:
            for row in ordered_rows:
                row["_result"] = "FAILED"
                row["_reason"] = f"{type(exc).__name__}:{exc}"
                failed_rows.add(int(row["_sheet_row"]))
                completed_rows.discard(int(row["_sheet_row"]))

    print("[Phase 5/5] Write run_id and final log")
    if completed_rows:
        _write_run_ids(ws, sorted(completed_rows), run_id, writeback_chunk_size)
    summary["rows_written"] = len(completed_rows)
    summary["rows_failed"] = len(failed_rows)
    summary["rows_skipped"] = summary["rows_blocked"]
    final_status = "SUCCESS"
    if failed_rows and completed_rows:
        final_status = "PARTIAL_SUCCESS"
    elif failed_rows and not completed_rows:
        final_status = "ERROR"
    _log_summary(
        logger,
        phase="apply",
        status=final_status,
        summary=summary,
        message=(
            f"Apply complete | completed={len(completed_rows)} | "
            f"failed={len(failed_rows)} | blocked={summary['rows_blocked']}"
        ),
    )
    return {
        "status": "applied" if not failed_rows else "partial_or_failed",
        "summary": summary,
        "preview": [_row_result(row) for row in rows[: max(0, int(preview_limit))]],
        "warnings": warnings,
        "meta": {
            "run_id": run_id,
            "input_sheet_url": input_sheet_url,
            "runlog_sheet_url": runlog_url,
            "snapshot_path": str(Path(snapshot_path).resolve()),
            "apply_enabled": True,
            "final_status": final_status,
        },
    }


__all__ = [
    "MODULE_PATH",
    "MODULE_VERSION",
    "INPUT_COLUMNS",
    "validate_input_rows",
    "build_target_media_order",
    "resolve_runtime_context",
    "update_existing_notebook_registry_row",
    "run",
]
