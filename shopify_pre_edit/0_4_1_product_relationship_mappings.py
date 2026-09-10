"""Build PBS OtherSize and Product-Group mapping-table plans.

This module owns deterministic, side-effect-free business transformation only.
Authentication, routing, worksheet creation, clear/overwrite, and read-back
verification belong to the Notebook Runner.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping, Sequence


MODULE_PATH = "shopify_pre_edit.0_4_1_product_relationship_mappings"
MODULE_VERSION = "2026-09-06-pbs-product-relationship-v1"

SOURCE_FIELDS = (
    "Product ID (numeric)",
    "Product Type Internal",
    "SPU-V",
    "SKU-2",
    "Variant Base",
    "Size-V",
)

OUTPUT_HEADERS_E = (
    "Product Type Internal",
    "SPU-V",
    "SKU",
    "Product ID (numeric)",
    "desired_value",
    "Variant Base",
    "是否成功",
    "报错内容",
)

OUTPUT_HEADERS_W = (
    "Product ID (numeric)",
    "desired_value",
    "是否成功",
    "报错内容",
)

OUTPUT_TAB_OTHER_E = "M_OtherSize_E"
OUTPUT_TAB_OTHER_W = "M_OtherSize_W"
OUTPUT_TAB_GROUP_E = "M_Product-Group_E"
OUTPUT_TAB_GROUP_W = "M_Product-Group_W"

SUCCESS_YES = "是"
SUCCESS_NO = "否"
MESSAGE_SEPARATOR = " || "


def _text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none"} else text


def _key(value: Any) -> str:
    return _text(value).casefold()


def _pid_sort_key(value: Any) -> tuple[int, int | str]:
    text = _text(value)
    if text.isdigit():
        return (0, int(text))
    return (1, text.casefold())


def _issue(level: str, code: str, **details: Any) -> str:
    rendered = " | ".join(
        f"{name}={_text(value)}" for name, value in details.items()
    )
    return f"{level} | {code}" + (f" | {rendered}" if rendered else "")


def _merge_messages(*groups: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for message in group:
            if message and message not in seen:
                seen.add(message)
                result.append(message)
    return result


def _records(rows: Any) -> list[dict[str, str]]:
    if hasattr(rows, "to_dict"):
        rows = rows.to_dict(orient="records")
    return [
        {str(name): _text(value) for name, value in dict(row).items()}
        for row in rows
    ]


def _validate_source(records: Sequence[Mapping[str, str]]) -> None:
    available: set[str] = set()
    for row in records:
        available.update(row)
    missing = [field for field in SOURCE_FIELDS if field not in available]
    if missing:
        raise ValueError(f"Missing required source fields: {missing}")


def build_size_order(rows: Any) -> dict[str, int]:
    """Validate and return the exact business size-order dictionary."""
    records = _records(rows)
    order_by_size: dict[str, int] = {}
    size_by_order: dict[int, str] = {}
    for source_row, row in enumerate(records, start=2):
        size = _text(row.get("size"))
        raw_order = _text(row.get("排序"))
        if not size and not raw_order:
            continue
        if not size or not raw_order:
            raise ValueError(
                f"Size顺序 row {source_row} requires both 排序 and size; "
                f"排序={raw_order!r}, size={size!r}."
            )
        try:
            order = int(raw_order)
        except ValueError as exc:
            raise ValueError(
                f"Size顺序 row {source_row} 排序 must be an integer; got={raw_order!r}."
            ) from exc
        if order < 1:
            raise ValueError(
                f"Size顺序 row {source_row} 排序 must be >= 1; got={order}."
            )
        size_key = _key(size)
        if size_key in order_by_size and order_by_size[size_key] != order:
            raise ValueError(
                f"Size顺序 size={size!r} has conflicting orders="
                f"{sorted({order_by_size[size_key], order})}."
            )
        if order in size_by_order and _key(size_by_order[order]) != size_key:
            raise ValueError(
                f"Size顺序 order={order} maps to multiple sizes="
                f"{[size_by_order[order], size]}."
            )
        order_by_size[size_key] = order
        size_by_order[order] = size
    if not order_by_size:
        raise ValueError("Size顺序 has no usable rows.")
    return order_by_size


def _dedupe_rows(rows: Sequence[Sequence[str]]) -> tuple[list[list[str]], int]:
    result: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    removed = 0
    for row in rows:
        key = tuple(row)
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        result.append(list(row))
    return result, removed


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> dict[str, Any]:
    return {"headers": list(headers), "rows": [list(row) for row in rows]}


def build_mapping_tables(source_rows: Any, size_order_rows: Any) -> dict[str, Any]:
    """Build all four mapping tables without external side effects.

    Successful W mapping key is ``Wholesale Product ID + desired_value``;
    physical deduplication removes only exact four-column rows so distinct
    inherited diagnostics are not discarded. Diagnostic failure
    rows retain their source E Product ID and Variant Base inside the stable
    error text. Consumers must filter ``是否成功 == 是`` and fail closed when a
    Wholesale Product ID has multiple distinct successful desired values.
    """
    records = _records(source_rows)
    _validate_source(records)
    size_order = build_size_order(size_order_rows)

    normalized: list[dict[str, str]] = []
    for source_row, row in enumerate(records, start=2):
        normalized.append({
            "source_row": str(source_row),
            "product_id": _text(row.get("Product ID (numeric)")),
            "product_type": _text(row.get("Product Type Internal")),
            "spu_v": _text(row.get("SPU-V")),
            "sku": _text(row.get("SKU-2")),
            "variant_base": _text(row.get("Variant Base")),
            "size_v": _text(row.get("Size-V")),
        })

    each_all = [r for r in normalized if _key(r["product_type"]) == "each box"]
    target_physical = [
        r for r in each_all
        if "-pack" not in r["sku"].casefold()
        and "-box" not in r["sku"].casefold()
    ]

    target: list[dict[str, str]] = []
    target_seen: set[tuple[str, ...]] = set()
    target_exact_duplicates_removed = 0
    for row in target_physical:
        identity = tuple(row[name] for name in (
            "product_type", "spu_v", "sku", "product_id", "variant_base", "size_v"
        ))
        if identity in target_seen:
            target_exact_duplicates_removed += 1
            continue
        target_seen.add(identity)
        target.append(row)

    qa_events: set[str] = set()
    all_sizes_by_pid: dict[str, set[str]] = defaultdict(set)
    for row in each_all:
        if row["product_id"]:
            all_sizes_by_pid[row["product_id"]].add(row["size_v"])
    multi_size_all = {
        product_id: sorted(sizes)
        for product_id, sizes in all_sizes_by_pid.items()
        if len(sizes) > 1
    }

    spus_by_pid: dict[str, set[str]] = defaultdict(set)
    display_spu: dict[str, str] = {}
    sizes_by_spu_pid: dict[tuple[str, str], set[str]] = defaultdict(set)
    member_ids_by_spu: dict[str, set[str]] = defaultdict(set)
    for row in target:
        spu_key = _key(row["spu_v"])
        product_id = row["product_id"]
        if not spu_key or not product_id:
            continue
        display_spu.setdefault(spu_key, row["spu_v"])
        member_ids_by_spu[spu_key].add(product_id)
        sizes_by_spu_pid[(spu_key, product_id)].add(row["size_v"])
        spus_by_pid[product_id].add(spu_key)
    multi_spu_pids = {
        product_id: sorted(display_spu[spu] for spu in spus)
        for product_id, spus in spus_by_pid.items()
        if len(spus) > 1
    }

    sorted_ids_by_spu: dict[str, list[str]] = {}
    messages_by_spu: dict[str, list[str]] = defaultdict(list)
    for spu_key, member_ids in member_ids_by_spu.items():
        spu_v = display_spu[spu_key]
        canonical_size: dict[str, str] = {}
        unresolved_size_pids: set[str] = set()
        for product_id in sorted(member_ids, key=_pid_sort_key):
            target_sizes = sorted(sizes_by_spu_pid[(spu_key, product_id)])
            if len(target_sizes) > 1:
                message = _issue(
                    "WARNING", "PRODUCT_ID_MULTI_SIZE_TARGET",
                    **{
                        "Product ID": product_id,
                        "SPU-V": spu_v,
                        "Size-V": f"[{','.join(target_sizes)}]",
                    },
                )
                messages_by_spu[spu_key].append(message)
                qa_events.add(message)
                unresolved_size_pids.add(product_id)
            else:
                canonical_size[product_id] = target_sizes[0]
            if product_id in multi_size_all:
                message = _issue(
                    "WARNING", "PRODUCT_ID_MULTI_SIZE_SOURCE",
                    **{
                        "Product ID": product_id,
                        "SPU-V": spu_v,
                        "All Each Box Size-V": f"[{','.join(multi_size_all[product_id])}]",
                        "Sorting Size-V": canonical_size.get(product_id, "UNRESOLVED"),
                    },
                )
                messages_by_spu[spu_key].append(message)
                qa_events.add(message)
            if product_id in multi_spu_pids:
                message = _issue(
                    "WARNING", "PRODUCT_ID_MULTI_SPU",
                    **{
                        "Product ID": product_id,
                        "SPU-V": f"[{','.join(multi_spu_pids[product_id])}]",
                    },
                )
                messages_by_spu[spu_key].append(message)
                qa_events.add(message)

        pids_by_size: dict[str, set[str]] = defaultdict(set)
        for product_id, size_v in canonical_size.items():
            if size_v:
                pids_by_size[size_v].add(product_id)
            else:
                message = _issue(
                    "WARNING", "SIZE_V_BLANK",
                    **{"Product ID": product_id, "SPU-V": spu_v, "Size-V": "<blank>"},
                )
                messages_by_spu[spu_key].append(message)
                qa_events.add(message)
            if size_v and _key(size_v) not in size_order:
                message = _issue(
                    "WARNING", "SIZE_NOT_IN_ORDER",
                    **{"Product ID": product_id, "SPU-V": spu_v, "Size-V": size_v},
                )
                messages_by_spu[spu_key].append(message)
                qa_events.add(message)
        for size_v, product_ids in pids_by_size.items():
            if len(product_ids) > 1:
                message = _issue(
                    "WARNING", "DUPLICATE_SIZE_IN_SPU",
                    **{
                        "SPU-V": spu_v,
                        "Size-V": size_v or "<BLANK>",
                        "Product IDs": f"[{','.join(sorted(product_ids, key=_pid_sort_key))}]",
                    },
                )
                messages_by_spu[spu_key].append(message)
                qa_events.add(message)

        def member_key(product_id: str) -> tuple[int, int, tuple[int, int | str]]:
            size = canonical_size.get(product_id, "")
            if product_id in unresolved_size_pids or not size:
                return (1, 0, _pid_sort_key(product_id))
            order = size_order.get(_key(size))
            if order is None:
                return (1, 0, _pid_sort_key(product_id))
            return (0, order, _pid_sort_key(product_id))

        sorted_ids_by_spu[spu_key] = sorted(member_ids, key=member_key)
        messages_by_spu[spu_key] = _merge_messages(messages_by_spu[spu_key])

    rows_other_e_raw: list[list[str]] = []
    rows_group_e_raw: list[list[str]] = []
    stages: list[dict[str, Any]] = []
    for row in target:
        errors: list[str] = []
        if not row["product_id"]:
            errors.append(_issue(
                "ERROR", "PRODUCT_ID_BLANK",
                **{"source_row": row["source_row"], "SKU": row["sku"]},
            ))
        if not row["spu_v"]:
            errors.append(_issue(
                "ERROR", "SPU_V_BLANK",
                **{
                    "source_row": row["source_row"],
                    "Product ID": row["product_id"],
                    "SKU": row["sku"],
                },
            ))
        for message in errors:
            qa_events.add(message)
        spu_key = _key(row["spu_v"])
        warnings = list(messages_by_spu.get(spu_key, []))
        ordered = sorted_ids_by_spu.get(spu_key, [])
        desired_other = "" if errors else ",".join(
            product_id for product_id in ordered if product_id != row["product_id"]
        )
        desired_group = "" if errors else ",".join(ordered)
        status = SUCCESS_NO if errors else SUCCESS_YES
        message_text = MESSAGE_SEPARATOR.join(_merge_messages(errors, warnings))
        common = [
            row["product_type"], row["spu_v"], row["sku"], row["product_id"]
        ]
        rows_other_e_raw.append(common + [desired_other, row["variant_base"], status, message_text])
        rows_group_e_raw.append(common + [desired_group, row["variant_base"], status, message_text])
        stages.append({
            **row,
            "status": status,
            "messages": _merge_messages(errors, warnings),
            "desired_other": desired_other,
            "desired_group": desired_group,
        })

    rows_other_e, dedupe_other_e = _dedupe_rows(rows_other_e_raw)
    rows_group_e, dedupe_group_e = _dedupe_rows(rows_group_e_raw)

    wholesale_by_variant: dict[str, set[str]] = defaultdict(set)
    for row in normalized:
        if _key(row["product_type"]) == "wholesale" and row["variant_base"] and row["product_id"]:
            wholesale_by_variant[_key(row["variant_base"])].add(row["product_id"])

    def build_w(desired_field: str) -> tuple[list[list[str]], int]:
        raw: list[list[str]] = []
        for stage in stages:
            inherited = list(stage["messages"])
            if stage["status"] == SUCCESS_NO:
                message = _issue(
                    "ERROR", "E_STAGE_FAILED",
                    **{"source Product ID": stage["product_id"] or "<BLANK>", "Variant Base": stage["variant_base"] or "<BLANK>"},
                )
                raw.append(["", "", SUCCESS_NO, MESSAGE_SEPARATOR.join(_merge_messages(inherited, [message]))])
                continue
            if not stage["variant_base"]:
                message = _issue(
                    "ERROR", "MISSING_VARIANT_BASE",
                    **{"source Product ID": stage["product_id"], "Variant Base": "<BLANK>"},
                )
                qa_events.add(message)
                raw.append(["", stage[desired_field], SUCCESS_NO, MESSAGE_SEPARATOR.join(_merge_messages(inherited, [message]))])
                continue
            wholesale_ids = sorted(
                wholesale_by_variant.get(_key(stage["variant_base"]), set()),
                key=_pid_sort_key,
            )
            if not wholesale_ids:
                message = _issue(
                    "ERROR", "WHOLESALE_NOT_FOUND",
                    **{"source Product ID": stage["product_id"], "Variant Base": stage["variant_base"]},
                )
                qa_events.add(message)
                raw.append(["", stage[desired_field], SUCCESS_NO, MESSAGE_SEPARATOR.join(_merge_messages(inherited, [message]))])
                continue
            extra: list[str] = []
            if len(wholesale_ids) > 1:
                message = _issue(
                    "WARNING", "MULTIPLE_WHOLESALE_MATCHES",
                    **{
                        "source Product ID": stage["product_id"],
                        "Variant Base": stage["variant_base"],
                        "Wholesale Product IDs": f"[{','.join(wholesale_ids)}]",
                    },
                )
                extra.append(message)
                qa_events.add(message)
            for wholesale_id in wholesale_ids:
                raw.append([
                    wholesale_id,
                    stage[desired_field],
                    SUCCESS_YES,
                    MESSAGE_SEPARATOR.join(_merge_messages(inherited, extra)),
                ])

        values_by_pid: dict[str, set[str]] = defaultdict(set)
        for row in raw:
            if row[0] and row[2] == SUCCESS_YES:
                values_by_pid[row[0]].add(row[1])
        conflicts = {pid: values for pid, values in values_by_pid.items() if len(values) > 1}
        if conflicts:
            rebuilt: list[list[str]] = []
            for row in raw:
                if row[0] in conflicts and row[2] == SUCCESS_YES:
                    message = _issue(
                        "WARNING", "WHOLESALE_MULTIPLE_DESIRED_VALUES",
                        **{"Wholesale Product ID": row[0], "desired_value count": len(conflicts[row[0]])},
                    )
                    qa_events.add(message)
                    row = row[:3] + [MESSAGE_SEPARATOR.join(_merge_messages([row[3]], [message]))]
                rebuilt.append(row)
            raw = rebuilt
        return _dedupe_rows(raw)

    rows_other_w, dedupe_other_w = build_w("desired_other")
    rows_group_w, dedupe_group_w = build_w("desired_group")
    tables = {
        OUTPUT_TAB_OTHER_E: _table(OUTPUT_HEADERS_E, rows_other_e),
        OUTPUT_TAB_OTHER_W: _table(OUTPUT_HEADERS_W, rows_other_w),
        OUTPUT_TAB_GROUP_E: _table(OUTPUT_HEADERS_E, rows_group_e),
        OUTPUT_TAB_GROUP_W: _table(OUTPUT_HEADERS_W, rows_group_w),
    }

    def table_stats(name: str) -> dict[str, int]:
        table = tables[name]
        status_index = table["headers"].index("是否成功")
        error_index = table["headers"].index("报错内容")
        return {
            "rows": len(table["rows"]),
            "success_rows": sum(row[status_index] == SUCCESS_YES for row in table["rows"]),
            "failed_rows": sum(row[status_index] == SUCCESS_NO for row in table["rows"]),
            "warning_rows": sum("WARNING |" in row[error_index] for row in table["rows"]),
        }

    summary = {
        "source_physical_rows": len(normalized),
        "size_order_rows": len(size_order),
        "each_box_physical_rows": len(each_all),
        "target_physical_rows": len(target_physical),
        "target_rows": len(target),
        "target_exact_duplicates_removed": target_exact_duplicates_removed,
        "unique_target_products": len({r["product_id"] for r in target if r["product_id"]}),
        "spu_groups": len(member_ids_by_spu),
        "qa_unique_events": len(qa_events),
        "dedupe_other_e": dedupe_other_e,
        "dedupe_other_w": dedupe_other_w,
        "dedupe_group_e": dedupe_group_e,
        "dedupe_group_w": dedupe_group_w,
        "tables": {name: table_stats(name) for name in tables},
    }
    return {
        "status": "SUCCESS_WITH_WARNINGS" if qa_events else "SUCCESS",
        "summary": summary,
        "tables": tables,
        "warnings": sorted(event for event in qa_events if event.startswith("WARNING |")),
        "errors": sorted(event for event in qa_events if event.startswith("ERROR |")),
        "meta": {
            "module_path": MODULE_PATH,
            "module_version": MODULE_VERSION,
            "side_effects": "NONE",
            "w_success_grain": "Wholesale Product ID + desired_value",
        },
    }
