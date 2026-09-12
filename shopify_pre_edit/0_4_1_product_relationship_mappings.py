"""Build PBS OtherSize and Product-Group mapping-table plans.

Product relationship source of truth (v2):
- Grouping key: ``Product Group SPU``.
- In-group sort value: ``Product Group Size``.
- Sort lookup priority: ``Size顺序`` first, then ``Variant顺序``.
- A source row may belong to one or more Product Group SPUs.

This module owns deterministic, side-effect-free business transformation only.
Authentication, routing, worksheet creation, clear/overwrite, and read-back
verification belong to the Notebook Runner.
"""

from __future__ import annotations

import ast
import json
from collections import defaultdict
from typing import Any, Iterable, Mapping, Sequence


MODULE_PATH = "shopify_pre_edit.0_4_1_product_relationship_mappings"
MODULE_VERSION = "2026-09-11-pbs-product-group-relationship-v2"

SOURCE_FIELDS = (
    "Product ID (numeric)",
    "Product Type Internal",
    "SKU-2",
    "Variant Base",
    "Product Group SPU",
    "Product Group Size",
)

SIZE_ORDER_VALUE_FIELD = "size"
VARIANT_ORDER_VALUE_FIELD = "Variant"
ORDER_FIELD = "排序"

OUTPUT_HEADERS_E = (
    "Product Type Internal",
    "Product Group SPU",
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
            cleaned = _text(message)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                result.append(cleaned)
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


def _parse_group_spus(value: Any) -> list[str]:
    """Parse Product Group SPU into a stable, deduplicated membership list.

    Accepted inputs:
    - JSON/Python-like list: ["G1", "G2"] / ['G1', 'G2']
    - one bare scalar string: G1

    A bracketed value that cannot be parsed fails closed instead of becoming a
    literal group key such as '["G1"'.
    """
    raw = _text(value)
    if not raw:
        return []

    parsed: Any
    if raw.startswith("[") or raw.endswith("]"):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(raw)
            except (ValueError, SyntaxError) as exc:
                raise ValueError(
                    f"Product Group SPU is not a valid list: {raw!r}"
                ) from exc
        if not isinstance(parsed, (list, tuple)):
            raise ValueError(
                f"Product Group SPU bracketed value must be a list; got={type(parsed).__name__}."
            )
        values = [_text(item) for item in parsed]
    else:
        values = [raw]

    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        if not item:
            continue
        item_key = _key(item)
        if item_key in seen:
            continue
        seen.add(item_key)
        result.append(item)
    return result


def _build_order_map(rows: Any, *, tab_name: str, value_field: str) -> dict[str, int]:
    """Validate one order table and return case-insensitive value -> order."""
    records = _records(rows)
    available: set[str] = set()
    for row in records:
        available.update(row)
    missing = [field for field in (ORDER_FIELD, value_field) if field not in available]
    if missing:
        raise ValueError(f"{tab_name} missing required fields: {missing}")

    order_by_value: dict[str, int] = {}
    display_by_value: dict[str, str] = {}
    value_by_order: dict[int, str] = {}

    for source_row, row in enumerate(records, start=2):
        value = _text(row.get(value_field))
        raw_order = _text(row.get(ORDER_FIELD))
        if not value and not raw_order:
            continue
        if not value or not raw_order:
            raise ValueError(
                f"{tab_name} row {source_row} requires both {ORDER_FIELD} and {value_field}; "
                f"{ORDER_FIELD}={raw_order!r}, {value_field}={value!r}."
            )
        try:
            order = int(raw_order)
        except ValueError as exc:
            raise ValueError(
                f"{tab_name} row {source_row} {ORDER_FIELD} must be an integer; got={raw_order!r}."
            ) from exc
        if order < 1:
            raise ValueError(
                f"{tab_name} row {source_row} {ORDER_FIELD} must be >= 1; got={order}."
            )

        value_key = _key(value)
        if value_key in order_by_value and order_by_value[value_key] != order:
            raise ValueError(
                f"{tab_name} {value_field}={value!r} has conflicting orders="
                f"{sorted({order_by_value[value_key], order})}."
            )
        if order in value_by_order and _key(value_by_order[order]) != value_key:
            raise ValueError(
                f"{tab_name} order={order} maps to multiple values="
                f"{[value_by_order[order], value]}."
            )

        order_by_value[value_key] = order
        display_by_value[value_key] = value
        value_by_order[order] = value

    if not order_by_value:
        raise ValueError(f"{tab_name} has no usable rows.")
    return order_by_value


def build_size_order(rows: Any) -> dict[str, int]:
    return _build_order_map(
        rows,
        tab_name="Size顺序",
        value_field=SIZE_ORDER_VALUE_FIELD,
    )


def build_variant_order(rows: Any) -> dict[str, int]:
    return _build_order_map(
        rows,
        tab_name="Variant顺序",
        value_field=VARIANT_ORDER_VALUE_FIELD,
    )


def _resolve_business_order(
    product_group_size: str,
    *,
    size_order: Mapping[str, int],
    variant_order: Mapping[str, int],
) -> tuple[int, int, str]:
    """Resolve one Product Group Size into one total sort key.

    Priority is intentionally source-based, not raw-number-based:
    0 = found in Size顺序
    1 = not in Size顺序, found in Variant顺序
    2 = unresolved in both tables

    This prevents unrelated order numbers from the two dictionaries from being
    compared as if they shared one numeric coordinate system.
    """
    value_key = _key(product_group_size)
    if value_key in size_order:
        return (0, size_order[value_key], "Size顺序")
    if value_key in variant_order:
        return (1, variant_order[value_key], "Variant顺序")
    return (2, 0, "UNRESOLVED")


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


def build_mapping_tables(
    source_rows: Any,
    size_order_rows: Any,
    variant_order_rows: Any,
) -> dict[str, Any]:
    """Build all four mapping tables without external side effects.

    Business contract:
    - All four tables use Product Group SPU as the grouping source of truth.
    - Product Group SPU list values are expanded into product × group membership.
    - Product Group Size sorts by Size顺序 first; only unresolved values fall
      back to Variant顺序. Values unresolved by both sort last by Product ID.
    - M_OtherSize_E excludes the current Product ID from its ordered group list.
    - M_Product-Group_E includes the current Product ID.
    - W tables reverse-match E rows to Wholesale through Variant Base exactly as
      before.
    """
    records = _records(source_rows)
    _validate_source(records)
    size_order = build_size_order(size_order_rows)
    variant_order = build_variant_order(variant_order_rows)

    normalized: list[dict[str, Any]] = []
    for source_row, row in enumerate(records, start=2):
        raw_group_spu = _text(row.get("Product Group SPU"))
        group_spus: list[str] = []
        group_parse_error = ""
        try:
            group_spus = _parse_group_spus(raw_group_spu)
        except ValueError as exc:
            group_parse_error = str(exc)

        normalized.append({
            "source_row": str(source_row),
            "product_id": _text(row.get("Product ID (numeric)")),
            "product_type": _text(row.get("Product Type Internal")),
            "sku": _text(row.get("SKU-2")),
            "variant_base": _text(row.get("Variant Base")),
            "product_group_spu_raw": raw_group_spu,
            "group_spus": group_spus,
            "group_parse_error": group_parse_error,
            "product_group_size": _text(row.get("Product Group Size")),
        })

    each_all = [r for r in normalized if _key(r["product_type"]) == "each box"]
    target_physical = [
        r for r in each_all
        if r["sku"]
        and "-pack" not in r["sku"].casefold()
        and "-box" not in r["sku"].casefold()
    ]
    blank_sku_each_rows_skipped = sum(
        1 for r in each_all if not r["sku"]
    )

    # Expand each filtered source row into product × Product Group SPU membership.
    # Invalid/blank group values still create one diagnostic row so failures do
    # not silently disappear from the E outputs.
    expanded_raw: list[dict[str, Any]] = []
    for row in target_physical:
        if row["group_spus"]:
            for group_spu in row["group_spus"]:
                expanded_raw.append({**row, "group_spu": group_spu})
        else:
            expanded_raw.append({**row, "group_spu": ""})

    target: list[dict[str, Any]] = []
    target_seen: set[tuple[str, ...]] = set()
    target_exact_duplicates_removed = 0
    for row in expanded_raw:
        identity = (
            _text(row["product_type"]),
            _text(row["group_spu"]),
            _text(row["sku"]),
            _text(row["product_id"]),
            _text(row["variant_base"]),
            _text(row["product_group_size"]),
            _text(row["group_parse_error"]),
        )
        if identity in target_seen:
            target_exact_duplicates_removed += 1
            continue
        target_seen.add(identity)
        target.append(row)

    qa_events: set[str] = set()

    # Source-level audit: one Product ID should ordinarily resolve to one
    # Product Group Size across all Each Box physical rows.
    all_group_sizes_by_pid: dict[str, set[str]] = defaultdict(set)
    for row in each_all:
        if row["product_id"]:
            all_group_sizes_by_pid[row["product_id"]].add(row["product_group_size"])
    multi_group_size_all = {
        product_id: sorted(sizes)
        for product_id, sizes in all_group_sizes_by_pid.items()
        if len(sizes) > 1
    }

    display_group: dict[str, str] = {}
    member_ids_by_group: dict[str, set[str]] = defaultdict(set)
    sizes_by_group_pid: dict[tuple[str, str], set[str]] = defaultdict(set)

    for row in target:
        group_key = _key(row["group_spu"])
        product_id = _text(row["product_id"])
        if not group_key or not product_id:
            continue
        display_group.setdefault(group_key, _text(row["group_spu"]))
        member_ids_by_group[group_key].add(product_id)
        sizes_by_group_pid[(group_key, product_id)].add(
            _text(row["product_group_size"])
        )

    sorted_ids_by_group: dict[str, list[str]] = {}
    messages_by_group: dict[str, list[str]] = defaultdict(list)
    order_source_counts = {"Size顺序": 0, "Variant顺序": 0, "UNRESOLVED": 0}

    for group_key, member_ids in member_ids_by_group.items():
        group_spu = display_group[group_key]
        canonical_size: dict[str, str] = {}
        unresolved_size_pids: set[str] = set()

        for product_id in sorted(member_ids, key=_pid_sort_key):
            target_sizes = sorted(sizes_by_group_pid[(group_key, product_id)])
            if len(target_sizes) > 1:
                message = _issue(
                    "WARNING",
                    "PRODUCT_ID_MULTI_PRODUCT_GROUP_SIZE_TARGET",
                    **{
                        "Product ID": product_id,
                        "Product Group SPU": group_spu,
                        "Product Group Size": f"[{','.join(target_sizes)}]",
                    },
                )
                messages_by_group[group_key].append(message)
                qa_events.add(message)
                unresolved_size_pids.add(product_id)
            else:
                canonical_size[product_id] = target_sizes[0] if target_sizes else ""

            if product_id in multi_group_size_all:
                message = _issue(
                    "WARNING",
                    "PRODUCT_ID_MULTI_PRODUCT_GROUP_SIZE_SOURCE",
                    **{
                        "Product ID": product_id,
                        "Product Group SPU": group_spu,
                        "All Each Box Product Group Size":
                            f"[{','.join(multi_group_size_all[product_id])}]",
                        "Sorting Product Group Size":
                            canonical_size.get(product_id, "UNRESOLVED"),
                    },
                )
                messages_by_group[group_key].append(message)
                qa_events.add(message)

        pids_by_group_size: dict[str, set[str]] = defaultdict(set)
        order_resolution_by_pid: dict[str, tuple[int, int, str]] = {}

        for product_id, group_size in canonical_size.items():
            if group_size:
                pids_by_group_size[_key(group_size)].add(product_id)
            else:
                message = _issue(
                    "WARNING",
                    "PRODUCT_GROUP_SIZE_BLANK",
                    **{
                        "Product ID": product_id,
                        "Product Group SPU": group_spu,
                        "Product Group Size": "<BLANK>",
                    },
                )
                messages_by_group[group_key].append(message)
                qa_events.add(message)

            resolved = _resolve_business_order(
                group_size,
                size_order=size_order,
                variant_order=variant_order,
            )
            order_resolution_by_pid[product_id] = resolved
            order_source_counts[resolved[2]] += 1
            if group_size and resolved[2] == "UNRESOLVED":
                message = _issue(
                    "WARNING",
                    "PRODUCT_GROUP_SIZE_NOT_IN_ANY_ORDER",
                    **{
                        "Product ID": product_id,
                        "Product Group SPU": group_spu,
                        "Product Group Size": group_size,
                    },
                )
                messages_by_group[group_key].append(message)
                qa_events.add(message)

        for size_key, product_ids in pids_by_group_size.items():
            if len(product_ids) <= 1:
                continue
            display_size = next(
                (
                    canonical_size[pid]
                    for pid in product_ids
                    if _key(canonical_size.get(pid, "")) == size_key
                ),
                size_key,
            )
            message = _issue(
                "WARNING",
                "DUPLICATE_PRODUCT_GROUP_SIZE_IN_GROUP",
                **{
                    "Product Group SPU": group_spu,
                    "Product Group Size": display_size or "<BLANK>",
                    "Product IDs":
                        f"[{','.join(sorted(product_ids, key=_pid_sort_key))}]",
                },
            )
            messages_by_group[group_key].append(message)
            qa_events.add(message)

        def member_key(product_id: str) -> tuple[Any, ...]:
            if product_id in unresolved_size_pids:
                return (2, 0, _pid_sort_key(product_id))
            source_priority, order, _source = order_resolution_by_pid.get(
                product_id,
                (2, 0, "UNRESOLVED"),
            )
            return (source_priority, order, _pid_sort_key(product_id))

        sorted_ids_by_group[group_key] = sorted(member_ids, key=member_key)
        messages_by_group[group_key] = _merge_messages(messages_by_group[group_key])

    rows_other_e_raw: list[list[str]] = []
    rows_group_e_raw: list[list[str]] = []
    stages: list[dict[str, Any]] = []

    for row in target:
        errors: list[str] = []
        warnings: list[str] = []

        if not row["product_id"]:
            errors.append(_issue(
                "ERROR",
                "PRODUCT_ID_BLANK",
                **{"source_row": row["source_row"], "SKU": row["sku"]},
            ))

        if row["group_parse_error"]:
            errors.append(_issue(
                "ERROR",
                "PRODUCT_GROUP_SPU_PARSE_FAILED",
                **{
                    "source_row": row["source_row"],
                    "Product ID": row["product_id"],
                    "SKU": row["sku"],
                    "Product Group SPU": row["product_group_spu_raw"],
                    "reason": row["group_parse_error"],
                },
            ))
        elif not row["group_spu"]:
            errors.append(_issue(
                "ERROR",
                "PRODUCT_GROUP_SPU_BLANK",
                **{
                    "source_row": row["source_row"],
                    "Product ID": row["product_id"],
                    "SKU": row["sku"],
                },
            ))

        for message in errors:
            qa_events.add(message)

        group_key = _key(row["group_spu"])
        if group_key:
            warnings = list(messages_by_group.get(group_key, []))
        ordered = sorted_ids_by_group.get(group_key, []) if group_key else []

        desired_other = "" if errors else ",".join(
            product_id
            for product_id in ordered
            if product_id != row["product_id"]
        )
        desired_group = "" if errors else ",".join(ordered)
        status = SUCCESS_NO if errors else SUCCESS_YES
        message_text = MESSAGE_SEPARATOR.join(_merge_messages(errors, warnings))
        common = [
            row["product_type"],
            row["group_spu"],
            row["sku"],
            row["product_id"],
        ]
        rows_other_e_raw.append(
            common + [desired_other, row["variant_base"], status, message_text]
        )
        rows_group_e_raw.append(
            common + [desired_group, row["variant_base"], status, message_text]
        )
        stages.append({
            **row,
            "status": status,
            "messages": _merge_messages(errors, warnings),
            "desired_other": desired_other,
            "desired_group": desired_group,
        })

    rows_other_e, dedupe_other_e = _dedupe_rows(rows_other_e_raw)
    rows_group_e, dedupe_group_e = _dedupe_rows(rows_group_e_raw)

    # Wholesale reverse matching remains exactly Variant Base based.
    wholesale_by_variant: dict[str, set[str]] = defaultdict(set)
    for row in normalized:
        if (
            _key(row["product_type"]) == "wholesale"
            and row["variant_base"]
            and row["product_id"]
        ):
            wholesale_by_variant[_key(row["variant_base"])].add(row["product_id"])

    def build_w(desired_field: str) -> tuple[list[list[str]], int]:
        raw: list[list[str]] = []
        for stage in stages:
            inherited = list(stage["messages"])

            if stage["status"] == SUCCESS_NO:
                message = _issue(
                    "ERROR",
                    "E_STAGE_FAILED",
                    **{
                        "source Product ID": stage["product_id"] or "<BLANK>",
                        "Product Group SPU": stage["group_spu"] or "<BLANK>",
                        "Variant Base": stage["variant_base"] or "<BLANK>",
                    },
                )
                raw.append([
                    "",
                    "",
                    SUCCESS_NO,
                    MESSAGE_SEPARATOR.join(_merge_messages(inherited, [message])),
                ])
                continue

            if not stage["variant_base"]:
                message = _issue(
                    "ERROR",
                    "MISSING_VARIANT_BASE",
                    **{
                        "source Product ID": stage["product_id"],
                        "Product Group SPU": stage["group_spu"],
                        "Variant Base": "<BLANK>",
                    },
                )
                qa_events.add(message)
                raw.append([
                    "",
                    stage[desired_field],
                    SUCCESS_NO,
                    MESSAGE_SEPARATOR.join(_merge_messages(inherited, [message])),
                ])
                continue

            wholesale_ids = sorted(
                wholesale_by_variant.get(_key(stage["variant_base"]), set()),
                key=_pid_sort_key,
            )
            if not wholesale_ids:
                message = _issue(
                    "ERROR",
                    "WHOLESALE_NOT_FOUND",
                    **{
                        "source Product ID": stage["product_id"],
                        "Product Group SPU": stage["group_spu"],
                        "Variant Base": stage["variant_base"],
                    },
                )
                qa_events.add(message)
                raw.append([
                    "",
                    stage[desired_field],
                    SUCCESS_NO,
                    MESSAGE_SEPARATOR.join(_merge_messages(inherited, [message])),
                ])
                continue

            extra: list[str] = []
            if len(wholesale_ids) > 1:
                message = _issue(
                    "WARNING",
                    "MULTIPLE_WHOLESALE_MATCHES",
                    **{
                        "source Product ID": stage["product_id"],
                        "Product Group SPU": stage["group_spu"],
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
        for result_row in raw:
            if result_row[0] and result_row[2] == SUCCESS_YES:
                values_by_pid[result_row[0]].add(result_row[1])

        conflicts = {
            pid: values
            for pid, values in values_by_pid.items()
            if len(values) > 1
        }
        if conflicts:
            rebuilt: list[list[str]] = []
            for result_row in raw:
                if result_row[0] in conflicts and result_row[2] == SUCCESS_YES:
                    message = _issue(
                        "WARNING",
                        "WHOLESALE_MULTIPLE_DESIRED_VALUES",
                        **{
                            "Wholesale Product ID": result_row[0],
                            "desired_value count": len(conflicts[result_row[0]]),
                        },
                    )
                    qa_events.add(message)
                    result_row = result_row[:3] + [
                        MESSAGE_SEPARATOR.join(
                            _merge_messages([result_row[3]], [message])
                        )
                    ]
                rebuilt.append(result_row)
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
            "success_rows": sum(
                row[status_index] == SUCCESS_YES for row in table["rows"]
            ),
            "failed_rows": sum(
                row[status_index] == SUCCESS_NO for row in table["rows"]
            ),
            "warning_rows": sum(
                "WARNING |" in row[error_index] for row in table["rows"]
            ),
        }

    summary = {
        "source_physical_rows": len(normalized),
        "size_order_rows": len(size_order),
        "variant_order_rows": len(variant_order),
        "each_box_physical_rows": len(each_all),
        "target_physical_rows": len(target_physical),
        "blank_sku_each_rows_skipped": blank_sku_each_rows_skipped,
        "expanded_membership_rows": len(expanded_raw),
        "target_rows": len(target),
        "target_exact_duplicates_removed": target_exact_duplicates_removed,
        "unique_target_products": len({
            _text(r["product_id"]) for r in target if _text(r["product_id"])
        }),
        "product_group_spu_groups": len(member_ids_by_group),
        "source_multi_product_group_size_product_ids": len(multi_group_size_all),
        "sort_hits_size_order": order_source_counts["Size顺序"],
        "sort_hits_variant_order": order_source_counts["Variant顺序"],
        "sort_unresolved": order_source_counts["UNRESOLVED"],
        "qa_unique_events": len(qa_events),
        "dedupe_other_e": dedupe_other_e,
        "dedupe_other_w": dedupe_other_w,
        "dedupe_group_e": dedupe_group_e,
        "dedupe_group_w": dedupe_group_w,
        "tables": {name: table_stats(name) for name in tables},
    }

    warnings = sorted(
        event for event in qa_events if event.startswith("WARNING |")
    )
    errors = sorted(
        event for event in qa_events if event.startswith("ERROR |")
    )

    return {
        "status": "SUCCESS_WITH_WARNINGS" if qa_events else "SUCCESS",
        "summary": summary,
        "tables": tables,
        "warnings": warnings,
        "errors": errors,
        "meta": {
            "module_path": MODULE_PATH,
            "module_version": MODULE_VERSION,
            "side_effects": "NONE",
            "grouping_source_of_truth": "Product Group SPU",
            "sorting_value": "Product Group Size",
            "sorting_priority": ["Size顺序", "Variant顺序", "Product ID (numeric)"],
            "w_success_grain": "Wholesale Product ID + desired_value",
        },
    }
