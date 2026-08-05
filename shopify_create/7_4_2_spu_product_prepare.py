# -*- coding: utf-8 -*-
"""Prepare SPU Product Creation Input into a validated Preview plan.

Formal repository target:
    ecom/shopify_create/7_4_2_spu_product_prepare.py
Import path:
    shopify_create.7_4_2_spu_product_prepare

Scope
-----
- Read ``Input`` as the authoritative output of 7.4.1. Input is read-only here.
- Read ``Defaults``, ``Cfg__Fields``, ``Cfg__Locations`` and
  ``V_V_Handle`` using the existing Console Core routing/auth boundary.
- Preserve CREATE and ADD as distinct actions.
- CREATE Product group identity = SPU-V.
- ADD Product group identity = sys.target_product_id.
- Keep sys.product_key as Variant Base / source business identity; it is NOT the
  Shopify Product group key for SPU Creation.
- Apply all Defaults to CREATE rows; apply only VARIANT/INVENTORY Defaults to
  ADD rows because ADD only adds Variants and never mutates Product-level data.
- Require Product ``Size`` from 7.4.1 to resolve as a PRODUCT field and let
  Product-group consistency validation protect that aggregate value.
- Validate Product-group consistency, Variant uniqueness, ADD existing Product
  identity, CREATE Handle non-existence, ADD SKU non-existence, SPU Variant
  relationships and Price reconciliation.
- Propagate any row-level blocking error to its whole Product group while
  allowing unrelated Product groups to remain READY.
- Overwrite ``Preview`` with a two-row machine-readable plan.
- ``Result`` and ``Input`` are untouched. This module never calls Shopify.

Overall status
--------------
- SUCCESS: all Product groups are READY.
- SUCCESS_WITH_ERRORS: at least one Product group is READY and at least one is
  ERROR.
- FAILED_VALIDATION: no Product group is READY.

Infrastructure/schema failures raise explicitly. Business-data validation is
written to Preview instead of stopping unrelated Product groups.
"""
from __future__ import annotations

import argparse
import importlib
import json
import re
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


MODULE_VERSION = "2026-08-05-v-v-handle-product-size-v2"
MODULE_PATH = "shopify_create.7_4_2_spu_product_prepare"
DEFAULT_JOB_NAME = "spu_product_prepare"

INFRA_MODULE_PATH = "shopify_create.7_1_1_generic_product_prepare"
EXPECTED_INFRA_MODULE_VERSION = "2026-08-02-runtime-boundary-v1"

SPU_SYSTEM_FIELD_DEFINITIONS: Dict[str, Dict[str, str]] = {
    "sys.target_product_id": {
        "display_name": "Target Product ID",
        "scope": "SYSTEM",
        "data_type": "string",
    },
    "sys.input_error": {
        "display_name": "Input Error",
        "scope": "SYSTEM",
        "data_type": "string",
    },
    "sys.source_title": {
        "display_name": "SKU Title",
        "scope": "SYSTEM",
        "data_type": "string",
    },
}

PREVIEW_SYSTEM_FIELDS: List[Tuple[str, str]] = [
    ("Plan Status", "sys.plan_status"),
    ("Source Row", "sys.source_row"),
    ("Product Group Key", "sys.product_group_key"),
    ("Error Count", "sys.error_count"),
    ("Warning Count", "sys.warning_count"),
    ("Validation Messages", "sys.validation_messages"),
    ("Defaulted Fields", "sys.defaulted_fields"),
    ("Inherited Fields", "sys.inherited_fields"),
    ("Product Variant Count", "sys.product_variant_count"),
]

REQUIRED_INPUT_KEYS = {
    "sys.action",
    "sys.product_key",
    "sys.variant_key",
    "sys.target_product_id",
    "sys.input_error",
    "core.title",
    "core.handle",
    "core.option1_name",
    "core.option1_value",
    "core.option2_name",
    "core.option2_value",
    "core.sku",
    "core.price",
    "inventory.location_code",
    "inventory.quantity",
}

CREATE_REQUIRED_PRODUCT_FIELDS = {
    "core.title",
    "core.handle",
    "core.option1_name",
    "core.option2_name",
}

REQUIRED_DISPLAY_NAMES = {
    "Size",
    "SPU-V",
    "Variant Base",
    "SKU Suffix-V",
    "Size-V",
    "Unit Count-V",
    "Settlement Quantity-V",
    "Multiplier-V",
    "SKU Unit Price-V",
    "Max Quantity-V",
    "SKU Group",
}

SUFFIX_TO_QUANTITY = {
    "Nr01": 1,
    "Nr02": 2,
    "Nr05": 5,
    "Nr10": 10,
    "Nr20": 20,
    "Nr30": 30,
}

PRODUCT_HANDLE_REQUIRED_HEADERS = [
    "SKU",
    "Product ID (numeric)",
    "Variant ID (numeric)",
    "Product Title",
    "Variant Base",
    "Product Handle",
]


def _infra():
    module = importlib.import_module(INFRA_MODULE_PATH)
    loaded_version = _safe_str(getattr(module, "MODULE_VERSION", ""))
    if loaded_version != EXPECTED_INFRA_MODULE_VERSION:
        raise RuntimeError(
            "SPU Prepare requires the validated Generic Prepare runtime "
            f"infrastructure version {EXPECTED_INFRA_MODULE_VERSION}; "
            f"loaded={loaded_version or '<blank>'}."
        )
    return module


def read_secret(*args, **kwargs):
    return _infra().read_secret(*args, **kwargs)


def _build_gspread_client(*args, **kwargs):
    return _infra()._build_gspread_client(*args, **kwargs)


def _sheets_retry(*args, **kwargs):
    return _infra()._sheets_retry(*args, **kwargs)


def update_existing_notebook_registry_row(*args, **kwargs):
    return _infra().update_existing_notebook_registry_row(*args, **kwargs)


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    return str(value).strip()


def _normalize_header(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_str(value)).strip()


def _normalize_action(value: Any) -> str:
    return _safe_str(value).upper()


def _parse_decimal(value: Any, *, label: str) -> Decimal:
    text = _safe_str(value).replace(",", "")
    if not text:
        raise ValueError(f"{label} is blank.")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{label} is not a valid decimal: {value!r}") from exc


def _parse_integer(value: Any, *, label: str) -> int:
    number = _parse_decimal(value, label=label)
    if number != number.to_integral_value():
        raise ValueError(f"{label} must be an integer: {value!r}")
    return int(number)


def _money2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _field_definition(
    field_key: str,
    cfg_fields: Mapping[str, Any],
) -> Optional[Dict[str, str]]:
    key = _safe_str(field_key)
    if key in SPU_SYSTEM_FIELD_DEFINITIONS:
        definition = dict(SPU_SYSTEM_FIELD_DEFINITIONS[key])
        definition["field_id"] = f"SYSTEM|{key}"
        return definition
    return _infra()._field_definition(key, cfg_fields)


def _field_scope(field_key: str, cfg_fields: Mapping[str, Any]) -> str:
    definition = _field_definition(field_key, cfg_fields) or {}
    return _safe_str(definition.get("scope")).upper()


def _field_id(field_key: str, cfg_fields: Mapping[str, Any]) -> str:
    definition = _field_definition(field_key, cfg_fields) or {}
    return _safe_str(definition.get("field_id"))


def _validate_known_fields(
    field_keys: Iterable[str],
    cfg_fields: Mapping[str, Any],
) -> None:
    unknown: List[str] = []
    scope_errors: List[str] = []
    for field_key in field_keys:
        definition = _field_definition(field_key, cfg_fields)
        if definition is None:
            unknown.append(field_key)
            continue
        if definition.get("scope_error"):
            scope_errors.append(str(definition["scope_error"]))
    if unknown:
        raise ValueError(
            "Unrecognized Input/Defaults field_key values: "
            f"{sorted(set(unknown))}. Input is read-only in SPU Prepare; "
            "fix 7.4.1 / Cfg__Fields instead of backfilling here."
        )
    if scope_errors:
        raise ValueError("Field scope mismatch: " + " | ".join(scope_errors))


def _read_input_matrix_strict(
    values: Sequence[Sequence[Any]],
    cfg_fields: Mapping[str, Any],
) -> Dict[str, Any]:
    if len(values) < 2:
        raise ValueError(
            "Input requires two header rows: Row 1 display name and Row 2 field_key."
        )

    width = max(len(values[0]), len(values[1]))
    display_headers = [
        _normalize_header(values[0][i] if i < len(values[0]) else "")
        for i in range(width)
    ]
    field_keys = [
        _safe_str(values[1][i] if i < len(values[1]) else "")
        for i in range(width)
    ]
    while width and not display_headers[-1] and not field_keys[-1]:
        display_headers.pop()
        field_keys.pop()
        width -= 1

    if not display_headers:
        raise ValueError("Input Row 1 is empty.")
    if any(not header for header in display_headers):
        missing = [i + 1 for i, header in enumerate(display_headers) if not header]
        raise ValueError(f"Input has blank display-name headers at columns={missing}.")
    if any(not key for key in field_keys):
        missing = [i + 1 for i, key in enumerate(field_keys) if not key]
        raise ValueError(
            "Input Row 2 field_key is incomplete at columns="
            f"{missing}. SPU Prepare never writes Input Row 2."
        )

    duplicates = sorted({key for key in field_keys if field_keys.count(key) > 1})
    if duplicates:
        raise ValueError(f"Input has duplicate field_key values: {duplicates}")

    _validate_known_fields(field_keys, cfg_fields)

    missing_required_keys = sorted(REQUIRED_INPUT_KEYS - set(field_keys))
    if missing_required_keys:
        raise ValueError(
            "Input is missing required SPU Prepare field_key values: "
            f"{missing_required_keys}"
        )
    missing_displays = sorted(REQUIRED_DISPLAY_NAMES - set(display_headers))
    if missing_displays:
        raise ValueError(
            "Input is missing required SPU display columns: "
            f"{missing_displays}"
        )

    display_to_key: Dict[str, str] = {}
    for display, key in zip(display_headers, field_keys):
        if display in display_to_key and display_to_key[display] != key:
            raise ValueError(
                f"Input display name {display!r} maps to multiple field_keys."
            )
        display_to_key[display] = key

    product_size_key = display_to_key["Size"]
    if _field_scope(product_size_key, cfg_fields) != "PRODUCT":
        raise ValueError(
            "Input Size must resolve through Cfg__Fields as a PRODUCT field; "
            f"field_key={product_size_key!r}."
        )

    columns: List[Dict[str, Any]] = []
    mapping_records: List[Dict[str, str]] = []
    for index, (display, key) in enumerate(zip(display_headers, field_keys), start=1):
        definition = _field_definition(key, cfg_fields) or {}
        columns.append(
            {
                "display_name": display,
                "field_key": key,
                "column_number": index,
            }
        )
        mapping_records.append(
            {
                "column_number": str(index),
                "display_name": display,
                "field_key": key,
                "mapping_source": "EXPLICIT_ROW_2",
                "entity_type": _safe_str(definition.get("scope")),
                "field_id": _field_id(key, cfg_fields),
            }
        )

    rows: List[Dict[str, Any]] = []
    for source_row, raw_row in enumerate(values[2:], start=3):
        padded = list(raw_row) + [""] * max(0, width - len(raw_row))
        row_values = {
            field_keys[i]: _safe_str(padded[i] if i < len(padded) else "")
            for i in range(width)
        }
        if not any(row_values.values()):
            continue
        rows.append({"source_row": source_row, "values": row_values})

    return {
        "display_headers": display_headers,
        "field_keys": field_keys,
        "columns": columns,
        "rows": rows,
        "display_to_key": display_to_key,
        "mapping_records": mapping_records,
    }


def _read_product_handle(values: Sequence[Sequence[Any]]) -> Dict[str, Any]:
    if not values:
        raise ValueError("V_V_Handle is empty.")
    headers = [_normalize_header(v) for v in values[0]]
    header_map = {header: i for i, header in enumerate(headers) if header}
    missing = [h for h in PRODUCT_HANDLE_REQUIRED_HEADERS if h not in header_map]
    if missing:
        raise ValueError(f"V_V_Handle missing required columns: {missing}")

    records: List[Dict[str, str]] = []
    by_product_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    existing_skus: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    existing_handles: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for sheet_row, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        record = {
            header: _safe_str(padded[index])
            for header, index in header_map.items()
        }
        if not any(record.values()):
            continue
        record["source_row"] = str(sheet_row)
        records.append(record)

        product_id = record.get("Product ID (numeric)", "")
        if product_id:
            by_product_id[product_id].append(record)
        sku = record.get("SKU", "")
        if sku:
            existing_skus[sku.casefold()].append(record)
        handle = record.get("Product Handle", "")
        if handle:
            existing_handles[handle.casefold()].append(record)

    return {
        "records": records,
        "by_product_id": dict(by_product_id),
        "existing_skus": dict(existing_skus),
        "existing_handles": dict(existing_handles),
    }


def _add_issue(
    row_state: Dict[str, Any],
    level: str,
    code: str,
    message: str,
) -> None:
    issue = {
        "level": _safe_str(level).upper(),
        "code": _safe_str(code),
        "message": _safe_str(message),
    }
    target = row_state["errors"] if issue["level"] == "ERROR" else row_state["warnings"]
    if issue not in target:
        target.append(issue)


def _evaluate_default_for_action(
    *,
    action: str,
    field_key: str,
    spec: Any,
    values: Mapping[str, str],
    locations: Mapping[str, Any],
    cfg_fields: Mapping[str, Any],
) -> Optional[str]:
    scope = _field_scope(field_key, cfg_fields)
    if action == "ADD" and scope not in {"VARIANT", "INVENTORY"}:
        return None
    return _infra()._evaluate_default(spec, values, locations)


def _validate_typed_value(
    field_key: str,
    value: str,
    cfg_fields: Mapping[str, Any],
) -> None:
    if not _safe_str(value):
        return
    if field_key.startswith("sys."):
        return
    _infra()._normalize_typed_value(field_key, value, cfg_fields)


def _unique_nonblank(rows: Sequence[Dict[str, Any]], field_key: str) -> List[str]:
    return sorted(
        {
            _safe_str(row["values"].get(field_key))
            for row in rows
            if _safe_str(row["values"].get(field_key))
        }
    )


def _row_field_by_display(
    row_state: Mapping[str, Any],
    display_to_key: Mapping[str, str],
    display_name: str,
) -> str:
    key = display_to_key[display_name]
    return _safe_str(row_state["values"].get(key))


def _group_issue(
    rows: Sequence[Dict[str, Any]],
    code: str,
    message: str,
) -> None:
    for row in rows:
        _add_issue(row, "ERROR", code, message)


def _validate_spu_variant_relationships(
    row_state: Dict[str, Any],
    display_to_key: Mapping[str, str],
) -> None:
    values = row_state["values"]
    variant_key = _safe_str(values.get("sys.variant_key"))
    sku = _safe_str(values.get("core.sku"))
    if variant_key and sku and variant_key != sku:
        _add_issue(
            row_state,
            "ERROR",
            "VARIANT_KEY_SKU_MISMATCH",
            f"sys.variant_key={variant_key!r} must equal core.sku={sku!r}.",
        )

    suffix = _row_field_by_display(row_state, display_to_key, "SKU Suffix-V")
    option2 = _safe_str(values.get("core.option2_value"))
    unit_count = _row_field_by_display(row_state, display_to_key, "Unit Count-V")
    settlement = _row_field_by_display(row_state, display_to_key, "Settlement Quantity-V")
    multiplier = _row_field_by_display(row_state, display_to_key, "Multiplier-V")
    unit_price = _row_field_by_display(row_state, display_to_key, "SKU Unit Price-V")
    max_quantity = _row_field_by_display(row_state, display_to_key, "Max Quantity-V")
    size_v = _row_field_by_display(row_state, display_to_key, "Size-V")
    option1 = _safe_str(values.get("core.option1_value"))
    option1_name = _safe_str(values.get("core.option1_name"))
    option2_name = _safe_str(values.get("core.option2_name"))
    option3_name = _safe_str(values.get("core.option3_name"))
    option3_value = _safe_str(values.get("core.option3_value"))
    sku_group = _row_field_by_display(row_state, display_to_key, "SKU Group")

    if option1_name != "Size":
        _add_issue(
            row_state,
            "ERROR",
            "OPTION1_NAME_MISMATCH",
            f"Option 1 Name must be 'Size'; got={option1_name!r}.",
        )
    if option2_name != "Quantity":
        _add_issue(
            row_state,
            "ERROR",
            "OPTION2_NAME_MISMATCH",
            f"Option 2 Name must be 'Quantity'; got={option2_name!r}.",
        )
    if option3_name or option3_value:
        _add_issue(
            row_state,
            "ERROR",
            "OPTION3_NOT_BLANK",
            "SPU Creation uses only Size + Quantity; Option 3 Name/Value must be blank.",
        )

    if suffix:
        expected_quantity = SUFFIX_TO_QUANTITY.get(suffix)
        if expected_quantity is None:
            _add_issue(
                row_state,
                "ERROR",
                "INVALID_SKU_SUFFIX",
                f"SKU Suffix-V={suffix!r} is not a legal SPU suffix.",
            )
        else:
            if variant_key and not variant_key.endswith("-" + suffix):
                _add_issue(
                    row_state,
                    "ERROR",
                    "VARIANT_KEY_SUFFIX_MISMATCH",
                    f"Variant Key {variant_key!r} does not end with -{suffix}.",
                )
            for label, raw in [
                ("Option 2 Value", option2),
                ("Unit Count-V", unit_count),
                ("Settlement Quantity-V", settlement),
            ]:
                try:
                    actual = _parse_integer(raw, label=label)
                except ValueError as exc:
                    _add_issue(row_state, "ERROR", "INVALID_QUANTITY_VALUE", str(exc))
                    continue
                if actual != expected_quantity:
                    _add_issue(
                        row_state,
                        "ERROR",
                        "SUFFIX_QUANTITY_MISMATCH",
                        f"{label}={actual} does not match {suffix} -> {expected_quantity}.",
                    )

    if option1 != size_v:
        _add_issue(
            row_state,
            "ERROR",
            "SIZE_OPTION_MISMATCH",
            f"Option 1 Value={option1!r} must equal Size-V={size_v!r}.",
        )

    try:
        max_qty_int = _parse_integer(max_quantity, label="Max Quantity-V")
        option2_int = _parse_integer(option2, label="Option 2 Value")
        if max_qty_int not in {10, 30}:
            _add_issue(
                row_state,
                "ERROR",
                "INVALID_MAX_QUANTITY",
                f"Max Quantity-V must be 10 or 30; got={max_qty_int}.",
            )
        expected_multiplier = Decimal("0.95") if option2_int == max_qty_int else Decimal("1")
        actual_multiplier = _parse_decimal(multiplier, label="Multiplier-V")
        if actual_multiplier != expected_multiplier:
            _add_issue(
                row_state,
                "ERROR",
                "MULTIPLIER_RECONCILIATION_FAILED",
                f"Multiplier-V={actual_multiplier} but expected={expected_multiplier} "
                f"for quantity={option2_int}, max={max_qty_int}.",
            )
    except ValueError as exc:
        _add_issue(row_state, "ERROR", "INVALID_SPQ_QUANTITY_MODEL", str(exc))

    try:
        input_price = _money2(_parse_decimal(values.get("core.price"), label="Price"))
        unit_price_dec = _parse_decimal(unit_price, label="SKU Unit Price-V")
        unit_count_int = _parse_integer(unit_count, label="Unit Count-V")
        multiplier_dec = _parse_decimal(multiplier, label="Multiplier-V")
        expected_price = _money2(unit_price_dec * Decimal(unit_count_int) * multiplier_dec)
        if input_price != expected_price:
            _add_issue(
                row_state,
                "ERROR",
                "PRICE_RECONCILIATION_FAILED",
                f"Price={input_price} but expected={expected_price} from "
                "SKU Unit Price-V × Unit Count-V × Multiplier-V.",
            )
    except ValueError as exc:
        _add_issue(row_state, "ERROR", "PRICE_RECONCILIATION_FAILED", str(exc))

    if sku_group and sku_group != "1":
        _add_issue(
            row_state,
            "ERROR",
            "SKU_GROUP_MISMATCH",
            f"SKU Group must be 1 for SPU Creation; got={sku_group!r}.",
        )


def _build_prepare_plan(
    *,
    input_contract: Mapping[str, Any],
    defaults: Mapping[str, Any],
    cfg_fields: Mapping[str, Any],
    locations: Mapping[str, Any],
    product_handle: Mapping[str, Any],
) -> Dict[str, Any]:
    input_field_keys = list(input_contract["field_keys"])
    display_to_key = dict(input_contract["display_to_key"])
    spu_v_key = display_to_key["SPU-V"]
    product_size_key = display_to_key["Size"]

    all_field_keys = list(input_field_keys)
    for field_key in defaults:
        if field_key not in all_field_keys:
            all_field_keys.append(field_key)
    _validate_known_fields(all_field_keys, cfg_fields)

    row_states: List[Dict[str, Any]] = []
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for raw_row in input_contract["rows"]:
        values = {key: _safe_str(value) for key, value in raw_row["values"].items()}
        action = _normalize_action(values.get("sys.action"))
        row_state = {
            "source_row": int(raw_row["source_row"]),
            "values": values,
            "action": action,
            "product_group_key": "",
            "errors": [],
            "warnings": [],
            "defaulted_fields": [],
            "inherited_fields": [],
            "status": "",
        }
        row_states.append(row_state)

        if action == "CREATE":
            spu_v = _safe_str(values.get(spu_v_key))
            if not spu_v:
                _add_issue(
                    row_state,
                    "ERROR",
                    "MISSING_SPU_V",
                    "CREATE row requires SPU-V to determine the Product group.",
                )
                group_key = f"INVALID_CREATE_ROW_{row_state['source_row']}"
            else:
                group_key = spu_v
            if _safe_str(values.get("sys.target_product_id")):
                _add_issue(
                    row_state,
                    "ERROR",
                    "CREATE_TARGET_PRODUCT_ID_NOT_BLANK",
                    "CREATE row must have blank sys.target_product_id.",
                )
        elif action == "ADD":
            target_id = _safe_str(values.get("sys.target_product_id"))
            if not target_id:
                _add_issue(
                    row_state,
                    "ERROR",
                    "MISSING_TARGET_PRODUCT_ID",
                    "ADD row requires sys.target_product_id.",
                )
                group_key = f"INVALID_ADD_ROW_{row_state['source_row']}"
            else:
                group_key = target_id
        else:
            _add_issue(
                row_state,
                "ERROR",
                "INVALID_ACTION",
                f"SPU Prepare accepts only CREATE or ADD; got={action!r}.",
            )
            group_key = f"INVALID_ACTION_ROW_{row_state['source_row']}"

        row_state["product_group_key"] = group_key
        groups[group_key].append(row_state)

    # Defaults: CREATE gets full Generic defaults; ADD gets Variant/Inventory only.
    for row_state in row_states:
        action = row_state["action"]
        values = row_state["values"]
        for field_key, spec in defaults.items():
            if _safe_str(values.get(field_key)):
                continue
            default_value = _evaluate_default_for_action(
                action=action,
                field_key=field_key,
                spec=spec,
                values=values,
                locations=locations,
                cfg_fields=cfg_fields,
            )
            if default_value is None:
                continue
            if _safe_str(default_value):
                try:
                    normalized = _infra()._normalize_typed_value(
                        field_key,
                        _safe_str(default_value),
                        cfg_fields,
                    )
                except (ValueError, TypeError) as exc:
                    _add_issue(
                        row_state,
                        "ERROR",
                        "INVALID_DEFAULT_VALUE",
                        f"Default for {field_key} is invalid: {exc}",
                    )
                    normalized = _safe_str(default_value)
                values[field_key] = normalized
                row_state["defaulted_fields"].append(field_key)

        for field_key in all_field_keys:
            raw = _safe_str(values.get(field_key))
            if not raw:
                continue
            try:
                _validate_typed_value(field_key, raw, cfg_fields)
            except (ValueError, TypeError) as exc:
                _add_issue(
                    row_state,
                    "ERROR",
                    "INVALID_FIELD_VALUE",
                    f"{field_key}: {exc}",
                )

        if _safe_str(values.get("sys.input_error")):
            _add_issue(
                row_state,
                "ERROR",
                "UPSTREAM_INPUT_ERROR",
                _safe_str(values.get("sys.input_error")),
            )

        for required_key in {
            "sys.product_key",
            "sys.variant_key",
            "core.sku",
            "core.price",
            "core.option2_value",
            "inventory.location_code",
            "inventory.quantity",
        }:
            if not _safe_str(values.get(required_key)):
                _add_issue(
                    row_state,
                    "ERROR",
                    "MISSING_REQUIRED_VALUE",
                    f"Required field {required_key} is blank.",
                )

        _validate_spu_variant_relationships(row_state, display_to_key)

    # Global Variant Key / SKU uniqueness inside this Input.
    for field_key, code in [
        ("sys.variant_key", "DUPLICATE_VARIANT_KEY"),
        ("core.sku", "DUPLICATE_SKU_IN_INPUT"),
    ]:
        by_value: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in row_states:
            value = _safe_str(row["values"].get(field_key))
            if value:
                by_value[value.casefold()].append(row)
        for normalized, rows in by_value.items():
            if len(rows) <= 1:
                continue
            displayed = sorted({_safe_str(r["values"].get(field_key)) for r in rows})
            message = (
                f"{field_key} must be unique in Input; values={displayed}; "
                f"source_rows={[r['source_row'] for r in rows]}."
            )
            for row in rows:
                _add_issue(row, "ERROR", code, message)

    # CREATE Handle may be shared by Variants in one Product group, but not by
    # different CREATE groups and not with an existing Shopify Product.
    create_groups_by_handle: Dict[str, List[str]] = defaultdict(list)
    for group_key, rows in groups.items():
        actions = {row["action"] for row in rows}
        if actions == {"CREATE"}:
            handles = _unique_nonblank(rows, "core.handle")
            if len(handles) == 1:
                create_groups_by_handle[handles[0].casefold()].append(group_key)
    for normalized_handle, group_keys in create_groups_by_handle.items():
        if len(set(group_keys)) > 1:
            for group_key in set(group_keys):
                _group_issue(
                    groups[group_key],
                    "CREATE_HANDLE_REUSED_ACROSS_GROUPS",
                    f"CREATE handle {normalized_handle!r} is used by multiple "
                    f"SPU Product groups={sorted(set(group_keys))}.",
                )

    existing_handles = product_handle["existing_handles"]
    existing_skus = product_handle["existing_skus"]
    by_product_id = product_handle["by_product_id"]

    for group_key, rows in groups.items():
        actions = {row["action"] for row in rows}
        if len(actions) != 1:
            _group_issue(
                rows,
                "MIXED_ACTION_PRODUCT_GROUP",
                f"Product group {group_key!r} has mixed actions={sorted(actions)}.",
            )
            continue
        action = next(iter(actions))
        if action not in {"CREATE", "ADD"}:
            continue

        # PRODUCT Size is generated once per SPU group in 7.4.1 and repeated
        # on Variant-grain Input rows. It must remain one consistent value for
        # both CREATE and ADD. Blank is allowed only when every new Size-V is blank.
        product_size_values = _unique_nonblank(rows, product_size_key)
        if len(product_size_values) > 1:
            _group_issue(
                rows,
                "PRODUCT_SIZE_CONFLICT",
                f"Product group {group_key!r} has conflicting Product Size "
                f"values={product_size_values}.",
            )
        any_size_v = any(
            _row_field_by_display(row, display_to_key, "Size-V")
            for row in rows
        )
        if any_size_v and not product_size_values:
            _group_issue(
                rows,
                "PRODUCT_SIZE_MISSING",
                f"Product group {group_key!r} has nonblank Size-V rows but "
                "PRODUCT Size is blank.",
            )

        # Option combinations must be unique inside a Product group.
        by_options: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            values = row["values"]
            combo = (
                _safe_str(values.get("core.option1_value")),
                _safe_str(values.get("core.option2_value")),
                _safe_str(values.get("core.option3_value")),
            )
            by_options[combo].append(row)
        for combo, combo_rows in by_options.items():
            if len(combo_rows) > 1:
                _group_issue(
                    rows,
                    "DUPLICATE_OPTION_COMBINATION",
                    f"Product group {group_key!r} has duplicate Option values "
                    f"{combo!r} at source_rows="
                    f"{[r['source_row'] for r in combo_rows]}.",
                )
                break

        if action == "CREATE":
            if _safe_str(group_key).startswith("INVALID_"):
                continue
            target_ids = _unique_nonblank(rows, "sys.target_product_id")
            if target_ids:
                _group_issue(
                    rows,
                    "CREATE_TARGET_PRODUCT_ID_NOT_BLANK",
                    f"CREATE Product group {group_key!r} must not contain Target Product ID.",
                )

            # Product-level fields must resolve to at most one nonblank value.
            product_fields = [
                key
                for key in all_field_keys
                if _field_scope(key, cfg_fields) == "PRODUCT"
            ]
            for field_key in product_fields:
                values = _unique_nonblank(rows, field_key)
                if len(values) > 1:
                    _group_issue(
                        rows,
                        "PRODUCT_FIELD_CONFLICT",
                        f"CREATE Product group {group_key!r} has conflicting "
                        f"{field_key} values={values}.",
                    )
            for required in CREATE_REQUIRED_PRODUCT_FIELDS:
                if not _unique_nonblank(rows, required):
                    _group_issue(
                        rows,
                        "MISSING_CREATE_PRODUCT_FIELD",
                        f"CREATE Product group {group_key!r} requires {required}.",
                    )

            handles = _unique_nonblank(rows, "core.handle")
            if len(handles) > 1:
                _group_issue(
                    rows,
                    "PRODUCT_FIELD_CONFLICT",
                    f"CREATE Product group {group_key!r} has multiple Handles={handles}.",
                )
            elif len(handles) == 1:
                existing = existing_handles.get(handles[0].casefold(), [])
                if existing:
                    product_ids = sorted(
                        {
                            _safe_str(item.get("Product ID (numeric)"))
                            for item in existing
                            if _safe_str(item.get("Product ID (numeric)"))
                        }
                    )
                    _group_issue(
                        rows,
                        "CREATE_HANDLE_ALREADY_EXISTS",
                        f"CREATE Handle {handles[0]!r} already exists in "
                        f"V_V_Handle; product_ids={product_ids}.",
                    )

        elif action == "ADD":
            target_ids = _unique_nonblank(rows, "sys.target_product_id")
            if len(target_ids) != 1:
                _group_issue(
                    rows,
                    "ADD_TARGET_PRODUCT_ID_INVALID",
                    f"ADD Product group must have exactly one Target Product ID; "
                    f"found={target_ids}.",
                )
                continue
            target_id = target_ids[0]

            spu_values = _unique_nonblank(rows, spu_v_key)
            if len(spu_values) != 1:
                _group_issue(
                    rows,
                    "ADD_SPU_V_INVALID",
                    f"ADD Target Product ID {target_id} must map to exactly one "
                    f"SPU-V; found={spu_values}.",
                )

            existing_rows = by_product_id.get(target_id, [])
            if not existing_rows:
                _group_issue(
                    rows,
                    "TARGET_PRODUCT_NOT_FOUND",
                    f"Target Product ID {target_id} was not found in V_V_Handle.",
                )
            else:
                existing_titles = sorted(
                    {
                        _safe_str(item.get("Product Title"))
                        for item in existing_rows
                        if _safe_str(item.get("Product Title"))
                    }
                )
                existing_handles_for_product = sorted(
                    {
                        _safe_str(item.get("Product Handle"))
                        for item in existing_rows
                        if _safe_str(item.get("Product Handle"))
                    }
                )
                if len(existing_titles) != 1 or len(existing_handles_for_product) != 1:
                    _group_issue(
                        rows,
                        "TARGET_PRODUCT_IDENTITY_AMBIGUOUS",
                        f"Target Product ID {target_id} does not resolve to one "
                        f"Title/Handle; titles={existing_titles}; "
                        f"handles={existing_handles_for_product}.",
                    )
                else:
                    input_titles = _unique_nonblank(rows, "core.title")
                    input_handles = _unique_nonblank(rows, "core.handle")
                    if input_titles != existing_titles:
                        _group_issue(
                            rows,
                            "ADD_TITLE_MISMATCH",
                            f"ADD Input Title={input_titles} does not match existing "
                            f"Product Title={existing_titles} for target={target_id}.",
                        )
                    if input_handles != existing_handles_for_product:
                        _group_issue(
                            rows,
                            "ADD_HANDLE_MISMATCH",
                            f"ADD Input Handle={input_handles} does not match existing "
                            f"Product Handle={existing_handles_for_product} for target={target_id}.",
                        )

            duplicate_existing_skus = sorted(
                {
                    _safe_str(row["values"].get("core.sku"))
                    for row in rows
                    if _safe_str(row["values"].get("core.sku"))
                    and _safe_str(row["values"].get("core.sku")).casefold()
                    in existing_skus
                }
            )
            if duplicate_existing_skus:
                _group_issue(
                    rows,
                    "ADD_SKU_ALREADY_EXISTS",
                    f"ADD Product group {target_id} contains SKU(s) already present "
                    f"in V_V_Handle: {duplicate_existing_skus}.",
                )

    # A blocking issue on any row blocks the whole Product group, but not other groups.
    group_status: Dict[str, str] = {}
    for group_key, rows in groups.items():
        has_error = any(row["errors"] for row in rows)
        group_status[group_key] = "ERROR" if has_error else "READY"
        if has_error:
            blocking_codes = sorted(
                {
                    issue["code"]
                    for row in rows
                    for issue in row["errors"]
                    if issue["code"] != "PRODUCT_GROUP_ERROR"
                }
            )
            for row in rows:
                if not row["errors"]:
                    _add_issue(
                        row,
                        "ERROR",
                        "PRODUCT_GROUP_ERROR",
                        f"Product group {group_key!r} is blocked by another row; "
                        f"blocking_codes={blocking_codes}.",
                    )
        for row in rows:
            row["status"] = group_status[group_key]

    display_by_key = {
        column["field_key"]: column["display_name"]
        for column in input_contract["columns"]
    }
    for field_key, spec in defaults.items():
        display_by_key.setdefault(
            field_key,
            _safe_str(getattr(spec, "display_name", ""))
            or _safe_str((_field_definition(field_key, cfg_fields) or {}).get("display_name"))
            or field_key,
        )
    ordered_field_keys = list(input_field_keys)
    for field_key in all_field_keys:
        if field_key not in ordered_field_keys:
            ordered_field_keys.append(field_key)

    preview_display_headers = [item[0] for item in PREVIEW_SYSTEM_FIELDS] + [
        display_by_key.get(key, key) for key in ordered_field_keys
    ]
    preview_field_keys = [item[1] for item in PREVIEW_SYSTEM_FIELDS] + ordered_field_keys

    preview_rows: List[List[str]] = []
    preview_records: List[Dict[str, str]] = []
    for row in row_states:
        group_key = row["product_group_key"]
        messages = row["errors"] + row["warnings"]
        system_values = [
            row["status"],
            str(row["source_row"]),
            group_key,
            str(len(row["errors"])),
            str(len(row["warnings"])),
            json.dumps(messages, ensure_ascii=False, separators=(",", ":")),
            ";".join(sorted(set(row["defaulted_fields"]))),
            ";".join(sorted(set(row["inherited_fields"]))),
            str(len(groups[group_key])),
        ]
        data_values = [_safe_str(row["values"].get(key)) for key in ordered_field_keys]
        preview_rows.append(system_values + data_values)
        preview_records.append(
            dict(zip(preview_field_keys, system_values + data_values))
        )

    ready_groups = sorted(key for key, status in group_status.items() if status == "READY")
    error_groups = sorted(key for key, status in group_status.items() if status == "ERROR")
    ready_rows = sum(1 for row in row_states if row["status"] == "READY")
    error_rows = sum(1 for row in row_states if row["status"] == "ERROR")
    error_count = sum(len(row["errors"]) for row in row_states)
    warning_count = sum(len(row["warnings"]) for row in row_states)

    if not error_groups:
        status = "SUCCESS"
    elif ready_groups:
        status = "SUCCESS_WITH_ERRORS"
    else:
        status = "FAILED_VALIDATION"

    errors = [
        {
            "source_row": row["source_row"],
            "product_group_key": row["product_group_key"],
            **issue,
        }
        for row in row_states
        for issue in row["errors"]
    ]
    warnings = [
        {
            "source_row": row["source_row"],
            "product_group_key": row["product_group_key"],
            **issue,
        }
        for row in row_states
        for issue in row["warnings"]
    ]

    stats = {
        "rows_loaded": len(row_states),
        "rows_pending": len(row_states),
        "rows_recognized": len(row_states),
        "rows_planned": ready_rows,
        "rows_written": 0,
        "rows_skipped": 0,
        "rows_ready": ready_rows,
        "rows_error": error_rows,
        "product_groups": len(groups),
        "product_groups_ready": len(ready_groups),
        "product_groups_error": len(error_groups),
        "create_groups": sum(
            1 for rows in groups.values() if {r["action"] for r in rows} == {"CREATE"}
        ),
        "add_groups": sum(
            1 for rows in groups.values() if {r["action"] for r in rows} == {"ADD"}
        ),
        "warning_count": warning_count,
        "error_count": error_count,
        "business_objects_planned": len(ready_groups),
        "api_operations_planned": 0,
    }

    return {
        "status": status,
        "ready_for_apply": bool(ready_groups),
        "stats": stats,
        "group_status": group_status,
        "ready_groups": ready_groups,
        "error_groups": error_groups,
        "errors": errors,
        "warnings": warnings,
        "preview_matrix": [preview_display_headers, preview_field_keys] + preview_rows,
        "preview_records": preview_records,
        "ordered_field_keys": ordered_field_keys,
    }


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    tab_cfg_locations: str = "Cfg__Locations",
    config_sheet_label: str = "config",
    create_sheet_label: str = "create_spu",
    runlog_sheet_label: str = "runlog_sheet",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_input: str = "Input",
    tab_defaults: str = "Defaults",
    tab_product_handle: str = "V_V_Handle",
    tab_preview: str = "Preview",
    tab_result: str = "Result",
    tab_runlog: str = "Ops__RunLog",
    write_preview: bool = True,
    preview_rows: int = 50,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    sa_b64_value: Optional[str] = None,
) -> Dict[str, Any]:
    gp = _infra()
    site_code = gp._normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not _safe_str(console_core_url):
        raise ValueError("console_core_url is required.")
    if not _safe_str(bootstrap_gsheet_sa_b64_secret):
        raise ValueError("bootstrap_gsheet_sa_b64_secret is required.")

    run_id = run_id or gp._make_run_id(job_name, tz_name)
    phase = "prepare"
    started = time.monotonic()

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 10, f"Resolve Google access | site={site_code}")
    secret = read_secret(
        bootstrap_gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, auth_meta = _build_gspread_client(secret)
    console = _sheets_retry("open Console Core", lambda: gc.open_by_url(console_core_url))

    account = gp._load_account_values(console, tab_cfg_account_id)
    configured_secret = _safe_str(account.get("GSHEET_SA_B64_SECRET"))
    if configured_secret and configured_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "Bootstrap Google Secret does not match Cfg__account_id. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}; cfg={configured_secret}"
        )

    progress(
        2,
        10,
        "Resolve routed workbooks | "
        f"create={create_sheet_label} | config={config_sheet_label}",
    )
    create_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, create_sheet_label
    )
    config_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, config_sheet_label
    )
    runlog_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, runlog_sheet_label
    )
    create_book = _sheets_retry("open create_spu workbook", lambda: gc.open_by_url(create_url))
    config_book = _sheets_retry("open config workbook", lambda: gc.open_by_url(config_url))
    runlog_ws = _sheets_retry(
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
        progress(3, 10, f"Read Config field dictionary | tab={tab_cfg_fields}")
        cfg_fields_ws = gp._require_worksheet(config_book, tab_cfg_fields)
        cfg_fields = gp._read_cfg_fields(
            _sheets_retry(f"read {tab_cfg_fields}", cfg_fields_ws.get_all_values)
        )
        print(
            "[Cfg__Fields] "
            f"records={cfg_fields['stats']['records']} | "
            f"unique_field_ids={cfg_fields['stats']['unique_field_ids']} | "
            f"distinct_field_keys={cfg_fields['stats']['distinct_field_keys']} | "
            f"repeated_field_keys={cfg_fields['stats']['repeated_field_keys']}"
        )

        progress(4, 10, f"Read Input strictly | tab={tab_input} | read_only=True")
        input_ws = gp._require_worksheet(create_book, tab_input)
        input_contract = _read_input_matrix_strict(
            _sheets_retry(f"read {tab_input}", input_ws.get_all_values),
            cfg_fields,
        )
        if not input_contract["rows"]:
            raise ValueError("Input contains no data rows.")
        print(
            "[Input] "
            f"columns={len(input_contract['field_keys'])} | "
            f"rows={len(input_contract['rows'])} | "
            "row2_mapping=EXPLICIT_ONLY | write_back=False"
        )

        progress(5, 10, f"Read Defaults and default Location | tabs={tab_defaults}, {tab_cfg_locations}")
        defaults_ws = gp._require_worksheet(create_book, tab_defaults)
        defaults = gp._read_defaults_matrix(
            _sheets_retry(f"read {tab_defaults}", defaults_ws.get_all_values)
        )
        locations_ws = gp._require_worksheet(console, tab_cfg_locations)
        locations = gp._read_locations(
            _sheets_retry(f"read {tab_cfg_locations}", locations_ws.get_all_values),
            site_code,
        )
        default_location = locations["default"]
        print(
            "[Location] "
            f"code={default_location.get('location_code')} | "
            f"name={default_location.get('location_name')} | "
            f"gid={default_location.get('location_gid')}"
        )

        progress(6, 10, f"Read existing Shopify identity snapshot | tab={tab_product_handle}")
        product_handle_ws = gp._require_worksheet(create_book, tab_product_handle)
        product_handle = _read_product_handle(
            _sheets_retry(
                f"read {tab_product_handle}", product_handle_ws.get_all_values
            )
        )
        print(
            "[V_V_Handle] "
            f"rows={len(product_handle['records'])} | "
            f"products={len(product_handle['by_product_id'])} | "
            f"skus={len(product_handle['existing_skus'])}"
        )

        progress(7, 10, "Build SPU Product groups and apply action-scoped Defaults")
        plan = _build_prepare_plan(
            input_contract=input_contract,
            defaults=defaults,
            cfg_fields=cfg_fields,
            locations=locations,
            product_handle=product_handle,
        )
        stats = plan["stats"]
        print(
            "[Plan] "
            f"rows={stats['rows_loaded']} | ready_rows={stats['rows_ready']} | "
            f"error_rows={stats['rows_error']} | groups={stats['product_groups']} | "
            f"ready_groups={stats['product_groups_ready']} | "
            f"error_groups={stats['product_groups_error']} | "
            f"CREATE_groups={stats['create_groups']} | ADD_groups={stats['add_groups']}"
        )

        progress(8, 10, "Validate SPU relationships / existing Handle / existing ADD SKU")
        if plan["error_groups"]:
            print("[Validation] ERROR groups:", plan["error_groups"])
        else:
            print("[Validation] all Product groups READY")

        preview_rows_written = 0
        if write_preview:
            progress(9, 10, f"Overwrite Preview | tab={tab_preview}")
            preview_rows_written = gp._write_matrix_overwrite(
                create_book,
                tab_preview,
                plan["preview_matrix"],
            )
        else:
            progress(9, 10, "Preview write disabled; no Preview change")

        status = plan["status"]
        stats["rows_written"] = preview_rows_written
        logger.log(
            phase=phase,
            log_type="summary",
            status=status,
            entity_type="SPU_PRODUCT_CREATE",
            rows_loaded=stats["rows_loaded"],
            rows_pending=stats["rows_pending"],
            rows_recognized=stats["rows_recognized"],
            rows_planned=stats["rows_planned"],
            rows_written=preview_rows_written,
            rows_skipped=stats["rows_skipped"],
            message=(
                f"spu_prepare | ready_for_apply={plan['ready_for_apply']} | "
                f"product_groups={stats['product_groups']} | "
                f"ready_groups={stats['product_groups_ready']} | "
                f"error_groups={stats['product_groups_error']} | "
                f"ready_rows={stats['rows_ready']} | error_rows={stats['rows_error']} | "
                f"warnings={stats['warning_count']} | errors={stats['error_count']} | "
                f"preview_rows_written={preview_rows_written}"
            ),
            error_reason=(
                "NO_READY_PRODUCT_GROUP"
                if status == "FAILED_VALIDATION"
                else ("PARTIAL_PRODUCT_GROUP_ERRORS" if status == "SUCCESS_WITH_ERRORS" else "")
            ),
        )
        for issue in (plan["errors"] + plan["warnings"])[:30]:
            logger.log(
                phase=phase,
                log_type="detail",
                status=issue["level"],
                entity_type="SPU_PRODUCT_CREATE",
                gid=_safe_str(issue.get("product_group_key")),
                message=issue["message"],
                error_reason=issue["code"],
            )
        logger.flush()

        progress(10, 10, f"Done | status={status} | ready_for_apply={plan['ready_for_apply']}")
        elapsed = round(time.monotonic() - started, 2)
        mapping_df = pd.DataFrame(input_contract["mapping_records"])
        preview_df = pd.DataFrame(plan["preview_records"])
        preview_display = (
            preview_df.head(int(preview_rows))
            if int(preview_rows) > 0
            else preview_df
        )
        return {
            "status": status,
            "ready_for_apply": plan["ready_for_apply"],
            "job_name": job_name,
            "run_id": run_id,
            "module_version": MODULE_VERSION,
            "infra_module_path": INFRA_MODULE_PATH,
            "infra_module_version": EXPECTED_INFRA_MODULE_VERSION,
            "auth_type": auth_meta["source_type"],
            "summary": {
                **stats,
                "elapsed_seconds": elapsed,
                "preview_rows_written": preview_rows_written,
                "ready_groups": plan["ready_groups"],
                "error_groups": plan["error_groups"],
            },
            "default_location": dict(default_location),
            "field_mapping": mapping_df,
            "preview": preview_display,
            "errors": plan["errors"],
            "warnings": plan["warnings"],
        }
    except Exception as exc:
        try:
            logger.log(
                phase=phase,
                log_type="summary",
                status="FAILED",
                entity_type="SPU_PRODUCT_CREATE",
                message="SPU Prepare failed before completion.",
                error_reason=f"{type(exc).__name__}: {exc}",
            )
            logger.flush()
        except Exception as log_exc:
            print(
                "[RunLog warning] Could not write failure evidence: "
                f"{type(log_exc).__name__}: {log_exc}"
            )
        raise


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument("--gsheet-secret", required=True)
    parser.add_argument("--no-write-preview", action="store_true")
    args = parser.parse_args(argv)

    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=args.gsheet_secret,
        write_preview=not args.no_write_preview,
    )
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] in {"SUCCESS", "SUCCESS_WITH_ERRORS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
