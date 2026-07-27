# -*- coding: utf-8 -*-
"""Apply a validated Generic Shopify Product Creation plan.

GitHub target: ``ecom/shopify_create/generic_apply.py``
Import path: ``shopify_create.generic_apply``

Safety model
------------
1. Re-read Input, Defaults, Cfg__Fields and Cfg__Locations.
2. Rebuild the Prepare plan using ``shopify_create.generic_prepare``.
3. Compare the rebuilt plan with the existing Preview snapshot.
4. Query Shopify again for Handle conflicts only.
5. DRY_RUN produces a final execution plan without Shopify writes.
6. Live writes require ``dry_run=False`` and ``confirmed=True``.
7. Live mode requires explicit product selection unless
   ``apply_all_ready_products=True``.
8. Products are Draft by default. Non-Draft creation is blocked unless
   explicitly enabled.
9. Result and RunLog retain execution evidence.
10. Images/media are intentionally out of scope.

SKU and Barcode are not duplicate identities and do not participate in
conflict blocking. The module uses Shopify Admin GraphQL ``productSet``
synchronously to create
a Product with its options, variants, product/variant metafields, inventory
item attributes, and initial inventory quantities in one product operation.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import random
import re
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

import gspread
import pandas as pd
import requests
from zoneinfo import ZoneInfo

from shopify_create import generic_prepare as gp


MODULE_VERSION = "1.1.0"
MODULE_PATH = "shopify_create.generic_apply"
DEFAULT_JOB_NAME = "generic_create_apply"
EXPECTED_PREPARE_MODULE_VERSION = "1.3.0"

RESULT_HEADERS = [
    "run_id",
    "applied_at",
    "site_code",
    "runtime_mode",
    "dry_run",
    "product_key",
    "variant_key",
    "source_row",
    "apply_status",
    "product_gid",
    "product_handle",
    "product_title",
    "product_status",
    "variant_gid",
    "inventory_item_gid",
    "sku",
    "barcode",
    "option_values",
    "price",
    "compare_at_price",
    "cost",
    "inventory_location_code",
    "inventory_location_gid",
    "inventory_quantity",
    "product_metafields_planned",
    "variant_metafields_planned",
    "api_operations_planned",
    "api_operations_succeeded",
    "api_operations_failed",
    "message",
    "error_reason",
    "shopify_admin_url",
    "storefront_url",
]

Q_PRODUCT_BY_HANDLE = """
query ProductByHandle($identifier: ProductIdentifierInput!) {
  product: productByIdentifier(identifier: $identifier) {
    id
    handle
    title
    status
  }
}
"""

M_PRODUCT_SET = """
mutation GenericCreateProduct(
  $input: ProductSetInput!,
  $synchronous: Boolean!
) {
  productSet(input: $input, synchronous: $synchronous) {
    product {
      id
      handle
      title
      status
      options(first: 3) {
        name
        position
        optionValues {
          name
        }
      }
      variants(first: 250) {
        nodes {
          id
          sku
          barcode
          price
          compareAtPrice
          selectedOptions {
            name
            value
          }
          inventoryItem {
            id
            sku
            unitCost {
              amount
              currencyCode
            }
            measurement {
              weight {
                value
                unit
              }
            }
            inventoryLevels(first: 20) {
              nodes {
                location {
                  id
                  name
                }
                quantities(names: ["available"]) {
                  name
                  quantity
                }
              }
            }
          }
        }
      }
    }
    userErrors {
      code
      field
      message
    }
  }
}
"""


class ShopifyClient:
    def __init__(
        self,
        *,
        shop_domain: str,
        api_version: str,
        access_token: str,
        timeout_seconds: int = 120,
        max_retries: int = 6,
        print_progress: bool = True,
    ) -> None:
        self.shop_domain = gp._safe_str(shop_domain)
        self.api_version = gp._safe_str(api_version)
        self.graphql_url = (
            f"https://{self.shop_domain}/admin/api/"
            f"{self.api_version}/graphql.json"
        )
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        self.timeout_seconds = int(timeout_seconds)
        self.max_retries = int(max_retries)
        self.print_progress = bool(print_progress)
        self.request_count = 0
        self.retry_count = 0

    def gql(
        self,
        query: str,
        variables: Optional[Mapping[str, Any]] = None,
        *,
        operation_name: str,
    ) -> Dict[str, Any]:
        payload = {
            "query": query,
            "variables": dict(variables or {}),
        }
        last_error: Optional[BaseException] = None

        for attempt in range(1, self.max_retries + 1):
            self.request_count += 1
            try:
                response = requests.post(
                    self.graphql_url,
                    headers=self.headers,
                    json=payload,
                    timeout=self.timeout_seconds,
                )
                if response.status_code in {
                    429,
                    500,
                    502,
                    503,
                    504,
                }:
                    wait_seconds = min(2 ** (attempt - 1), 20)
                    wait_seconds += random.random()
                    self.retry_count += 1
                    if self.print_progress:
                        print(
                            "[Shopify retry] "
                            f"operation={operation_name} | "
                            f"attempt={attempt}/{self.max_retries} | "
                            f"HTTP={response.status_code} | "
                            f"wait={wait_seconds:.1f}s"
                        )
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
                body = response.json()
                if body.get("errors"):
                    raise RuntimeError(
                        f"{operation_name} GraphQL errors: "
                        f"{body['errors']}"
                    )
                data = body.get("data")
                if not isinstance(data, dict):
                    raise RuntimeError(
                        f"{operation_name} returned no GraphQL data."
                    )
                return data

            except (
                requests.Timeout,
                requests.ConnectionError,
                RuntimeError,
                ValueError,
            ) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                wait_seconds = min(2 ** (attempt - 1), 20)
                wait_seconds += random.random()
                self.retry_count += 1
                if self.print_progress:
                    print(
                        "[Shopify retry] "
                        f"operation={operation_name} | "
                        f"attempt={attempt}/{self.max_retries} | "
                        f"error={type(exc).__name__} | "
                        f"wait={wait_seconds:.1f}s"
                    )
                time.sleep(wait_seconds)

        raise RuntimeError(
            f"{operation_name} failed after "
            f"{self.max_retries} attempts: {last_error}"
        )


def _safe_list(values: Optional[Iterable[Any]]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values or []:
        text = gp._safe_str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _parse_tags(value: Any) -> List[str]:
    text = gp._safe_str(value)
    if not text:
        return []
    separator = ";" if ";" in text else ","
    return [
        item.strip()
        for item in text.split(separator)
        if item.strip()
    ]


def _parse_metafield_identity(field_key: str) -> Tuple[str, str]:
    text = gp._safe_str(field_key)
    if text.startswith("mf."):
        remainder = text[3:]
    elif text.startswith("v_mf."):
        remainder = text[5:]
    else:
        raise ValueError(
            f"Not a writable metafield field_key: {field_key!r}"
        )
    if "." not in remainder:
        raise ValueError(
            f"Metafield field_key must contain namespace and key: "
            f"{field_key!r}"
        )
    namespace, key = remainder.split(".", 1)
    if not namespace or not key:
        raise ValueError(
            f"Invalid metafield identity: {field_key!r}"
        )
    return namespace, key


def _canonical_metafield_value(
    *,
    raw_value: Any,
    data_type: str,
) -> str:
    raw = gp._safe_str(raw_value)
    type_name = gp._safe_str(data_type)
    if not raw:
        return ""

    lowered = type_name.lower()

    if lowered.startswith("list."):
        if raw.startswith("["):
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                raise ValueError(
                    f"{type_name} requires a JSON array."
                )
        else:
            parsed = [
                item.strip()
                for item in raw.split(";")
                if item.strip()
            ]
        return json.dumps(
            parsed,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    if lowered == "json":
        parsed = json.loads(raw)
        return json.dumps(
            parsed,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    if "boolean" in lowered:
        parsed = gp._normalize_bool(raw)
        if parsed is None:
            return ""
        return "true" if parsed else "false"

    if "number_integer" in lowered or lowered == "integer":
        return gp._integer_text(raw)

    if (
        "number_decimal" in lowered
        or lowered in {"decimal", "float"}
    ):
        return gp._decimal_text(raw)

    return raw


def _metafield_input(
    *,
    field_key: str,
    raw_value: Any,
    cfg_fields: Mapping[str, Any],
) -> Optional[Dict[str, str]]:
    value = gp._safe_str(raw_value)
    if not value:
        return None

    definition = gp._field_definition(
        field_key,
        cfg_fields,
    )
    if not definition:
        raise ValueError(
            f"Cannot resolve metafield definition: {field_key}"
        )
    data_type = gp._safe_str(definition.get("data_type"))
    if not data_type:
        raise ValueError(
            f"Cfg__Fields has no data_type for "
            f"{definition.get('field_id') or field_key}."
        )

    namespace, key = _parse_metafield_identity(field_key)
    canonical_value = _canonical_metafield_value(
        raw_value=value,
        data_type=data_type,
    )
    return {
        "namespace": namespace,
        "key": key,
        "type": data_type,
        "value": canonical_value,
    }


def _read_preview_records(
    values: Sequence[Sequence[Any]],
) -> Dict[str, Any]:
    if len(values) < 2:
        raise ValueError(
            "Preview requires two header rows. Run Prepare first."
        )

    max_cols = max(len(values[0]), len(values[1]))
    display_headers = [
        gp._safe_str(
            values[0][index]
            if index < len(values[0])
            else ""
        )
        for index in range(max_cols)
    ]
    field_keys = [
        gp._safe_str(
            values[1][index]
            if index < len(values[1])
            else ""
        )
        for index in range(max_cols)
    ]

    while max_cols and not display_headers[-1] and not field_keys[-1]:
        display_headers.pop()
        field_keys.pop()
        max_cols -= 1

    active_keys = [key for key in field_keys if key]
    duplicate_keys = sorted(
        key
        for key in set(active_keys)
        if active_keys.count(key) > 1
    )
    if duplicate_keys:
        raise ValueError(
            f"Preview contains duplicate field_key values: "
            f"{duplicate_keys}"
        )

    records: List[Dict[str, str]] = []
    for sheet_row, row in enumerate(values[2:], start=3):
        padded = list(row) + [""] * max(
            0,
            max_cols - len(row),
        )
        record = {
            field_keys[index]: gp._safe_str(padded[index])
            for index in range(max_cols)
            if field_keys[index]
        }
        if not any(record.values()):
            continue
        record["__preview_sheet_row"] = str(sheet_row)
        records.append(record)

    return {
        "field_keys": active_keys,
        "records": records,
    }


def _verify_preview_snapshot(
    *,
    prepare_plan: Mapping[str, Any],
    preview_contract: Mapping[str, Any],
) -> Dict[str, Any]:
    current_by_variant: Dict[str, Dict[str, str]] = {}
    for record in prepare_plan["preview_records"]:
        variant_key = gp._safe_str(
            record.get("sys.variant_key")
        )
        if not variant_key:
            continue
        current_by_variant[variant_key] = {
            str(key): gp._safe_str(value)
            for key, value in record.items()
        }

    preview_by_variant: Dict[str, Dict[str, str]] = {}
    for record in preview_contract["records"]:
        variant_key = gp._safe_str(
            record.get("sys.variant_key")
        )
        if not variant_key:
            continue
        if variant_key in preview_by_variant:
            raise ValueError(
                f"Preview duplicate sys.variant_key={variant_key!r}."
            )
        preview_by_variant[variant_key] = record

    current_keys = set(current_by_variant)
    preview_keys = set(preview_by_variant)
    if current_keys != preview_keys:
        raise ValueError(
            "Preview no longer matches Input. Variant-key sets differ. "
            f"current_only={sorted(current_keys - preview_keys)}; "
            f"preview_only={sorted(preview_keys - current_keys)}. "
            "Rerun Prepare."
        )

    compare_fields = list(prepare_plan["ordered_field_keys"])
    compare_fields.extend(
        [
            "sys.plan_status",
            "sys.error_count",
            "sys.warning_count",
        ]
    )

    mismatches: List[str] = []
    for variant_key in sorted(current_keys):
        current = current_by_variant[variant_key]
        preview = preview_by_variant[variant_key]
        for field_key in compare_fields:
            current_value = gp._safe_str(
                current.get(field_key)
            )
            preview_value = gp._safe_str(
                preview.get(field_key)
            )
            if current_value != preview_value:
                mismatches.append(
                    f"{variant_key}:{field_key}:"
                    f"Preview={preview_value!r}:"
                    f"Current={current_value!r}"
                )
                if len(mismatches) >= 20:
                    break
        if len(mismatches) >= 20:
            break

    if mismatches:
        raise ValueError(
            "Preview is stale or was edited. Rerun Prepare. "
            f"First mismatches={mismatches}"
        )

    invalid_statuses = {
        gp._safe_str(record.get("sys.plan_status"))
        for record in preview_by_variant.values()
        if gp._safe_str(record.get("sys.plan_status"))
        not in {"READY", "READY_WITH_WARNINGS", "SKIPPED"}
    }
    if invalid_statuses:
        raise ValueError(
            "Preview contains non-ready rows: "
            f"{sorted(invalid_statuses)}"
        )

    return {
        "variant_count": len(current_keys),
        "verified_fields": len(compare_fields),
    }


def _select_product_keys(
    *,
    prepare_plan: Mapping[str, Any],
    only_product_keys: Optional[Iterable[str]],
    apply_all_ready_products: bool,
    max_products: int,
    live_mode: bool,
) -> List[str]:
    ready_keys = sorted({
        gp._safe_str(record.get("sys.product_key"))
        for record in prepare_plan["preview_records"]
        if gp._safe_str(record.get("sys.plan_status"))
        in {"READY", "READY_WITH_WARNINGS"}
        and gp._safe_str(record.get("sys.product_key"))
    })

    requested = _safe_list(only_product_keys)
    if requested:
        unknown = sorted(set(requested) - set(ready_keys))
        if unknown:
            raise ValueError(
                "ONLY_PRODUCT_KEYS contains keys that are not READY: "
                f"{unknown}"
            )
        selected = requested
    else:
        if live_mode and not apply_all_ready_products:
            raise ValueError(
                "Live Apply requires explicit ONLY_PRODUCT_KEYS, or set "
                "APPLY_ALL_READY_PRODUCTS=True."
            )
        selected = ready_keys

    if not selected:
        raise ValueError("No READY Product groups selected.")

    if len(selected) > int(max_products):
        raise ValueError(
            f"Selected Product groups={len(selected)} exceeds "
            f"MAX_PRODUCTS_PER_RUN={max_products}."
        )
    return selected


def _product_rows(
    *,
    prepare_plan: Mapping[str, Any],
    selected_product_keys: Sequence[str],
) -> Dict[str, List[Dict[str, str]]]:
    selected = set(selected_product_keys)
    result: Dict[str, List[Dict[str, str]]] = {}
    for record in prepare_plan["preview_records"]:
        if gp._safe_str(record.get("sys.plan_status")) not in {
            "READY",
            "READY_WITH_WARNINGS",
        }:
            continue
        product_key = gp._safe_str(
            record.get("sys.product_key")
        )
        if product_key not in selected:
            continue
        result.setdefault(product_key, []).append(
            {
                str(key): gp._safe_str(value)
                for key, value in record.items()
            }
        )

    missing = sorted(
        set(selected_product_keys) - set(result)
    )
    if missing:
        raise ValueError(
            f"Selected Product groups have no ready rows: {missing}"
        )

    for product_key, rows in result.items():
        rows.sort(
            key=lambda record: int(
                gp._safe_str(
                    record.get("sys.source_row")
                )
                or "0"
            )
        )
    return result


def _build_options(
    rows: Sequence[Mapping[str, str]],
) -> Tuple[List[Dict[str, Any]], List[List[Dict[str, str]]]]:
    option_definitions: List[Dict[str, Any]] = []
    variant_option_values: List[List[Dict[str, str]]] = []

    active_option_numbers: List[int] = []
    for number in (1, 2, 3):
        name_key = f"core.option{number}_name"
        value_key = f"core.option{number}_value"
        names = {
            gp._safe_str(row.get(name_key))
            for row in rows
            if gp._safe_str(row.get(name_key))
        }
        values = [
            gp._safe_str(row.get(value_key))
            for row in rows
            if gp._safe_str(row.get(value_key))
        ]
        if names or values:
            if len(names) != 1:
                raise ValueError(
                    f"Option {number} must have exactly one name."
                )
            if not values or len(values) != len(rows):
                raise ValueError(
                    f"Option {number} requires a value on every Variant."
                )
            active_option_numbers.append(number)

    if not active_option_numbers:
        option_definitions = [
            {
                "name": "Title",
                "position": 1,
                "values": [{"name": "Default Title"}],
            }
        ]
        variant_option_values = [
            [
                {
                    "optionName": "Title",
                    "name": "Default Title",
                }
            ]
            for _ in rows
        ]
        return option_definitions, variant_option_values

    expected = list(
        range(1, max(active_option_numbers) + 1)
    )
    if active_option_numbers != expected:
        raise ValueError(
            "Options must be sequential without gaps. "
            f"active={active_option_numbers}"
        )

    for position, number in enumerate(
        active_option_numbers,
        start=1,
    ):
        name_key = f"core.option{number}_name"
        value_key = f"core.option{number}_value"
        option_name = gp._safe_str(rows[0].get(name_key))
        ordered_values: List[str] = []
        for row in rows:
            value = gp._safe_str(row.get(value_key))
            if value not in ordered_values:
                ordered_values.append(value)
        option_definitions.append(
            {
                "name": option_name,
                "position": position,
                "values": [
                    {"name": value}
                    for value in ordered_values
                ],
            }
        )

    for row in rows:
        values: List[Dict[str, str]] = []
        for number in active_option_numbers:
            values.append(
                {
                    "optionName": gp._safe_str(
                        row.get(
                            f"core.option{number}_name"
                        )
                    ),
                    "name": gp._safe_str(
                        row.get(
                            f"core.option{number}_value"
                        )
                    ),
                }
            )
        variant_option_values.append(values)

    return option_definitions, variant_option_values


def _product_metafields(
    *,
    row: Mapping[str, str],
    ordered_field_keys: Sequence[str],
    cfg_fields: Mapping[str, Any],
) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    for field_key in ordered_field_keys:
        if not field_key.startswith("mf."):
            continue
        item = _metafield_input(
            field_key=field_key,
            raw_value=row.get(field_key),
            cfg_fields=cfg_fields,
        )
        if item:
            result.append(item)
    return result


def _variant_metafields(
    *,
    row: Mapping[str, str],
    ordered_field_keys: Sequence[str],
    cfg_fields: Mapping[str, Any],
) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    for field_key in ordered_field_keys:
        if not field_key.startswith("v_mf."):
            continue
        item = _metafield_input(
            field_key=field_key,
            raw_value=row.get(field_key),
            cfg_fields=cfg_fields,
        )
        if item:
            result.append(item)
    return result


def _weight_unit_enum(value: Any) -> str:
    mapping = {
        "g": "GRAMS",
        "kg": "KILOGRAMS",
        "oz": "OUNCES",
        "lb": "POUNDS",
    }
    normalized = gp._safe_str(value).lower()
    if normalized not in mapping:
        raise ValueError(
            f"Unsupported Weight Unit: {value!r}"
        )
    return mapping[normalized]


def _build_product_set_input(
    *,
    product_key: str,
    rows: Sequence[Mapping[str, str]],
    ordered_field_keys: Sequence[str],
    cfg_fields: Mapping[str, Any],
    allow_non_draft_status: bool,
) -> Dict[str, Any]:
    if not rows:
        raise ValueError(
            f"Product group {product_key} has no Variant rows."
        )
    first = rows[0]

    requested_status = (
        gp._safe_str(first.get("core.status")).upper()
        or "DRAFT"
    )
    if (
        requested_status != "DRAFT"
        and not allow_non_draft_status
    ):
        raise ValueError(
            f"Product {product_key} requests status="
            f"{requested_status}. Non-Draft creation is blocked."
        )

    product_options, variant_option_values = _build_options(
        rows
    )

    product_input: Dict[str, Any] = {
        "title": gp._safe_str(first.get("core.title")),
        "handle": gp._safe_str(first.get("core.handle")),
        "descriptionHtml": gp._safe_str(
            first.get("core.description_html")
        ),
        "vendor": gp._safe_str(first.get("core.vendor")),
        "productType": gp._safe_str(
            first.get("core.product_type")
        ),
        "status": requested_status,
        "productOptions": product_options,
        "variants": [],
    }

    tags = _parse_tags(first.get("core.tags"))
    if tags:
        product_input["tags"] = tags

    template_suffix = gp._safe_str(
        first.get("core.template_suffix")
    )
    if template_suffix:
        product_input["templateSuffix"] = template_suffix

    seo_title = gp._safe_str(
        first.get("core.seo_title")
    )
    seo_description = gp._safe_str(
        first.get("core.seo_description")
    )
    if seo_title or seo_description:
        product_input["seo"] = {
            "title": seo_title,
            "description": seo_description,
        }

    product_metafields = _product_metafields(
        row=first,
        ordered_field_keys=ordered_field_keys,
        cfg_fields=cfg_fields,
    )
    if product_metafields:
        product_input["metafields"] = product_metafields

    for index, row in enumerate(rows):
        variant: Dict[str, Any] = {
            "optionValues": variant_option_values[index],
            "sku": gp._safe_str(row.get("core.sku")),
            "price": gp._safe_str(row.get("core.price")),
            "inventoryPolicy": (
                gp._safe_str(
                    row.get("core.inventory_policy")
                ).upper()
                or "DENY"
            ),
            "taxable": bool(
                gp._normalize_bool(
                    row.get("core.taxable")
                )
            ),
        }

        barcode = gp._safe_str(row.get("core.barcode"))
        if barcode:
            variant["barcode"] = barcode

        compare_at = gp._safe_str(
            row.get("core.compare_at_price")
        )
        if compare_at:
            variant["compareAtPrice"] = compare_at

        inventory_item: Dict[str, Any] = {
            "requiresShipping": bool(
                gp._normalize_bool(
                    row.get("core.requires_shipping")
                )
            ),
            "tracked": True,
        }
        cost = gp._safe_str(row.get("core.cost"))
        if cost:
            inventory_item["cost"] = cost

        weight = gp._safe_str(row.get("core.weight"))
        if weight:
            inventory_item["measurement"] = {
                "weight": {
                    "value": float(Decimal(weight)),
                    "unit": _weight_unit_enum(
                        row.get("core.weight_unit")
                    ),
                }
            }
        variant["inventoryItem"] = inventory_item

        location_gid = gp._safe_str(
            row.get("sys.inventory_location_gid")
        )
        location_code = gp._safe_str(
            row.get("inventory.location_code")
        )
        quantity = gp._safe_str(
            row.get("inventory.quantity")
        )
        if not location_gid:
            raise ValueError(
                f"Product {product_key}, SKU="
                f"{row.get('core.sku')} has no resolved "
                "Location GID."
            )
        variant["inventoryQuantities"] = [
            {
                "locationId": location_gid,
                "name": "available",
                "quantity": int(quantity),
            }
        ]

        variant_metafields = _variant_metafields(
            row=row,
            ordered_field_keys=ordered_field_keys,
            cfg_fields=cfg_fields,
        )
        if variant_metafields:
            variant["metafields"] = variant_metafields

        product_input["variants"].append(variant)

    # Remove optional empty scalar fields. Required values remain.
    for key in [
        "handle",
        "descriptionHtml",
        "vendor",
        "productType",
    ]:
        if not gp._safe_str(product_input.get(key)):
            product_input.pop(key, None)

    return product_input


def _attach_location_gids(
    *,
    product_rows: Mapping[str, Sequence[Dict[str, str]]],
    locations: Mapping[str, Any],
) -> None:
    active_by_code = locations["active_by_code"]
    for rows in product_rows.values():
        for row in rows:
            code = gp._safe_str(
                row.get("inventory.location_code")
            )
            record = active_by_code.get(code)
            if not record:
                raise ValueError(
                    f"Unknown active Location code={code!r}."
                )
            row["sys.inventory_location_gid"] = gp._safe_str(
                record.get("location_gid")
            )
            row["sys.inventory_location_name"] = gp._safe_str(
                record.get("location_name")
            )


def _preflight_shopify_handle_conflicts(
    *,
    client: ShopifyClient,
    product_rows: Mapping[str, Sequence[Mapping[str, str]]],
    progress_every: int,
) -> Dict[str, Any]:
    """Block only when the target Product Handle already exists.

    SKU and Barcode are intentionally excluded from duplicate checks.
    """
    errors: List[Dict[str, Any]] = []
    handles = sorted({
        gp._safe_str(rows[0].get("core.handle"))
        for rows in product_rows.values()
        if rows and gp._safe_str(rows[0].get("core.handle"))
    })

    total_checks = len(handles)
    for completed, handle in enumerate(handles, start=1):
        data = client.gql(
            Q_PRODUCT_BY_HANDLE,
            {"identifier": {"handle": handle}},
            operation_name="preflight_handle",
        )
        product = data.get("product")
        if product:
            errors.append(
                {
                    "code": "HANDLE_ALREADY_EXISTS",
                    "value": handle,
                    "matches": [product],
                }
            )

        if progress_every and (
            completed % progress_every == 0
            or completed == total_checks
        ):
            print(
                "[Preflight] "
                f"{completed}/{total_checks} | "
                f"handle_errors={len(errors)}"
            )

    return {
        "errors": errors,
        "warnings": [],
        "checks": total_checks,
        "identity_field": "core.handle",
        "sku_checked": False,
        "barcode_checked": False,
    }


def _source_option_signature(
    variant_input: Mapping[str, Any],
) -> Tuple[Tuple[str, str], ...]:
    return tuple(
        (
            gp._safe_str(item.get("optionName")),
            gp._safe_str(item.get("name")),
        )
        for item in variant_input.get("optionValues", [])
    )


def _returned_option_signature(
    variant: Mapping[str, Any],
) -> Tuple[Tuple[str, str], ...]:
    return tuple(
        (
            gp._safe_str(item.get("name")),
            gp._safe_str(item.get("value")),
        )
        for item in variant.get("selectedOptions", [])
    )


def _match_returned_variants_by_options(
    *,
    source_rows: Sequence[Mapping[str, str]],
    product_input: Mapping[str, Any],
    returned_variants: Sequence[Mapping[str, Any]],
) -> List[Mapping[str, Any]]:
    """Match Shopify Variants by option values, never by SKU or Barcode."""
    input_variants = list(product_input.get("variants", []))
    if len(input_variants) != len(source_rows):
        raise RuntimeError(
            "Internal variant payload/source row count mismatch."
        )

    returned_by_signature: Dict[
        Tuple[Tuple[str, str], ...],
        List[Mapping[str, Any]],
    ] = {}
    for variant in returned_variants:
        signature = _returned_option_signature(variant)
        returned_by_signature.setdefault(signature, []).append(variant)

    matched: List[Mapping[str, Any]] = []
    used_ids = set()
    for source, variant_input in zip(source_rows, input_variants):
        signature = _source_option_signature(variant_input)
        candidates = [
            item
            for item in returned_by_signature.get(signature, [])
            if gp._safe_str(item.get("id")) not in used_ids
        ]
        if len(candidates) != 1:
            raise RuntimeError(
                "Post-write Variant option verification failed. "
                f"variant_key={source.get('sys.variant_key')!r}; "
                f"signature={signature}; matches={len(candidates)}"
            )
        matched_variant = candidates[0]
        used_ids.add(gp._safe_str(matched_variant.get("id")))
        matched.append(matched_variant)

    if len(matched) != len(returned_variants):
        raise RuntimeError(
            "Post-write returned Variant count does not match "
            "the source plan."
        )
    return matched

def _ensure_result_header(
    spreadsheet: gspread.Spreadsheet,
    tab_name: str,
) -> gspread.Worksheet:
    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=tab_name,
            rows=200,
            cols=len(RESULT_HEADERS),
        )

    values = worksheet.get_all_values()
    if not values:
        if worksheet.col_count < len(RESULT_HEADERS):
            worksheet.resize(
                rows=max(worksheet.row_count, 200),
                cols=len(RESULT_HEADERS),
            )
        worksheet.update(
            range_name=(
                f"A1:{gp._a1_col(len(RESULT_HEADERS))}1"
            ),
            values=[RESULT_HEADERS],
            value_input_option="RAW",
        )
        try:
            worksheet.freeze(rows=1)
        except Exception:
            pass
        return worksheet

    current = [
        gp._safe_str(value)
        for value in values[0][: len(RESULT_HEADERS)]
    ]
    if current != RESULT_HEADERS:
        raise ValueError(
            f"Result header mismatch in {tab_name}. "
            f"Expected={RESULT_HEADERS}; actual={current}"
        )
    return worksheet


def _append_result_rows(
    worksheet: gspread.Worksheet,
    rows: Sequence[Sequence[Any]],
) -> int:
    if not rows:
        return 0
    values = worksheet.get_all_values()
    start_row = max(2, len(values) + 1)
    end_row = start_row + len(rows) - 1
    required_cols = len(RESULT_HEADERS)
    if (
        worksheet.row_count < end_row
        or worksheet.col_count < required_cols
    ):
        worksheet.resize(
            rows=max(worksheet.row_count, end_row + 100),
            cols=max(worksheet.col_count, required_cols),
        )
    worksheet.update(
        range_name=(
            f"A{start_row}:"
            f"{gp._a1_col(required_cols)}{end_row}"
        ),
        values=[list(row) for row in rows],
        value_input_option="RAW",
    )
    return len(rows)


def _option_summary(
    selected_options: Sequence[Mapping[str, Any]],
) -> str:
    return "; ".join(
        f"{gp._safe_str(item.get('name'))}="
        f"{gp._safe_str(item.get('value'))}"
        for item in selected_options
        if gp._safe_str(item.get("name"))
    )


def _inventory_quantity_from_variant(
    *,
    variant: Mapping[str, Any],
    location_gid: str,
) -> str:
    inventory_item = variant.get("inventoryItem") or {}
    levels = (
        inventory_item.get("inventoryLevels", {})
        .get("nodes", [])
    )
    for level in levels:
        location = level.get("location") or {}
        if gp._safe_str(location.get("id")) != location_gid:
            continue
        for quantity in level.get("quantities", []):
            if gp._safe_str(quantity.get("name")) == "available":
                return gp._safe_str(quantity.get("quantity"))
    return ""


def _result_rows_for_product(
    *,
    run_id: str,
    applied_at: str,
    site_code: str,
    runtime_mode: str,
    dry_run: bool,
    product_key: str,
    source_rows: Sequence[Mapping[str, str]],
    status: str,
    message: str,
    error_reason: str,
    product_response: Optional[Mapping[str, Any]],
    product_input: Mapping[str, Any],
    admin_product_base_url: str,
    storefront_product_base_url: str,
    api_operations_planned: int,
    api_operations_succeeded: int,
    api_operations_failed: int,
) -> List[List[Any]]:
    product_response = product_response or {}
    product_gid = gp._safe_str(
        product_response.get("id")
    )
    product_handle = gp._safe_str(
        product_response.get("handle")
    ) or gp._safe_str(product_input.get("handle"))
    product_title = gp._safe_str(
        product_response.get("title")
    ) or gp._safe_str(product_input.get("title"))
    product_status = gp._safe_str(
        product_response.get("status")
    ) or gp._safe_str(product_input.get("status"))

    returned_variants = (
        product_response.get("variants", {}).get("nodes", [])
        if product_response
        else []
    )
    matched_returned_variants: List[Mapping[str, Any]] = []
    if product_response:
        matched_returned_variants = (
            _match_returned_variants_by_options(
                source_rows=source_rows,
                product_input=product_input,
                returned_variants=returned_variants,
            )
        )

    admin_url = ""
    storefront_url = ""
    if product_gid:
        numeric_id = product_gid.rsplit("/", 1)[-1]
        if gp._safe_str(admin_product_base_url):
            admin_url = (
                gp._safe_str(admin_product_base_url)
                + numeric_id
            )
    if product_handle and gp._safe_str(
        storefront_product_base_url
    ):
        storefront_url = (
            gp._safe_str(storefront_product_base_url)
            + product_handle
        )

    product_metafields_count = len(
        product_input.get("metafields", [])
    )
    result_rows: List[List[Any]] = []

    for index, source in enumerate(source_rows):
        sku = gp._safe_str(source.get("core.sku"))
        returned = (
            matched_returned_variants[index]
            if matched_returned_variants
            else {}
        )
        inventory_item = returned.get("inventoryItem") or {}
        location_gid = gp._safe_str(
            source.get("sys.inventory_location_gid")
        )
        actual_quantity = (
            _inventory_quantity_from_variant(
                variant=returned,
                location_gid=location_gid,
            )
            if returned
            else gp._safe_str(
                source.get("inventory.quantity")
            )
        )
        option_values = (
            _option_summary(
                returned.get("selectedOptions", [])
            )
            if returned
            else "; ".join(
                f"{item.get('optionName')}={item.get('name')}"
                for item in product_input["variants"][index][
                    "optionValues"
                ]
            )
        )
        variant_metafields_count = len(
            product_input["variants"][index].get(
                "metafields",
                [],
            )
        )

        result_rows.append(
            [
                run_id,
                applied_at,
                site_code,
                runtime_mode,
                "TRUE" if dry_run else "FALSE",
                product_key,
                gp._safe_str(
                    source.get("sys.variant_key")
                ),
                gp._safe_str(
                    source.get("sys.source_row")
                ),
                status,
                product_gid,
                product_handle,
                product_title,
                product_status,
                gp._safe_str(returned.get("id")),
                gp._safe_str(inventory_item.get("id")),
                sku,
                gp._safe_str(source.get("core.barcode")),
                option_values,
                gp._safe_str(source.get("core.price")),
                gp._safe_str(
                    source.get("core.compare_at_price")
                ),
                gp._safe_str(source.get("core.cost")),
                gp._safe_str(
                    source.get("inventory.location_code")
                ),
                location_gid,
                actual_quantity,
                product_metafields_count,
                variant_metafields_count,
                api_operations_planned,
                api_operations_succeeded,
                api_operations_failed,
                message,
                error_reason,
                admin_url,
                storefront_url,
            ]
        )
    return result_rows


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    tab_cfg_locations: str = "Cfg__Locations",
    config_sheet_label: str = "config",
    create_sheet_label: str = "create_generic",
    runlog_sheet_label: str = "runlog_sheet",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_input: str = "Input",
    tab_defaults: str = "Defaults",
    tab_preview: str = "Preview",
    tab_result: str = "Result",
    tab_runlog: str = "Ops__RunLog",
    only_product_keys: Optional[Iterable[str]] = None,
    apply_all_ready_products: bool = False,
    max_products_per_run: int = 5,
    dry_run: bool = True,
    confirmed: bool = False,
    allow_non_draft_status: bool = False,
    stop_on_first_error: bool = True,
    write_result: bool = True,
    preflight_progress_every: int = 10,
    product_progress_every: int = 1,
    api_timeout_seconds: int = 120,
    api_max_retries: int = 6,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[
        Mapping[str, Mapping[str, str]]
    ] = None,
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
) -> Dict[str, Any]:
    if gp.MODULE_VERSION != EXPECTED_PREPARE_MODULE_VERSION:
        raise RuntimeError(
            "generic_apply requires generic_prepare "
            f"{EXPECTED_PREPARE_MODULE_VERSION}; "
            f"loaded={gp.MODULE_VERSION}."
        )

    site_code = gp._normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not gp._safe_str(console_core_url):
        raise ValueError("console_core_url is required.")

    live_mode = not bool(dry_run)
    if live_mode and not confirmed:
        raise ValueError(
            "Live Apply requires CONFIRMED=True."
        )

    run_id = run_id or gp._make_run_id(
        job_name,
        tz_name,
    )
    started = time.monotonic()
    phase = "apply"

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(
        1,
        12,
        f"Resolve Google access | site={site_code}",
    )
    google_secret = gp.read_secret(
        bootstrap_gsheet_sa_b64_secret,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, google_auth = gp._build_gspread_client(
        google_secret
    )
    console = gc.open_by_url(console_core_url)

    progress(
        2,
        12,
        "Resolve routed workbooks and account configuration",
    )
    account = gp._load_account_values(
        console,
        tab_cfg_account_id,
    )
    create_url = gp._resolve_sheet_url_by_label(
        console,
        tab_cfg_sites,
        site_code,
        create_sheet_label,
    )
    config_url = gp._resolve_sheet_url_by_label(
        console,
        tab_cfg_sites,
        site_code,
        config_sheet_label,
    )
    runlog_url = gp._resolve_sheet_url_by_label(
        console,
        tab_cfg_sites,
        site_code,
        runlog_sheet_label,
    )

    create_book = gc.open_by_url(create_url)
    config_book = gc.open_by_url(config_url)
    runlog_ws = gc.open_by_url(
        runlog_url
    ).worksheet(tab_runlog)
    logger = gp.RunLogger18(
        worksheet=runlog_ws,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )

    try:
        progress(
            3,
            12,
            "Re-read Input, Defaults, Preview, Cfg__Fields, "
            "and Cfg__Locations",
        )
        input_values = gp._require_worksheet(
            create_book,
            tab_input,
        ).get_all_values()
        defaults_values = gp._require_worksheet(
            create_book,
            tab_defaults,
        ).get_all_values()
        preview_values = gp._require_worksheet(
            create_book,
            tab_preview,
        ).get_all_values()
        cfg_values = gp._require_worksheet(
            config_book,
            tab_cfg_fields,
        ).get_all_values()
        location_values = gp._require_worksheet(
            console,
            tab_cfg_locations,
        ).get_all_values()

        cfg_fields = gp._read_cfg_fields(cfg_values)
        input_contract = gp._read_input_matrix(
            input_values,
            cfg_fields,
        )
        defaults = gp._read_defaults_matrix(
            defaults_values
        )
        locations = gp._read_locations(
            location_values,
            site_code,
        )
        preview_contract = _read_preview_records(
            preview_values
        )

        progress(
            4,
            12,
            "Rebuild current Prepare plan",
        )
        prepare_plan = gp._build_prepare_plan(
            input_contract=input_contract,
            defaults=defaults,
            cfg_fields=cfg_fields,
            locations=locations,
        )
        if not prepare_plan["ready_for_apply"]:
            raise ValueError(
                "Current Input is not READY for Apply. "
                f"errors={prepare_plan['stats']['error_count']} | "
                f"warnings={prepare_plan['stats']['warning_count']}"
            )

        progress(
            5,
            12,
            "Verify Preview snapshot against current Input",
        )
        preview_verification = _verify_preview_snapshot(
            prepare_plan=prepare_plan,
            preview_contract=preview_contract,
        )
        print(
            "[Preview verified] "
            f"variants={preview_verification['variant_count']} | "
            f"fields={preview_verification['verified_fields']}"
        )

        progress(
            6,
            12,
            "Select approved Product groups",
        )
        selected_product_keys = _select_product_keys(
            prepare_plan=prepare_plan,
            only_product_keys=only_product_keys,
            apply_all_ready_products=(
                apply_all_ready_products
            ),
            max_products=max_products_per_run,
            live_mode=live_mode,
        )
        product_rows = _product_rows(
            prepare_plan=prepare_plan,
            selected_product_keys=selected_product_keys,
        )
        _attach_location_gids(
            product_rows=product_rows,
            locations=locations,
        )
        print(
            "[Selection] "
            f"products={len(selected_product_keys)} | "
            f"variants={sum(len(rows) for rows in product_rows.values())} | "
            f"keys={selected_product_keys}"
        )

        progress(
            7,
            12,
            "Build final ProductSet execution payloads",
        )
        product_inputs: Dict[str, Dict[str, Any]] = {}
        for product_key in selected_product_keys:
            product_inputs[product_key] = (
                _build_product_set_input(
                    product_key=product_key,
                    rows=product_rows[product_key],
                    ordered_field_keys=prepare_plan[
                        "ordered_field_keys"
                    ],
                    cfg_fields=cfg_fields,
                    allow_non_draft_status=(
                        allow_non_draft_status
                    ),
                )
            )

        shop_domain = gp._safe_str(
            account.get("SHOP_DOMAIN")
        )
        api_version = gp._safe_str(
            account.get("SHOPIFY_API_VERSION")
        )
        token_secret_name = gp._safe_str(
            account.get("SHOPIFY_TOKEN_SECRET")
        )
        if not shop_domain:
            raise ValueError(
                "Cfg__account_id missing SHOP_DOMAIN."
            )
        if not api_version:
            raise ValueError(
                "Cfg__account_id missing SHOPIFY_API_VERSION."
            )
        if not token_secret_name:
            raise ValueError(
                "Cfg__account_id missing SHOPIFY_TOKEN_SECRET."
            )

        progress(
            8,
            12,
            "Resolve Shopify token and initialize client",
        )
        shopify_secret = gp.read_secret(
            token_secret_name,
            explicit_value=shopify_token_value,
            secret_home=secret_home,
            local_secret_aliases=local_secret_aliases,
        )
        client = ShopifyClient(
            shop_domain=shop_domain,
            api_version=api_version,
            access_token=shopify_secret.value,
            timeout_seconds=api_timeout_seconds,
            max_retries=api_max_retries,
            print_progress=print_progress,
        )

        progress(
            9,
            12,
            "Real-time Shopify Handle conflict preflight",
        )
        conflict_result = _preflight_shopify_handle_conflicts(
            client=client,
            product_rows=product_rows,
            progress_every=preflight_progress_every,
        )
        if conflict_result["errors"]:
            raise ValueError(
                "Shopify conflict preflight failed: "
                + json.dumps(
                    conflict_result["errors"][:20],
                    ensure_ascii=False,
                )
            )
        if conflict_result["warnings"]:
            print(
                "[Preflight warnings] "
                + json.dumps(
                    conflict_result["warnings"][:20],
                    ensure_ascii=False,
                )
            )

        progress(
            10,
            12,
            (
                "DRY RUN final plan"
                if dry_run
                else "Create Shopify Products"
            ),
        )
        result_rows: List[List[Any]] = []
        product_results: List[Dict[str, Any]] = []
        products_succeeded = 0
        products_failed = 0
        variants_succeeded = 0
        variants_failed = 0

        applied_at = gp._now_str(tz_name)
        admin_product_base_url = gp._safe_str(
            account.get("ADMIN_PRODUCT_BASE_URL")
        )
        storefront_product_base_url = gp._safe_str(
            account.get("STOREFRONT_PRODUCT_BASE_URL")
        )

        total_products = len(selected_product_keys)
        for index, product_key in enumerate(
            selected_product_keys,
            start=1,
        ):
            rows = product_rows[product_key]
            product_input = product_inputs[product_key]
            product_response: Optional[Dict[str, Any]] = None
            status = ""
            message = ""
            error_reason = ""
            succeeded_ops = 0
            failed_ops = 0

            try:
                if dry_run:
                    status = "PLANNED"
                    message = (
                        "DRY_RUN: Shopify productSet was not called."
                    )
                else:
                    data = client.gql(
                        M_PRODUCT_SET,
                        {
                            "input": product_input,
                            "synchronous": True,
                        },
                        operation_name="productSet_create",
                    )
                    payload = data.get("productSet") or {}
                    user_errors = payload.get("userErrors") or []
                    if user_errors:
                        raise RuntimeError(
                            "productSet userErrors: "
                            + json.dumps(
                                user_errors,
                                ensure_ascii=False,
                            )
                        )
                    product_response = payload.get("product")
                    if not product_response:
                        raise RuntimeError(
                            "productSet returned no Product."
                        )

                    returned_variants = (
                        product_response.get(
                            "variants",
                            {},
                        ).get("nodes", [])
                    )
                    _match_returned_variants_by_options(
                        source_rows=rows,
                        product_input=product_input,
                        returned_variants=returned_variants,
                    )

                    status = "SUCCESS"
                    message = (
                        "Product and all Variants created by "
                        "synchronous productSet."
                    )
                    succeeded_ops = 1
                    products_succeeded += 1
                    variants_succeeded += len(rows)

                if dry_run:
                    products_succeeded += 0

            except Exception as exc:
                status = "FAILED"
                message = str(exc)
                error_reason = type(exc).__name__
                failed_ops = 1
                products_failed += 1
                variants_failed += len(rows)

            result_rows.extend(
                _result_rows_for_product(
                    run_id=run_id,
                    applied_at=applied_at,
                    site_code=site_code,
                    runtime_mode=gp._runtime_mode(),
                    dry_run=dry_run,
                    product_key=product_key,
                    source_rows=rows,
                    status=status,
                    message=message,
                    error_reason=error_reason,
                    product_response=product_response,
                    product_input=product_input,
                    admin_product_base_url=(
                        admin_product_base_url
                    ),
                    storefront_product_base_url=(
                        storefront_product_base_url
                    ),
                    api_operations_planned=1,
                    api_operations_succeeded=succeeded_ops,
                    api_operations_failed=failed_ops,
                )
            )
            product_results.append(
                {
                    "product_key": product_key,
                    "status": status,
                    "message": message,
                    "product_gid": gp._safe_str(
                        (product_response or {}).get("id")
                    ),
                    "handle": gp._safe_str(
                        (product_response or {}).get(
                            "handle"
                        )
                    )
                    or gp._safe_str(
                        product_input.get("handle")
                    ),
                    "variants": len(rows),
                }
            )

            logger.log(
                phase=phase,
                log_type="detail",
                status=status,
                entity_type="PRODUCT",
                gid=gp._safe_str(
                    (product_response or {}).get("id")
                ),
                rows_loaded=prepare_plan["stats"][
                    "rows_loaded"
                ],
                rows_pending=sum(
                    len(items)
                    for items in product_rows.values()
                ),
                rows_recognized=prepare_plan["stats"][
                    "rows_recognized"
                ],
                rows_planned=len(rows),
                rows_written=(
                    len(rows)
                    if status == "SUCCESS"
                    else 0
                ),
                rows_skipped=(
                    len(rows)
                    if status in {"PLANNED", "FAILED"}
                    else 0
                ),
                message=(
                    f"product_key={product_key} | {message}"
                ),
                error_reason=error_reason,
            )

            if product_progress_every and (
                index % product_progress_every == 0
                or index == total_products
            ):
                print(
                    "[Apply progress] "
                    f"{index}/{total_products} | "
                    f"products_succeeded={products_succeeded} | "
                    f"products_failed={products_failed} | "
                    f"variants_succeeded={variants_succeeded} | "
                    f"variants_failed={variants_failed}"
                )

            if status == "FAILED" and stop_on_first_error:
                break

        progress(
            11,
            12,
            "Write Result and RunLog evidence",
        )
        result_rows_written = 0
        if write_result:
            result_ws = _ensure_result_header(
                create_book,
                tab_result,
            )
            result_rows_written = _append_result_rows(
                result_ws,
                result_rows,
            )

        if dry_run:
            final_status = (
                "DRY_RUN_READY"
                if not products_failed
                else "DRY_RUN_FAILED"
            )
        elif products_failed:
            final_status = (
                "PARTIAL_FAILURE"
                if products_succeeded
                else "FAILED"
            )
        else:
            final_status = "SUCCESS"

        elapsed = round(
            time.monotonic() - started,
            2,
        )
        rows_written = (
            variants_succeeded
            if not dry_run
            else 0
        )
        rows_skipped = (
            sum(len(rows) for rows in product_rows.values())
            - rows_written
        )

        logger.log(
            phase=phase,
            log_type="summary",
            status=final_status,
            entity_type="GENERIC_PRODUCT_CREATE",
            rows_loaded=prepare_plan["stats"]["rows_loaded"],
            rows_pending=sum(
                len(rows)
                for rows in product_rows.values()
            ),
            rows_recognized=prepare_plan["stats"][
                "rows_recognized"
            ],
            rows_planned=sum(
                len(rows)
                for rows in product_rows.values()
            ),
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            message=(
                f"dry_run={dry_run} | confirmed={confirmed} | "
                f"products_selected={len(selected_product_keys)} | "
                f"products_succeeded={products_succeeded} | "
                f"products_failed={products_failed} | "
                f"variants_succeeded={variants_succeeded} | "
                f"variants_failed={variants_failed} | "
                f"result_rows_written={result_rows_written} | "
                f"shopify_requests={client.request_count} | "
                f"shopify_retries={client.retry_count}"
            ),
            error_reason=(
                "PRODUCT_CREATE_FAILURE"
                if products_failed
                else ""
            ),
        )
        logger.flush()

        progress(
            12,
            12,
            f"Completed | status={final_status} | "
            f"elapsed={elapsed}s",
        )

        return {
            "ok": final_status in {
                "DRY_RUN_READY",
                "SUCCESS",
            },
            "status": final_status,
            "phase": phase,
            "run_id": run_id,
            "dry_run": dry_run,
            "confirmed": confirmed,
            "selected_product_keys": selected_product_keys,
            "summary": {
                "rows_loaded": prepare_plan["stats"][
                    "rows_loaded"
                ],
                "rows_pending": sum(
                    len(rows)
                    for rows in product_rows.values()
                ),
                "rows_planned": sum(
                    len(rows)
                    for rows in product_rows.values()
                ),
                "rows_written": rows_written,
                "rows_skipped": rows_skipped,
                "warning_count": len(
                    conflict_result["warnings"]
                ),
                "error_count": products_failed,
                "business_objects_planned": len(
                    selected_product_keys
                ),
                "api_operations_planned": len(
                    selected_product_keys
                ),
                "api_operations_succeeded": (
                    products_succeeded
                    if not dry_run
                    else 0
                ),
                "api_operations_failed": products_failed,
                "products_succeeded": products_succeeded,
                "products_failed": products_failed,
                "variants_succeeded": variants_succeeded,
                "variants_failed": variants_failed,
                "result_rows_written": result_rows_written,
                "shopify_requests": client.request_count,
                "shopify_retries": client.retry_count,
                "elapsed_seconds": elapsed,
            },
            "products": product_results,
            "warnings": conflict_result["warnings"],
            "preview_verification": preview_verification,
            "runtime": {
                "runtime_mode": gp._runtime_mode(),
                "auth_type": (
                    "GOOGLE_SERVICE_ACCOUNT + "
                    "SHOPIFY_ADMIN_TOKEN"
                ),
                "interactive_auth_used": False,
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "google_secret_source": google_auth[
                    "source_type"
                ],
                "shopify_secret_source": (
                    shopify_secret.source_type
                ),
                "shop_domain": shop_domain,
                "api_version": api_version,
            },
            "targets": {
                "create_sheet_url": create_url,
                "result_tab": tab_result,
                "runlog_sheet_url": runlog_url,
                "runlog_tab": tab_runlog,
                "module_path": MODULE_PATH,
                "module_version": MODULE_VERSION,
                "prepare_module_version": gp.MODULE_VERSION,
            },
            "result_preview": pd.DataFrame(
                [
                    dict(zip(RESULT_HEADERS, row))
                    for row in result_rows
                ]
            ),
        }

    except BaseException as exc:
        try:
            logger.log(
                phase=phase,
                log_type="summary",
                status="FAILED",
                entity_type="GENERIC_PRODUCT_CREATE",
                message=str(exc),
                error_reason=type(exc).__name__,
            )
            logger.flush()
        except Exception as log_exc:
            if print_progress:
                print(
                    "[RunLog warning] failed to write failure "
                    f"log: {log_exc}"
                )
        if print_progress:
            print(
                f"[FAILED] {type(exc).__name__}: {exc}"
            )
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Apply validated Generic Shopify Product Creation plans."
        )
    )
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument(
        "--bootstrap-gsheet-secret",
        required=True,
    )
    parser.add_argument(
        "--product-key",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--apply-all-ready-products",
        action="store_true",
    )
    parser.add_argument("--max-products", type=int, default=5)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirmed", action="store_true")
    parser.add_argument("--secret-home", default="")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=(
            args.bootstrap_gsheet_secret
        ),
        only_product_keys=args.product_key,
        apply_all_ready_products=(
            args.apply_all_ready_products
        ),
        max_products_per_run=args.max_products,
        dry_run=not args.apply,
        confirmed=args.confirmed,
        secret_home=args.secret_home or None,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "run_id": result["run_id"],
                "summary": result["summary"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
