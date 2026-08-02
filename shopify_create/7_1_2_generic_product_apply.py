# -*- coding: utf-8 -*-
"""Apply Generic Shopify Product Creation rows.

GitHub target: ``ecom/shopify_create/generic_apply.py``
Import path: ``shopify_create.generic_apply``

Execution contract
------------------
1. Read current Input and Defaults directly; Preview is not required.
2. Read existing Handles once from ``V_Product_Handle`` column
   ``Product Handle`` and compare locally.
3. Only ``core.handle`` participates in existence checking.
4. Existing Handles are written to Result as ``SKIPPED_HANDLE_EXISTS`` and
   are not uploaded or treated as errors.
5. New Handles are created with bounded Product-level concurrency.
6. Google Sheets writes remain on the main thread and Result rows are flushed
   periodically in batches.
7. DRY_RUN performs no Shopify writes.
8. Live writes require ``dry_run=False`` and ``confirmed=True``.
9. Images/media are intentionally out of scope.

SKU, Barcode, Product Key, Variant Key, numeric IDs, Title, and every other
field are excluded from duplicate lookup. Shopify Admin GraphQL is used only
for actual create/publication operations.
"""
from __future__ import annotations

import argparse
import datetime as dt
import threading
from concurrent.futures import (
    FIRST_COMPLETED,
    ThreadPoolExecutor,
    wait,
)
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


MODULE_VERSION = "1.5.10"
MODULE_PATH = "shopify_create.generic_apply"
DEFAULT_JOB_NAME = "generic_create_apply"
EXPECTED_PREPARE_MODULE_VERSION = "1.6.6"

LEGACY_RESULT_HEADERS = [
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

RESULT_HEADERS = LEGACY_RESULT_HEADERS + [
    "category_id",
    "template_suffix",
    "publish_all_channels",
    "publications_planned",
    "publications_published",
    "publication_ids",
]

Q_PUBLICATIONS_PAGE = """
query PublicationsPage($first: Int!, $after: String) {
  publications(first: $first, after: $after) {
    nodes {
      id
      autoPublish
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

M_PUBLISHABLE_PUBLISH = """
mutation PublishProductToPublications(
  $id: ID!,
  $input: [PublicationInput!]!
) {
  publishablePublish(id: $id, input: $input) {
    publishable {
      availablePublicationsCount {
        count
      }
      resourcePublicationsCount {
        count
      }
    }
    userErrors {
      field
      message
    }
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
          taxable
          inventoryItem {
            id
            sku
            tracked
            requiresShipping
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
                  isFulfillmentService
                  fulfillmentService {
                    handle
                  }
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
        self._counter_lock = threading.Lock()

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
            with self._counter_lock:
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
                    with self._counter_lock:
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
                with self._counter_lock:
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


def _project_secret_name_for_runtime(
    secret_name: Any,
    project_code: Any,
) -> str:
    """Preserve exact Colab names and use canonical project names locally."""
    logical_name = gp._safe_str(secret_name)
    if not logical_name:
        raise ValueError("Secret name is empty.")

    if gp._runtime_mode() != "LOCAL":
        return logical_name

    resolved_project_code = gp._normalize_site_code(project_code)
    if not resolved_project_code:
        raise ValueError(
            "PROJECT_CODE is required for Local Secret normalization."
        )

    normalized_name = logical_name.upper()
    for suffix in (
        "_SHOPIFY_ACCESS_TOKEN",
        "_SHOPIFY_TOKEN",
    ):
        if normalized_name.endswith(suffix):
            return f"{resolved_project_code}{suffix}"

    return logical_name


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
    selected_handles: Sequence[str],
) -> Dict[str, Any]:
    """Verify only the selected Product rows by physical source row.

    Validation errors on unrelated Input rows must not block an explicitly
    selected ready Product. sys.variant_key remains a trace value and may
    repeat across different Handles.
    """
    selected = {
        gp._safe_str(value)
        for value in selected_handles
        if gp._safe_str(value)
    }
    if not selected:
        raise ValueError(
            "Preview verification requires at least one selected Handle."
        )

    current_by_source_row: Dict[str, Dict[str, str]] = {}
    for record in prepare_plan["preview_records"]:
        handle = gp._safe_str(record.get("core.handle"))
        if handle not in selected:
            continue
        source_row = gp._safe_str(record.get("sys.source_row"))
        if not source_row:
            continue
        if source_row in current_by_source_row:
            raise ValueError(
                f"Current selected plan has duplicate "
                f"sys.source_row={source_row!r}."
            )
        current_by_source_row[source_row] = {
            str(key): gp._safe_str(value)
            for key, value in record.items()
        }

    selected_source_rows = set(current_by_source_row)
    preview_by_source_row: Dict[str, Dict[str, str]] = {}
    for record in preview_contract["records"]:
        source_row = gp._safe_str(record.get("sys.source_row"))
        if source_row not in selected_source_rows:
            continue
        if source_row in preview_by_source_row:
            raise ValueError(
                f"Preview selected scope has duplicate "
                f"sys.source_row={source_row!r}."
            )
        preview_by_source_row[source_row] = record

    current_keys = set(current_by_source_row)
    preview_keys = set(preview_by_source_row)
    if current_keys != preview_keys:
        raise ValueError(
            "Preview no longer matches the selected Input rows. "
            "Source-row sets differ. "
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
    for source_row in sorted(current_keys, key=lambda value: int(value)):
        current = current_by_source_row[source_row]
        preview = preview_by_source_row[source_row]
        for field_key in compare_fields:
            current_value = gp._safe_str(current.get(field_key))
            preview_value = gp._safe_str(preview.get(field_key))
            if current_value != preview_value:
                mismatches.append(
                    f"source_row={source_row}:{field_key}:"
                    f"Preview={preview_value!r}:"
                    f"Current={current_value!r}"
                )
                if len(mismatches) >= 20:
                    break
        if len(mismatches) >= 20:
            break

    if mismatches:
        raise ValueError(
            "Preview is stale or was edited for the selected Product rows. "
            "Rerun Prepare. "
            f"First mismatches={mismatches}"
        )

    invalid_statuses = {
        gp._safe_str(record.get("sys.plan_status"))
        for record in current_by_source_row.values()
        if gp._safe_str(record.get("sys.plan_status"))
        not in {"READY", "READY_WITH_WARNINGS"}
    }
    if invalid_statuses:
        raise ValueError(
            "Selected Preview rows are not ready: "
            f"{sorted(invalid_statuses)}"
        )

    return {
        "variant_count": len(current_keys),
        "row_count": len(current_keys),
        "row_identity": "sys.source_row",
        "selection_identity": "core.handle",
        "selected_handles": sorted(selected),
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
    """Resolve selected Product Handles without requiring the full Input ready.

    ONLY_PRODUCT_KEYS remains backward compatible:
    - an exact core.handle selects that Product;
    - a sys.product_key expands to every Handle carrying that business key.

    An explicitly selected Product must itself be READY. Errors on unrelated
    Handles are ignored for this Apply run.
    """
    all_records = [
        record
        for record in prepare_plan["preview_records"]
        if gp._safe_str(record.get("sys.plan_status")) != "SKIPPED"
        and gp._safe_str(record.get("core.handle"))
    ]

    records_by_handle: Dict[str, List[Mapping[str, Any]]] = {}
    handles_by_product_key: Dict[str, List[str]] = {}
    for record in all_records:
        handle = gp._safe_str(record.get("core.handle"))
        records_by_handle.setdefault(handle, []).append(record)

        product_key = gp._safe_str(record.get("sys.product_key"))
        if product_key:
            bucket = handles_by_product_key.setdefault(product_key, [])
            if handle not in bucket:
                bucket.append(handle)

    ready_statuses = {"READY", "READY_WITH_WARNINGS"}
    ready_handles = sorted(
        handle
        for handle, records in records_by_handle.items()
        if records
        and all(
            gp._safe_str(record.get("sys.plan_status"))
            in ready_statuses
            for record in records
        )
    )

    requested = _safe_list(only_product_keys)
    if requested:
        selected: List[str] = []
        unknown: List[str] = []

        for value in requested:
            if value in records_by_handle:
                matches = [value]
            elif value in handles_by_product_key:
                matches = handles_by_product_key[value]
            else:
                unknown.append(value)
                continue

            for handle in matches:
                if handle not in selected:
                    selected.append(handle)

        if unknown:
            raise ValueError(
                "ONLY_PRODUCT_KEYS contains values that are neither "
                "core.handle nor sys.product_key in the current Input: "
                f"{sorted(unknown)}"
            )

        selected_not_ready: List[Dict[str, Any]] = []
        for handle in selected:
            records = records_by_handle.get(handle, [])
            invalid_records = [
                record
                for record in records
                if gp._safe_str(record.get("sys.plan_status"))
                not in ready_statuses
            ]
            if not invalid_records:
                continue

            selected_not_ready.append(
                {
                    "handle": handle,
                    "source_rows": [
                        gp._safe_str(record.get("sys.source_row"))
                        for record in invalid_records
                    ],
                    "statuses": sorted({
                        gp._safe_str(record.get("sys.plan_status"))
                        for record in invalid_records
                    }),
                    "validation_messages": [
                        gp._safe_str(
                            record.get("sys.validation_messages")
                        )
                        for record in invalid_records[:5]
                    ],
                }
            )

        if selected_not_ready:
            raise ValueError(
                "Selected Product Handles are not READY: "
                + json.dumps(
                    selected_not_ready[:20],
                    ensure_ascii=False,
                )
            )
    else:
        if live_mode and not apply_all_ready_products:
            raise ValueError(
                "Live Apply requires explicit ONLY_PRODUCT_KEYS "
                "(core.handle or sys.product_key), or set "
                "APPLY_ALL_READY_PRODUCTS=True."
            )
        selected = ready_handles

    if not selected:
        raise ValueError("No READY Product Handles selected.")

    if len(selected) > int(max_products):
        raise ValueError(
            f"Selected Product Handles={len(selected)} exceeds "
            f"MAX_PRODUCTS_PER_RUN={max_products}. "
            f"Handles={selected[:20]}"
        )

    return selected

def _product_rows(
    *,
    prepare_plan: Mapping[str, Any],
    selected_product_keys: Sequence[str],
) -> Dict[str, List[Dict[str, str]]]:
    """Group ready rows by core.handle.

    selected_product_keys is retained as the parameter name for Notebook/API
    compatibility, but its values are resolved Product Handles.
    """
    selected_handles = set(selected_product_keys)
    result: Dict[str, List[Dict[str, str]]] = {}
    for record in prepare_plan["preview_records"]:
        if gp._safe_str(record.get("sys.plan_status")) not in {
            "READY",
            "READY_WITH_WARNINGS",
        }:
            continue
        handle = gp._safe_str(record.get("core.handle"))
        if handle not in selected_handles:
            continue
        result.setdefault(handle, []).append(
            {
                str(key): gp._safe_str(value)
                for key, value in record.items()
            }
        )

    missing = sorted(selected_handles - set(result))
    if missing:
        raise ValueError(
            f"Selected Product Handles have no ready rows: {missing}"
        )

    for handle, rows in result.items():
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

    publish_all_channels = bool(
        gp._normalize_bool(
            first.get("publish.all_channels")
        )
    )

    # Publication association and customer visibility are separate states.
    # A Draft Product may be associated with Publications, while remaining
    # unavailable to customers until its status becomes ACTIVE.
    if (
        publish_all_channels
        and requested_status not in {"ACTIVE", "DRAFT"}
    ):
        raise ValueError(
            f"Product {product_key} requests all-channel publishing "
            f"with unsupported status={requested_status}. "
            "Supported statuses are ACTIVE and DRAFT."
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

    category_id = gp._safe_str(
        first.get("core.category_id")
    )
    if category_id:
        product_input["category"] = category_id

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

        inventory_tracker = (
            gp._safe_str(
                row.get("core.inventory_tracker")
            ).lower()
            or "shopify"
        )
        if inventory_tracker != "shopify":
            raise ValueError(
                "core.inventory_tracker currently supports only "
                "shopify."
            )

        fulfillment_service = (
            gp._safe_str(
                row.get("core.fulfillment_service")
            ).lower()
            or "manual"
        )
        if fulfillment_service != "manual":
            raise ValueError(
                "core.fulfillment_service currently supports only "
                "manual. Shopify derives fulfillment ownership from "
                "the selected inventory Location."
            )

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


def _normalize_handle_lookup(value: Any) -> str:
    """Normalize a Handle for local snapshot comparison only."""
    return gp._safe_str(value).casefold()


def _read_existing_product_handle_snapshot(
    values: Sequence[Sequence[Any]],
    *,
    header_name: str,
    tab_name: str,
) -> Dict[str, Any]:
    """Read only the Product Handle column from V_Product_Handle."""
    if not values:
        raise ValueError(
            f"{tab_name} is empty."
        )

    normalized_target = gp._safe_str(header_name).casefold()
    headers = [
        gp._safe_str(value)
        for value in values[0]
    ]
    matches = [
        index
        for index, header in enumerate(headers)
        if header.casefold() == normalized_target
    ]
    if len(matches) != 1:
        raise ValueError(
            f"{tab_name} must contain exactly one "
            f"{header_name!r} column; matches={len(matches)}; "
            f"headers={headers}."
        )

    handle_index = matches[0]
    handles_by_normalized: Dict[str, str] = {}
    nonblank_rows = 0

    for row in values[1:]:
        raw_handle = gp._safe_str(
            row[handle_index]
            if handle_index < len(row)
            else ""
        )
        if not raw_handle:
            continue
        nonblank_rows += 1
        normalized = _normalize_handle_lookup(raw_handle)
        # Duplicate snapshot rows are silently deduplicated.
        handles_by_normalized.setdefault(
            normalized,
            raw_handle,
        )

    return {
        "tab_name": tab_name,
        "header_name": header_name,
        "rows_loaded": max(0, len(values) - 1),
        "nonblank_handle_rows": nonblank_rows,
        "unique_handles": len(handles_by_normalized),
        "handles_by_normalized": handles_by_normalized,
    }


def _check_handles_against_snapshot(
    *,
    product_rows: Mapping[str, Sequence[Mapping[str, str]]],
    snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    """Compare selected core.handle values against the local snapshot only."""
    existing_lookup = snapshot["handles_by_normalized"]
    duplicates: List[Dict[str, Any]] = []
    duplicates_by_handle: Dict[str, Dict[str, Any]] = {}

    handles = sorted({
        gp._safe_str(rows[0].get("core.handle"))
        for rows in product_rows.values()
        if rows and gp._safe_str(rows[0].get("core.handle"))
    })

    for handle in handles:
        normalized = _normalize_handle_lookup(handle)
        snapshot_handle = existing_lookup.get(normalized)
        if snapshot_handle is None:
            continue

        existing_product = {
            "id": "",
            "handle": snapshot_handle,
            "title": "",
            "status": "",
            "source_tab": gp._safe_str(
                snapshot.get("tab_name")
            ),
        }
        duplicate = {
            "code": "HANDLE_ALREADY_EXISTS",
            "handle": handle,
            "existing_product": existing_product,
        }
        duplicates.append(duplicate)
        duplicates_by_handle[handle] = existing_product

    return {
        "duplicates": duplicates,
        "duplicates_by_handle": duplicates_by_handle,
        "checks": len(handles),
        "identity_field": "core.handle",
        "source_tab": gp._safe_str(
            snapshot.get("tab_name")
        ),
        "source_header": gp._safe_str(
            snapshot.get("header_name")
        ),
        "snapshot_rows_loaded": int(
            snapshot.get("rows_loaded", 0)
        ),
        "snapshot_nonblank_handle_rows": int(
            snapshot.get("nonblank_handle_rows", 0)
        ),
        "snapshot_unique_handles": int(
            snapshot.get("unique_handles", 0)
        ),
        "queried_fields": ["core.handle"],
        "shopify_handle_api_requests": 0,
        "sku_checked": False,
        "barcode_checked": False,
        "product_key_checked": False,
        "variant_key_checked": False,
        "product_id_checked": False,
        "variant_id_checked": False,
        "errors": [],
        "warnings": [],
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

def _list_all_publications(
    client: ShopifyClient,
) -> List[Dict[str, Any]]:
    """Return every Publication accessible to the Admin API token."""
    result: List[Dict[str, Any]] = []
    after: Optional[str] = None

    while True:
        data = client.gql(
            Q_PUBLICATIONS_PAGE,
            {
                "first": 100,
                "after": after,
            },
            operation_name="list_publications",
        )
        connection = data.get("publications") or {}
        nodes = connection.get("nodes") or []
        for node in nodes:
            publication_id = gp._safe_str(node.get("id"))
            if not publication_id:
                continue
            result.append(
                {
                    "id": publication_id,
                    "auto_publish": bool(node.get("autoPublish")),
                }
            )

        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = gp._safe_str(page_info.get("endCursor"))
        if not after:
            raise RuntimeError(
                "Publications pagination reported another page "
                "without an endCursor."
            )

    deduped: Dict[str, Dict[str, Any]] = {}
    for item in result:
        deduped[item["id"]] = item
    publications = list(deduped.values())

    if not publications:
        raise RuntimeError(
            "No Shopify Publications were returned. Ensure the "
            "Admin API token has read_publications access."
        )
    return publications


def _publish_product_to_all_publications(
    *,
    client: ShopifyClient,
    product_gid: str,
    publications: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    publication_ids = [
        gp._safe_str(item.get("id"))
        for item in publications
        if gp._safe_str(item.get("id"))
    ]
    if not publication_ids:
        raise ValueError("No Publication IDs are available.")

    data = client.gql(
        M_PUBLISHABLE_PUBLISH,
        {
            "id": product_gid,
            "input": [
                {"publicationId": publication_id}
                for publication_id in publication_ids
            ],
        },
        operation_name="publishablePublish_all_channels",
    )
    payload = data.get("publishablePublish") or {}
    user_errors = payload.get("userErrors") or []
    publishable = payload.get("publishable") or {}

    available_count = (
        (publishable.get("availablePublicationsCount") or {})
        .get("count")
    )
    resource_count = (
        (publishable.get("resourcePublicationsCount") or {})
        .get("count")
    )

    return {
        "publication_ids": publication_ids,
        "planned_count": len(publication_ids),
        "published_count": (
            int(resource_count)
            if resource_count is not None
            else (0 if user_errors else len(publication_ids))
        ),
        "available_count": (
            int(available_count)
            if available_count is not None
            else None
        ),
        "user_errors": user_errors,
    }


def _matrix_has_nonempty_value(
    values: Sequence[Sequence[Any]],
) -> bool:
    return any(
        gp._safe_str(cell)
        for row in values
        for cell in row
    )


def _write_result_header(
    worksheet: gspread.Worksheet,
) -> None:
    required_cols = len(RESULT_HEADERS)
    if (
        worksheet.row_count < 2
        or worksheet.col_count < required_cols
    ):
        worksheet.resize(
            rows=max(worksheet.row_count, 200),
            cols=max(worksheet.col_count, required_cols),
        )

    worksheet.update(
        range_name=(
            f"A1:{gp._a1_col(required_cols)}1"
        ),
        values=[RESULT_HEADERS],
        value_input_option="RAW",
    )
    try:
        worksheet.freeze(rows=1)
    except Exception:
        pass


def _ensure_result_header(
    spreadsheet: gspread.Spreadsheet,
    tab_name: str,
) -> gspread.Worksheet:
    """Return a Result worksheet with the exact standard header.

    Supported initialization states:
    - Tab does not exist: create it and write the header.
    - Tab exists and is completely blank: write the header.
    - Tab contains only blank rows/cells: clear and write the header.
    - Tab already has the exact header: reuse it.
    - Tab contains any non-empty incompatible content: fail explicitly.
    """
    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=tab_name,
            rows=200,
            cols=len(RESULT_HEADERS),
        )
        _write_result_header(worksheet)
        return worksheet

    values = worksheet.get_all_values()

    # gspread can return [], [[]], or multiple empty rows for a visually
    # blank worksheet. All of these are valid initialization states.
    if not values or not _matrix_has_nonempty_value(values):
        worksheet.clear()
        _write_result_header(worksheet)
        return worksheet

    current_full = [
        gp._safe_str(value)
        for value in values[0][: len(RESULT_HEADERS)]
    ]
    current_legacy = current_full[: len(LEGACY_RESULT_HEADERS)]

    if current_full == RESULT_HEADERS:
        return worksheet

    # Safe schema migration: preserve existing Result rows and append
    # the new Category / Template / Publication evidence columns.
    trailing = current_full[len(LEGACY_RESULT_HEADERS):]
    if (
        current_legacy == LEGACY_RESULT_HEADERS
        and not any(trailing)
    ):
        _write_result_header(worksheet)
        return worksheet

    raise ValueError(
        f"Result header mismatch in {tab_name}. "
        f"Expected={RESULT_HEADERS}; actual={current_full}. "
        "The Result tab contains non-empty incompatible content, "
        "so it was not overwritten."
    )



def _google_api_status(exc: BaseException) -> Optional[int]:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is None:
        status = getattr(response, "status", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _retry_after_seconds(exc: BaseException) -> Optional[float]:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None


def _google_write_with_retry(
    operation,
    *,
    action: str,
    max_retries: int = 8,
    base_seconds: float = 10.0,
    max_seconds: float = 90.0,
    print_progress: bool = True,
):
    """Retry quota/transient Sheets writes without dropping buffered rows."""
    retriable_statuses = {429, 500, 502, 503, 504}
    last_error: Optional[BaseException] = None

    for attempt in range(1, int(max_retries) + 1):
        try:
            return operation()
        except BaseException as exc:
            last_error = exc
            status = _google_api_status(exc)
            if (
                status not in retriable_statuses
                or attempt >= int(max_retries)
            ):
                raise

            wait_seconds = min(
                float(max_seconds),
                float(base_seconds) * (2 ** (attempt - 1)),
            )
            retry_after = _retry_after_seconds(exc)
            if retry_after is not None:
                wait_seconds = max(wait_seconds, retry_after)

            if print_progress:
                print(
                    "[Google Sheets write retry] "
                    f"action={action} | status={status} | "
                    f"attempt={attempt}/{max_retries} | "
                    f"wait={wait_seconds:.1f}s"
                )
            time.sleep(wait_seconds)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{action} failed without an exception.")


class _ResultBatchWriter:
    """Main-thread Result writer with quota-safe retries."""

    def __init__(
        self,
        worksheet: gspread.Worksheet,
        *,
        expected_append_rows: int = 0,
        print_progress: bool = True,
    ) -> None:
        self.worksheet = worksheet
        self.print_progress = bool(print_progress)

        values = worksheet.get_all_values()
        self.next_row = max(2, len(values) + 1)
        self.rows_written = 0
        self.flush_count = 0

        expected_append_rows = max(0, int(expected_append_rows))
        required_rows = max(
            self.next_row,
            self.next_row + expected_append_rows - 1,
        )
        required_cols = len(RESULT_HEADERS)

        if (
            worksheet.row_count < required_rows
            or worksheet.col_count < required_cols
        ):
            _google_write_with_retry(
                lambda: worksheet.resize(
                    rows=max(
                        worksheet.row_count,
                        required_rows + 100,
                    ),
                    cols=max(
                        worksheet.col_count,
                        required_cols,
                    ),
                ),
                action="pre-size Result worksheet",
                print_progress=self.print_progress,
            )

    def append(
        self,
        rows: Sequence[Sequence[Any]],
    ) -> int:
        if not rows:
            return 0

        start_row = self.next_row
        end_row = start_row + len(rows) - 1
        required_cols = len(RESULT_HEADERS)

        if (
            self.worksheet.row_count < end_row
            or self.worksheet.col_count < required_cols
        ):
            _google_write_with_retry(
                lambda: self.worksheet.resize(
                    rows=max(
                        self.worksheet.row_count,
                        end_row + 100,
                    ),
                    cols=max(
                        self.worksheet.col_count,
                        required_cols,
                    ),
                ),
                action="expand Result worksheet",
                print_progress=self.print_progress,
            )

        _google_write_with_retry(
            lambda: self.worksheet.update(
                range_name=(
                    f"A{start_row}:"
                    f"{gp._a1_col(required_cols)}{end_row}"
                ),
                values=[list(row) for row in rows],
                value_input_option="RAW",
            ),
            action=f"write Result rows {start_row}-{end_row}",
            print_progress=self.print_progress,
        )

        written = len(rows)
        self.next_row = end_row + 1
        self.rows_written += written
        self.flush_count += 1
        return written



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


def _verify_inventory_delivery_fields(
    *,
    source_rows: Sequence[Mapping[str, str]],
    product_input: Mapping[str, Any],
    returned_variants: Sequence[Mapping[str, Any]],
) -> None:
    """Verify Shopify readback for inventory and delivery fields.

    ``core.fulfillment_service=manual`` is represented by stocking the
    InventoryItem at the configured ordinary Shopify Location. Shopify's
    current ProductVariantSetInput has no direct fulfillment-service field.
    """
    matched = _match_returned_variants_by_options(
        source_rows=source_rows,
        product_input=product_input,
        returned_variants=returned_variants,
    )

    mismatches: List[str] = []
    for index, source in enumerate(source_rows):
        returned = matched[index]
        inventory_item = returned.get("inventoryItem") or {}
        expected_taxable = bool(
            gp._normalize_bool(source.get("core.taxable"))
        )
        expected_requires_shipping = bool(
            gp._normalize_bool(
                source.get("core.requires_shipping")
            )
        )
        expected_tracked = (
            gp._safe_str(
                source.get("core.inventory_tracker")
            ).lower()
            or "shopify"
        ) == "shopify"

        actual_taxable = bool(returned.get("taxable"))
        actual_requires_shipping = bool(
            inventory_item.get("requiresShipping")
        )
        actual_tracked = bool(inventory_item.get("tracked"))

        sku = gp._safe_str(source.get("core.sku"))
        if actual_taxable != expected_taxable:
            mismatches.append(
                f"SKU={sku} taxable expected={expected_taxable} "
                f"actual={actual_taxable}"
            )
        if actual_requires_shipping != expected_requires_shipping:
            mismatches.append(
                f"SKU={sku} requiresShipping "
                f"expected={expected_requires_shipping} "
                f"actual={actual_requires_shipping}"
            )
        if actual_tracked != expected_tracked:
            mismatches.append(
                f"SKU={sku} tracked expected={expected_tracked} "
                f"actual={actual_tracked}"
            )

        fulfillment_service = (
            gp._safe_str(
                source.get("core.fulfillment_service")
            ).lower()
            or "manual"
        )
        if fulfillment_service == "manual":
            location_gid = gp._safe_str(
                source.get("sys.inventory_location_gid")
            )
            levels = (
                inventory_item.get("inventoryLevels", {})
                .get("nodes", [])
            )
            matching_levels = [
                level
                for level in levels
                if gp._safe_str(
                    (level.get("location") or {}).get("id")
                ) == location_gid
            ]
            if not matching_levels:
                mismatches.append(
                    f"SKU={sku} manual fulfillment location "
                    f"was not returned for {location_gid}"
                )
            else:
                location = matching_levels[0].get("location") or {}
                if bool(location.get("isFulfillmentService")):
                    handle = gp._safe_str(
                        (location.get("fulfillmentService") or {}).get(
                            "handle"
                        )
                    )
                    mismatches.append(
                        f"SKU={sku} expected ordinary manual Location; "
                        f"returned fulfillment-service Location "
                        f"handle={handle or '(unknown)'}"
                    )

    if mismatches:
        raise RuntimeError(
            "Shopify readback mismatch for inventory/delivery fields: "
            + " | ".join(mismatches[:20])
        )


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
    publication_result: Optional[Mapping[str, Any]] = None,
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
    if product_response and status != "SKIPPED_HANDLE_EXISTS":
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
    publication_result = dict(publication_result or {})
    publication_ids = [
        gp._safe_str(value)
        for value in publication_result.get(
            "publication_ids",
            [],
        )
        if gp._safe_str(value)
    ]
    category_id = gp._safe_str(product_input.get("category"))
    template_suffix = gp._safe_str(
        product_input.get("templateSuffix")
    )
    publish_all_channels = bool(
        gp._normalize_bool(
            source_rows[0].get("publish.all_channels")
            if source_rows
            else ""
        )
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
                gp._safe_str(
                    source.get("sys.product_key")
                ) or product_key,
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
                category_id,
                template_suffix,
                "TRUE" if publish_all_channels else "FALSE",
                publication_result.get("planned_count", 0),
                publication_result.get("published_count", 0),
                ";".join(publication_ids),
            ]
        )
    return result_rows


def _result_only_product_input(
    rows: Sequence[Mapping[str, str]],
) -> Dict[str, Any]:
    """Build non-validating Result metadata for an existing Handle."""
    first = rows[0] if rows else {}
    variants: List[Dict[str, Any]] = []

    for row in rows:
        option_values: List[Dict[str, str]] = []
        for number in (1, 2, 3):
            name = gp._safe_str(
                row.get(f"core.option{number}_name")
            )
            value = gp._safe_str(
                row.get(f"core.option{number}_value")
            )
            if name and value:
                option_values.append(
                    {
                        "optionName": name,
                        "name": value,
                    }
                )
        variants.append({"optionValues": option_values})

    result: Dict[str, Any] = {
        "title": gp._safe_str(first.get("core.title")),
        "handle": gp._safe_str(first.get("core.handle")),
        "status": (
            gp._safe_str(first.get("core.status")).upper()
            or "DRAFT"
        ),
        "variants": variants,
    }

    category_id = gp._safe_str(first.get("core.category_id"))
    if category_id:
        result["category"] = category_id

    template_suffix = gp._safe_str(
        first.get("core.template_suffix")
    )
    if template_suffix:
        result["templateSuffix"] = template_suffix

    return result


def _execute_product_task(
    *,
    product_key: str,
    rows: Sequence[Mapping[str, str]],
    product_input: Mapping[str, Any],
    duplicate_match: Optional[Mapping[str, Any]],
    publish_this_product: bool,
    publications: Sequence[Mapping[str, Any]],
    dry_run: bool,
    client: ShopifyClient,
    run_id: str,
    applied_at: str,
    site_code: str,
    admin_product_base_url: str,
    storefront_product_base_url: str,
    tab_product_handle: str,
    product_handle_header: str,
) -> Dict[str, Any]:
    """Execute one Product without writing Google Sheets."""
    product_response: Optional[Dict[str, Any]] = (
        dict(duplicate_match) if duplicate_match else None
    )
    status = ""
    message = ""
    error_reason = ""
    succeeded_ops = 0
    failed_ops = 0
    publication_result: Dict[str, Any] = {
        "publication_ids": [
            gp._safe_str(item.get("id")) for item in publications
        ] if publish_this_product else [],
        "planned_count": len(publications) if publish_this_product else 0,
        "published_count": 0,
        "user_errors": [],
    }
    products_succeeded = 0
    products_failed = 0
    products_skipped_handle_exists = 0
    variants_succeeded = 0
    variants_failed = 0
    variants_skipped_handle_exists = 0

    try:
        if duplicate_match:
            status = "SKIPPED_HANDLE_EXISTS"
            products_skipped_handle_exists = 1
            variants_skipped_handle_exists = len(rows)
            publication_result = {
                "publication_ids": [],
                "planned_count": 0,
                "published_count": 0,
                "user_errors": [],
            }
            message = (
                "Handle exists in V_Product_Handle. "
                "Product was not uploaded. "
                f"handle={product_key}; "
                f"source_tab={tab_product_handle}; "
                f"source_header={product_handle_header}."
            )
        elif dry_run:
            status = "PLANNED"
            message = (
                "DRY_RUN: Shopify productSet was not called. "
                + (
                    f"All-channel Publication association planned for "
                    f"{len(publications)} Publications; "
                    + (
                        "the Product will remain unavailable to customers "
                        "while its status is DRAFT."
                        if gp._safe_str(product_input.get("status")).upper()
                        == "DRAFT"
                        else "the Product is planned as ACTIVE."
                    )
                    if publish_this_product
                    else "Channel publication is disabled."
                )
            )
        else:
            data = client.gql(
                M_PRODUCT_SET,
                {"input": dict(product_input), "synchronous": True},
                operation_name="productSet_create",
            )
            payload = data.get("productSet") or {}
            user_errors = payload.get("userErrors") or []
            if user_errors:
                raise RuntimeError(
                    "productSet userErrors: "
                    + json.dumps(user_errors, ensure_ascii=False)
                )
            raw_product = payload.get("product")
            if not raw_product:
                raise RuntimeError("productSet returned no Product.")
            product_response = dict(raw_product)
            returned_variants = product_response.get("variants", {}).get(
                "nodes", []
            )
            _verify_inventory_delivery_fields(
                source_rows=rows,
                product_input=product_input,
                returned_variants=returned_variants,
            )
            succeeded_ops = 1
            variants_succeeded = len(rows)
            if publish_this_product:
                publication_result = _publish_product_to_all_publications(
                    client=client,
                    product_gid=gp._safe_str(product_response.get("id")),
                    publications=publications,
                )
                if publication_result["user_errors"]:
                    status = "PARTIAL_FAILURE"
                    failed_ops = 1
                    products_failed = 1
                    message = (
                        "Product created, but all-channel publication "
                        "returned errors: "
                        + json.dumps(
                            publication_result["user_errors"],
                            ensure_ascii=False,
                        )
                    )
                    error_reason = "PUBLISHABLE_PUBLISH_USER_ERROR"
                else:
                    status = "SUCCESS"
                    succeeded_ops = 2
                    products_succeeded = 1
                    if gp._safe_str(product_input.get("status")).upper() == "DRAFT":
                        message = (
                            "Product and Variants created as DRAFT; associated "
                            f"with {publication_result['planned_count']} Shopify "
                            "Publications. The Product remains unavailable to "
                            "customers until its status becomes ACTIVE."
                        )
                    else:
                        message = (
                            "Product and Variants created as ACTIVE; published "
                            f"to {publication_result['planned_count']} Shopify "
                            "Publications."
                        )
            else:
                status = "SUCCESS"
                products_succeeded = 1
                message = (
                    "Product and all Variants created by synchronous "
                    "productSet; channel publication disabled."
                )
    except Exception as exc:
        status = "FAILED"
        message = str(exc)
        error_reason = type(exc).__name__
        failed_ops = 1
        products_failed = 1
        variants_failed = len(rows)

    result_rows = _result_rows_for_product(
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
        admin_product_base_url=admin_product_base_url,
        storefront_product_base_url=storefront_product_base_url,
        api_operations_planned=(
            0 if duplicate_match else (2 if publish_this_product else 1)
        ),
        api_operations_succeeded=succeeded_ops,
        api_operations_failed=failed_ops,
        publication_result=publication_result,
    )
    product_result = {
        "product_key": gp._safe_str(rows[0].get("sys.product_key")),
        "status": status,
        "message": message,
        "product_gid": gp._safe_str((product_response or {}).get("id")),
        "handle": gp._safe_str((product_response or {}).get("handle"))
        or gp._safe_str(product_input.get("handle")),
        "variants": len(rows),
        "category_id": gp._safe_str(product_input.get("category")),
        "template_suffix": gp._safe_str(
            product_input.get("templateSuffix")
        ),
        "publish_all_channels": publish_this_product,
        "publications_planned": publication_result.get("planned_count", 0),
        "publications_published": publication_result.get(
            "published_count", 0
        ),
    }
    return {
        "product_key": product_key,
        "rows": list(rows),
        "status": status,
        "message": message,
        "error_reason": error_reason,
        "product_response": product_response,
        "product_input": dict(product_input),
        "result_rows": result_rows,
        "product_result": product_result,
        "products_succeeded": products_succeeded,
        "products_failed": products_failed,
        "products_skipped_handle_exists": products_skipped_handle_exists,
        "variants_succeeded": variants_succeeded,
        "variants_failed": variants_failed,
        "variants_skipped_handle_exists": variants_skipped_handle_exists,
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
    create_sheet_label: str = "create_generic",
    runlog_sheet_label: str = "runlog_sheet",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_input: str = "Input",
    tab_defaults: str = "Defaults",
    tab_preview: str = "Preview",
    tab_product_handle: str = "V_Product_Handle",
    product_handle_header: str = "Product Handle",
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
    product_concurrency: int = 4,
    result_flush_every: int = 100,
    result_flush_min_interval_seconds: float = 15.0,
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

    product_concurrency = int(product_concurrency)
    result_flush_every = int(result_flush_every)
    result_flush_min_interval_seconds = float(
        result_flush_min_interval_seconds
    )
    if product_concurrency < 1:
        raise ValueError("PRODUCT_CONCURRENCY must be >= 1.")
    if result_flush_every < 1:
        raise ValueError("RESULT_FLUSH_EVERY must be >= 1.")
    if result_flush_min_interval_seconds < 0:
        raise ValueError(
            "RESULT_FLUSH_MIN_INTERVAL_SECONDS must be >= 0."
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
        project_code=site_code,
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
            "Read current Input, Defaults, V_Product_Handle, "
            "Cfg__Fields, and Cfg__Locations",
        )
        input_values = gp._require_worksheet(
            create_book,
            tab_input,
        ).get_all_values()
        defaults_values = gp._require_worksheet(
            create_book,
            tab_defaults,
        ).get_all_values()
        product_handle_values = gp._require_worksheet(
            create_book,
            tab_product_handle,
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
        existing_handle_snapshot = (
            _read_existing_product_handle_snapshot(
                product_handle_values,
                header_name=product_handle_header,
                tab_name=tab_product_handle,
            )
        )
        print(
            "[Existing Handle snapshot] "
            f"tab={tab_product_handle} | "
            f"header={product_handle_header} | "
            f"rows={existing_handle_snapshot['rows_loaded']} | "
            f"nonblank={existing_handle_snapshot['nonblank_handle_rows']} | "
            f"unique={existing_handle_snapshot['unique_handles']}"
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
        print(
            "[Prepare rebuild] "
            f"rows={prepare_plan['stats']['rows_loaded']} | "
            f"products={prepare_plan['stats']['product_groups']} | "
            f"errors_total={prepare_plan['stats']['error_count']} | "
            f"warnings_total={prepare_plan['stats']['warning_count']}"
        )

        progress(
            5,
            12,
            "Select ready Product Handles",
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

        progress(
            6,
            12,
            "Use current Input directly; Preview gate disabled",
        )
        preview_verification = {
            "status": "NOT_USED",
            "preview_read": False,
            "preview_compared": False,
            "reason": (
                "Apply uses current Input and compares core.handle "
                "with V_Product_Handle locally."
            ),
        }
        print(
            "[Preview gate] DISABLED | "
            f"selected_handles={len(selected_product_keys)}"
        )
        product_rows = _product_rows(
            prepare_plan=prepare_plan,
            selected_product_keys=selected_product_keys,
        )
        print(
            "[Selection] "
            f"products={len(selected_product_keys)} | "
            f"variants={sum(len(rows) for rows in product_rows.values())} | "
            f"handles={selected_product_keys}"
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
            7,
            12,
            "Resolve Shopify token and initialize client",
        )
        runtime_token_secret_name = _project_secret_name_for_runtime(
            token_secret_name,
            site_code,
        )
        if print_progress:
            print(
                "[Shopify Secret] "
                f"configured_name={token_secret_name} | "
                f"runtime_name={runtime_token_secret_name} | "
                f"runtime={gp._runtime_mode()}"
            )
        shopify_secret = gp.read_secret(
            runtime_token_secret_name,
            project_code=site_code,
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
            8,
            12,
            "Compare core.handle with V_Product_Handle locally",
        )
        conflict_result = _check_handles_against_snapshot(
            product_rows=product_rows,
            snapshot=existing_handle_snapshot,
        )
        duplicate_handles = set(
            conflict_result["duplicates_by_handle"]
        )
        print(
            "[Handle check result] "
            f"source={tab_product_handle}.{product_handle_header} | "
            f"checked={conflict_result['checks']} | "
            f"existing={len(duplicate_handles)} | "
            f"new={len(selected_product_keys) - len(duplicate_handles)} | "
            "shopify_handle_api_requests=0"
        )

        upload_product_rows = {
            handle: rows
            for handle, rows in product_rows.items()
            if handle not in duplicate_handles
        }
        if upload_product_rows:
            _attach_location_gids(
                product_rows=upload_product_rows,
                locations=locations,
            )

        progress(
            9,
            12,
            "Build payloads for new Handles only",
        )
        product_inputs: Dict[str, Dict[str, Any]] = {}
        for handle in selected_product_keys:
            rows = product_rows[handle]
            if handle in duplicate_handles:
                product_inputs[handle] = (
                    _result_only_product_input(rows)
                )
                continue

            product_inputs[handle] = _build_product_set_input(
                product_key=handle,
                rows=rows,
                ordered_field_keys=prepare_plan[
                    "ordered_field_keys"
                ],
                cfg_fields=cfg_fields,
                allow_non_draft_status=(
                    allow_non_draft_status
                ),
            )

        publish_product_keys = {
            handle
            for handle, rows in upload_product_rows.items()
            if rows
            and gp._normalize_bool(
                rows[0].get("publish.all_channels")
            ) is True
        }
        publications: List[Dict[str, Any]] = []
        if publish_product_keys:
            print(
                "[Publications] resolving all accessible "
                "Shopify channels for new Handles only..."
            )
            publications = _list_all_publications(client)
            print(
                "[Publications] "
                f"count={len(publications)} | "
                f"auto_publish="
                f"{sum(1 for item in publications if item['auto_publish'])}"
            )

        progress(
            10,
            12,
            (
                "DRY RUN concurrent plan"
                if dry_run
                else (
                    "Create Shopify Products concurrently "
                    f"| workers={product_concurrency}"
                )
            ),
        )
        result_rows: List[List[Any]] = []
        result_rows_buffer: List[List[Any]] = []
        product_results: List[Dict[str, Any]] = []
        products_succeeded = 0
        products_failed = 0
        products_skipped_handle_exists = 0
        variants_succeeded = 0
        variants_failed = 0
        variants_skipped_handle_exists = 0
        result_rows_written = 0
        result_flush_count = 0
        products_since_result_flush = 0

        applied_at = gp._now_str(tz_name)
        admin_product_base_url = gp._safe_str(
            account.get("ADMIN_PRODUCT_BASE_URL")
        )
        storefront_product_base_url = gp._safe_str(
            account.get("STOREFRONT_PRODUCT_BASE_URL")
        )
        result_writer: Optional[_ResultBatchWriter] = None
        if write_result:
            result_ws = _ensure_result_header(create_book, tab_result)
            result_writer = _ResultBatchWriter(
                result_ws,
                expected_append_rows=sum(
                    len(rows)
                    for rows in product_rows.values()
                ),
                print_progress=print_progress,
            )

        total_products = len(selected_product_keys)
        completed_products = 0
        stop_requested = False
        processing_started = time.monotonic()
        last_result_flush_at = processing_started

        def submit_product(executor: ThreadPoolExecutor, product_key: str):
            rows = product_rows[product_key]
            duplicate_match = conflict_result["duplicates_by_handle"].get(
                product_key
            )
            publish_this_product = (
                product_key in publish_product_keys
                and duplicate_match is None
            )
            return executor.submit(
                _execute_product_task,
                product_key=product_key,
                rows=rows,
                product_input=product_inputs[product_key],
                duplicate_match=duplicate_match,
                publish_this_product=publish_this_product,
                publications=publications,
                dry_run=dry_run,
                client=client,
                run_id=run_id,
                applied_at=applied_at,
                site_code=site_code,
                admin_product_base_url=admin_product_base_url,
                storefront_product_base_url=storefront_product_base_url,
                tab_product_handle=tab_product_handle,
                product_handle_header=product_handle_header,
            )

        key_iterator = iter(selected_product_keys)
        worker_count = min(product_concurrency, max(1, total_products))
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="shopify-create",
        ) as executor:
            pending = {}
            while len(pending) < worker_count:
                try:
                    key = next(key_iterator)
                except StopIteration:
                    break
                pending[submit_product(executor, key)] = key

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    product_key = pending.pop(future)
                    try:
                        outcome = future.result()
                    except BaseException as exc:
                        rows = product_rows[product_key]
                        product_input = product_inputs[product_key]
                        outcome = {
                            "product_key": product_key,
                            "rows": list(rows),
                            "status": "FAILED",
                            "message": str(exc),
                            "error_reason": type(exc).__name__,
                            "product_response": None,
                            "product_input": product_input,
                            "result_rows": _result_rows_for_product(
                                run_id=run_id,
                                applied_at=applied_at,
                                site_code=site_code,
                                runtime_mode=gp._runtime_mode(),
                                dry_run=dry_run,
                                product_key=product_key,
                                source_rows=rows,
                                status="FAILED",
                                message=str(exc),
                                error_reason=type(exc).__name__,
                                product_response=None,
                                product_input=product_input,
                                admin_product_base_url=admin_product_base_url,
                                storefront_product_base_url=(
                                    storefront_product_base_url
                                ),
                                api_operations_planned=1,
                                api_operations_succeeded=0,
                                api_operations_failed=1,
                                publication_result={
                                    "publication_ids": [],
                                    "planned_count": 0,
                                    "published_count": 0,
                                },
                            ),
                            "product_result": {
                                "product_key": gp._safe_str(
                                    rows[0].get("sys.product_key")
                                ),
                                "status": "FAILED",
                                "message": str(exc),
                                "product_gid": "",
                                "handle": product_key,
                                "variants": len(rows),
                                "category_id": "",
                                "template_suffix": "",
                                "publish_all_channels": False,
                                "publications_planned": 0,
                                "publications_published": 0,
                            },
                            "products_succeeded": 0,
                            "products_failed": 1,
                            "products_skipped_handle_exists": 0,
                            "variants_succeeded": 0,
                            "variants_failed": len(rows),
                            "variants_skipped_handle_exists": 0,
                        }

                    completed_products += 1
                    rows = outcome["rows"]
                    status = outcome["status"]
                    products_succeeded += outcome["products_succeeded"]
                    products_failed += outcome["products_failed"]
                    products_skipped_handle_exists += outcome[
                        "products_skipped_handle_exists"
                    ]
                    variants_succeeded += outcome["variants_succeeded"]
                    variants_failed += outcome["variants_failed"]
                    variants_skipped_handle_exists += outcome[
                        "variants_skipped_handle_exists"
                    ]
                    product_results.append(outcome["product_result"])
                    result_rows.extend(outcome["result_rows"])
                    result_rows_buffer.extend(outcome["result_rows"])
                    products_since_result_flush += 1

                    logger.log(
                        phase=phase,
                        log_type="detail",
                        status=status,
                        entity_type="PRODUCT",
                        gid=gp._safe_str(
                            (outcome["product_response"] or {}).get("id")
                        ),
                        rows_loaded=prepare_plan["stats"]["rows_loaded"],
                        rows_pending=sum(
                            len(items) for items in product_rows.values()
                        ),
                        rows_recognized=prepare_plan["stats"][
                            "rows_recognized"
                        ],
                        rows_planned=len(rows),
                        rows_written=len(rows) if status == "SUCCESS" else 0,
                        rows_skipped=(
                            len(rows)
                            if status
                            in {"PLANNED", "FAILED", "SKIPPED_HANDLE_EXISTS"}
                            else 0
                        ),
                        message=(
                            f"handle={product_key} | source_product_key="
                            f"{gp._safe_str(rows[0].get('sys.product_key'))} | "
                            f"{outcome['message']}"
                        ),
                        error_reason=outcome["error_reason"],
                    )

                    flush_interval_elapsed = (
                        time.monotonic()
                        - last_result_flush_at
                    ) >= result_flush_min_interval_seconds
                    final_completion = (
                        completed_products == total_products
                    )
                    if (
                        result_writer is not None
                        and (
                            (
                                products_since_result_flush
                                >= result_flush_every
                                and flush_interval_elapsed
                            )
                            or final_completion
                        )
                    ):
                        written = result_writer.append(result_rows_buffer)
                        result_rows_written += written
                        result_flush_count += 1
                        if print_progress:
                            print(
                                "[Result flush] "
                                f"batch={result_flush_count} | "
                                f"products={products_since_result_flush} | "
                                f"rows={written} | total_rows_written="
                                f"{result_rows_written}"
                            )
                        result_rows_buffer.clear()
                        products_since_result_flush = 0
                        last_result_flush_at = time.monotonic()

                    if status == "FAILED" and stop_on_first_error:
                        stop_requested = True

                    if product_progress_every and (
                        completed_products % product_progress_every == 0
                        or completed_products == total_products
                    ):
                        processing_elapsed = max(
                            0.001, time.monotonic() - processing_started
                        )
                        rate = completed_products / processing_elapsed
                        remaining = max(0, total_products - completed_products)
                        eta_seconds = remaining / rate if rate > 0 else 0
                        print(
                            "[Apply progress] "
                            f"completed={completed_products}/{total_products} | "
                            f"running={len(pending)} | "
                            f"success={products_succeeded} | "
                            f"failed={products_failed} | "
                            f"handle_exists_skipped="
                            f"{products_skipped_handle_exists} | "
                            f"result_buffer_products="
                            f"{products_since_result_flush}/"
                            f"{result_flush_every} | "
                            f"flush_min_interval="
                            f"{result_flush_min_interval_seconds:.0f}s | "
                            f"rate={rate:.2f}/s | eta={eta_seconds:.0f}s"
                        )

                while not stop_requested and len(pending) < worker_count:
                    try:
                        key = next(key_iterator)
                    except StopIteration:
                        break
                    pending[submit_product(executor, key)] = key

        if result_writer is not None and result_rows_buffer:
            written = result_writer.append(result_rows_buffer)
            result_rows_written += written
            result_flush_count += 1
            if print_progress:
                print(
                    "[Result flush] "
                    f"batch={result_flush_count} | "
                    f"products={products_since_result_flush} | "
                    f"rows={written} | total_rows_written="
                    f"{result_rows_written}"
                )
            result_rows_buffer.clear()
            products_since_result_flush = 0

        progress(11, 12, "Finalize Result and RunLog evidence")

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
                f"products_skipped_handle_exists="
                f"{products_skipped_handle_exists} | "
                f"variants_succeeded={variants_succeeded} | "
                f"variants_failed={variants_failed} | "
                f"result_rows_written={result_rows_written} | "
                f"result_flushes={result_flush_count} | "
                f"product_concurrency={worker_count} | "
                f"products_completed={completed_products} | "
                f"shopify_requests={client.request_count} | "
                f"shopify_retries={client.retry_count}"
            ),
            error_reason=(
                "PRODUCT_CREATE_FAILURE"
                if products_failed
                else ""
            ),
        )
        _google_write_with_retry(
            logger.flush,
            action="write final RunLog",
            print_progress=print_progress,
        )

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
            "selected_product_keys": sorted({
                gp._safe_str(row.get("sys.product_key"))
                for rows in product_rows.values()
                for row in rows
                if gp._safe_str(row.get("sys.product_key"))
            }),
            "selected_product_handles": selected_product_keys,
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
                "input_warning_count_total": prepare_plan["stats"][
                    "warning_count"
                ],
                "input_error_count_total": prepare_plan["stats"][
                    "error_count"
                ],
                "unselected_input_errors_ignored": prepare_plan["stats"][
                    "error_count"
                ],
                "business_objects_planned": len(
                    selected_product_keys
                ),
                "api_operations_planned": sum(
                    (
                        0
                        if key in duplicate_handles
                        else (
                            2
                            if key in publish_product_keys
                            else 1
                        )
                    )
                    for key in selected_product_keys
                ),
                "api_operations_succeeded": sum(
                    (
                        0
                        if dry_run
                        else (
                            2
                            if item["status"] == "SUCCESS"
                            and item["publish_all_channels"]
                            else (
                                1
                                if item["status"] in {
                                    "SUCCESS",
                                    "PARTIAL_FAILURE",
                                }
                                else 0
                            )
                        )
                    )
                    for item in product_results
                ),
                "api_operations_failed": products_failed,
                "products_succeeded": products_succeeded,
                "products_failed": products_failed,
                "products_skipped_handle_exists": (
                    products_skipped_handle_exists
                ),
                "variants_succeeded": variants_succeeded,
                "variants_skipped_handle_exists": (
                    variants_skipped_handle_exists
                ),
                "variants_failed": variants_failed,
                "result_rows_written": result_rows_written,
                "result_flush_count": result_flush_count,
                "result_flush_every": result_flush_every,
                "result_flush_min_interval_seconds": (
                    result_flush_min_interval_seconds
                ),
                "product_concurrency": worker_count,
                "products_completed": completed_products,
                "products_not_processed": max(
                    0, total_products - completed_products
                ),
                "shopify_requests": client.request_count,
                "shopify_retries": client.retry_count,
                "publications_available": len(publications),
                "products_publish_all_channels": len(
                    publish_product_keys
                ),
                "elapsed_seconds": elapsed,
            },
            "products": product_results,
            "warnings": [],
            "handle_check": {
                "identity_field": "core.handle",
                "source_type": "GOOGLE_SHEETS_SNAPSHOT",
                "source_tab": tab_product_handle,
                "source_header": product_handle_header,
                "snapshot_rows_loaded": conflict_result[
                    "snapshot_rows_loaded"
                ],
                "snapshot_nonblank_handle_rows": conflict_result[
                    "snapshot_nonblank_handle_rows"
                ],
                "snapshot_unique_handles": conflict_result[
                    "snapshot_unique_handles"
                ],
                "checked": conflict_result["checks"],
                "existing_count": len(
                    conflict_result["duplicates"]
                ),
                "duplicates": conflict_result["duplicates"],
                "shopify_handle_api_requests": 0,
                "sku_checked": False,
                "barcode_checked": False,
                "product_key_checked": False,
                "variant_key_checked": False,
                "product_id_checked": False,
                "variant_id_checked": False,
            },
            "preview_verification": preview_verification,
            "preview_gate_disabled": True,
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
                "product_concurrency": worker_count,
                "result_flush_every": result_flush_every,
                "result_flush_min_interval_seconds": (
                    result_flush_min_interval_seconds
                ),
            },
            "targets": {
                "create_sheet_url": create_url,
                "product_handle_tab": tab_product_handle,
                "product_handle_header": product_handle_header,
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
            _google_write_with_retry(
                logger.flush,
                action="write failure RunLog",
                print_progress=print_progress,
            )
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
    parser.add_argument("--product-concurrency", type=int, default=4)
    parser.add_argument("--result-flush-every", type=int, default=100)
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
        product_concurrency=args.product_concurrency,
        result_flush_every=args.result_flush_every,
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
