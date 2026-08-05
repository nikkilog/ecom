# -*- coding: utf-8 -*-
"""Apply validated SPU Product Creation plans to Shopify.

Formal repository target:
    ecom/shopify_create/7_4_3_spu_product_apply.py
Import path:
    shopify_create.7_4_3_spu_product_apply

Execution contract
------------------
1. Rebuild the current 7.4.2 SPU Prepare plan from Input / Defaults /
   Cfg__Fields / Cfg__Locations / V_V_Handle. Preview is then verified as a
   human-reviewed snapshot for the selected Product groups.
2. CREATE Product group identity = SPU-V / sys.product_group_key. CREATE uses
   the validated Generic Apply ``productSet`` payload builder so Product core,
   Product metafields, Variant fields, Variant metafields and inventory keep
   the existing Generic Apply contract.
3. ADD Product group identity = sys.target_product_id. ADD never mutates
   Product-level fields; it only adds new Variants to the existing Product by
   ``productVariantsBulkCreate``.
4. CREATE does an Apply-time Shopify handle lookup. Existing handle => skipped,
   not recreated.
5. ADD does an Apply-time Shopify Product/Variant read. Existing SKU with the
   same option combination => skipped for idempotent retry. A conflicting SKU
   or already-used option combination blocks that ADD Product group.
6. DRY_RUN performs Shopify reads and builds exact payloads, but performs no
   Shopify mutations. Live writes require dry_run=False and confirmed=True.
7. Product-group failures are isolated unless stop_on_first_error=True.
8. Result owns a dedicated SPU schema. If Result still contains a different
   (for example old Generic Apply) schema, its values are cleared once and the
   SPU header is initialized before writing current results.
9. Input is read-only; Preview is read-only; Result and Ops__RunLog are the
   only Sheet write targets.
10. Images/media are out of scope.
"""
from __future__ import annotations

import argparse
import importlib
import json
import math
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


MODULE_VERSION = "2026-08-05-spu-apply-create-add-v1"
MODULE_PATH = "shopify_create.7_4_3_spu_product_apply"
DEFAULT_JOB_NAME = "spu_product_apply"

PREPARE_MODULE_PATH = "shopify_create.7_4_2_spu_product_prepare"
EXPECTED_PREPARE_MODULE_VERSION = "2026-08-05-v-v-handle-product-size-v2"
GENERIC_APPLY_MODULE_PATH = "shopify_create.7_1_2_generic_product_apply"
EXPECTED_GENERIC_APPLY_MODULE_VERSION = "2026-08-02-runtime-boundary-v1"
GENERIC_PREPARE_MODULE_PATH = "shopify_create.7_1_1_generic_product_prepare"
EXPECTED_GENERIC_PREPARE_MODULE_VERSION = "2026-08-02-runtime-boundary-v1"

SPU_RESULT_HEADERS = [
    "run_id",
    "applied_at",
    "site_code",
    "runtime_mode",
    "dry_run",
    "action",
    "product_group_key",
    "target_product_id",
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
    "category_id",
    "template_suffix",
    "publish_all_channels",
    "publications_planned",
    "publications_published",
    "publication_ids",
]

Q_PRODUCT_BY_HANDLE = """
query SPUProductByHandle($handle: String!) {
  productByHandle(handle: $handle) {
    id
    handle
    title
    status
  }
}
"""

Q_PRODUCT_CURRENT = """
query SPUProductCurrent($id: ID!, $after: String) {
  product(id: $id) {
    id
    handle
    title
    status
    options {
      id
      name
      position
      optionValues {
        id
        name
      }
    }
    variants(first: 250, after: $after) {
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
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""

M_PRODUCT_VARIANTS_BULK_CREATE = """
mutation SPUAddVariants(
  $productId: ID!,
  $variants: [ProductVariantsBulkInput!]!,
  $strategy: ProductVariantsBulkCreateStrategy
) {
  productVariantsBulkCreate(
    productId: $productId,
    variants: $variants,
    strategy: $strategy
  ) {
    product {
      id
      handle
      title
      status
    }
    productVariants {
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

Q_PUBLICATIONS_PAGE = """
query SPUPublicationsPage($first: Int!, $after: String) {
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
mutation SPUPublishProduct($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    publishable {
      availablePublicationsCount { count }
      resourcePublicationsCount { count }
    }
    userErrors {
      field
      message
    }
  }
}
"""


def _load_module(path: str, expected_version: str, label: str):
    module = importlib.import_module(path)
    loaded = _safe_str(getattr(module, "MODULE_VERSION", ""))
    if loaded != expected_version:
        raise RuntimeError(
            f"SPU Apply requires {label} version {expected_version}; "
            f"loaded={loaded or '<blank>'}."
        )
    return module


def _sp():
    return _load_module(
        PREPARE_MODULE_PATH,
        EXPECTED_PREPARE_MODULE_VERSION,
        "SPU Prepare",
    )


def _ga():
    return _load_module(
        GENERIC_APPLY_MODULE_PATH,
        EXPECTED_GENERIC_APPLY_MODULE_VERSION,
        "Generic Apply donor",
    )


def _gp():
    return _load_module(
        GENERIC_PREPARE_MODULE_PATH,
        EXPECTED_GENERIC_PREPARE_MODULE_VERSION,
        "Generic Prepare infrastructure",
    )


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    return str(value).strip()


def _safe_list(values: Optional[Iterable[Any]]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values or []:
        text = _safe_str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _product_gid(value: Any) -> str:
    text = _safe_str(value)
    if not text:
        return ""
    if text.startswith("gid://shopify/Product/"):
        return text
    if text.isdigit():
        return f"gid://shopify/Product/{text}"
    raise ValueError(f"Invalid Shopify Product ID: {value!r}")


def _numeric_gid(value: Any) -> str:
    text = _safe_str(value)
    return text.rsplit("/", 1)[-1] if "/" in text else text


def _option_pairs(row: Mapping[str, Any]) -> Tuple[Tuple[str, str], ...]:
    pairs: List[Tuple[str, str]] = []
    for number in (1, 2, 3):
        name = _safe_str(row.get(f"core.option{number}_name"))
        value = _safe_str(row.get(f"core.option{number}_value"))
        if not name and not value:
            continue
        if not name or not value:
            raise ValueError(
                f"Incomplete Option {number} for SKU={row.get('core.sku')!r}."
            )
        pairs.append((name, value))
    return tuple(pairs)


def _option_text(row: Mapping[str, Any]) -> str:
    return ";".join(f"{name}={value}" for name, value in _option_pairs(row))


def _selected_option_pairs(variant: Mapping[str, Any]) -> Tuple[Tuple[str, str], ...]:
    return tuple(
        (
            _safe_str(item.get("name")),
            _safe_str(item.get("value")),
        )
        for item in (variant.get("selectedOptions") or [])
        if _safe_str(item.get("name")) or _safe_str(item.get("value"))
    )


def _select_group_keys(
    *,
    prepare_plan: Mapping[str, Any],
    only_product_group_keys: Optional[Iterable[str]],
    apply_all_ready_groups: bool,
    max_product_groups_per_run: int,
    live_mode: bool,
) -> List[str]:
    ready = set(_safe_list(prepare_plan.get("ready_groups")))
    group_status = {
        _safe_str(key): _safe_str(value)
        for key, value in (prepare_plan.get("group_status") or {}).items()
    }

    ordered_ready: List[str] = []
    for record in prepare_plan.get("preview_records", []):
        key = _safe_str(record.get("sys.product_group_key"))
        if key in ready and key not in ordered_ready:
            ordered_ready.append(key)

    requested = _safe_list(only_product_group_keys)
    if requested:
        unknown = [key for key in requested if key not in group_status]
        if unknown:
            raise ValueError(
                "ONLY_PRODUCT_GROUP_KEYS contains unknown Product groups: "
                f"{unknown}"
            )
        not_ready = [key for key in requested if group_status.get(key) != "READY"]
        if not_ready:
            raise ValueError(
                "Selected Product groups are not READY: "
                + json.dumps(
                    {key: group_status.get(key) for key in not_ready},
                    ensure_ascii=False,
                )
            )
        selected = requested
    else:
        if live_mode and not apply_all_ready_groups:
            raise ValueError(
                "Live SPU Apply requires explicit ONLY_PRODUCT_GROUP_KEYS, "
                "or APPLY_ALL_READY_GROUPS=True."
            )
        selected = ordered_ready

    if not selected:
        raise ValueError("No READY SPU Product groups selected.")

    cap = int(max_product_groups_per_run or 0)
    if cap > 0 and len(selected) > cap:
        raise ValueError(
            f"Selected Product groups={len(selected)} exceeds "
            f"MAX_PRODUCT_GROUPS_PER_RUN={cap}."
        )
    return selected


def _group_rows(
    *,
    prepare_plan: Mapping[str, Any],
    selected_group_keys: Sequence[str],
) -> Dict[str, List[Dict[str, str]]]:
    selected = set(selected_group_keys)
    groups: Dict[str, List[Dict[str, str]]] = {}
    for raw in prepare_plan.get("preview_records", []):
        if _safe_str(raw.get("sys.plan_status")) != "READY":
            continue
        key = _safe_str(raw.get("sys.product_group_key"))
        if key not in selected:
            continue
        groups.setdefault(key, []).append(
            {str(k): _safe_str(v) for k, v in raw.items()}
        )
    missing = sorted(selected - set(groups))
    if missing:
        raise ValueError(f"Selected READY groups have no rows: {missing}")
    for rows in groups.values():
        rows.sort(key=lambda row: int(_safe_str(row.get("sys.source_row")) or "0"))
    return groups


def _read_preview_contract(values: Sequence[Sequence[Any]]) -> Dict[str, Any]:
    if len(values) < 2:
        raise ValueError("Preview requires two header rows. Run 7.4.2 first.")
    max_cols = max(len(values[0]), len(values[1]))
    keys = [
        _safe_str(values[1][i] if i < len(values[1]) else "")
        for i in range(max_cols)
    ]
    while max_cols and not _safe_str(values[0][max_cols - 1] if max_cols - 1 < len(values[0]) else "") and not keys[-1]:
        keys.pop()
        max_cols -= 1
    active = [key for key in keys if key]
    duplicates = sorted(key for key in set(active) if active.count(key) > 1)
    if duplicates:
        raise ValueError(f"Preview contains duplicate field_key values: {duplicates}")
    records: List[Dict[str, str]] = []
    for sheet_row, raw in enumerate(values[2:], start=3):
        padded = list(raw) + [""] * max(0, max_cols - len(raw))
        record = {keys[i]: _safe_str(padded[i]) for i in range(max_cols) if keys[i]}
        if not any(record.values()):
            continue
        record["__preview_sheet_row"] = str(sheet_row)
        records.append(record)
    return {"field_keys": active, "records": records}


def _verify_preview_snapshot(
    *,
    prepare_plan: Mapping[str, Any],
    preview_contract: Mapping[str, Any],
    selected_group_keys: Sequence[str],
) -> Dict[str, Any]:
    selected = set(selected_group_keys)
    current: Dict[str, Dict[str, str]] = {}
    for record in prepare_plan.get("preview_records", []):
        if _safe_str(record.get("sys.product_group_key")) not in selected:
            continue
        source_row = _safe_str(record.get("sys.source_row"))
        if not source_row:
            continue
        if source_row in current:
            raise ValueError(f"Current SPU plan has duplicate source_row={source_row}.")
        current[source_row] = {str(k): _safe_str(v) for k, v in record.items()}

    preview: Dict[str, Dict[str, str]] = {}
    for record in preview_contract.get("records", []):
        if _safe_str(record.get("sys.product_group_key")) not in selected:
            continue
        source_row = _safe_str(record.get("sys.source_row"))
        if not source_row:
            continue
        if source_row in preview:
            raise ValueError(f"Preview has duplicate selected source_row={source_row}.")
        preview[source_row] = {str(k): _safe_str(v) for k, v in record.items()}

    if set(current) != set(preview):
        raise ValueError(
            "Preview no longer matches the selected current SPU plan. "
            f"current_only={sorted(set(current)-set(preview))}; "
            f"preview_only={sorted(set(preview)-set(current))}. Rerun 7.4.2."
        )

    compare_fields = list(prepare_plan.get("ordered_field_keys", [])) + [
        "sys.plan_status",
        "sys.product_group_key",
        "sys.error_count",
        "sys.warning_count",
    ]
    mismatches: List[str] = []
    for source_row in sorted(current, key=lambda value: int(value)):
        a = current[source_row]
        b = preview[source_row]
        for field_key in compare_fields:
            av = _safe_str(a.get(field_key))
            bv = _safe_str(b.get(field_key))
            if av != bv:
                mismatches.append(
                    f"source_row={source_row}:{field_key}:Preview={bv!r}:Current={av!r}"
                )
                if len(mismatches) >= 20:
                    break
        if len(mismatches) >= 20:
            break
    if mismatches:
        raise ValueError(
            "Preview is stale or was edited for selected SPU groups. "
            f"Rerun 7.4.2. First mismatches={mismatches}"
        )
    invalid = sorted(
        {
            _safe_str(record.get("sys.plan_status"))
            for record in current.values()
            if _safe_str(record.get("sys.plan_status")) != "READY"
        }
    )
    if invalid:
        raise ValueError(f"Selected Preview rows are not READY: {invalid}")
    return {
        "row_count": len(current),
        "product_groups": len(selected),
        "selected_group_keys": list(selected_group_keys),
        "verified_fields": len(compare_fields),
    }


def _fetch_product_by_handle(client, handle: str) -> Optional[Dict[str, Any]]:
    data = client.gql(
        Q_PRODUCT_BY_HANDLE,
        {"handle": handle},
        operation_name="spu_product_by_handle",
    )
    product = data.get("productByHandle")
    return dict(product) if product else None


def _fetch_product_current(client, product_gid: str) -> Dict[str, Any]:
    after: Optional[str] = None
    product_base: Optional[Dict[str, Any]] = None
    variants: List[Dict[str, Any]] = []
    while True:
        data = client.gql(
            Q_PRODUCT_CURRENT,
            {"id": product_gid, "after": after},
            operation_name="spu_product_current",
        )
        product = data.get("product")
        if not product:
            raise ValueError(f"Target Shopify Product does not exist: {product_gid}")
        if product_base is None:
            product_base = {
                key: value
                for key, value in product.items()
                if key != "variants"
            }
        connection = product.get("variants") or {}
        variants.extend(dict(node) for node in (connection.get("nodes") or []))
        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = _safe_str(page_info.get("endCursor")) or None
        if not after:
            raise RuntimeError("Shopify variants pagination hasNextPage without endCursor.")
    assert product_base is not None
    product_base["variants"] = variants
    return product_base


def _list_publications(client) -> List[str]:
    ids: List[str] = []
    after: Optional[str] = None
    while True:
        data = client.gql(
            Q_PUBLICATIONS_PAGE,
            {"first": 100, "after": after},
            operation_name="spu_publications_page",
        )
        connection = data.get("publications") or {}
        for node in connection.get("nodes") or []:
            publication_id = _safe_str(node.get("id"))
            if publication_id and publication_id not in ids:
                ids.append(publication_id)
        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = _safe_str(page_info.get("endCursor")) or None
        if not after:
            raise RuntimeError("Publications pagination hasNextPage without endCursor.")
    return ids


def _publish_product(client, product_gid: str, publication_ids: Sequence[str]) -> int:
    ids = _safe_list(publication_ids)
    if not ids:
        return 0
    data = client.gql(
        M_PUBLISHABLE_PUBLISH,
        {
            "id": product_gid,
            "input": [{"publicationId": value} for value in ids],
        },
        operation_name="spu_publishable_publish",
    )
    payload = data.get("publishablePublish") or {}
    errors = payload.get("userErrors") or []
    if errors:
        raise RuntimeError(
            "publishablePublish userErrors: "
            + json.dumps(errors, ensure_ascii=False)
        )
    return len(ids)


def _build_add_variant_inputs(
    *,
    rows: Sequence[Mapping[str, str]],
    ordered_field_keys: Sequence[str],
    cfg_fields: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    ga = _ga()
    gp = _gp()
    if not rows:
        return []
    _, option_values = ga._build_options(rows)
    result: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        inventory_tracker = (
            _safe_str(row.get("core.inventory_tracker")).lower() or "shopify"
        )
        if inventory_tracker != "shopify":
            raise ValueError("ADD supports core.inventory_tracker=shopify only.")
        fulfillment_service = (
            _safe_str(row.get("core.fulfillment_service")).lower() or "manual"
        )
        if fulfillment_service != "manual":
            raise ValueError("ADD supports core.fulfillment_service=manual only.")

        variant: Dict[str, Any] = {
            "optionValues": option_values[index],
            "price": _safe_str(row.get("core.price")),
            "inventoryPolicy": (
                _safe_str(row.get("core.inventory_policy")).upper() or "DENY"
            ),
            "taxable": bool(gp._normalize_bool(row.get("core.taxable"))),
        }
        barcode = _safe_str(row.get("core.barcode"))
        if barcode:
            variant["barcode"] = barcode
        compare_at = _safe_str(row.get("core.compare_at_price"))
        if compare_at:
            variant["compareAtPrice"] = compare_at

        inventory_item: Dict[str, Any] = {
            "sku": _safe_str(row.get("core.sku")),
            "requiresShipping": bool(
                gp._normalize_bool(row.get("core.requires_shipping"))
            ),
            "tracked": True,
        }
        cost = _safe_str(row.get("core.cost"))
        if cost:
            inventory_item["cost"] = cost
        weight = _safe_str(row.get("core.weight"))
        if weight:
            inventory_item["measurement"] = {
                "weight": {
                    "value": float(Decimal(weight)),
                    "unit": ga._weight_unit_enum(row.get("core.weight_unit")),
                }
            }
        variant["inventoryItem"] = inventory_item

        location_gid = _safe_str(row.get("sys.inventory_location_gid"))
        if not location_gid:
            raise ValueError(
                f"ADD SKU={row.get('core.sku')} has no resolved Location GID."
            )
        variant["inventoryQuantities"] = [
            {
                "locationId": location_gid,
                "name": "available",
                "quantity": int(_safe_str(row.get("inventory.quantity")) or "0"),
            }
        ]
        metafields = ga._variant_metafields(
            row=row,
            ordered_field_keys=ordered_field_keys,
            cfg_fields=cfg_fields,
        )
        if metafields:
            variant["metafields"] = metafields
        result.append(variant)
    return result


def _create_result_row(
    *,
    run_id: str,
    applied_at: str,
    site_code: str,
    runtime_mode: str,
    dry_run: bool,
    action: str,
    product_group_key: str,
    target_product_id: str,
    source: Mapping[str, str],
    apply_status: str,
    product_gid: str,
    product_handle: str,
    product_title: str,
    product_status: str,
    variant_gid: str = "",
    inventory_item_gid: str = "",
    product_metafields_planned: int = 0,
    variant_metafields_planned: int = 0,
    api_operations_planned: int = 0,
    api_operations_succeeded: int = 0,
    api_operations_failed: int = 0,
    message: str = "",
    error_reason: str = "",
    admin_product_base_url: str = "",
    storefront_product_base_url: str = "",
    publications_planned: int = 0,
    publications_published: int = 0,
    publication_ids: Optional[Sequence[str]] = None,
) -> List[Any]:
    admin_url = ""
    if product_gid and admin_product_base_url:
        admin_url = admin_product_base_url + _numeric_gid(product_gid)
    storefront_url = ""
    if product_handle and storefront_product_base_url:
        storefront_url = storefront_product_base_url + product_handle
    return [
        run_id,
        applied_at,
        site_code,
        runtime_mode,
        "TRUE" if dry_run else "FALSE",
        action,
        product_group_key,
        target_product_id,
        _safe_str(source.get("sys.product_key")),
        _safe_str(source.get("sys.variant_key")),
        _safe_str(source.get("sys.source_row")),
        apply_status,
        product_gid,
        product_handle,
        product_title,
        product_status,
        variant_gid,
        inventory_item_gid,
        _safe_str(source.get("core.sku")),
        _safe_str(source.get("core.barcode")),
        _option_text(source),
        _safe_str(source.get("core.price")),
        _safe_str(source.get("core.compare_at_price")),
        _safe_str(source.get("core.cost")),
        _safe_str(source.get("inventory.location_code")),
        _safe_str(source.get("sys.inventory_location_gid")),
        _safe_str(source.get("inventory.quantity")),
        int(product_metafields_planned),
        int(variant_metafields_planned),
        int(api_operations_planned),
        int(api_operations_succeeded),
        int(api_operations_failed),
        message,
        error_reason,
        admin_url,
        storefront_url,
        _safe_str(source.get("core.category_id")),
        _safe_str(source.get("core.template_suffix")),
        _safe_str(source.get("publish.all_channels")),
        int(publications_planned),
        int(publications_published),
        ";".join(_safe_list(publication_ids)),
    ]


def _ensure_result_schema(create_book, tab_result: str) -> Tuple[Any, Dict[str, Any]]:
    gp = _gp()
    try:
        ws = gp._sheets_retry(
            f"open {tab_result}",
            lambda: create_book.worksheet(tab_result),
        )
    except Exception as exc:
        # Only create when the worksheet is genuinely absent.
        if exc.__class__.__name__ != "WorksheetNotFound":
            raise
        ws = gp._sheets_retry(
            f"create {tab_result}",
            lambda: create_book.add_worksheet(
                title=tab_result,
                rows=500,
                cols=max(50, len(SPU_RESULT_HEADERS)),
            ),
        )

    values = gp._sheets_retry(f"read {tab_result}", ws.get_all_values)
    existing_header = [
        _safe_str(value)
        for value in (values[0][: len(SPU_RESULT_HEADERS)] if values else [])
    ]
    reset = bool(values) and existing_header != SPU_RESULT_HEADERS
    if reset:
        gp._sheets_retry(f"clear legacy {tab_result}", ws.clear)
        values = []
    if not values:
        if ws.row_count < 2 or ws.col_count < len(SPU_RESULT_HEADERS):
            gp._sheets_retry(
                f"resize {tab_result}",
                lambda: ws.resize(
                    rows=max(ws.row_count, 500),
                    cols=max(ws.col_count, len(SPU_RESULT_HEADERS)),
                ),
            )
        end_col = gp._a1_col(len(SPU_RESULT_HEADERS))
        gp._sheets_retry(
            f"write {tab_result} header",
            lambda: ws.update(
                range_name=f"A1:{end_col}1",
                values=[SPU_RESULT_HEADERS],
                value_input_option="RAW",
            ),
        )
    return ws, {
        "schema_reset": reset,
        "previous_header": existing_header,
        "header_columns": len(SPU_RESULT_HEADERS),
    }


def _append_result_rows(ws, rows: Sequence[Sequence[Any]]) -> int:
    if not rows:
        return 0
    gp = _gp()
    values = gp._sheets_retry(f"read {ws.title}", ws.get_all_values)
    start_row = max(2, len(values) + 1)
    required_rows = start_row + len(rows) - 1
    required_cols = len(SPU_RESULT_HEADERS)
    if ws.row_count < required_rows or ws.col_count < required_cols:
        gp._sheets_retry(
            f"resize {ws.title}",
            lambda: ws.resize(
                rows=max(ws.row_count, required_rows + 100),
                cols=max(ws.col_count, required_cols),
            ),
        )
    end_col = gp._a1_col(required_cols)
    end_row = start_row + len(rows) - 1
    gp._sheets_retry(
        f"append {ws.title} rows={len(rows)}",
        lambda: ws.update(
            range_name=f"A{start_row}:{end_col}{end_row}",
            values=[list(row) for row in rows],
            value_input_option="RAW",
        ),
    )
    return len(rows)


def _apply_create_group(
    *,
    client,
    group_key: str,
    rows: Sequence[Dict[str, str]],
    ordered_field_keys: Sequence[str],
    cfg_fields: Mapping[str, Any],
    dry_run: bool,
    allow_non_draft_status: bool,
    publications: Sequence[str],
    run_context: Mapping[str, Any],
) -> Dict[str, Any]:
    ga = _ga()
    gp = _gp()
    first = rows[0]
    handle = _safe_str(first.get("core.handle"))
    title = _safe_str(first.get("core.title"))
    requested_status = _safe_str(first.get("core.status")).upper() or "DRAFT"
    product_input = ga._build_product_set_input(
        product_key=group_key,
        rows=rows,
        ordered_field_keys=ordered_field_keys,
        cfg_fields=cfg_fields,
        allow_non_draft_status=allow_non_draft_status,
    )
    product_metafields_count = len(product_input.get("metafields", []))
    variant_mf_count_by_sku = {
        _safe_str(row.get("core.sku")): len(
            ga._variant_metafields(
                row=row,
                ordered_field_keys=ordered_field_keys,
                cfg_fields=cfg_fields,
            )
        )
        for row in rows
    }
    publish_all = bool(gp._normalize_bool(first.get("publish.all_channels")))
    publication_ids = list(publications) if publish_all else []
    api_planned = 1 + (1 if publication_ids else 0)

    current = _fetch_product_by_handle(client, handle) if handle else None
    if current:
        product_gid = _safe_str(current.get("id"))
        result_rows = [
            _create_result_row(
                **run_context,
                action="CREATE",
                product_group_key=group_key,
                target_product_id="",
                source=row,
                apply_status="SKIPPED_HANDLE_EXISTS",
                product_gid=product_gid,
                product_handle=_safe_str(current.get("handle")) or handle,
                product_title=_safe_str(current.get("title")) or title,
                product_status=_safe_str(current.get("status")),
                product_metafields_planned=product_metafields_count,
                variant_metafields_planned=variant_mf_count_by_sku.get(
                    _safe_str(row.get("core.sku")), 0
                ),
                api_operations_planned=0,
                message="CREATE skipped: Shopify Product handle already exists.",
                error_reason="HANDLE_EXISTS",
                publications_planned=0,
                publications_published=0,
                publication_ids=[],
            )
            for row in rows
        ]
        return {
            "product_group_key": group_key,
            "action": "CREATE",
            "status": "SKIPPED_HANDLE_EXISTS",
            "product_gid": product_gid,
            "handle": _safe_str(current.get("handle")) or handle,
            "variants_total": len(rows),
            "variants_created": 0,
            "variants_skipped": len(rows),
            "variants_failed": 0,
            "api_operations_planned": 0,
            "api_operations_succeeded": 0,
            "api_operations_failed": 0,
            "message": "Existing Shopify handle; CREATE not called.",
            "result_rows": result_rows,
        }

    if dry_run:
        message = "DRY_RUN: Shopify productSet was not called."
        if publication_ids:
            message += f" Publication association planned for {len(publication_ids)} Publications."
        result_rows = [
            _create_result_row(
                **run_context,
                action="CREATE",
                product_group_key=group_key,
                target_product_id="",
                source=row,
                apply_status="PLANNED_CREATE",
                product_gid="",
                product_handle=handle,
                product_title=title,
                product_status=requested_status,
                product_metafields_planned=product_metafields_count,
                variant_metafields_planned=variant_mf_count_by_sku.get(
                    _safe_str(row.get("core.sku")), 0
                ),
                api_operations_planned=api_planned,
                message=message,
                publications_planned=len(publication_ids),
                publications_published=0,
                publication_ids=publication_ids,
            )
            for row in rows
        ]
        return {
            "product_group_key": group_key,
            "action": "CREATE",
            "status": "PLANNED",
            "product_gid": "",
            "handle": handle,
            "variants_total": len(rows),
            "variants_created": 0,
            "variants_skipped": 0,
            "variants_failed": 0,
            "api_operations_planned": api_planned,
            "api_operations_succeeded": 0,
            "api_operations_failed": 0,
            "message": message,
            "result_rows": result_rows,
        }

    data = client.gql(
        ga.M_PRODUCT_SET,
        {"input": dict(product_input), "synchronous": True},
        operation_name="spu_productSet_create",
    )
    payload = data.get("productSet") or {}
    user_errors = payload.get("userErrors") or []
    if user_errors:
        raise RuntimeError(
            "productSet userErrors: " + json.dumps(user_errors, ensure_ascii=False)
        )
    product = payload.get("product")
    if not product:
        raise RuntimeError("productSet returned no Product.")
    product_gid = _safe_str(product.get("id"))
    returned_variants = product.get("variants", {}).get("nodes", [])
    returned_by_sku = {
        _safe_str(item.get("sku")): item
        for item in returned_variants
        if _safe_str(item.get("sku"))
    }
    missing_skus = sorted(
        _safe_str(row.get("core.sku"))
        for row in rows
        if _safe_str(row.get("core.sku")) not in returned_by_sku
    )
    if missing_skus:
        raise RuntimeError(
            "productSet response is missing created SKUs: " + json.dumps(missing_skus)
        )
    published_count = 0
    succeeded_ops = 1
    if publication_ids:
        published_count = _publish_product(client, product_gid, publication_ids)
        succeeded_ops += 1
    result_rows: List[List[Any]] = []
    for row in rows:
        sku = _safe_str(row.get("core.sku"))
        returned = returned_by_sku[sku]
        inventory_item = returned.get("inventoryItem") or {}
        result_rows.append(
            _create_result_row(
                **run_context,
                action="CREATE",
                product_group_key=group_key,
                target_product_id="",
                source=row,
                apply_status="SUCCESS",
                product_gid=product_gid,
                product_handle=_safe_str(product.get("handle")) or handle,
                product_title=_safe_str(product.get("title")) or title,
                product_status=_safe_str(product.get("status")) or requested_status,
                variant_gid=_safe_str(returned.get("id")),
                inventory_item_gid=_safe_str(inventory_item.get("id")),
                product_metafields_planned=product_metafields_count,
                variant_metafields_planned=variant_mf_count_by_sku.get(sku, 0),
                api_operations_planned=api_planned,
                api_operations_succeeded=succeeded_ops,
                message=(
                    "Product and Variants created by synchronous productSet"
                    + (
                        f"; associated with {published_count} Publications."
                        if publication_ids
                        else "."
                    )
                ),
                publications_planned=len(publication_ids),
                publications_published=published_count,
                publication_ids=publication_ids,
            )
        )
    return {
        "product_group_key": group_key,
        "action": "CREATE",
        "status": "SUCCESS",
        "product_gid": product_gid,
        "handle": _safe_str(product.get("handle")) or handle,
        "variants_total": len(rows),
        "variants_created": len(rows),
        "variants_skipped": 0,
        "variants_failed": 0,
        "api_operations_planned": api_planned,
        "api_operations_succeeded": succeeded_ops,
        "api_operations_failed": 0,
        "message": "CREATE completed.",
        "result_rows": result_rows,
    }


def _apply_add_group(
    *,
    client,
    group_key: str,
    rows: Sequence[Dict[str, str]],
    ordered_field_keys: Sequence[str],
    cfg_fields: Mapping[str, Any],
    dry_run: bool,
    add_variant_batch_size: int,
    run_context: Mapping[str, Any],
) -> Dict[str, Any]:
    ga = _ga()
    first = rows[0]
    target_id = _safe_str(first.get("sys.target_product_id"))
    target_gid = _product_gid(target_id)
    current = _fetch_product_current(client, target_gid)
    current_gid = _safe_str(current.get("id"))
    if current_gid != target_gid:
        raise ValueError(
            f"ADD target Product mismatch: requested={target_gid}; current={current_gid}."
        )
    expected_title = _safe_str(first.get("core.title"))
    expected_handle = _safe_str(first.get("core.handle"))
    if _safe_str(current.get("title")) != expected_title:
        raise ValueError(
            "ADD target Product Title changed since Prepare. "
            f"expected={expected_title!r}; current={current.get('title')!r}."
        )
    if _safe_str(current.get("handle")) != expected_handle:
        raise ValueError(
            "ADD target Product Handle changed since Prepare. "
            f"expected={expected_handle!r}; current={current.get('handle')!r}."
        )

    existing_by_sku: Dict[str, Dict[str, Any]] = {}
    existing_by_combo: Dict[Tuple[Tuple[str, str], ...], Dict[str, Any]] = {}
    for variant in current.get("variants", []):
        sku = _safe_str(variant.get("sku"))
        combo = _selected_option_pairs(variant)
        if sku:
            existing_by_sku[sku] = variant
        if combo:
            existing_by_combo[combo] = variant

    skipped_rows: List[Tuple[Dict[str, str], Dict[str, Any]]] = []
    pending_rows: List[Dict[str, str]] = []
    conflicts: List[str] = []
    for row in rows:
        sku = _safe_str(row.get("core.sku"))
        combo = _option_pairs(row)
        by_sku = existing_by_sku.get(sku)
        if by_sku:
            existing_combo = _selected_option_pairs(by_sku)
            if existing_combo != combo:
                conflicts.append(
                    f"SKU {sku!r} already exists with options={existing_combo}, planned={combo}."
                )
            else:
                skipped_rows.append((row, by_sku))
            continue
        by_combo = existing_by_combo.get(combo)
        if by_combo:
            conflicts.append(
                f"Option combination {combo} already exists as SKU={_safe_str(by_combo.get('sku'))!r}."
            )
            continue
        pending_rows.append(row)

    if conflicts:
        raise ValueError(
            "ADD current Shopify Variant collision: "
            + " | ".join(conflicts[:20])
        )

    batch_size = max(1, int(add_variant_batch_size))
    batches = [
        pending_rows[index : index + batch_size]
        for index in range(0, len(pending_rows), batch_size)
    ]
    api_planned = len(batches)
    variant_mf_count_by_sku = {
        _safe_str(row.get("core.sku")): len(
            ga._variant_metafields(
                row=row,
                ordered_field_keys=ordered_field_keys,
                cfg_fields=cfg_fields,
            )
        )
        for row in rows
    }
    result_rows: List[List[Any]] = []
    for row, existing in skipped_rows:
        inv = existing.get("inventoryItem") or {}
        result_rows.append(
            _create_result_row(
                **run_context,
                action="ADD",
                product_group_key=group_key,
                target_product_id=target_id,
                source=row,
                apply_status="SKIPPED_SKU_EXISTS",
                product_gid=current_gid,
                product_handle=_safe_str(current.get("handle")),
                product_title=_safe_str(current.get("title")),
                product_status=_safe_str(current.get("status")),
                variant_gid=_safe_str(existing.get("id")),
                inventory_item_gid=_safe_str(inv.get("id")),
                product_metafields_planned=0,
                variant_metafields_planned=variant_mf_count_by_sku.get(
                    _safe_str(row.get("core.sku")), 0
                ),
                api_operations_planned=0,
                message="ADD skipped: SKU with the same option combination already exists on target Product.",
                error_reason="SKU_EXISTS",
            )
        )

    if dry_run:
        for row in pending_rows:
            result_rows.append(
                _create_result_row(
                    **run_context,
                    action="ADD",
                    product_group_key=group_key,
                    target_product_id=target_id,
                    source=row,
                    apply_status="PLANNED_ADD",
                    product_gid=current_gid,
                    product_handle=_safe_str(current.get("handle")),
                    product_title=_safe_str(current.get("title")),
                    product_status=_safe_str(current.get("status")),
                    product_metafields_planned=0,
                    variant_metafields_planned=variant_mf_count_by_sku.get(
                        _safe_str(row.get("core.sku")), 0
                    ),
                    api_operations_planned=api_planned,
                    message="DRY_RUN: productVariantsBulkCreate was not called; Product-level fields will not be changed.",
                )
            )
        status = "SKIPPED_ALREADY_APPLIED" if not pending_rows else "PLANNED"
        return {
            "product_group_key": group_key,
            "action": "ADD",
            "status": status,
            "product_gid": current_gid,
            "handle": _safe_str(current.get("handle")),
            "variants_total": len(rows),
            "variants_created": 0,
            "variants_skipped": len(skipped_rows),
            "variants_failed": 0,
            "api_operations_planned": api_planned,
            "api_operations_succeeded": 0,
            "api_operations_failed": 0,
            "message": (
                "All planned ADD SKUs already exist."
                if not pending_rows
                else f"DRY_RUN: {len(pending_rows)} Variants ready to ADD in {api_planned} batch(es)."
            ),
            "result_rows": result_rows,
        }

    succeeded_ops = 0
    failed_ops = 0
    created_count = 0
    failed_count = 0
    blocked_later = False
    for batch_index, batch_rows in enumerate(batches, start=1):
        if blocked_later:
            for row in batch_rows:
                failed_count += 1
                result_rows.append(
                    _create_result_row(
                        **run_context,
                        action="ADD",
                        product_group_key=group_key,
                        target_product_id=target_id,
                        source=row,
                        apply_status="NOT_ATTEMPTED",
                        product_gid=current_gid,
                        product_handle=_safe_str(current.get("handle")),
                        product_title=_safe_str(current.get("title")),
                        product_status=_safe_str(current.get("status")),
                        variant_metafields_planned=variant_mf_count_by_sku.get(
                            _safe_str(row.get("core.sku")), 0
                        ),
                        api_operations_planned=api_planned,
                        api_operations_succeeded=succeeded_ops,
                        api_operations_failed=failed_ops,
                        message="Not attempted because an earlier ADD batch failed.",
                        error_reason="EARLIER_BATCH_FAILED",
                    )
                )
            continue

        variant_inputs = _build_add_variant_inputs(
            rows=batch_rows,
            ordered_field_keys=ordered_field_keys,
            cfg_fields=cfg_fields,
        )
        data = client.gql(
            M_PRODUCT_VARIANTS_BULK_CREATE,
            {
                "productId": current_gid,
                "variants": variant_inputs,
                "strategy": "DEFAULT",
            },
            operation_name=f"spu_add_variants_batch_{batch_index}",
        )
        payload = data.get("productVariantsBulkCreate") or {}
        user_errors = payload.get("userErrors") or []
        if user_errors:
            failed_ops += 1
            blocked_later = True
            message = "productVariantsBulkCreate userErrors: " + json.dumps(
                user_errors, ensure_ascii=False
            )
            for row in batch_rows:
                failed_count += 1
                result_rows.append(
                    _create_result_row(
                        **run_context,
                        action="ADD",
                        product_group_key=group_key,
                        target_product_id=target_id,
                        source=row,
                        apply_status="FAILED",
                        product_gid=current_gid,
                        product_handle=_safe_str(current.get("handle")),
                        product_title=_safe_str(current.get("title")),
                        product_status=_safe_str(current.get("status")),
                        variant_metafields_planned=variant_mf_count_by_sku.get(
                            _safe_str(row.get("core.sku")), 0
                        ),
                        api_operations_planned=api_planned,
                        api_operations_succeeded=succeeded_ops,
                        api_operations_failed=failed_ops,
                        message=message,
                        error_reason="SHOPIFY_USER_ERROR",
                    )
                )
            continue

        returned = payload.get("productVariants") or []
        returned_by_sku = {
            _safe_str(item.get("sku")): item
            for item in returned
            if _safe_str(item.get("sku"))
        }
        missing = [
            _safe_str(row.get("core.sku"))
            for row in batch_rows
            if _safe_str(row.get("core.sku")) not in returned_by_sku
        ]
        if missing:
            failed_ops += 1
            blocked_later = True
            message = "ADD response missing created SKUs: " + json.dumps(missing)
            for row in batch_rows:
                failed_count += 1
                result_rows.append(
                    _create_result_row(
                        **run_context,
                        action="ADD",
                        product_group_key=group_key,
                        target_product_id=target_id,
                        source=row,
                        apply_status="FAILED_VERIFY",
                        product_gid=current_gid,
                        product_handle=_safe_str(current.get("handle")),
                        product_title=_safe_str(current.get("title")),
                        product_status=_safe_str(current.get("status")),
                        variant_metafields_planned=variant_mf_count_by_sku.get(
                            _safe_str(row.get("core.sku")), 0
                        ),
                        api_operations_planned=api_planned,
                        api_operations_succeeded=succeeded_ops,
                        api_operations_failed=failed_ops,
                        message=message,
                        error_reason="RESPONSE_RECONCILIATION_FAILED",
                    )
                )
            continue

        succeeded_ops += 1
        for row in batch_rows:
            sku = _safe_str(row.get("core.sku"))
            item = returned_by_sku[sku]
            inv = item.get("inventoryItem") or {}
            created_count += 1
            result_rows.append(
                _create_result_row(
                    **run_context,
                    action="ADD",
                    product_group_key=group_key,
                    target_product_id=target_id,
                    source=row,
                    apply_status="SUCCESS",
                    product_gid=current_gid,
                    product_handle=_safe_str(current.get("handle")),
                    product_title=_safe_str(current.get("title")),
                    product_status=_safe_str(current.get("status")),
                    variant_gid=_safe_str(item.get("id")),
                    inventory_item_gid=_safe_str(inv.get("id")),
                    product_metafields_planned=0,
                    variant_metafields_planned=variant_mf_count_by_sku.get(sku, 0),
                    api_operations_planned=api_planned,
                    api_operations_succeeded=succeeded_ops,
                    api_operations_failed=failed_ops,
                    message="Variant added to existing Product; Product-level fields were not changed.",
                )
            )

    if failed_count:
        status = "PARTIAL_SUCCESS" if created_count or skipped_rows else "FAILED"
    else:
        status = "SUCCESS" if pending_rows else "SKIPPED_ALREADY_APPLIED"
    return {
        "product_group_key": group_key,
        "action": "ADD",
        "status": status,
        "product_gid": current_gid,
        "handle": _safe_str(current.get("handle")),
        "variants_total": len(rows),
        "variants_created": created_count,
        "variants_skipped": len(skipped_rows),
        "variants_failed": failed_count,
        "api_operations_planned": api_planned,
        "api_operations_succeeded": succeeded_ops,
        "api_operations_failed": failed_ops,
        "message": (
            f"ADD completed: created={created_count}, skipped={len(skipped_rows)}, failed={failed_count}."
        ),
        "result_rows": result_rows,
    }


def _failure_group_result(
    *,
    group_key: str,
    rows: Sequence[Dict[str, str]],
    exc: BaseException,
    run_context: Mapping[str, Any],
) -> Dict[str, Any]:
    action = _safe_str(rows[0].get("sys.action")) if rows else ""
    target_id = _safe_str(rows[0].get("sys.target_product_id")) if rows else ""
    handle = _safe_str(rows[0].get("core.handle")) if rows else ""
    title = _safe_str(rows[0].get("core.title")) if rows else ""
    status = _safe_str(rows[0].get("core.status")) if rows else ""
    message = f"{type(exc).__name__}: {exc}"
    result_rows = [
        _create_result_row(
            **run_context,
            action=action,
            product_group_key=group_key,
            target_product_id=target_id,
            source=row,
            apply_status="FAILED",
            product_gid=_product_gid(target_id) if action == "ADD" and target_id else "",
            product_handle=handle,
            product_title=title,
            product_status=status,
            api_operations_failed=1,
            message=message,
            error_reason=type(exc).__name__,
        )
        for row in rows
    ]
    return {
        "product_group_key": group_key,
        "action": action,
        "status": "FAILED",
        "product_gid": _product_gid(target_id) if action == "ADD" and target_id else "",
        "handle": handle,
        "variants_total": len(rows),
        "variants_created": 0,
        "variants_skipped": 0,
        "variants_failed": len(rows),
        "api_operations_planned": 0,
        "api_operations_succeeded": 0,
        "api_operations_failed": 1,
        "message": message,
        "result_rows": result_rows,
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
    tab_preview: str = "Preview",
    tab_product_handle: str = "V_V_Handle",
    tab_result: str = "Result",
    tab_runlog: str = "Ops__RunLog",
    only_product_group_keys: Optional[Iterable[str]] = None,
    apply_all_ready_groups: bool = True,
    max_product_groups_per_run: int = 5000,
    dry_run: bool = True,
    confirmed: bool = False,
    allow_non_draft_status: bool = True,
    stop_on_first_error: bool = False,
    write_result: bool = True,
    product_group_concurrency: int = 4,
    add_variant_batch_size: int = 100,
    api_timeout_seconds: int = 120,
    api_max_retries: int = 6,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    preview_rows: int = 50,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
) -> Dict[str, Any]:
    gp = _gp()
    sp = _sp()
    ga = _ga()
    site_code = gp._normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not _safe_str(console_core_url):
        raise ValueError("console_core_url is required.")
    if not _safe_str(bootstrap_gsheet_sa_b64_secret):
        raise ValueError("bootstrap_gsheet_sa_b64_secret is required.")
    if not dry_run and not confirmed:
        raise ValueError(
            "Live SPU Apply is blocked: set DRY_RUN=False and CONFIRMED=True."
        )

    run_id = run_id or gp._make_run_id(job_name, tz_name)
    phase = "apply"
    started = time.monotonic()
    live_mode = not bool(dry_run)

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 12, f"Resolve Google access | site={site_code}")
    secret = gp.read_secret(
        bootstrap_gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, auth_meta = gp._build_gspread_client(secret)
    console = gp._sheets_retry("open Console Core", lambda: gc.open_by_url(console_core_url))
    account = gp._load_account_values(console, tab_cfg_account_id)
    configured_secret = _safe_str(account.get("GSHEET_SA_B64_SECRET"))
    if configured_secret and configured_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "Bootstrap Google Secret does not match Cfg__account_id. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}; cfg={configured_secret}"
        )

    progress(2, 12, "Resolve routed workbooks")
    create_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, create_sheet_label
    )
    config_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, config_sheet_label
    )
    runlog_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, runlog_sheet_label
    )
    create_book = gp._sheets_retry("open create_spu workbook", lambda: gc.open_by_url(create_url))
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
        progress(3, 12, "Rebuild current 7.4.2 SPU Prepare plan")
        cfg_ws = gp._require_worksheet(config_book, tab_cfg_fields)
        cfg_fields = gp._read_cfg_fields(
            gp._sheets_retry(f"read {tab_cfg_fields}", cfg_ws.get_all_values)
        )
        input_ws = gp._require_worksheet(create_book, tab_input)
        input_contract = sp._read_input_matrix_strict(
            gp._sheets_retry(f"read {tab_input}", input_ws.get_all_values),
            cfg_fields,
        )
        defaults_ws = gp._require_worksheet(create_book, tab_defaults)
        defaults = gp._read_defaults_matrix(
            gp._sheets_retry(f"read {tab_defaults}", defaults_ws.get_all_values)
        )
        locations_ws = gp._require_worksheet(console, tab_cfg_locations)
        locations = gp._read_locations(
            gp._sheets_retry(f"read {tab_cfg_locations}", locations_ws.get_all_values),
            site_code,
        )
        handle_ws = gp._require_worksheet(create_book, tab_product_handle)
        handle_snapshot = sp._read_product_handle(
            gp._sheets_retry(f"read {tab_product_handle}", handle_ws.get_all_values)
        )
        prepare_plan = sp._build_prepare_plan(
            input_contract=input_contract,
            defaults=defaults,
            cfg_fields=cfg_fields,
            locations=locations,
            product_handle=handle_snapshot,
        )
        if not prepare_plan.get("ready_for_apply"):
            raise ValueError("Current SPU Prepare plan has no READY Product groups.")
        print(
            "[Prepare Plan] "
            f"status={prepare_plan['status']} | "
            f"ready_groups={len(prepare_plan['ready_groups'])} | "
            f"error_groups={len(prepare_plan['error_groups'])} | "
            f"rows_ready={prepare_plan['stats']['rows_ready']}"
        )

        progress(4, 12, "Select READY SPU Product groups")
        selected_groups = _select_group_keys(
            prepare_plan=prepare_plan,
            only_product_group_keys=only_product_group_keys,
            apply_all_ready_groups=apply_all_ready_groups,
            max_product_groups_per_run=max_product_groups_per_run,
            live_mode=live_mode,
        )
        product_rows = _group_rows(
            prepare_plan=prepare_plan,
            selected_group_keys=selected_groups,
        )
        ga._attach_location_gids(product_rows=product_rows, locations=locations)
        print(
            "[Selection] "
            f"groups={len(selected_groups)} | "
            f"variants={sum(len(rows) for rows in product_rows.values())} | "
            f"keys={selected_groups}"
        )

        progress(5, 12, f"Verify reviewed Preview snapshot | tab={tab_preview}")
        preview_ws = gp._require_worksheet(create_book, tab_preview)
        preview_contract = _read_preview_contract(
            gp._sheets_retry(f"read {tab_preview}", preview_ws.get_all_values)
        )
        preview_verification = _verify_preview_snapshot(
            prepare_plan=prepare_plan,
            preview_contract=preview_contract,
            selected_group_keys=selected_groups,
        )
        print(
            "[Preview Verification] "
            f"groups={preview_verification['product_groups']} | "
            f"rows={preview_verification['row_count']} | PASS"
        )

        progress(6, 12, "Resolve Shopify configuration")
        shop_domain = _safe_str(account.get("SHOP_DOMAIN"))
        api_version = _safe_str(account.get("SHOPIFY_API_VERSION"))
        token_secret_name = _safe_str(account.get("SHOPIFY_TOKEN_SECRET"))
        if not shop_domain:
            raise ValueError("Cfg__account_id missing SHOP_DOMAIN.")
        if not api_version:
            raise ValueError("Cfg__account_id missing SHOPIFY_API_VERSION.")
        if not token_secret_name:
            raise ValueError("Cfg__account_id missing SHOPIFY_TOKEN_SECRET.")
        runtime_token_name = ga._project_secret_name_for_runtime(
            token_secret_name,
            site_code,
        )
        print(
            "[Shopify Config] "
            f"shop={shop_domain} | api_version={api_version} | "
            f"token_secret={runtime_token_name}"
        )

        progress(7, 12, "Resolve Shopify token and initialize Admin GraphQL client")
        shopify_secret = gp.read_secret(
            runtime_token_name,
            project_code=site_code,
            explicit_value=shopify_token_value,
            secret_home=secret_home,
            local_secret_aliases=local_secret_aliases,
        )
        client = ga.ShopifyClient(
            shop_domain=shop_domain,
            api_version=api_version,
            access_token=shopify_secret.value,
            timeout_seconds=api_timeout_seconds,
            max_retries=api_max_retries,
            print_progress=print_progress,
        )

        any_create_publish = any(
            _safe_str(rows[0].get("sys.action")) == "CREATE"
            and bool(gp._normalize_bool(rows[0].get("publish.all_channels")))
            for rows in product_rows.values()
        )
        publications: List[str] = []
        if any_create_publish:
            publications = _list_publications(client)
            print(f"[Publications] planned={len(publications)}")

        progress(8, 12, "Initialize SPU Result schema")
        result_ws = None
        result_schema = {
            "schema_reset": False,
            "previous_header": [],
            "header_columns": len(SPU_RESULT_HEADERS),
        }
        if write_result:
            result_ws, result_schema = _ensure_result_schema(create_book, tab_result)
            print(
                "[Result Schema] "
                f"reset={result_schema['schema_reset']} | "
                f"columns={result_schema['header_columns']}"
            )
        else:
            print("[Result Schema] WRITE_RESULT=False")

        progress(
            9,
            12,
            f"Apply CREATE + ADD | concurrency={max(1, int(product_group_concurrency))} | dry_run={dry_run}",
        )
        applied_at = gp._now_str(tz_name)
        run_context = {
            "run_id": run_id,
            "applied_at": applied_at,
            "site_code": site_code,
            "runtime_mode": gp._runtime_mode(),
            "dry_run": bool(dry_run),
            "admin_product_base_url": _safe_str(account.get("ADMIN_PRODUCT_BASE_URL")),
            "storefront_product_base_url": _safe_str(
                account.get("STOREFRONT_PRODUCT_BASE_URL")
            ),
        }

        group_results: List[Dict[str, Any]] = []
        result_rows: List[List[Any]] = []
        concurrency = max(1, int(product_group_concurrency))
        executor = ThreadPoolExecutor(max_workers=concurrency)
        future_map = {}
        try:
            for group_key in selected_groups:
                rows = product_rows[group_key]
                action = _safe_str(rows[0].get("sys.action"))
                if action == "CREATE":
                    future = executor.submit(
                        _apply_create_group,
                        client=client,
                        group_key=group_key,
                        rows=rows,
                        ordered_field_keys=prepare_plan["ordered_field_keys"],
                        cfg_fields=cfg_fields,
                        dry_run=bool(dry_run),
                        allow_non_draft_status=bool(allow_non_draft_status),
                        publications=publications,
                        run_context=run_context,
                    )
                elif action == "ADD":
                    future = executor.submit(
                        _apply_add_group,
                        client=client,
                        group_key=group_key,
                        rows=rows,
                        ordered_field_keys=prepare_plan["ordered_field_keys"],
                        cfg_fields=cfg_fields,
                        dry_run=bool(dry_run),
                        add_variant_batch_size=int(add_variant_batch_size),
                        run_context=run_context,
                    )
                else:
                    raise ValueError(f"Unsupported SPU action in READY group: {action!r}")
                future_map[future] = group_key

            pending = set(future_map)
            stop_requested = False
            completed_count = 0
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    group_key = future_map[future]
                    rows = product_rows[group_key]
                    try:
                        item = future.result()
                    except Exception as exc:
                        item = _failure_group_result(
                            group_key=group_key,
                            rows=rows,
                            exc=exc,
                            run_context=run_context,
                        )
                    group_results.append(item)
                    result_rows.extend(item.get("result_rows", []))
                    completed_count += 1
                    print(
                        "[Group] "
                        f"{completed_count}/{len(selected_groups)} | "
                        f"{group_key} | action={item['action']} | "
                        f"status={item['status']} | "
                        f"created={item['variants_created']} | "
                        f"skipped={item['variants_skipped']} | "
                        f"failed={item['variants_failed']}"
                    )
                    if stop_on_first_error and item["status"] in {"FAILED", "PARTIAL_SUCCESS"}:
                        stop_requested = True
                if stop_requested and pending:
                    for future in list(pending):
                        future.cancel()
                    break
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

        group_results.sort(
            key=lambda item: selected_groups.index(item["product_group_key"])
        )
        # Result rows should follow Product-group selection order then source row.
        header_index = {name: index for index, name in enumerate(SPU_RESULT_HEADERS)}
        result_rows.sort(
            key=lambda row: (
                selected_groups.index(_safe_str(row[header_index["product_group_key"]])),
                int(_safe_str(row[header_index["source_row"]]) or "0"),
            )
        )

        progress(10, 12, f"Write Result | rows={len(result_rows)}")
        result_rows_written = 0
        if write_result and result_ws is not None:
            result_rows_written = _append_result_rows(result_ws, result_rows)
        else:
            print("[Result] write disabled")

        failed_groups = [
            item for item in group_results if item["status"] in {"FAILED", "PARTIAL_SUCCESS"}
        ]
        success_like_groups = [
            item
            for item in group_results
            if item["status"]
            in {"SUCCESS", "PLANNED", "SKIPPED_HANDLE_EXISTS", "SKIPPED_ALREADY_APPLIED"}
        ]
        if dry_run:
            status = "DRY_RUN_READY" if not failed_groups else "DRY_RUN_WITH_ERRORS"
        elif failed_groups and success_like_groups:
            status = "PARTIAL_SUCCESS"
        elif failed_groups:
            status = "FAILED"
        else:
            status = "SUCCESS"

        progress(11, 12, "Write Ops__RunLog evidence")
        summary = {
            "product_groups_selected": len(selected_groups),
            "create_groups_selected": sum(item["action"] == "CREATE" for item in group_results),
            "add_groups_selected": sum(item["action"] == "ADD" for item in group_results),
            "groups_success": sum(item["status"] == "SUCCESS" for item in group_results),
            "groups_planned": sum(item["status"] == "PLANNED" for item in group_results),
            "groups_skipped": sum(
                item["status"] in {"SKIPPED_HANDLE_EXISTS", "SKIPPED_ALREADY_APPLIED"}
                for item in group_results
            ),
            "groups_failed_or_partial": len(failed_groups),
            "variant_rows_selected": sum(len(rows) for rows in product_rows.values()),
            "variants_created": sum(int(item["variants_created"]) for item in group_results),
            "variants_skipped": sum(int(item["variants_skipped"]) for item in group_results),
            "variants_failed": sum(int(item["variants_failed"]) for item in group_results),
            "api_operations_planned": sum(int(item["api_operations_planned"]) for item in group_results),
            "api_operations_succeeded": sum(int(item["api_operations_succeeded"]) for item in group_results),
            "api_operations_failed": sum(int(item["api_operations_failed"]) for item in group_results),
            "shopify_requests": int(getattr(client, "request_count", 0)),
            "shopify_retries": int(getattr(client, "retry_count", 0)),
            "result_rows_written": result_rows_written,
            "result_schema_reset": bool(result_schema.get("schema_reset")),
        }
        logger.log(
            phase=phase,
            log_type="summary",
            status=status,
            entity_type="SPU_PRODUCT_APPLY",
            rows_loaded=prepare_plan["stats"]["rows_loaded"],
            rows_pending=summary["variant_rows_selected"],
            rows_recognized=summary["variant_rows_selected"],
            rows_planned=summary["variant_rows_selected"],
            rows_written=result_rows_written,
            rows_skipped=summary["variants_skipped"],
            message=(
                f"spu_apply | dry_run={dry_run} | groups={len(selected_groups)} | "
                f"create={summary['create_groups_selected']} | add={summary['add_groups_selected']} | "
                f"variants_created={summary['variants_created']} | "
                f"variants_skipped={summary['variants_skipped']} | "
                f"variants_failed={summary['variants_failed']} | "
                f"result_schema_reset={summary['result_schema_reset']}"
            ),
            error_reason=("PRODUCT_GROUP_FAILURES" if failed_groups else ""),
        )
        for item in group_results:
            logger.log(
                phase=phase,
                log_type="detail",
                status=item["status"],
                entity_type="SPU_PRODUCT_APPLY",
                gid=item.get("product_gid", ""),
                message=(
                    f"group={item['product_group_key']} | action={item['action']} | "
                    f"created={item['variants_created']} | skipped={item['variants_skipped']} | "
                    f"failed={item['variants_failed']} | {item['message']}"
                ),
                error_reason=(
                    "GROUP_APPLY_FAILED"
                    if item["status"] in {"FAILED", "PARTIAL_SUCCESS"}
                    else ""
                ),
            )
        logger.flush()

        progress(12, 12, f"Complete | status={status}")
        elapsed = round(time.monotonic() - started, 2)
        summary["elapsed_seconds"] = elapsed
        result_df = pd.DataFrame(result_rows, columns=SPU_RESULT_HEADERS)
        result_preview = (
            result_df.head(int(preview_rows)) if int(preview_rows) > 0 else result_df
        )
        return {
            "status": status,
            "ok": status in {"DRY_RUN_READY", "SUCCESS", "PARTIAL_SUCCESS"},
            "job_name": job_name,
            "run_id": run_id,
            "dry_run": bool(dry_run),
            "confirmed": bool(confirmed),
            "module_version": MODULE_VERSION,
            "prepare_module_version": EXPECTED_PREPARE_MODULE_VERSION,
            "generic_apply_module_version": EXPECTED_GENERIC_APPLY_MODULE_VERSION,
            "selected_product_group_keys": selected_groups,
            "summary": summary,
            "products": [
                {key: value for key, value in item.items() if key != "result_rows"}
                for item in group_results
            ],
            "preview_verification": preview_verification,
            "result_schema": result_schema,
            "result_preview": result_preview,
            "warnings": prepare_plan.get("warnings", []),
            "runtime": {
                "runtime_mode": gp._runtime_mode(),
                "shop_domain": shop_domain,
                "shopify_api_version": api_version,
                "auth_type": auth_meta["source_type"],
            },
            "targets": {
                "create_sheet_label": create_sheet_label,
                "input_tab": tab_input,
                "preview_tab": tab_preview,
                "product_handle_tab": tab_product_handle,
                "result_tab": tab_result,
                "runlog_tab": tab_runlog,
            },
        }
    except Exception as exc:
        try:
            logger.log(
                phase=phase,
                log_type="summary",
                status="FAILED",
                entity_type="SPU_PRODUCT_APPLY",
                message="SPU Apply failed before completion.",
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
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirmed", action="store_true")
    args = parser.parse_args(argv)
    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=args.gsheet_secret,
        dry_run=not args.live,
        confirmed=bool(args.confirmed),
    )
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] in {"DRY_RUN_READY", "SUCCESS", "PARTIAL_SUCCESS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
