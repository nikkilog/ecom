# -*- coding: utf-8 -*-
"""Prepare Shopify Flexible Collection creation plans from Google Sheets.

GitHub target: ``ecom/shopify_create/7_5_1_collection_create_prepare.py``
Import path: ``shopify_create.7_5_1_collection_create_prepare``

Scope
-----
- Resolve the ``create_collection`` workbook through Console Core / Cfg__Sites.
- Read ``Input`` with one row per collection condition.
- Group rows by Collection handle; mixed condition types are supported.
- Resolve metafield definitions from Shopify and convert ``AUTO`` relations.
- Detect existing Collection handles before Apply.
- Overwrite ``Preview`` with READY/BLOCKED plans and a stable plan hash.
- Seed/read ``Defaults`` with ``publish_all_channels=TRUE``.
- Write RunLog evidence.

This module performs Shopify reads only. It never creates, edits, publishes, or
unpublishes a Collection.

Input contract
--------------
Required columns:
``title, handle, match_type, condition_type, namespace, key, relation, value``

Supported condition types in v1:
- PRODUCT_VENDOR
- PRODUCT_TYPE
- PRODUCT_STATUS
- PRODUCT_TAG
- PRODUCT_METAFIELD

For PRODUCT_METAFIELD v1, supported Shopify metafield definition types are:
- single_line_text_field -> metafieldString / EQUALS
- list.single_line_text_field -> metafieldStringList / INCLUDES

``relation=AUTO`` resolves to the canonical relation for the condition type.
Explicit relations are accepted only when Shopify supports them.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib
import json
import re
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import gspread
import pandas as pd


gp = importlib.import_module("shopify_create.7_1_1_generic_product_prepare")
ga = importlib.import_module("shopify_create.7_1_2_generic_product_apply")

MODULE_VERSION = "2026-09-04-flexible-collection-create-v1"
MODULE_PATH = "shopify_create.7_5_1_collection_create_prepare"
DEFAULT_JOB_NAME = "collection_create_prepare"
MIN_API_VERSION = (2026, 7)
FLEXIBLE_COLLECTION_API_VERSION = "2026-07"

# Thin-runner compatibility aliases reused from the existing Console Core boundary.
read_secret = gp.read_secret
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

PREVIEW_HEADERS = [
    "run_id",
    "prepared_at",
    "site_code",
    "title",
    "handle",
    "match_type",
    "condition_count",
    "resolved_conditions",
    "publish_all_channels",
    "status",
    "block_reason",
    "existing_collection_gid",
    "plan_hash",
    "payload_json",
]

DEFAULTS_HEADERS = ["config_key", "config_value", "notes"]
DEFAULTS_SEED = [
    [
        "publish_all_channels",
        "TRUE",
        "After collectionCreate, publish the new Collection to all accessible Publications.",
    ]
]

SUPPORTED_CONDITION_TYPES = {
    "PRODUCT_VENDOR",
    "PRODUCT_TYPE",
    "PRODUCT_STATUS",
    "PRODUCT_TAG",
    "PRODUCT_METAFIELD",
}

PRODUCT_VENDOR_RELATIONS = {
    "CONTAINS",
    "DOES_NOT_CONTAIN",
    "ENDS_WITH",
    "EQUALS",
    "NOT_EQUALS",
    "STARTS_WITH",
}
PRODUCT_TYPE_RELATIONS = set(PRODUCT_VENDOR_RELATIONS)
PRODUCT_STATUS_RELATIONS = {"EQUALS", "NOT_EQUALS"}
PRODUCT_TAG_RELATIONS = {"TAGGED_WITH", "NOT_TAGGED_WITH"}
PRODUCT_STATUS_VALUES = {"ACTIVE", "ARCHIVED", "DRAFT"}

Q_COLLECTION_METAFIELD_DEFINITIONS = """
query CollectionConditionMetafieldDefinitions {
  collectionConditionMetafieldDefinitions {
    id
    namespace
    key
    name
    ownerType
    type {
      name
      category
    }
  }
}
"""

Q_COLLECTION_BY_IDENTIFIER = """
query CollectionByIdentifier($identifier: CollectionIdentifierInput!) {
  collectionByIdentifier(identifier: $identifier) {
    id
    title
    handle
  }
}
"""


@dataclass(frozen=True)
class InputRow:
    source_row: int
    title: str
    handle: str
    match_type: str
    condition_type: str
    namespace: str
    key: str
    relation: str
    value: str


def _json_canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _hash_plan(value: Any) -> str:
    return hashlib.sha256(_json_canonical(value).encode("utf-8")).hexdigest()


def _api_version_tuple(value: Any) -> Tuple[int, int]:
    text = gp._safe_str(value)
    match = re.fullmatch(r"(\d{4})-(\d{2})", text)
    if not match:
        raise ValueError(
            "SHOPIFY_API_VERSION must use YYYY-MM format; "
            f"got={text!r}. Flexible Collection sources require 2026-07+."
        )
    return int(match.group(1)), int(match.group(2))


def _effective_flexible_collection_api(api_version: Any) -> Tuple[str, bool]:
    """Return the API version used only by this Collection workflow.

    Existing Console jobs can remain on an older configured API version. Flexible
    Collection ``sources`` require 2026-07+, so this module locally raises the
    effective request version to 2026-07 when the account-level version is older.
    The choice is printed at runtime and does not mutate Console configuration.
    """
    text = gp._safe_str(api_version)
    if not text:
        raise ValueError("Cfg__account_id missing SHOPIFY_API_VERSION.")
    configured = _api_version_tuple(text)
    if configured < MIN_API_VERSION:
        return FLEXIBLE_COLLECTION_API_VERSION, True
    return text, False


def _normalize_header(value: Any) -> str:
    return re.sub(r"[\s\-]+", "_", gp._safe_str(value).lower()).strip("_")


def _normalize_handle(value: Any) -> str:
    text = gp._safe_str(value).lower()
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^a-z0-9_-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def _read_input(values: Sequence[Sequence[Any]]) -> List[InputRow]:
    if not values:
        raise ValueError("Input is empty.")

    headers = [_normalize_header(v) for v in values[0]]
    if len(set(h for h in headers if h)) != len([h for h in headers if h]):
        raise ValueError("Input contains duplicated header names.")

    positions = {name: idx for idx, name in enumerate(headers) if name}
    missing = [name for name in INPUT_HEADERS if name not in positions]
    if missing:
        raise ValueError(f"Input missing required columns: {missing}")

    result: List[InputRow] = []
    for source_row, raw in enumerate(values[1:], start=2):
        padded = list(raw) + [""] * max(0, len(headers) - len(raw))
        record = {
            name: gp._safe_str(padded[idx])
            for name, idx in positions.items()
        }
        if not any(record.get(name, "") for name in INPUT_HEADERS):
            continue

        title = record["title"]
        handle = _normalize_handle(record["handle"])
        match_type = record["match_type"].upper()
        condition_type = record["condition_type"].upper()
        namespace = record["namespace"]
        key = record["key"]
        relation = (record["relation"] or "AUTO").upper()
        value = record["value"]

        result.append(
            InputRow(
                source_row=source_row,
                title=title,
                handle=handle,
                match_type=match_type,
                condition_type=condition_type,
                namespace=namespace,
                key=key,
                relation=relation,
                value=value,
            )
        )

    if not result:
        raise ValueError("Input contains no data rows.")
    return result


def _ensure_defaults(
    create_book: gspread.Spreadsheet,
    tab_defaults: str,
) -> Dict[str, str]:
    ws = gp._get_or_create_preview_worksheet(
        create_book,
        tab_defaults,
        100,
        10,
    )
    values = gp._sheets_retry(f"read {tab_defaults}", ws.get_all_values)

    if not values or not any(gp._safe_str(cell) for row in values for cell in row):
        matrix = [DEFAULTS_HEADERS] + DEFAULTS_SEED
        if ws.row_count < len(matrix) + 10 or ws.col_count < len(DEFAULTS_HEADERS):
            gp._sheets_retry(
                f"resize {tab_defaults}",
                lambda: ws.resize(
                    rows=max(ws.row_count, len(matrix) + 20),
                    cols=max(ws.col_count, len(DEFAULTS_HEADERS) + 2),
                ),
            )
        gp._sheets_retry(f"clear {tab_defaults}", ws.clear)
        gp._sheets_retry(
            f"seed {tab_defaults}",
            lambda: ws.update(
                range_name="A1:C2",
                values=matrix,
                value_input_option="RAW",
            ),
        )
        values = matrix

    headers = [_normalize_header(v) for v in values[0]]
    positions = {name: idx for idx, name in enumerate(headers) if name}
    required = {"config_key", "config_value"}
    missing = sorted(required - set(positions))
    if missing:
        raise ValueError(f"Defaults missing required columns: {missing}")

    defaults: Dict[str, str] = {}
    for source_row, raw in enumerate(values[1:], start=2):
        padded = list(raw) + [""] * max(0, len(headers) - len(raw))
        key = gp._safe_str(padded[positions["config_key"]]).lower()
        value = gp._safe_str(padded[positions["config_value"]])
        if not key and not value:
            continue
        if not key:
            raise ValueError(f"Defaults row {source_row} has no config_key.")
        if key in defaults:
            raise ValueError(f"Defaults has duplicate config_key={key!r}.")
        defaults[key] = value

    if "publish_all_channels" not in defaults:
        raise ValueError(
            "Defaults must contain config_key=publish_all_channels. "
            "Set config_value to TRUE or FALSE."
        )
    gp._normalize_bool(defaults["publish_all_channels"])
    return defaults


def _load_metafield_definitions(client: ga.ShopifyClient) -> Dict[Tuple[str, str], Dict[str, Any]]:
    data = client.gql(
        Q_COLLECTION_METAFIELD_DEFINITIONS,
        operation_name="collectionConditionMetafieldDefinitions",
    )
    nodes = data.get("collectionConditionMetafieldDefinitions") or []
    result: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for node in nodes:
        namespace = gp._safe_str(node.get("namespace"))
        key = gp._safe_str(node.get("key"))
        owner_type = gp._safe_str(node.get("ownerType")).upper()
        if owner_type and owner_type != "PRODUCT":
            continue
        identity = (namespace, key)
        if identity in result:
            raise RuntimeError(
                "Shopify returned duplicated collection-condition metafield definition "
                f"for {namespace}.{key}."
            )
        result[identity] = dict(node)
    return result


def _collection_by_handle(client: ga.ShopifyClient, handle: str) -> Optional[Dict[str, Any]]:
    data = client.gql(
        Q_COLLECTION_BY_IDENTIFIER,
        {"identifier": {"handle": handle}},
        operation_name=f"collectionByIdentifier:{handle}",
    )
    node = data.get("collectionByIdentifier")
    return dict(node) if isinstance(node, Mapping) else None


def _auto_relation(condition_type: str, metafield_type: str = "") -> str:
    if condition_type == "PRODUCT_VENDOR":
        return "EQUALS"
    if condition_type == "PRODUCT_TYPE":
        return "EQUALS"
    if condition_type == "PRODUCT_STATUS":
        return "EQUALS"
    if condition_type == "PRODUCT_TAG":
        return "TAGGED_WITH"
    if condition_type == "PRODUCT_METAFIELD":
        if metafield_type == "single_line_text_field":
            return "EQUALS"
        if metafield_type == "list.single_line_text_field":
            return "INCLUDES"
    raise ValueError(
        f"AUTO relation cannot be resolved for condition_type={condition_type!r}, "
        f"metafield_type={metafield_type!r}."
    )


def _resolve_condition(
    row: InputRow,
    metafield_definitions: Mapping[Tuple[str, str], Mapping[str, Any]],
) -> Tuple[Dict[str, Any], str]:
    if row.condition_type not in SUPPORTED_CONDITION_TYPES:
        raise ValueError(
            f"row {row.source_row}: unsupported condition_type={row.condition_type!r}. "
            f"Supported={sorted(SUPPORTED_CONDITION_TYPES)}"
        )
    if not row.value:
        raise ValueError(f"row {row.source_row}: value is required.")

    relation = row.relation or "AUTO"
    detail = ""

    if row.condition_type == "PRODUCT_VENDOR":
        relation = _auto_relation(row.condition_type) if relation == "AUTO" else relation
        if relation not in PRODUCT_VENDOR_RELATIONS:
            raise ValueError(
                f"row {row.source_row}: invalid PRODUCT_VENDOR relation={relation!r}."
            )
        payload = {
            "productVendor": {
                "matchType": "ANY",
                "relation": relation,
                "values": [row.value],
            }
        }
        detail = f"PRODUCT_VENDOR {relation} {row.value}"

    elif row.condition_type == "PRODUCT_TYPE":
        relation = _auto_relation(row.condition_type) if relation == "AUTO" else relation
        if relation not in PRODUCT_TYPE_RELATIONS:
            raise ValueError(
                f"row {row.source_row}: invalid PRODUCT_TYPE relation={relation!r}."
            )
        payload = {
            "productType": {
                "matchType": "ANY",
                "relation": relation,
                "values": [row.value],
            }
        }
        detail = f"PRODUCT_TYPE {relation} {row.value}"

    elif row.condition_type == "PRODUCT_STATUS":
        relation = _auto_relation(row.condition_type) if relation == "AUTO" else relation
        value = row.value.upper()
        if relation not in PRODUCT_STATUS_RELATIONS:
            raise ValueError(
                f"row {row.source_row}: invalid PRODUCT_STATUS relation={relation!r}."
            )
        if value not in PRODUCT_STATUS_VALUES:
            raise ValueError(
                f"row {row.source_row}: invalid PRODUCT_STATUS value={row.value!r}; "
                f"allowed={sorted(PRODUCT_STATUS_VALUES)}."
            )
        payload = {
            "productStatus": {
                "matchType": "ANY",
                "relation": relation,
                "values": [value],
            }
        }
        detail = f"PRODUCT_STATUS {relation} {value}"

    elif row.condition_type == "PRODUCT_TAG":
        relation = _auto_relation(row.condition_type) if relation == "AUTO" else relation
        if relation not in PRODUCT_TAG_RELATIONS:
            raise ValueError(
                f"row {row.source_row}: invalid PRODUCT_TAG relation={relation!r}."
            )
        payload = {
            "productTag": {
                "matchType": "ANY",
                "relation": relation,
                "values": [row.value],
            }
        }
        detail = f"PRODUCT_TAG {relation} {row.value}"

    else:
        if not row.namespace or not row.key:
            raise ValueError(
                f"row {row.source_row}: PRODUCT_METAFIELD requires namespace and key."
            )
        definition = metafield_definitions.get((row.namespace, row.key))
        if not definition:
            raise ValueError(
                f"row {row.source_row}: metafield definition not available as a "
                f"Collection condition: {row.namespace}.{row.key}."
            )
        definition_id = gp._safe_str(definition.get("id"))
        type_name = gp._safe_str((definition.get("type") or {}).get("name"))
        if type_name not in {"single_line_text_field", "list.single_line_text_field"}:
            raise ValueError(
                f"row {row.source_row}: metafield {row.namespace}.{row.key} has "
                f"type={type_name!r}, which is not supported by Collection Create v1."
            )
        relation = _auto_relation(row.condition_type, type_name) if relation == "AUTO" else relation
        expected_relation = _auto_relation(row.condition_type, type_name)
        if relation != expected_relation:
            raise ValueError(
                f"row {row.source_row}: metafield {row.namespace}.{row.key} "
                f"type={type_name} requires relation={expected_relation}; got={relation}."
            )
        key_name = (
            "metafieldString"
            if type_name == "single_line_text_field"
            else "metafieldStringList"
        )
        payload = {
            key_name: {
                "definitionId": definition_id,
                "matchType": "ANY",
                "relation": relation,
                "values": [row.value],
            }
        }
        detail = (
            f"PRODUCT_METAFIELD {row.namespace}.{row.key} "
            f"type={type_name} {relation} {row.value}"
        )

    return payload, detail


def _group_rows(rows: Sequence[InputRow]) -> "OrderedDict[str, List[InputRow]]":
    grouped: "OrderedDict[str, List[InputRow]]" = OrderedDict()
    for row in rows:
        grouped.setdefault(row.handle, []).append(row)
    return grouped


def _build_plans(
    *,
    rows: Sequence[InputRow],
    metafield_definitions: Mapping[Tuple[str, str], Mapping[str, Any]],
    publish_all_channels: bool,
    existing_by_handle: Mapping[str, Optional[Mapping[str, Any]]],
) -> List[Dict[str, Any]]:
    plans: List[Dict[str, Any]] = []
    for handle, group in _group_rows(rows).items():
        errors: List[str] = []
        title_values = sorted({row.title for row in group if row.title})
        match_values = sorted({row.match_type for row in group if row.match_type})

        if not handle:
            errors.append("handle is required")
        if len(title_values) != 1:
            errors.append(
                "same handle must have exactly one nonblank title; "
                f"found={title_values}"
            )
        if len(match_values) != 1:
            errors.append(
                "same handle must have exactly one match_type; "
                f"found={match_values}"
            )
        elif match_values[0] not in {"ALL", "ANY"}:
            errors.append(
                f"match_type must be ALL or ANY; got={match_values[0]!r}"
            )

        conditions: List[Dict[str, Any]] = []
        details: List[str] = []
        for row in group:
            if not row.title:
                errors.append(f"row {row.source_row}: title is required")
            if not row.handle:
                errors.append(f"row {row.source_row}: handle is required")
            if not row.match_type:
                errors.append(f"row {row.source_row}: match_type is required")
            try:
                condition, detail = _resolve_condition(row, metafield_definitions)
                conditions.append(condition)
                details.append(detail)
            except Exception as exc:
                errors.append(str(exc))

        existing = existing_by_handle.get(handle)
        if existing:
            errors.append(
                "Collection handle already exists in Shopify: "
                f"{handle} ({gp._safe_str(existing.get('id'))})"
            )

        title = title_values[0] if len(title_values) == 1 else (group[0].title if group else "")
        match_type = match_values[0] if len(match_values) == 1 else (group[0].match_type if group else "")
        payload = {
            "title": title,
            "handle": handle,
            "sources": [
                {
                    "source": {
                        "title": f"{title} conditions" if title else f"{handle} conditions",
                        "targetType": "PRODUCTS",
                        "inclusion": {
                            "matchType": match_type,
                            "conditions": conditions,
                        },
                    }
                }
            ],
        }
        plan_basis = {
            "title": title,
            "handle": handle,
            "match_type": match_type,
            "conditions": conditions,
            "publish_all_channels": bool(publish_all_channels),
        }
        plans.append(
            {
                "title": title,
                "handle": handle,
                "match_type": match_type,
                "condition_count": len(group),
                "resolved_conditions": " | ".join(details),
                "publish_all_channels": bool(publish_all_channels),
                "status": "BLOCKED" if errors else "READY",
                "block_reason": " || ".join(errors),
                "existing_collection_gid": gp._safe_str(existing.get("id")) if existing else "",
                "plan_hash": _hash_plan(plan_basis),
                "payload_json": _json_canonical(payload),
                "payload": payload,
                "source_rows": [row.source_row for row in group],
            }
        )
    return plans


def _write_single_header_matrix_overwrite(
    spreadsheet: gspread.Spreadsheet,
    tab_name: str,
    matrix: Sequence[Sequence[Any]],
) -> int:
    """Overwrite a one-header-row tab and return data-row count."""
    if not matrix:
        raise ValueError(f"Refusing to write an empty matrix to {tab_name}.")
    rows = len(matrix)
    cols = max(len(row) for row in matrix)
    worksheet = gp._get_or_create_preview_worksheet(
        spreadsheet,
        tab_name,
        rows + 50,
        cols + 5,
    )
    if worksheet.row_count < rows or worksheet.col_count < cols:
        gp._sheets_retry(
            f"resize {tab_name}",
            lambda: worksheet.resize(
                rows=max(worksheet.row_count, rows + 50),
                cols=max(worksheet.col_count, cols + 5),
            ),
        )
    gp._sheets_retry(f"clear {tab_name}", worksheet.clear)
    end_col = gp._a1_col(cols)
    gp._sheets_retry(
        f"write {tab_name}",
        lambda: worksheet.update(
            range_name=f"A1:{end_col}{rows}",
            values=[list(row) for row in matrix],
            value_input_option="RAW",
        ),
    )
    try:
        gp._sheets_retry(
            f"freeze {tab_name}",
            lambda: worksheet.freeze(rows=1),
        )
    except Exception:
        pass
    return max(0, rows - 1)


def _preview_matrix(
    *,
    plans: Sequence[Mapping[str, Any]],
    run_id: str,
    site_code: str,
    tz_name: str,
) -> List[List[Any]]:
    prepared_at = dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")
    matrix: List[List[Any]] = [PREVIEW_HEADERS]
    for plan in plans:
        matrix.append(
            [
                run_id,
                prepared_at,
                site_code,
                plan["title"],
                plan["handle"],
                plan["match_type"],
                plan["condition_count"],
                plan["resolved_conditions"],
                "TRUE" if plan["publish_all_channels"] else "FALSE",
                plan["status"],
                plan["block_reason"],
                plan["existing_collection_gid"],
                plan["plan_hash"],
                plan["payload_json"],
            ]
        )
    return matrix


def _project_secret_name_for_runtime(secret_name: Any, site_code: str) -> str:
    return ga._project_secret_name_for_runtime(secret_name, site_code)


def _shopify_client_from_account(
    *,
    account: Mapping[str, Any],
    site_code: str,
    shopify_token_value: Optional[str],
    secret_home: Optional[str],
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]],
    api_timeout_seconds: int,
    api_max_retries: int,
    print_progress: bool,
) -> ga.ShopifyClient:
    shop_domain = gp._safe_str(account.get("SHOP_DOMAIN"))
    configured_api_version = gp._safe_str(account.get("SHOPIFY_API_VERSION"))
    api_version, api_version_raised = _effective_flexible_collection_api(configured_api_version)
    token_secret_name = gp._safe_str(account.get("SHOPIFY_TOKEN_SECRET"))
    if not shop_domain:
        raise ValueError("Cfg__account_id missing SHOP_DOMAIN.")
    if not token_secret_name:
        raise ValueError("Cfg__account_id missing SHOPIFY_TOKEN_SECRET.")

    runtime_secret_name = _project_secret_name_for_runtime(token_secret_name, site_code)
    if print_progress:
        print(
            "[Shopify API] "
            f"configured={configured_api_version} | effective={api_version} | "
            f"collection_min={FLEXIBLE_COLLECTION_API_VERSION} | "
            f"scoped_upgrade={'YES' if api_version_raised else 'NO'}"
        )
        print(
            "[Shopify Secret] "
            f"configured_name={token_secret_name} | runtime_name={runtime_secret_name} | "
            f"runtime={gp._runtime_mode()}"
        )
    token = gp.read_secret(
        runtime_secret_name,
        project_code=site_code,
        explicit_value=shopify_token_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    return ga.ShopifyClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=token.value,
        timeout_seconds=api_timeout_seconds,
        max_retries=api_max_retries,
        print_progress=print_progress,
    )


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    create_sheet_label: str = "create_collection",
    runlog_sheet_label: str = "runlog_sheet",
    tab_input: str = "Input",
    tab_defaults: str = "Defaults",
    tab_preview: str = "Preview",
    tab_result: str = "Result",
    tab_runlog: str = "Ops__RunLog",
    write_preview: bool = True,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
    api_timeout_seconds: int = 120,
    api_max_retries: int = 6,
) -> Dict[str, Any]:
    site_code = gp._normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not gp._safe_str(console_core_url):
        raise ValueError("console_core_url is required.")
    if not gp._safe_str(bootstrap_gsheet_sa_b64_secret):
        raise ValueError("bootstrap_gsheet_sa_b64_secret is required.")

    run_id = run_id or gp._make_run_id(job_name, tz_name)
    started = time.monotonic()
    phase = "prepare"

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 9, f"Resolve Google access | site={site_code}")
    google_secret = gp.read_secret(
        bootstrap_gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, google_auth = gp._build_gspread_client(google_secret)
    console = gp._sheets_retry("open Console Core", lambda: gc.open_by_url(console_core_url))

    progress(2, 9, "Resolve routed workbooks and account configuration")
    account = gp._load_account_values(console, tab_cfg_account_id)
    configured_secret = gp._safe_str(account.get("GSHEET_SA_B64_SECRET"))
    if configured_secret and configured_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "Bootstrap Google Secret does not match Cfg__account_id. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}; cfg={configured_secret}"
        )
    create_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, create_sheet_label
    )
    runlog_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, runlog_sheet_label
    )
    create_book = gp._sheets_retry("open create_collection workbook", lambda: gc.open_by_url(create_url))
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
        progress(3, 9, f"Read Input and Defaults | tabs={tab_input}, {tab_defaults}")
        input_ws = gp._require_worksheet(create_book, tab_input)
        input_values = gp._sheets_retry(f"read {tab_input}", input_ws.get_all_values)
        rows = _read_input(input_values)
        defaults = _ensure_defaults(create_book, tab_defaults)
        publish_all_channels = bool(gp._normalize_bool(defaults["publish_all_channels"]))
        print(
            "[Input] "
            f"condition_rows={len(rows)} | unique_handles={len(_group_rows(rows))} | "
            f"publish_all_channels={publish_all_channels}"
        )

        progress(4, 9, "Initialize Shopify read client")
        client = _shopify_client_from_account(
            account=account,
            site_code=site_code,
            shopify_token_value=shopify_token_value,
            secret_home=secret_home,
            local_secret_aliases=local_secret_aliases,
            api_timeout_seconds=api_timeout_seconds,
            api_max_retries=api_max_retries,
            print_progress=print_progress,
        )

        progress(5, 9, "Load metafield definitions usable as Collection conditions")
        definitions = _load_metafield_definitions(client)
        print(f"[Metafield definitions] usable_product_definitions={len(definitions)}")

        progress(6, 9, "Check existing Collection handles")
        existing_by_handle: Dict[str, Optional[Mapping[str, Any]]] = {}
        for index, handle in enumerate(_group_rows(rows), start=1):
            if not handle:
                existing_by_handle[handle] = None
                continue
            existing_by_handle[handle] = _collection_by_handle(client, handle)
            if print_progress:
                print(
                    f"  [{index}/{len(_group_rows(rows))}] {handle} | "
                    f"exists={bool(existing_by_handle[handle])}"
                )

        progress(7, 9, "Resolve mixed conditions and build Preview plans")
        plans = _build_plans(
            rows=rows,
            metafield_definitions=definitions,
            publish_all_channels=publish_all_channels,
            existing_by_handle=existing_by_handle,
        )
        ready = [plan for plan in plans if plan["status"] == "READY"]
        blocked = [plan for plan in plans if plan["status"] == "BLOCKED"]
        print(f"[Plan] collections={len(plans)} | READY={len(ready)} | BLOCKED={len(blocked)}")
        for plan in blocked[:20]:
            print(f"  BLOCKED | {plan['handle']} | {plan['block_reason']}")

        rows_written = 0
        progress(8, 9, f"Overwrite Preview | enabled={write_preview}")
        if write_preview:
            matrix = _preview_matrix(
                plans=plans,
                run_id=run_id,
                site_code=site_code,
                tz_name=tz_name,
            )
            rows_written = _write_single_header_matrix_overwrite(create_book, tab_preview, matrix)

        final_status = "READY_FOR_APPLY" if ready and not blocked else (
            "PARTIAL_BLOCKED" if ready and blocked else "BLOCKED"
        )
        logger.log(
            phase=phase,
            log_type="summary",
            status=final_status,
            entity_type="COLLECTION_CREATE",
            rows_loaded=len(rows),
            rows_pending=len(plans),
            rows_recognized=len(rows),
            rows_planned=len(ready),
            rows_written=rows_written,
            rows_skipped=len(blocked),
            message=(
                f"collections={len(plans)} | ready={len(ready)} | blocked={len(blocked)} | "
                f"publish_all_channels={publish_all_channels} | "
                f"shopify_requests={client.request_count} | shopify_retries={client.retry_count}"
            ),
            error_reason="COLLECTIONS_BLOCKED" if blocked else "",
        )
        gp._sheets_retry("write final RunLog", logger.flush)

        elapsed = round(time.monotonic() - started, 2)
        progress(9, 9, f"Completed | status={final_status} | elapsed={elapsed}s")
        df = pd.DataFrame(
            [
                {
                    key: plan[key]
                    for key in [
                        "title",
                        "handle",
                        "match_type",
                        "condition_count",
                        "resolved_conditions",
                        "publish_all_channels",
                        "status",
                        "block_reason",
                        "plan_hash",
                    ]
                }
                for plan in plans
            ]
        )
        return {
            "ok": bool(ready),
            "status": final_status,
            "ready_for_apply": bool(ready),
            "run_id": run_id,
            "job_name": job_name,
            "summary": {
                "input_condition_rows": len(rows),
                "collections_total": len(plans),
                "collections_ready": len(ready),
                "collections_blocked": len(blocked),
                "preview_rows_written": rows_written,
                "publish_all_channels": publish_all_channels,
                "shopify_requests": client.request_count,
                "shopify_retries": client.retry_count,
                "elapsed_seconds": elapsed,
            },
            "plans": plans,
            "preview": df,
            "runtime": {
                "runtime_mode": gp._runtime_mode(),
                "google_secret_source": google_auth["source_type"],
                "shopify_api_version": client.api_version,
                "shop_domain": client.shop_domain,
                "python": sys.version.split()[0],
            },
            "targets": {
                "create_sheet_label": create_sheet_label,
                "input_tab": tab_input,
                "preview_tab": tab_preview,
                "result_tab": tab_result,
                "defaults_tab": tab_defaults,
                "runlog_sheet_label": runlog_sheet_label,
                "runlog_tab": tab_runlog,
            },
        }
    except Exception as exc:
        try:
            logger.log(
                phase=phase,
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
                error_reason="PREPARE_FAILED",
            )
            gp._sheets_retry("write failed RunLog", logger.flush)
        except Exception:
            pass
        raise


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare Shopify Collection Create plans")
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument("--bootstrap-gsheet-sa-b64-secret", required=True)
    parser.add_argument("--create-sheet-label", default="create_collection")
    parser.add_argument("--runlog-sheet-label", default="runlog_sheet")
    parser.add_argument("--no-write-preview", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=args.bootstrap_gsheet_sa_b64_secret,
        create_sheet_label=args.create_sheet_label,
        runlog_sheet_label=args.runlog_sheet_label,
        write_preview=not args.no_write_preview,
    )
    print(json.dumps({k: v for k, v in result.items() if k not in {"plans", "preview"}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
