# shopify_ops/config_fields.py
# -*- coding: utf-8 -*-

"""
Job: config_fields

Purpose:
- Multi-site Shopify field dictionary sync.
- Read site account config from Console Core / Cfg__account_id.
- Cfg__account_id uses vertical key-value structure:
    SHOP_DOMAIN              544104.myshopify.com
    SHOPIFY_API_VERSION      2026-01
    META_AD_ACCOUNT_ID       399370612438108
    AWIN_ADVERTISER_ID       77532
    STOREFRONT_BASE_URL      https://plumbingsell.com/
    ADMIN_BASE_URL           https://admin.shopify.com/store/544104/
    GSHEET_SA_B64_SECRET     PBS_GSHEET
    SHOPIFY_TOKEN_SECRET     PBS_SHOPIFY_ACCESS_TOKEN

- Read business sheet route from Console Core / Cfg__Sites.
- Export core fields + Shopify metafield definitions into Cfg__Fields.
- Generate admin/storefront URL formula fields using ADMIN_BASE_URL and STOREFRONT_BASE_URL.
- Skip metafield namespace = "shopify".
- No fallback. Missing tab/field/label/secret mismatch raises clear error.

Main entry:
    run(...)
"""

from __future__ import annotations

import base64
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Constants
# ============================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

ACCOUNT_TAB = "Cfg__account_id"
SITES_TAB = "Cfg__Sites"
FIELDS_TAB_DEFAULT = "Cfg__Fields"

REQUIRED_ACCOUNT_FIELDS = [
    "SHOP_DOMAIN",
    "SHOPIFY_API_VERSION",
    "GSHEET_SA_B64_SECRET",
    "SHOPIFY_TOKEN_SECRET",
    "STOREFRONT_BASE_URL",
    "ADMIN_BASE_URL",
    "META_AD_ACCOUNT_ID",
    "AWIN_ADVERTISER_ID",
]

REQUIRED_SITES_FIELDS = [
    "site_code",
    "label",
    "sheet_url",
]

CFG_FIELDS_COLUMNS = [
    "field_handle",
    "field_id",
    "display_name",
    "entity_type",
    "field_key",
    "expr",
    "field_type",
    "data_type",
    "source_type",
    "namespace",
    "key",
    "purpose_1",
    "purpose_2",
    "seq",
    "lookup_key",
    "join_key",
    "unit",
    "suffix_role",
    "concept_id",
    "group",
    "applies_big_type",
    "applies_sub_type",
    "notes",
]

ENTITY_ORDER = {
    "VARIANT": 10,
    "PRODUCT": 20,
    "COLLECTION": 30,
    "PAGE": 40,
    "ORDER": 50,
    "METAOBJECT_ENTRY": 60,
}

SHOPIFY_OWNER_TYPES = [
    "PRODUCT",
    "PRODUCTVARIANT",
    "COLLECTION",
    "PAGE",
    "ORDER",
]


# ============================================================
# Errors
# ============================================================

class ConfigFieldsError(RuntimeError):
    pass


# ============================================================
# Secret / Google Auth
# ============================================================

def _read_secret(secret_name: str) -> str:
    """
    Read secret by explicit secret name.
    In Colab, reads google.colab.userdata.
    Outside Colab, reads environment variable.
    No default credential fallback.
    """
    if not secret_name:
        raise ConfigFieldsError("Secret name is empty.")

    try:
        from google.colab import userdata  # type: ignore
        value = userdata.get(secret_name)
        if value:
            return str(value).strip()
    except Exception:
        pass

    value = os.environ.get(secret_name)
    if value:
        return str(value).strip()

    raise ConfigFieldsError(
        f"Missing secret: {secret_name}. "
        f"Please create this exact secret in Colab Secrets or environment variables."
    )


def _make_gspread_client_from_b64_secret(secret_name: str) -> gspread.Client:
    raw_b64 = _read_secret(secret_name)

    try:
        sa_json = base64.b64decode(raw_b64).decode("utf-8")
        sa_info = json.loads(sa_json)
    except Exception as e:
        raise ConfigFieldsError(
            f"Secret {secret_name} is not a valid base64-encoded Google service account JSON."
        ) from e

    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)


# ============================================================
# Google Sheets Helpers
# ============================================================

def _open_spreadsheet(gc: gspread.Client, sheet_url: str) -> gspread.Spreadsheet:
    if not sheet_url:
        raise ConfigFieldsError("Google Sheet URL is empty.")

    try:
        return gc.open_by_url(sheet_url)
    except Exception as e:
        raise ConfigFieldsError(f"Failed to open Google Sheet by URL: {sheet_url}") from e


def _get_worksheet(ss: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return ss.worksheet(title)
    except Exception as e:
        raise ConfigFieldsError(
            f"Missing worksheet: {title} in spreadsheet: {ss.url}"
        ) from e


def _get_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception as e:
        raise ConfigFieldsError(f"Failed to read worksheet: {ws.title}") from e


def _normalize_header_name(s: Any) -> str:
    return str(s or "").strip()


def _require_columns(rows: List[Dict[str, Any]], required: List[str], tab_name: str) -> None:
    if not rows:
        raise ConfigFieldsError(f"Tab {tab_name} has no data rows.")

    columns = set(_normalize_header_name(k) for k in rows[0].keys())
    missing = [c for c in required if c not in columns]

    if missing:
        raise ConfigFieldsError(
            f"Tab {tab_name} missing required columns: {missing}. "
            f"Existing columns: {sorted(columns)}"
        )


def _clear_and_write(
    ws: gspread.Worksheet,
    rows: List[Dict[str, Any]],
    columns: List[str],
) -> None:
    values = [columns]

    for r in rows:
        values.append([r.get(c, "") for c in columns])

    ws.clear()

    if values:
        ws.update("A1", values, value_input_option="USER_ENTERED")


# ============================================================
# Console Core Config
# ============================================================

def _read_account_config(
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    site_code: str,
) -> Tuple[gspread.Client, Dict[str, str]]:
    """
    Read Cfg__account_id from Console Core.

    Expected structure: vertical key-value table.

    Example:
        SHOP_DOMAIN              544104.myshopify.com
        SHOPIFY_API_VERSION      2026-01
        META_AD_ACCOUNT_ID       399370612438108
        AWIN_ADVERTISER_ID       77532
        STOREFRONT_BASE_URL      https://plumbingsell.com/
        ADMIN_BASE_URL           https://admin.shopify.com/store/544104/
        GSHEET_SA_B64_SECRET     PBS_GSHEET
        SHOPIFY_TOKEN_SECRET     PBS_SHOPIFY_ACCESS_TOKEN

    No fallback:
    - missing tab raises
    - missing required key raises
    - empty required value raises, except META_AD_ACCOUNT_ID / AWIN_ADVERTISER_ID
    - BOOTSTRAP_GSHEET_SA_B64_SECRET mismatch raises
    """
    gc = _make_gspread_client_from_b64_secret(bootstrap_gsheet_sa_b64_secret)
    ss = _open_spreadsheet(gc, console_core_url)
    ws = _get_worksheet(ss, ACCOUNT_TAB)

    try:
        values = ws.get_all_values()
    except Exception as e:
        raise ConfigFieldsError(f"Failed to read worksheet: {ACCOUNT_TAB}") from e

    if not values:
        raise ConfigFieldsError(f"Tab {ACCOUNT_TAB} is empty.")

    cfg: Dict[str, str] = {}

    for row_idx, row in enumerate(values, start=1):
        if not row:
            continue

        key = str(row[0] if len(row) >= 1 else "").strip()
        value = str(row[1] if len(row) >= 2 else "").strip()

        if not key:
            continue

        # Skip possible header rows.
        if key.lower() in ["key", "field", "config_key", "name"]:
            continue

        cfg[key] = value

    missing = [k for k in REQUIRED_ACCOUNT_FIELDS if k not in cfg]
    if missing:
        raise ConfigFieldsError(
            f"Tab {ACCOUNT_TAB} missing required config keys: {missing}. "
            f"Existing keys: {sorted(cfg.keys())}"
        )

    empty_required = [
        k for k in REQUIRED_ACCOUNT_FIELDS
        if str(cfg.get(k, "")).strip() == ""
        and k not in ["META_AD_ACCOUNT_ID", "AWIN_ADVERTISER_ID"]
    ]

    if empty_required:
        raise ConfigFieldsError(
            f"Tab {ACCOUNT_TAB} has empty required config values: {empty_required}"
        )

    if cfg["GSHEET_SA_B64_SECRET"] != bootstrap_gsheet_sa_b64_secret:
        raise ConfigFieldsError(
            "BOOTSTRAP_GSHEET_SA_B64_SECRET mismatch. "
            f"Cell 1 has {bootstrap_gsheet_sa_b64_secret}, "
            f"but {ACCOUNT_TAB}.GSHEET_SA_B64_SECRET has {cfg['GSHEET_SA_B64_SECRET']}. "
            "Stop. Do not fallback."
        )

    cfg["SITE_CODE"] = site_code

    return gc, cfg


def _read_site_routes(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
) -> List[Dict[str, Any]]:
    ss = _open_spreadsheet(gc, console_core_url)
    ws = _get_worksheet(ss, SITES_TAB)
    rows = _get_records(ws)

    _require_columns(rows, REQUIRED_SITES_FIELDS, SITES_TAB)

    site_rows = [
        r for r in rows
        if str(r.get("site_code", "")).strip().upper() == str(site_code).strip().upper()
    ]

    if not site_rows:
        raise ConfigFieldsError(
            f"No rows found in {SITES_TAB} for SITE_CODE={site_code}."
        )

    return site_rows


def _find_sheet_url_by_label(
    routes: List[Dict[str, Any]],
    label: str,
    site_code: str,
) -> str:
    matched = [
        r for r in routes
        if str(r.get("label", "")).strip() == label
    ]

    if not matched:
        raise ConfigFieldsError(
            f"Missing label={label} in {SITES_TAB} for SITE_CODE={site_code}."
        )

    if len(matched) > 1:
        raise ConfigFieldsError(
            f"Multiple rows found for label={label} in {SITES_TAB} for SITE_CODE={site_code}."
        )

    sheet_url = str(matched[0].get("sheet_url", "")).strip()

    if not sheet_url:
        raise ConfigFieldsError(
            f"{SITES_TAB} label={label} has empty sheet_url for SITE_CODE={site_code}."
        )

    return sheet_url


# ============================================================
# Shopify Helpers
# ============================================================

def _shopify_graphql(
    shop_domain: str,
    api_version: str,
    token: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"

    resp = requests.post(
        url,
        headers={
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "variables": variables or {},
        },
        timeout=60,
    )

    if resp.status_code >= 400:
        raise ConfigFieldsError(
            f"Shopify GraphQL HTTP error {resp.status_code}: {resp.text[:1000]}"
        )

    data = resp.json()

    if data.get("errors"):
        raise ConfigFieldsError(
            f"Shopify GraphQL errors: {json.dumps(data['errors'], ensure_ascii=False)}"
        )

    return data


def _fetch_metafield_definitions(
    shop_domain: str,
    api_version: str,
    token: str,
) -> List[Dict[str, Any]]:
    query = """
    query MetafieldDefinitions($ownerType: MetafieldOwnerType!, $after: String) {
      metafieldDefinitions(first: 250, ownerType: $ownerType, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            id
            name
            namespace
            key
            ownerType
            type {
              name
            }
            description
          }
        }
      }
    }
    """

    out: List[Dict[str, Any]] = []

    for owner_type in SHOPIFY_OWNER_TYPES:
        after = None

        while True:
            data = _shopify_graphql(
                shop_domain=shop_domain,
                api_version=api_version,
                token=token,
                query=query,
                variables={
                    "ownerType": owner_type,
                    "after": after,
                },
            )

            conn = data["data"]["metafieldDefinitions"]

            for edge in conn.get("edges", []):
                node = edge.get("node", {})
                namespace = str(node.get("namespace", "")).strip()

                # Requirement:
                # namespace = shopify must not be exported.
                if namespace.lower() == "shopify":
                    continue

                out.append(node)

            page_info = conn.get("pageInfo", {})

            if not page_info.get("hasNextPage"):
                break

            after = page_info.get("endCursor")
            time.sleep(0.2)

    return out


# ============================================================
# Field Builders
# ============================================================

def _ensure_trailing_slash(url: str, field_name: str) -> str:
    url = str(url or "").strip()

    if not url:
        raise ConfigFieldsError(f"{field_name} is empty.")

    return url if url.endswith("/") else url + "/"


def _formula_safe_url(url: str) -> str:
    return str(url).replace('"', '""')


def _core_rows(admin_base_url: str, storefront_base_url: str) -> List[Dict[str, Any]]:
    admin_base_url = _formula_safe_url(
        _ensure_trailing_slash(admin_base_url, "ADMIN_BASE_URL")
    )
    storefront_base_url = _formula_safe_url(
        _ensure_trailing_slash(storefront_base_url, "STOREFRONT_BASE_URL")
    )

    return [
        {
            "field_handle": "PRODUCT|core.gid",
            "field_id": "PRODUCT|core.gid",
            "display_name": "Product GID",
            "entity_type": "PRODUCT",
            "field_key": "core.gid",
            "expr": "id",
            "field_type": "CORE",
            "data_type": "single_line_text_field",
            "source_type": "CORE",
            "namespace": "",
            "key": "gid",
            "purpose_1": "product identity",
            "purpose_2": "",
            "seq": 100,
            "lookup_key": "PRODUCT|core.gid",
            "join_key": "PRODUCT|core.gid",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "core",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "",
        },
        {
            "field_handle": "PRODUCT|core.legacy_id",
            "field_id": "PRODUCT|core.legacy_id",
            "display_name": "Product ID",
            "entity_type": "PRODUCT",
            "field_key": "core.legacy_id",
            "expr": "legacyResourceId",
            "field_type": "CORE",
            "data_type": "number_integer",
            "source_type": "CORE",
            "namespace": "",
            "key": "legacy_id",
            "purpose_1": "product identity",
            "purpose_2": "",
            "seq": 110,
            "lookup_key": "PRODUCT|core.legacy_id",
            "join_key": "PRODUCT|core.legacy_id",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "core",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "",
        },
        {
            "field_handle": "PRODUCT|core.handle",
            "field_id": "PRODUCT|core.handle",
            "display_name": "Handle",
            "entity_type": "PRODUCT",
            "field_key": "core.handle",
            "expr": "handle",
            "field_type": "CORE",
            "data_type": "single_line_text_field",
            "source_type": "CORE",
            "namespace": "",
            "key": "handle",
            "purpose_1": "product handle",
            "purpose_2": "",
            "seq": 120,
            "lookup_key": "PRODUCT|core.handle",
            "join_key": "PRODUCT|core.handle",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "core",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "",
        },
        {
            "field_handle": "PRODUCT|core.title",
            "field_id": "PRODUCT|core.title",
            "display_name": "Product Title",
            "entity_type": "PRODUCT",
            "field_key": "core.title",
            "expr": "title",
            "field_type": "CORE",
            "data_type": "single_line_text_field",
            "source_type": "CORE",
            "namespace": "",
            "key": "title",
            "purpose_1": "product title",
            "purpose_2": "",
            "seq": 130,
            "lookup_key": "PRODUCT|core.title",
            "join_key": "PRODUCT|core.title",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "core",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "",
        },
        {
            "field_handle": "PRODUCT|core.product.admin_url",
            "field_id": "PRODUCT|core.product.admin_url",
            "display_name": "Admin URL",
            "entity_type": "PRODUCT",
            "field_key": "core.product.admin_url",
            "expr": (
                f'=IF(LEN({{PRODUCT|core.legacy_id}}&"")=0,"",'
                f'"{admin_base_url}products/"&{{PRODUCT|core.legacy_id}})'
            ),
            "field_type": "CALC",
            "data_type": "url",
            "source_type": "CALC",
            "namespace": "",
            "key": "admin_url",
            "purpose_1": "product admin url",
            "purpose_2": "",
            "seq": 140,
            "lookup_key": "",
            "join_key": "",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "url",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "ADMIN_BASE_URL from Cfg__account_id",
        },
        {
            "field_handle": "PRODUCT|core.product.storefront_url",
            "field_id": "PRODUCT|core.product.storefront_url",
            "display_name": "StoreFront URL",
            "entity_type": "PRODUCT",
            "field_key": "core.product.storefront_url",
            "expr": (
                f'=IF(LEN({{PRODUCT|core.handle}}&"")=0,"",'
                f'"{storefront_base_url}products/"&{{PRODUCT|core.handle}})'
            ),
            "field_type": "CALC",
            "data_type": "url",
            "source_type": "CALC",
            "namespace": "",
            "key": "storefront_url",
            "purpose_1": "product storefront url",
            "purpose_2": "",
            "seq": 150,
            "lookup_key": "",
            "join_key": "",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "url",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "STOREFRONT_BASE_URL from Cfg__account_id",
        },
        {
            "field_handle": "VARIANT|core.gid",
            "field_id": "VARIANT|core.gid",
            "display_name": "Variant GID",
            "entity_type": "VARIANT",
            "field_key": "core.gid",
            "expr": "id",
            "field_type": "CORE",
            "data_type": "single_line_text_field",
            "source_type": "CORE",
            "namespace": "",
            "key": "gid",
            "purpose_1": "variant identity",
            "purpose_2": "",
            "seq": 200,
            "lookup_key": "VARIANT|core.gid",
            "join_key": "VARIANT|core.gid",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "core",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "",
        },
        {
            "field_handle": "VARIANT|core.legacy_id",
            "field_id": "VARIANT|core.legacy_id",
            "display_name": "Variant ID",
            "entity_type": "VARIANT",
            "field_key": "core.legacy_id",
            "expr": "legacyResourceId",
            "field_type": "CORE",
            "data_type": "number_integer",
            "source_type": "CORE",
            "namespace": "",
            "key": "legacy_id",
            "purpose_1": "variant identity",
            "purpose_2": "",
            "seq": 210,
            "lookup_key": "VARIANT|core.legacy_id",
            "join_key": "VARIANT|core.legacy_id",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "core",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "",
        },
        {
            "field_handle": "VARIANT|core.sku",
            "field_id": "VARIANT|core.sku",
            "display_name": "SKU",
            "entity_type": "VARIANT",
            "field_key": "core.sku",
            "expr": "sku",
            "field_type": "CORE",
            "data_type": "single_line_text_field",
            "source_type": "CORE",
            "namespace": "",
            "key": "sku",
            "purpose_1": "variant sku",
            "purpose_2": "",
            "seq": 220,
            "lookup_key": "VARIANT|core.sku",
            "join_key": "VARIANT|core.sku",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "core",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "",
        },
        {
            "field_handle": "VARIANT|core.variant.admin_url",
            "field_id": "VARIANT|core.variant.admin_url",
            "display_name": "Variant Admin URL",
            "entity_type": "VARIANT",
            "field_key": "core.variant.admin_url",
            "expr": (
                f'=IF(OR(LEN({{PRODUCT|core.legacy_id}}&"")=0,'
                f'LEN({{VARIANT|core.legacy_id}}&"")=0),"",'
                f'"{admin_base_url}products/"&{{PRODUCT|core.legacy_id}}'
                f'&"/variants/"&{{VARIANT|core.legacy_id}})'
            ),
            "field_type": "CALC",
            "data_type": "url",
            "source_type": "CALC",
            "namespace": "",
            "key": "admin_url",
            "purpose_1": "variant admin url",
            "purpose_2": "",
            "seq": 230,
            "lookup_key": "",
            "join_key": "",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "url",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "ADMIN_BASE_URL from Cfg__account_id",
        },
        {
            "field_handle": "VARIANT|core.variant.storefront_url",
            "field_id": "VARIANT|core.variant.storefront_url",
            "display_name": "Variant StoreFront URL",
            "entity_type": "VARIANT",
            "field_key": "core.variant.storefront_url",
            "expr": (
                f'=IF(LEN({{PRODUCT|core.handle}}&"")=0,"",'
                f'"{storefront_base_url}products/"&{{PRODUCT|core.handle}})'
            ),
            "field_type": "CALC",
            "data_type": "url",
            "source_type": "CALC",
            "namespace": "",
            "key": "storefront_url",
            "purpose_1": "variant storefront url",
            "purpose_2": "",
            "seq": 240,
            "lookup_key": "",
            "join_key": "",
            "unit": "",
            "suffix_role": "",
            "concept_id": "",
            "group": "url",
            "applies_big_type": "",
            "applies_sub_type": "",
            "notes": "STOREFRONT_BASE_URL from Cfg__account_id",
        },
    ]


def _owner_type_to_entity_type(owner_type: str) -> str:
    owner_type = str(owner_type or "").strip().upper()

    if owner_type == "PRODUCTVARIANT":
        return "VARIANT"

    return owner_type


def _metafield_rows(defs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for i, d in enumerate(defs, start=1):
        namespace = str(d.get("namespace", "")).strip()
        key = str(d.get("key", "")).strip()

        if namespace.lower() == "shopify":
            continue

        entity_type = _owner_type_to_entity_type(d.get("ownerType", ""))
        prefix = "v_mf" if entity_type == "VARIANT" else "mf"
        field_key = f"{prefix}.{namespace}.{key}"

        display_name = str(d.get("name", "") or key).strip()

        data_type = ""
        if isinstance(d.get("type"), dict):
            data_type = str(d["type"].get("name", "")).strip()

        rows.append(
            {
                "field_handle": f"{entity_type}|{field_key}",
                "field_id": f"{entity_type}|{field_key}",
                "display_name": display_name,
                "entity_type": entity_type,
                "field_key": field_key,
                "expr": f"metafield:{namespace}.{key}",
                "field_type": "METAFIELD",
                "data_type": data_type,
                "source_type": "SHOPIFY_METAFIELD_DEFINITION",
                "namespace": namespace,
                "key": key,
                "purpose_1": "",
                "purpose_2": "",
                "seq": 10000 + i,
                "lookup_key": f"{entity_type}|{field_key}",
                "join_key": "",
                "unit": "",
                "suffix_role": "",
                "concept_id": "",
                "group": "metafield",
                "applies_big_type": "",
                "applies_sub_type": "",
                "notes": str(d.get("description", "") or "").strip(),
            }
        )

    return rows


def _sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(r: Dict[str, Any]):
        entity_type = str(r.get("entity_type", "")).strip().upper()

        seq_raw = r.get("seq", "")
        try:
            seq = int(float(seq_raw))
        except Exception:
            seq = 999999

        field_key = str(r.get("field_key", ""))

        return (
            ENTITY_ORDER.get(entity_type, 999),
            seq,
            field_key,
        )

    return sorted(rows, key=sort_key)


def _normalize_rows_to_columns(
    rows: List[Dict[str, Any]],
    columns: List[str],
) -> List[Dict[str, Any]]:
    out = []

    for r in rows:
        out.append({c: r.get(c, "") for c in columns})

    return out


# ============================================================
# Main
# ============================================================

def run(
    SITE_CODE: str,
    JOB_NAME: str,
    CONSOLE_CORE_URL: str,
    BOOTSTRAP_GSHEET_SA_B64_SECRET: str,
    CONFIG_LABEL: str = "config",
    FIELDS_TAB: str = FIELDS_TAB_DEFAULT,
    WRITE_TO_SHEET: bool = True,
    DRY_RUN: bool = False,
) -> Dict[str, Any]:
    """
    Main entry.

    Cell 1 only passes:
    - SITE_CODE
    - JOB_NAME
    - CONSOLE_CORE_URL
    - BOOTSTRAP_GSHEET_SA_B64_SECRET
    - tab names / labels
    - job switches

    All site-specific config is read from Cfg__account_id.
    """
    if JOB_NAME != "config_fields":
        raise ConfigFieldsError(
            f"Unexpected JOB_NAME={JOB_NAME}. This module expects JOB_NAME=config_fields."
        )

    print(f"=== {JOB_NAME} start ===")
    print(f"SITE_CODE={SITE_CODE}")
    print(f"CONSOLE_CORE_URL={CONSOLE_CORE_URL}")
    print(f"CONFIG_LABEL={CONFIG_LABEL}")
    print(f"FIELDS_TAB={FIELDS_TAB}")
    print(f"WRITE_TO_SHEET={WRITE_TO_SHEET}")
    print(f"DRY_RUN={DRY_RUN}")

    gc, account_cfg = _read_account_config(
        console_core_url=CONSOLE_CORE_URL,
        bootstrap_gsheet_sa_b64_secret=BOOTSTRAP_GSHEET_SA_B64_SECRET,
        site_code=SITE_CODE,
    )

    print("✅ Account config loaded from Cfg__account_id")
    print(f"SHOP_DOMAIN={account_cfg['SHOP_DOMAIN']}")
    print(f"SHOPIFY_API_VERSION={account_cfg['SHOPIFY_API_VERSION']}")
    print(f"STOREFRONT_BASE_URL={account_cfg['STOREFRONT_BASE_URL']}")
    print(f"ADMIN_BASE_URL={account_cfg['ADMIN_BASE_URL']}")

    routes = _read_site_routes(
        gc=gc,
        console_core_url=CONSOLE_CORE_URL,
        site_code=SITE_CODE,
    )

    config_sheet_url = _find_sheet_url_by_label(
        routes=routes,
        label=CONFIG_LABEL,
        site_code=SITE_CODE,
    )

    print(f"✅ Config sheet resolved by Cfg__Sites label={CONFIG_LABEL}")
    print(f"config_sheet_url={config_sheet_url}")

    shopify_token_secret = account_cfg["SHOPIFY_TOKEN_SECRET"]
    shopify_token = _read_secret(shopify_token_secret)

    print("Fetching Shopify metafield definitions...")

    defs = _fetch_metafield_definitions(
        shop_domain=account_cfg["SHOP_DOMAIN"],
        api_version=account_cfg["SHOPIFY_API_VERSION"],
        token=shopify_token,
    )

    print(
        "✅ Shopify metafield definitions loaded, "
        f"after namespace=shopify filter: {len(defs)}"
    )

    rows: List[Dict[str, Any]] = []

    rows.extend(
        _core_rows(
            admin_base_url=account_cfg["ADMIN_BASE_URL"],
            storefront_base_url=account_cfg["STOREFRONT_BASE_URL"],
        )
    )

    rows.extend(_metafield_rows(defs))

    rows = _sort_rows(rows)
    rows = _normalize_rows_to_columns(rows, CFG_FIELDS_COLUMNS)

    summary = {
        "job_name": JOB_NAME,
        "site_code": SITE_CODE,
        "config_sheet_url": config_sheet_url,
        "fields_tab": FIELDS_TAB,
        "rows_planned": len(rows),
        "write_to_sheet": WRITE_TO_SHEET,
        "dry_run": DRY_RUN,
    }

    print("=== Plan Summary ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if DRY_RUN or not WRITE_TO_SHEET:
        print("DRY_RUN=True or WRITE_TO_SHEET=False. Skip writing.")
        return {
            "status": "dry_run",
            "summary": summary,
            "sample_rows": rows[:10],
        }

    config_ss = _open_spreadsheet(gc, config_sheet_url)
    fields_ws = _get_worksheet(config_ss, FIELDS_TAB)

    print(f"Writing {len(rows)} rows to {FIELDS_TAB} ...")

    _clear_and_write(
        ws=fields_ws,
        rows=rows,
        columns=CFG_FIELDS_COLUMNS,
    )

    print("✅ Done.")

    return {
        "status": "ok",
        "summary": summary,
    }
