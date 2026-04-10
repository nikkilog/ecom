# -*- coding: utf-8 -*-
"""
shopify_ops/config_fields.py

目的：
- 从 Shopify 拉取 metafield definitions / metaobject definitions
- 合并 CORE_FIXED
- 过滤 namespace 含 shopify 的字段
- 按固定规则排序
- 与现有 Cfg__Fields 按 PK(entity_type + field_key) merge
- 保留人工列
- 整表重写 Cfg__Fields
- 写 RunLog（新版 18 列）

依赖：
- gspread
- pandas
- requests
- google-auth

约定：
- console_core_url + worksheet title 读表
- 不依赖 gid
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials


# =========================================================
# 常量
# =========================================================

CFG_SITES_TAB = "Cfg__Sites"
CFG_FIELDS_TAB = "Cfg__Fields"
RUNLOG_TAB = "Ops__RunLog"

ENTITY_ORDER = {
    "VARIANT": 1,
    "PRODUCT": 2,
    "COLLECTION": 3,
    "PAGE": 4,
    "ORDER": 5,
    "METAOBJECT_ENTRY": 6,
}

SOURCE_TYPE_ORDER = {
    "CORE": 1,
    "SHOPIFY_METAFIELD": 2,
    "METAOBJECT_FIELD": 3,
}

OWNER_TYPE_TO_ENTITY = {
    "PRODUCT": "PRODUCT",
    "PRODUCTVARIANT": "VARIANT",
    "COLLECTION": "COLLECTION",
    "PAGE": "PAGE",
    "ORDER": "ORDER",
}

OWNER_TYPES = ["PRODUCT", "PRODUCTVARIANT", "COLLECTION", "PAGE", "ORDER"]

BASE_HEADERS = [
    "field_handle",   # 公式列
    "field_id",       # 公式列
    "entity_type",
    "field_key",
    "expr",
    "field_type",
    "data_type",
    "display_name",
    "source_type",
    "namespace",
    "key",
]

RUNLOG_HEADERS = [
    "run_id",
    "ts_cn",
    "job_name",
    "phase",
    "log_type",
    "status",
    "site_code",
    "entity_type",
    "gid",
    "field_key",
    "rows_loaded",
    "rows_pending",
    "rows_recognized",
    "rows_planned",
    "rows_written",
    "rows_skipped",
    "message",
    "error_reason",
]


# =========================================================
# CORE_FIXED
# 这里放“固定核心字段字典”
# 可继续追加，但建议保持 field_type = RAW / CALC 的统一语义
# =========================================================

def split_field_key(field_key: str) -> Tuple[str, str]:
    if not field_key:
        return "", ""
    if field_key.startswith("mf.") or field_key.startswith("v_mf.") or field_key.startswith("mo."):
        parts = field_key.split(".")
        if len(parts) >= 3:
            return parts[1], ".".join(parts[2:])
    if field_key.startswith("core."):
        return "core", field_key.replace("core.", "", 1)
    if "." in field_key:
        ns, k = field_key.split(".", 1)
        return ns, k
    return "", field_key


def _core_row(
    entity_type: str,
    field_key: str,
    expr: str,
    data_type: str,
    display_name: str,
) -> Dict[str, Any]:
    namespace, key = split_field_key(field_key)
    return {
        "entity_type": entity_type,
        "field_key": field_key,
        "expr": expr,
        "field_type": "RAW",
        "data_type": data_type,
        "display_name": display_name,
        "source_type": "CORE",
        "namespace": namespace,
        "key": key,
    }


CORE_FIXED: List[Dict[str, Any]] = [
    # ---------------- PRODUCT ----------------
    _core_row("PRODUCT", "core.gid", "product.id", "id", "GID"),
    _core_row("PRODUCT", "core.legacy_id", "product.legacyResourceId", "number", "Product ID (numeric)"),
    _core_row("PRODUCT", "core.handle", "product.handle", "string", "Handle"),
    _core_row("PRODUCT", "core.title", "product.title", "string", "Title"),
    _core_row("PRODUCT", "core.status", "product.status", "string", "Status"),
    _core_row("PRODUCT", "core.vendor", "product.vendor", "string", "Vendor"),
    _core_row("PRODUCT", "core.product_type", "product.productType", "string", "Product Type"),
    _core_row("PRODUCT", "core.tags", "JSON(product.tags)", "json", "Tags"),
    _core_row("PRODUCT", "core.created_at", "product.createdAt", "datetime", "Created At"),
    _core_row("PRODUCT", "core.updated_at", "product.updatedAt", "datetime", "Updated At"),
    _core_row("PRODUCT", "core.online_store_url", "product.onlineStoreUrl", "string", "Online Store URL"),
    _core_row("PRODUCT", "core.featured_image_url", 'GET(product.featuredMedia.preview.image, 0).url', "string", "Featured Image URL"),

    # ---------------- VARIANT ----------------
    _core_row("VARIANT", "core.gid", "variant.id", "id", "GID"),
    _core_row("VARIANT", "core.legacy_id", "variant.legacyResourceId", "number", "Variant ID (numeric)"),
    _core_row("VARIANT", "core.sku", "variant.sku", "string", "SKU"),
    _core_row("VARIANT", "core.title", "variant.title", "string", "Variant Title"),
    _core_row("VARIANT", "core.display_name", "variant.displayName", "string", "Display Name"),
    _core_row("VARIANT", "core.barcode", "variant.barcode", "string", "Barcode"),
    _core_row("VARIANT", "core.price", "variant.price", "number", "Price"),
    _core_row("VARIANT", "core.compare_at_price", "variant.compareAtPrice", "number", "Compare At Price"),
    _core_row("VARIANT", "core.position", "variant.position", "number", "Position"),
    _core_row("VARIANT", "core.taxable", "variant.taxable", "boolean", "Taxable"),
    _core_row("VARIANT", "core.inventory_policy", "variant.inventoryPolicy", "string", "Inventory Policy"),
    _core_row("VARIANT", "core.inventory_quantity", "variant.inventoryQuantity", "number", "Inventory Quantity"),
    _core_row("VARIANT", "core.created_at", "variant.createdAt", "datetime", "Created At"),
    _core_row("VARIANT", "core.updated_at", "variant.updatedAt", "datetime", "Updated At"),
    _core_row("VARIANT", "core.product_gid", "variant.product.id", "id", "Product GID"),
    _core_row("VARIANT", "core.product_legacy_id", "variant.product.legacyResourceId", "number", "Product ID (numeric)"),
    _core_row("VARIANT", "core.weight", "variant.inventoryItem.measurement.weight.value", "number", "Weight"),
    _core_row("VARIANT", "core.weight_unit", "variant.inventoryItem.measurement.weight.unit", "string", "Weight Unit"),
    _core_row("VARIANT", "core.cost", "variant.inventoryItem.unitCost.amount", "number", "Cost"),
    _core_row("VARIANT", "core.cost_currency", "variant.inventoryItem.unitCost.currencyCode", "string", "Cost Currency"),

    # ---------------- COLLECTION ----------------
    _core_row("COLLECTION", "core.gid", "collection.id", "id", "GID"),
    _core_row("COLLECTION", "core.legacy_id", "collection.legacyResourceId", "number", "Collection ID (numeric)"),
    _core_row("COLLECTION", "core.handle", "collection.handle", "string", "Handle"),
    _core_row("COLLECTION", "core.title", "collection.title", "string", "Title"),
    _core_row("COLLECTION", "core.updated_at", "collection.updatedAt", "datetime", "Updated At"),
    _core_row("COLLECTION", "core.online_store_url", "collection.onlineStoreUrl", "string", "Online Store URL"),

    # ---------------- PAGE ----------------
    _core_row("PAGE", "core.gid", "page.id", "id", "GID"),
    _core_row("PAGE", "core.legacy_id", "page.legacyResourceId", "number", "Page ID (numeric)"),
    _core_row("PAGE", "core.handle", "page.handle", "string", "Handle"),
    _core_row("PAGE", "core.title", "page.title", "string", "Title"),
    _core_row("PAGE", "core.created_at", "page.createdAt", "datetime", "Created At"),
    _core_row("PAGE", "core.updated_at", "page.updatedAt", "datetime", "Updated At"),

    # ---------------- ORDER ----------------
    _core_row("ORDER", "core.gid", "order.id", "id", "GID"),
    _core_row("ORDER", "core.legacy_id", "order.legacyResourceId", "number", "Order ID (numeric)"),
    _core_row("ORDER", "core.name", "order.name", "string", "Name"),
    _core_row("ORDER", "core.email", "order.email", "string", "Email"),
    _core_row("ORDER", "core.created_at", "order.createdAt", "datetime", "Created At"),
    _core_row("ORDER", "core.updated_at", "order.updatedAt", "datetime", "Updated At"),
    _core_row("ORDER", "core.processed_at", "order.processedAt", "datetime", "Processed At"),
    _core_row("ORDER", "core.cancelled_at", "order.cancelledAt", "datetime", "Cancelled At"),
    _core_row("ORDER", "core.currency_code", "order.currencyCode", "string", "Currency Code"),
    _core_row("ORDER", "core.display_financial_status", "order.displayFinancialStatus", "string", "Display Financial Status"),
    _core_row("ORDER", "core.display_fulfillment_status", "order.displayFulfillmentStatus", "string", "Display Fulfillment Status"),
    _core_row("ORDER", "core.total_price_amount", "order.currentTotalPriceSet.shopMoney.amount", "number", "Total Price Amount"),
    _core_row("ORDER", "core.subtotal_price_amount", "order.currentSubtotalPriceSet.shopMoney.amount", "number", "Subtotal Amount"),
    _core_row("ORDER", "core.total_tax_amount", "order.currentTotalTaxSet.shopMoney.amount", "number", "Total Tax Amount"),
    _core_row("ORDER", "core.total_shipping_amount", "order.totalShippingPriceSet.shopMoney.amount", "number", "Total Shipping Amount"),
    _core_row("ORDER", "core.total_discounts_amount", "order.currentTotalDiscountsSet.shopMoney.amount", "number", "Total Discounts Amount"),
    _core_row("ORDER", "core.shipping_address_name", "order.shippingAddress.name", "string", "Shipping Name"),
    _core_row("ORDER", "core.shipping_address_phone", "order.shippingAddress.phone", "string", "Shipping Phone"),
    _core_row("ORDER", "core.shipping_address_company", "order.shippingAddress.company", "string", "Shipping Company"),
    _core_row("ORDER", "core.shipping_address_address1", "order.shippingAddress.address1", "string", "Shipping Address 1"),
    _core_row("ORDER", "core.shipping_address_address2", "order.shippingAddress.address2", "string", "Shipping Address 2"),
    _core_row("ORDER", "core.shipping_address_city", "order.shippingAddress.city", "string", "Shipping City"),
    _core_row("ORDER", "core.shipping_address_province", "order.shippingAddress.province", "string", "Shipping Province"),
    _core_row("ORDER", "core.shipping_address_zip", "order.shippingAddress.zip", "string", "Shipping ZIP"),
    _core_row("ORDER", "core.shipping_address_country", "order.shippingAddress.country", "string", "Shipping Country"),
    _core_row("ORDER", "core.shipping_address_country_code", "order.shippingAddress.countryCodeV2", "string", "Shipping Country Code"),
    _core_row("ORDER", "core.shipping_lines_json", "JSON(order.shippingLines)", "json", "Shipping Lines JSON"),
    _core_row("ORDER", "core.tax_lines_json", "JSON(order.taxLines)", "json", "Tax Lines JSON"),
    _core_row("ORDER", "core.fulfillments_json", "JSON(order.fulfillments)", "json", "Fulfillments JSON"),
    _core_row("ORDER", "core.refunds_json", "JSON(order.refunds)", "json", "Refunds JSON"),
]


# =========================================================
# dataclass
# =========================================================

@dataclass
class RunContext:
    site_code: str
    job_name: str
    run_id: str
    ts_cn: str


# =========================================================
# 通用工具
# =========================================================

def now_ts_cn() -> str:
    # Asia/Shanghai
    from datetime import timedelta
    utc_now = datetime.now(timezone.utc)
    cn_now = utc_now.astimezone(timezone(timedelta(hours=8)))
    return cn_now.strftime("%Y-%m-%d %H:%M:%S")


def make_run_id(prefix: str = "cfg") -> str:
    return f"{prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


def extract_sheet_id(sheet_url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url or "")
    if not m:
        raise ValueError(f"无法从 sheet_url 提取 sheet_id: {sheet_url}")
    return m.group(1)




def normalize_text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def contains_shopify_namespace(namespace_value: Any) -> bool:
    return "shopify" in normalize_text(namespace_value).lower()


def build_pk(entity_type: Any, field_key: Any) -> str:
    return f"{normalize_text(entity_type)}||{normalize_text(field_key)}"


def chunked(seq: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def safe_get(d: Dict[str, Any], *keys, default=""):
    cur = d
    for k in keys:
        if cur is None:
            return default
        cur = cur.get(k)
    return default if cur is None else cur


# =========================================================
# Google Sheets
# =========================================================

def build_gspread_client_from_b64_secret(secret_b64: str) -> gspread.Client:
    raw = base64.b64decode(secret_b64).decode("utf-8")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_ws_by_url_and_title(gc: gspread.Client, sheet_url: str, worksheet_title: str):
    sh = gc.open_by_url(sheet_url)
    return sh.worksheet(worksheet_title)


def get_all_records_str(ws) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if not values:
        return []
    headers = values[0]
    rows = values[1:]
    out = []
    for row in rows:
        row = row + [""] * (len(headers) - len(row))
        out.append({headers[i]: row[i] for i in range(len(headers))})
    return out


def ensure_runlog_headers(ws):
    values = ws.get_all_values()
    if not values:
        ws.update("A1", [RUNLOG_HEADERS], value_input_option="USER_ENTERED")
        return
    head = values[0]
    if head[:len(RUNLOG_HEADERS)] != RUNLOG_HEADERS:
        ws.clear()
        ws.update("A1", [RUNLOG_HEADERS], value_input_option="USER_ENTERED")


# =========================================================
# Shopify GraphQL
# =========================================================

class ShopifyClient:
    def __init__(self, shop_domain: str, access_token: str, api_version: str = "2026-01", timeout: int = 60):
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_version = api_version
        self.timeout = timeout
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        })

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        resp = self.session.post(self.url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        if data.get("data") is None:
            raise RuntimeError(f"GraphQL no data: {data}")
        return data["data"]


Q_METAFIELD_DEFS = """
query($ownerType: MetafieldOwnerType!, $cursor: String) {
  metafieldDefinitions(first: 250, ownerType: $ownerType, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      name
      namespace
      key
      type { name }
      ownerType
    }
  }
}
"""

Q_METAOBJECT_DEFS = """
query($cursor: String) {
  metaobjectDefinitions(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      type
      name
      fieldDefinitions {
        key
        name
        type { name }
      }
    }
  }
}
"""


def fetch_metafield_definitions(shop: ShopifyClient, owner_type: str) -> List[Dict[str, Any]]:
    out = []
    cursor = None
    while True:
        data = shop.graphql(Q_METAFIELD_DEFS, {"ownerType": owner_type, "cursor": cursor})
        block = data["metafieldDefinitions"]
        out.extend(block["nodes"])
        if not block["pageInfo"]["hasNextPage"]:
            break
        cursor = block["pageInfo"]["endCursor"]
    return out


def fetch_metaobject_definitions(shop: ShopifyClient) -> List[Dict[str, Any]]:
    out = []
    cursor = None
    while True:
        data = shop.graphql(Q_METAOBJECT_DEFS, {"cursor": cursor})
        block = data["metaobjectDefinitions"]
        out.extend(block["nodes"])
        if not block["pageInfo"]["hasNextPage"]:
            break
        cursor = block["pageInfo"]["endCursor"]
    return out


# =========================================================
# 解析 / 组装行
# =========================================================

def build_rows_from_metafield_defs(nodes: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    rows = []
    skipped_shopify_ns = 0

    for node in nodes:
        namespace = normalize_text(node.get("namespace"))
        key = normalize_text(node.get("key"))
        owner_type = normalize_text(node.get("ownerType"))
        entity_type = OWNER_TYPE_TO_ENTITY.get(owner_type, "")

        if contains_shopify_namespace(namespace):
            skipped_shopify_ns += 1
            continue

        prefix = "v_mf" if entity_type == "VARIANT" else "mf"
        field_key = f"{prefix}.{namespace}.{key}"
        rows.append({
            "entity_type": entity_type,
            "field_key": field_key,
            "expr": f'MF_VALUE("{namespace}", "{key}")',
            "field_type": "RAW",
            "data_type": safe_get(node, "type", "name", default=""),
            "display_name": normalize_text(node.get("name")),
            "source_type": "SHOPIFY_METAFIELD",
            "namespace": namespace,
            "key": key,
        })

    return rows, skipped_shopify_ns


def build_rows_from_metaobject_defs(nodes: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    rows = []
    skipped_shopify_ns = 0

    for mo in nodes:
        mo_type = normalize_text(mo.get("type"))
        for fd in mo.get("fieldDefinitions", []) or []:
            fd_key = normalize_text(fd.get("key"))
            namespace = mo_type

            if contains_shopify_namespace(namespace):
                skipped_shopify_ns += 1
                continue

            field_key = f"mo.{mo_type}.{fd_key}"
            rows.append({
                "entity_type": "METAOBJECT_ENTRY",
                "field_key": field_key,
                "expr": f'MO_FIELD("{fd_key}")',
                "field_type": "RAW",
                "data_type": safe_get(fd, "type", "name", default=""),
                "display_name": normalize_text(fd.get("name")),
                "source_type": "METAOBJECT_FIELD",
                "namespace": mo_type,
                "key": fd_key,
            })

    return rows, skipped_shopify_ns


def build_core_rows() -> Tuple[List[Dict[str, Any]], int]:
    rows = []
    skipped_shopify_ns = 0
    for r in CORE_FIXED:
        if contains_shopify_namespace(r.get("namespace", "")):
            skipped_shopify_ns += 1
            continue
        rows.append(dict(r))
    return rows, skipped_shopify_ns


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = {}
    for r in rows:
        pk = build_pk(r.get("entity_type"), r.get("field_key"))
        seen[pk] = r
    return list(seen.values())


# =========================================================
# 排序 / merge / 公式
# =========================================================

def sort_key(row: Dict[str, Any]):
    return (
        ENTITY_ORDER.get(normalize_text(row.get("entity_type")), 999),
        SOURCE_TYPE_ORDER.get(normalize_text(row.get("source_type")), 999),
        "" if normalize_text(row.get("namespace")) == "" else "1",
        normalize_text(row.get("namespace")).lower(),
        normalize_text(row.get("key")).lower(),
        normalize_text(row.get("display_name")).lower(),
    )


def read_existing_cfg_fields(ws) -> Tuple[List[str], List[Dict[str, Any]]]:
    values = ws.get_all_values()
    if not values:
        return BASE_HEADERS.copy(), []
    headers = values[0]
    rows = []
    for raw in values[1:]:
        raw = raw + [""] * (len(headers) - len(raw))
        rows.append({headers[i]: raw[i] for i in range(len(headers))})
    return headers, rows


def merge_preserve_manual_columns(
    existing_headers: List[str],
    existing_rows: List[Dict[str, Any]],
    new_system_rows: List[Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]], int]:
    existing_by_pk = {
        build_pk(r.get("entity_type"), r.get("field_key")): r
        for r in existing_rows
        if normalize_text(r.get("entity_type")) and normalize_text(r.get("field_key"))
    }

    final_headers = BASE_HEADERS.copy()
    manual_headers = [h for h in existing_headers if h not in final_headers]
    final_headers.extend(manual_headers)

    preserved_cells = 0
    merged_rows = []

    for r in new_system_rows:
        pk = build_pk(r.get("entity_type"), r.get("field_key"))
        old = existing_by_pk.get(pk, {})

        merged = {}
        for h in BASE_HEADERS:
            merged[h] = r.get(h, "")

        for h in manual_headers:
            merged[h] = old.get(h, "")
            if normalize_text(old.get(h, "")) != "":
                preserved_cells += 1

        merged_rows.append(merged)

    return final_headers, merged_rows, preserved_cells


def add_formula_values(headers: List[str], rows: List[Dict[str, Any]]) -> List[List[Any]]:
    header_idx = {h: i + 1 for i, h in enumerate(headers)}  # 1-based
    out = [headers]

    col_entity = header_idx["entity_type"]
    col_field_key = header_idx["field_key"]
    col_display_name = header_idx["display_name"]

    for i, row in enumerate(rows, start=2):
        vals = []
        for h in headers:
            if h == "field_handle":
                formula = (
                    f'=IF(OR({col_letter(col_entity)}{i}="",{col_letter(col_display_name)}{i}=""),"",'
                    f'{col_letter(col_entity)}{i}&"|"&{col_letter(col_display_name)}{i})'
                )
                vals.append(formula)
            elif h == "field_id":
                formula = (
                    f'=IF(OR({col_letter(col_entity)}{i}="",{col_letter(col_field_key)}{i}=""),"",'
                    f'{col_letter(col_entity)}{i}&"|"&{col_letter(col_field_key)}{i})'
                )
                vals.append(formula)
            else:
                vals.append(row.get(h, ""))
        out.append(vals)

    return out


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


# =========================================================
# Cfg__Sites / labels
# =========================================================

def resolve_sheet_urls_from_cfg_sites(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
) -> Dict[str, str]:
    ws = open_ws_by_url_and_title(gc, console_core_url, CFG_SITES_TAB)
    rows = get_all_records_str(ws)

    hit = {}
    for r in rows:
        if normalize_text(r.get("site_code")).upper() != site_code.upper():
            continue
        label = normalize_text(r.get("label"))
        sheet_url = normalize_text(r.get("sheet_url"))
        if label and sheet_url:
            hit[label] = sheet_url

    need = ["config", "runlog_sheet"]
    miss = [x for x in need if x not in hit]
    if miss:
        raise RuntimeError(f"Cfg__Sites 缺少 label: {miss}")

    return hit


# =========================================================
# RunLog
# =========================================================

def append_runlog_rows(ws, rows: List[List[Any]]):
    if not rows:
        return
    ensure_runlog_headers(ws)
    start_row = len(ws.get_all_values()) + 1
    ws.update(f"A{start_row}", rows, value_input_option="USER_ENTERED")


def make_runlog_summary_row(
    ctx: RunContext,
    status: str,
    rows_loaded: int,
    rows_recognized: int,
    rows_planned: int,
    rows_written: int,
    rows_skipped: int,
    message: str,
    phase: str = "apply",
) -> List[Any]:
    return [
        ctx.run_id,
        ctx.ts_cn,
        ctx.job_name,
        phase,
        "summary",
        status,
        ctx.site_code,
        "",
        "",
        "",
        rows_loaded,
        rows_planned,
        rows_recognized,
        rows_planned,
        rows_written,
        rows_skipped,
        message,
        "",
    ]


# =========================================================
# 主执行
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    shop_domain: str,
    shopify_access_token: str,
    gsheet_sa_b64: str,
    api_version: str = "2026-01",
    job_name: str = "config_fields",
    worksheet_title: str = CFG_FIELDS_TAB,
    runlog_tab_name: str = RUNLOG_TAB,
    write_to_sheet: bool = True,
    write_runlog: bool = True,
    preview_limit: int = 20,
    sleep_sec: float = 0.0,
) -> Dict[str, Any]:
    """
    返回：
    {
      "summary": {...},
      "preview": pandas.DataFrame,
      "warnings": [...]
    }
    """
    gc = build_gspread_client_from_b64_secret(gsheet_sa_b64)
    ctx = RunContext(
        site_code=site_code,
        job_name=job_name,
        run_id=make_run_id("cfg"),
        ts_cn=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    )

    warnings = []

    # 1) resolve sheets
    url_map = resolve_sheet_urls_from_cfg_sites(gc, console_core_url, site_code)
    config_sheet_url = url_map["config"]
    runlog_sheet_url = url_map["runlog_sheet"]

    cfg_ws = open_ws_by_url_and_title(gc, config_sheet_url, worksheet_title)
    runlog_ws = open_ws_by_url_and_title(gc, runlog_sheet_url, runlog_tab_name)

    # 2) read existing
    existing_headers, existing_rows = read_existing_cfg_fields(cfg_ws)

    # 3) Shopify fetch
    shop = ShopifyClient(
        shop_domain=shop_domain,
        access_token=shopify_access_token,
        api_version=api_version,
        timeout=60,
    )

    all_mf_nodes = []
    for owner_type in OWNER_TYPES:
        nodes = fetch_metafield_definitions(shop, owner_type)
        all_mf_nodes.extend(nodes)
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    mo_nodes = fetch_metaobject_definitions(shop)

    # 4) build rows
    core_rows, skipped_core_shopify = build_core_rows()
    mf_rows, skipped_mf_shopify = build_rows_from_metafield_defs(all_mf_nodes)
    mo_rows, skipped_mo_shopify = build_rows_from_metaobject_defs(mo_nodes)

    raw_candidate_rows = core_rows + mf_rows + mo_rows
    raw_candidate_rows = dedupe_rows(raw_candidate_rows)

    # 5) sort
    raw_candidate_rows.sort(key=sort_key)

    # 6) merge preserve manual columns
    final_headers, merged_rows, preserved_cells = merge_preserve_manual_columns(
        existing_headers=existing_headers,
        existing_rows=existing_rows,
        new_system_rows=raw_candidate_rows,
    )

    # 7) build final sheet values
    final_values = add_formula_values(final_headers, merged_rows)

    # 8) write sheet
    rows_written = 0
    if write_to_sheet:
        cfg_ws.clear()
        for chunk in chunked(final_values, 500):
            start_row = rows_written + 1
            cfg_ws.update(
                f"A{start_row}",
                chunk,
                value_input_option="USER_ENTERED",
            )
            rows_written += len(chunk)

    # 9) summary
    rows_loaded = len(existing_rows)
    rows_recognized = len(raw_candidate_rows)
    rows_planned = len(merged_rows)
    rows_skipped = skipped_core_shopify + skipped_mf_shopify + skipped_mo_shopify

    summary = {
        "run_id": ctx.run_id,
        "site_code": site_code,
        "job_name": job_name,
        "rows_existing": rows_loaded,
        "core_fixed_count": len(core_rows),
        "metafield_def_count_raw": len(all_mf_nodes),
        "metaobject_def_count_raw": len(mo_nodes),
        "candidate_count_after_filter": rows_recognized,
        "preserved_manual_cells_count": preserved_cells,
        "rows_final": rows_planned,
        "rows_written_to_sheet": rows_written,
        "rows_skipped_shopify_namespace": rows_skipped,
    }

    if rows_skipped > 0:
        warnings.append(f"已过滤 namespace 含 shopify 的字段：{rows_skipped}")

    preview_df = pd.DataFrame(merged_rows).head(preview_limit)

    # 10) runlog
    if write_runlog:
        summary_row = make_runlog_summary_row(
            ctx=ctx,
            status="OK",
            rows_loaded=rows_loaded,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            message=f"Cfg__Fields rebuilt. preserved_manual_cells={preserved_cells}",
            phase="apply",
        )
        append_runlog_rows(runlog_ws, [summary_row])

    return {
        "summary": summary,
        "preview": preview_df,
        "warnings": warnings,
    }
