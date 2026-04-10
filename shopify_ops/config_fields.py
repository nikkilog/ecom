# -*- coding: utf-8 -*-
"""
shopify_ops/config_fields.py

目的
- 以“2_1_AP_Cfg__Fields (1).ipynb”为基准，增量同步 Cfg__Fields
- NEVER clear / NEVER overwrite existing rows（除 A/B 公式列）
- 只追加新的 PK(entity_type + field_key)
- 固定输出表头与顺序
- fixed fields 与 notebook 对齐，不漏字段
- 过滤 namespace/type 中包含 "shopify" 的字段
- 支持通过 console_core_url + Cfg__Sites(label=config / runlog_sheet) 定位目标表

说明
- 这是“notebook 逻辑的 py 化版本”
- 保留了 run() 入口，方便接入现有框架
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1


CFG_SITES_TAB = "Cfg__Sites"
CFG_FIELDS_TAB = "Cfg__Fields"
RUNLOG_TAB = "Ops__RunLog"

EXPECTED_HEADERS = [
    "field_handle", "field_id", "display_name", "entity_type", "field_key",
    "expr", "field_type", "data_type", "source_type", "namespace", "key",
    "purpose_1", "purpose_2", "seq", "lookup_key", "join_key", "unit",
    "suffix_role", "concept_id", "group", "applies_big_type", "applies_sub_type", "notes",
]

ALLOWED_COLS = [
    "display_name", "entity_type", "field_key", "expr", "field_type",
    "data_type", "source_type", "namespace", "key",
]

OWNER_TYPES_DEFAULT = ["PRODUCT", "PRODUCTVARIANT", "COLLECTION", "PAGE"]

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


def now_ts_cn() -> str:
    cn = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))
    return cn.strftime("%Y-%m-%d %H:%M:%S")


def make_run_id(prefix: str = "cfg") -> str:
    return f"{prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


def normalize_text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def namespace_blocked(ns: str) -> bool:
    return "shopify" in normalize_text(ns).lower()


def cfg_data_type_from_shopify_type_name(shopify_type_name: str) -> str:
    t = normalize_text(shopify_type_name)
    return t if t else "string"


def chunked(seq: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def col_letter(c: int) -> str:
    return rowcol_to_a1(1, c).replace("1", "")


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


def append_runlog_rows(ws, rows: List[List[Any]]):
    if not rows:
        return
    ensure_runlog_headers(ws)
    start_row = len(ws.get_all_values()) + 1
    ws.update(f"A{start_row}", rows, value_input_option="USER_ENTERED")


def make_runlog_summary_row(
    *,
    run_id: str,
    ts_cn: str,
    site_code: str,
    job_name: str,
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
        run_id,
        ts_cn,
        job_name,
        phase,
        "summary",
        status,
        site_code,
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


def resolve_sheet_urls_from_cfg_sites(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
) -> Dict[str, str]:
    ws = open_ws_by_url_and_title(gc, console_core_url, CFG_SITES_TAB)
    rows = get_all_records_str(ws)

    hit: Dict[str, str] = {}
    for r in rows:
        if normalize_text(r.get("site_code")).upper() != normalize_text(site_code).upper():
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


class ShopifyClient:
    def __init__(self, shop_domain: str, access_token: str, api_version: str = "2026-01", timeout: int = 60):
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        })
        self.timeout = timeout

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = self.session.post(self.url, json={"query": query, "variables": variables or {}}, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        if data.get("data") is None:
            raise RuntimeError(f"GraphQL no data: {data}")
        return data["data"]


Q_METAFIELD_DEFS = """
query MetafieldDefinitions($ownerType: MetafieldOwnerType!, $first: Int!, $after: String) {
  metafieldDefinitions(ownerType: $ownerType, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
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
query MetaobjectDefinitions($first: Int!, $after: String) {
  metaobjectDefinitions(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      type
      fieldDefinitions {
        name
        key
        required
        type { name }
      }
    }
  }
}
"""


def fetch_all_metafield_definitions(shop: ShopifyClient, owner_type: str, page_size: int = 250) -> List[Dict[str, Any]]:
    out, after = [], None
    while True:
        d = shop.graphql(Q_METAFIELD_DEFS, {"ownerType": owner_type, "first": page_size, "after": after})
        conn = d["metafieldDefinitions"]
        out.extend(conn["nodes"])
        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
    return out


def fetch_all_metaobject_definitions(shop: ShopifyClient, page_size: int = 250) -> List[Dict[str, Any]]:
    out, after = [], None
    while True:
        d = shop.graphql(Q_METAOBJECT_DEFS, {"first": page_size, "after": after})
        conn = d["metaobjectDefinitions"]
        out.extend(conn["nodes"])
        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
    return out


def build_core_fixed(admin_store_handle: str, storefront_base_url: str) -> List[Dict[str, Any]]:
    return [
        # COLLECTION
        {"display_name":"Collection GID","entity_type":"COLLECTION","field_key":"core.gid","expr":"collection.id","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Collection Handle","entity_type":"COLLECTION","field_key":"core.handle","expr":"collection.handle","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Collection Image","entity_type":"COLLECTION","field_key":"core.image_url","expr":"collection.image.url","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Collection ID (numeric)","entity_type":"COLLECTION","field_key":"core.legacy_id","expr":"collection.legacyResourceId","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Collection Name","entity_type":"COLLECTION","field_key":"core.title","expr":"collection.title","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Collection Description (HTML)","entity_type":"COLLECTION","field_key":"core.description_html","expr":"collection.descriptionHtml","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Collection Description (text)","entity_type":"COLLECTION","field_key":"core.description","expr":"collection.description","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},

        # METAOBJECT_ENTRY
        {"display_name":"Metaobject Entry Name","entity_type":"METAOBJECT_ENTRY","field_key":"core.display_name","expr":"metaobject.displayName","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Metaobject Entry GID","entity_type":"METAOBJECT_ENTRY","field_key":"core.gid","expr":"metaobject.id","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Metaobject Entry Handle","entity_type":"METAOBJECT_ENTRY","field_key":"core.handle","expr":"metaobject.handle","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Metaobject Entry ID (numeric)","entity_type":"METAOBJECT_ENTRY","field_key":"core.metaobject.id_numeric","expr":"GID_NUM({METAOBJECT_ENTRY|core.gid})","field_type":"CALC","data_type":"number","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Metaobject Type","entity_type":"METAOBJECT_ENTRY","field_key":"core.type","expr":"metaobject.type","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},

        # PAGE
        {"display_name":"Page GID","entity_type":"PAGE","field_key":"core.gid","expr":"page.id","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Page Handle","entity_type":"PAGE","field_key":"core.handle","expr":"page.handle","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Page ID (numeric)","entity_type":"PAGE","field_key":"core.page.id_numeric","expr":"GID_NUM({PAGE|core.gid})","field_type":"CALC","data_type":"number","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Page Name","entity_type":"PAGE","field_key":"core.title","expr":"page.title","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Page Body (HTML)","entity_type":"PAGE","field_key":"core.body_html","expr":"page.body","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},

        # PRODUCT
        {"display_name":"Created At","entity_type":"PRODUCT","field_key":"core.created_at","expr":"product.createdAt","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product GID","entity_type":"PRODUCT","field_key":"core.gid","expr":"product.id","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Handle","entity_type":"PRODUCT","field_key":"core.handle","expr":"product.handle","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product ID (numeric)","entity_type":"PRODUCT","field_key":"core.legacy_id","expr":"product.legacyResourceId","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Type","entity_type":"PRODUCT","field_key":"core.product_type","expr":"product.productType","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Description (HTML)","entity_type":"PRODUCT","field_key":"core.description_html","expr":"product.descriptionHtml","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Description (text)","entity_type":"PRODUCT","field_key":"core.description","expr":"product.description","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product SEO Title","entity_type":"PRODUCT","field_key":"core.seo_title","expr":"product.seo.title","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product SEO Description","entity_type":"PRODUCT","field_key":"core.seo_description","expr":"product.seo.description","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Weight","entity_type":"VARIANT","field_key":"core.weight","expr":"variant.weight","field_type":"RAW","data_type":"number","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Weight Unit","entity_type":"VARIANT","field_key":"core.weight_unit","expr":"variant.weightUnit","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"P_Admin URL","entity_type":"PRODUCT","field_key":"core.product.admin_url","expr":f'=IF(LEN({{PRODUCT|core.legacy_id}}&"")=0,"","https://admin.shopify.com/store/{admin_store_handle}/products/"&{{PRODUCT|core.legacy_id}})',"field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Image","entity_type":"PRODUCT","field_key":"core.product.image_preview","expr":'=IF(LEN({Product Image}&"")=0,"",IMAGE({Product Image}))',"field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Image","entity_type":"PRODUCT","field_key":"core.product.image_url","expr":"product.media[0].preview.image.url","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Options (JSON)","entity_type":"PRODUCT","field_key":"core.product.options_json","expr":"JSON({PRODUCT|raw.product.options})","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Storefront URL","entity_type":"PRODUCT","field_key":"core.product.storefront_url","expr":f'=IF(LEN({{PRODUCT|core.handle}}&"")=0,"","{storefront_base_url}/products/"&{{PRODUCT|core.handle}})',"field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Status","entity_type":"PRODUCT","field_key":"core.status","expr":"product.status","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Synced At","entity_type":"PRODUCT","field_key":"core.synced_at","expr":"","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Tags","entity_type":"PRODUCT","field_key":"core.tags","expr":"product.tags","field_type":"RAW","data_type":"list.string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Title","entity_type":"PRODUCT","field_key":"core.title","expr":"product.title","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Updated At","entity_type":"PRODUCT","field_key":"core.updated_at","expr":"product.updatedAt","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"(raw) Product featured image url","entity_type":"PRODUCT","field_key":"raw.product.featured_image_url","expr":"product.featuredImage.url","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"(raw) Product media(0) preview url","entity_type":"PRODUCT","field_key":"raw.product.media0_preview_url","expr":"product.media.nodes[0].preview.image.url","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"(raw) Product.options","entity_type":"PRODUCT","field_key":"raw.product.options","expr":"product.options","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},

        # VARIANT
        {"display_name":"Compare At Price","entity_type":"VARIANT","field_key":"core.compare_at_price","expr":"variant.compareAtPrice","field_type":"RAW","data_type":"number","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Variant Name","entity_type":"VARIANT","field_key":"core.display_name","expr":"variant.displayName","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Variant GID","entity_type":"VARIANT","field_key":"core.gid","expr":"variant.id","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Variant ID (numeric)","entity_type":"VARIANT","field_key":"core.legacy_id","expr":"variant.legacyResourceId","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Item_ID","entity_type":"VARIANT","field_key":"core.item_id","expr":'="shopify_us_"&{PRODUCT|core.legacy_id}&"_"&{VARIANT|core.legacy_id}',"field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Price","entity_type":"VARIANT","field_key":"core.price","expr":"variant.price","field_type":"RAW","data_type":"number","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product GID (parent)","entity_type":"VARIANT","field_key":"core.product_gid","expr":"variant.product.id","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Product Handle (parent)","entity_type":"VARIANT","field_key":"core.product_handle","expr":"variant.product.handle","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"SKU","entity_type":"VARIANT","field_key":"core.sku","expr":"variant.sku","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"V_Admin URL","entity_type":"VARIANT","field_key":"core.variant.admin_url","expr":f'=IF(LEN({{VARIANT|core.legacy_id}}&"")=0,"","https://admin.shopify.com/store/{admin_store_handle}/products/"&{{PRODUCT|core.legacy_id}}&"/variants/"&{{VARIANT|core.legacy_id}})',"field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Variant Image","entity_type":"VARIANT","field_key":"core.variant.image_url","expr":"COALESCE({VARIANT|raw.variant.image_url},{PRODUCT|core.product.image_url})","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Option1 Name","entity_type":"VARIANT","field_key":"core.variant.option1_name","expr":"GET({VARIANT|raw.variant.selected_options},1).name","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Option1 Value","entity_type":"VARIANT","field_key":"core.variant.option1_value","expr":"GET({VARIANT|raw.variant.selected_options},1).value","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Option2 Name","entity_type":"VARIANT","field_key":"core.variant.option2_name","expr":"GET({VARIANT|raw.variant.selected_options},2).name","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Option2 Value","entity_type":"VARIANT","field_key":"core.variant.option2_value","expr":"GET({VARIANT|raw.variant.selected_options},2).value","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Option3 Name","entity_type":"VARIANT","field_key":"core.variant.option3_name","expr":"GET({VARIANT|raw.variant.selected_options},3).name","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Option3 Value","entity_type":"VARIANT","field_key":"core.variant.option3_value","expr":"GET({VARIANT|raw.variant.selected_options},3).value","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Selected Options (JSON)","entity_type":"VARIANT","field_key":"core.variant.selected_options_json","expr":"JSON({VARIANT|raw.variant.selected_options})","field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Storefront URL","entity_type":"VARIANT","field_key":"core.variant.storefront_url","expr":f'=IF(LEN({{PRODUCT|core.handle}}&"")=0,"","{storefront_base_url}/products/"&{{PRODUCT|core.handle}})',"field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"(raw) Variant image url","entity_type":"VARIANT","field_key":"raw.variant.image_url","expr":"variant.image.url","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"(raw) Variant media(0) preview url","entity_type":"VARIANT","field_key":"raw.variant.media0_preview_url","expr":"variant.media.nodes[0].preview.image.url","field_type":"RAW","data_type":"string","source_type":"CORE","namespace":"","key":""},
        {"display_name":"(raw) SelectedOptions","entity_type":"VARIANT","field_key":"raw.variant.selected_options","expr":"variant.selectedOptions","field_type":"RAW","data_type":"list.json","source_type":"CORE","namespace":"","key":""},
        {"display_name":"Final Price-V","entity_type":"VARIANT","field_key":"core.final_price","expr":'=IF(LEN({VARIANT|v_mf.custom.sku_unit_price_v}&"")=0,"",IFERROR(ROUND(VALUE({VARIANT|v_mf.custom.sku_unit_price_v})*IF(LEN({VARIANT|v_mf.custom.settlement_quantity}&"")=0,1,VALUE({VARIANT|v_mf.custom.settlement_quantity}))*IF(LEN({VARIANT|v_mf.custom.multiplier}&"")=0,1,VALUE({VARIANT|v_mf.custom.multiplier})),2),""))',"field_type":"CALC","data_type":"string","source_type":"CORE","namespace":"","key":""},
    ]


def ensure_header(ws) -> List[str]:
    vals = ws.get_all_values()
    if not vals or len(vals[0]) == 0:
        ws.append_row(EXPECTED_HEADERS, value_input_option="RAW")
        return EXPECTED_HEADERS
    header = [normalize_text(h) for h in vals[0]]
    missing = [h for h in EXPECTED_HEADERS if h not in header]
    if missing:
        raise RuntimeError(f"Cfg__Fields header missing columns: {missing}. Please fix header first.")
    return header


def make_append_row(header: List[str], payload: Dict[str, Any]) -> List[str]:
    col_idx = {h: header.index(h) for h in header}
    row = [""] * len(header)
    for k, v in payload.items():
        if k in col_idx:
            row[col_idx[k]] = "" if v is None else str(v)
    return row


def rewrite_formula_columns(ws, header: List[str]):
    all_vals_after = ws.get_all_values()
    last_row = len(all_vals_after)
    if last_row < 2:
        return

    col_idx = {h: header.index(h) + 1 for h in header}
    l_handle = col_letter(col_idx["field_handle"])
    l_id = col_letter(col_idx["field_id"])
    l_entity = col_letter(col_idx["entity_type"])
    l_disp = col_letter(col_idx["display_name"])
    l_fk = col_letter(col_idx["field_key"])

    handle_formulas = []
    id_formulas = []
    for r in range(2, last_row + 1):
        handle_formulas.append([f'={l_entity}{r}&"|"&{l_disp}{r}'])
        id_formulas.append([f'={l_entity}{r}&"|"&{l_fk}{r}'])

    ws.update(f"{l_handle}2:{l_handle}{last_row}", handle_formulas, value_input_option="USER_ENTERED")
    ws.update(f"{l_id}2:{l_id}{last_row}", id_formulas, value_input_option="USER_ENTERED")


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
    metafield_owner_types: Optional[List[str]] = None,
    page_size: int = 250,
    admin_store_handle: str = "544104",
    storefront_base_url: str = "https://plumbingsell.com",
    write_to_sheet: bool = True,
    write_runlog: bool = True,
    preview_limit: int = 20,
    sleep_sec: float = 0.0,
    print_progress: bool = True,
    **kwargs,
) -> Dict[str, Any]:
    """
    notebook 对齐版：
    - 只 append 新行
    - 不清表
    - 不重排旧行
    - 仅重写 A/B 公式列
    """
    metafield_owner_types = metafield_owner_types or OWNER_TYPES_DEFAULT

    # 兼容旧调用参数：
    # - write_to_sheet=False 时仅预览，不写表
    # - preview_limit / sleep_sec 允许传入但不改变 notebook 对齐主逻辑
    # - 其余多余 kwargs 直接忽略，避免 colab 旧调用报 unexpected keyword

    gc = build_gspread_client_from_b64_secret(gsheet_sa_b64)
    run_id = make_run_id("cfg")
    ts_cn = now_ts_cn()

    url_map = resolve_sheet_urls_from_cfg_sites(gc, console_core_url, site_code)
    config_sheet_url = url_map["config"]
    runlog_sheet_url = url_map["runlog_sheet"]

    cfg_ws = open_ws_by_url_and_title(gc, config_sheet_url, worksheet_title)
    runlog_ws = open_ws_by_url_and_title(gc, runlog_sheet_url, runlog_tab_name)

    header = ensure_header(cfg_ws)
    col_idx = {h: header.index(h) for h in header}

    def get_cell(row_list: List[str], col_name: str) -> str:
        i = col_idx.get(col_name)
        if i is None or i >= len(row_list):
            return ""
        return normalize_text(row_list[i])

    all_vals = cfg_ws.get_all_values()
    existing_pks = set()
    if len(all_vals) >= 2:
        for r in all_vals[1:]:
            et = get_cell(r, "entity_type")
            fk = get_cell(r, "field_key")
            if et and fk:
                existing_pks.add(f"{et}||{fk}")

    if print_progress:
        print(f"Existing PKs: {len(existing_pks)}")

    shop = ShopifyClient(
        shop_domain=shop_domain,
        access_token=shopify_access_token,
        api_version=api_version,
        timeout=60,
    )

    mf_defs: List[Dict[str, Any]] = []
    for ot in metafield_owner_types:
        part = fetch_all_metafield_definitions(shop, ot, page_size=page_size)
        mf_defs.extend(part)
        if print_progress:
            print(f"metafieldDefinitions fetched | ownerType={ot} | rows={len(part)}")

    mo_defs = fetch_all_metaobject_definitions(shop, page_size=page_size)
    if print_progress:
        print(f"metaobjectDefinitions fetched | rows={len(mo_defs)}")

    core_fixed = build_core_fixed(admin_store_handle=admin_store_handle, storefront_base_url=storefront_base_url)

    rows_to_append: List[List[str]] = []
    new_pks_in_this_run = set()
    owner_to_entity = {
        "PRODUCT": "PRODUCT",
        "PRODUCTVARIANT": "VARIANT",
        "COLLECTION": "COLLECTION",
        "PAGE": "PAGE",
        "ORDER": "ORDER",
    }

    def try_add_row(payload: Dict[str, Any]):
        et = normalize_text(payload.get("entity_type"))
        fk = normalize_text(payload.get("field_key"))
        if not et or not fk:
            return
        pk = f"{et}||{fk}"
        if pk in existing_pks or pk in new_pks_in_this_run:
            return
        slim = {k: payload.get(k, "") for k in ALLOWED_COLS}
        rows_to_append.append(make_append_row(header, slim))
        new_pks_in_this_run.add(pk)

    # CORE_FIXED
    for x in core_fixed:
        try_add_row({
            "display_name": x["display_name"],
            "entity_type": x["entity_type"],
            "field_key": x["field_key"],
            "expr": x.get("expr", ""),
            "field_type": x.get("field_type", "RAW"),
            "data_type": x.get("data_type", "string"),
            "source_type": "CORE",
            "namespace": "",
            "key": "",
        })

    skipped_shopify_ns = 0
    for m in mf_defs:
        entity = owner_to_entity.get(normalize_text(m.get("ownerType")), normalize_text(m.get("ownerType")))
        ns = normalize_text(m.get("namespace"))
        k = normalize_text(m.get("key"))
        if namespace_blocked(ns):
            skipped_shopify_ns += 1
            continue

        shopify_t = normalize_text((m.get("type") or {}).get("name"))
        internal = f"v_mf.{ns}.{k}" if entity == "VARIANT" else f"mf.{ns}.{k}"

        try_add_row({
            "display_name": m.get("name") or internal,
            "entity_type": entity,
            "field_key": internal,
            "expr": f'MF_VALUE("{ns}","{k}")',
            "field_type": "RAW",
            "data_type": cfg_data_type_from_shopify_type_name(shopify_t),
            "source_type": "METAFIELD",
            "namespace": ns,
            "key": k,
        })

    skipped_shopify_mo_type = 0
    for d in mo_defs:
        mo_type = normalize_text(d.get("type"))
        mo_name = normalize_text(d.get("name")) or mo_type
        if namespace_blocked(mo_type):
            skipped_shopify_mo_type += 1
            continue

        for f in d.get("fieldDefinitions") or []:
            f_key = normalize_text(f.get("key"))
            f_name = normalize_text(f.get("name")) or f_key
            shopify_t = normalize_text((f.get("type") or {}).get("name"))
            internal = f"mo.{mo_type}.{f_key}"

            try_add_row({
                "display_name": f"{mo_name} · {f_name}",
                "entity_type": "METAOBJECT_ENTRY",
                "field_key": internal,
                "expr": f'MO_FIELD("{f_key}")',
                "field_type": shopify_t,
                "data_type": cfg_data_type_from_shopify_type_name(shopify_t),
                "source_type": "METAOBJECT_REF",
                "namespace": mo_type,
                "key": f_key,
            })

    if print_progress:
        print(f"New rows to append: {len(rows_to_append)}")
        print(f"Skipped metafieldDefinitions due to namespace contains 'shopify': {skipped_shopify_ns}")
        print(f"Skipped metaobjectDefinitions due to type contains 'shopify': {skipped_shopify_mo_type}")

    rows_written = 0
    if rows_to_append:
        for chunk in chunked(rows_to_append, 500):
            cfg_ws.append_rows(chunk, value_input_option="RAW", insert_data_option="INSERT_ROWS")
            rows_written += len(chunk)

    rewrite_formula_columns(cfg_ws, header)

    rows_loaded = max(len(all_vals) - 1, 0)
    rows_recognized = len(core_fixed) + len(mf_defs) + sum(len((d.get("fieldDefinitions") or [])) for d in mo_defs)
    rows_planned = len(rows_to_append)
    rows_skipped = skipped_shopify_ns + skipped_shopify_mo_type

    summary = {
        "run_id": run_id,
        "ts_cn": ts_cn,
        "site_code": site_code,
        "job_name": job_name,
        "rows_existing_before": rows_loaded,
        "core_fixed_count": len(core_fixed),
        "metafield_def_count_raw": len(mf_defs),
        "metaobject_def_count_raw": len(mo_defs),
        "rows_planned_append": rows_planned,
        "rows_written": rows_written,
        "rows_skipped_shopify_namespace": rows_skipped,
        "header": EXPECTED_HEADERS,
        "mode": "APPEND_ONLY",
    }

    if write_runlog:
        summary_row = make_runlog_summary_row(
            run_id=run_id,
            ts_cn=ts_cn,
            site_code=site_code,
            job_name=job_name,
            status="OK",
            rows_loaded=rows_loaded,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            message=f"Cfg__Fields incremental sync done. append_only=1; rows_written={rows_written}",
            phase="apply",
        )
        append_runlog_rows(runlog_ws, [summary_row])

    if print_progress:
        print("Done.")
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    return summary
