# -*- coding: utf-8 -*-
"""
job_name: config_fields
module_path: shopify_ops/config_fields.py

Multi-site Console Core version.
Gold standard: PBS Cfg__Fields notebook logic.
"""

from __future__ import annotations

import base64
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1


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

DEFAULT_MF_OWNER_TYPES = ["PRODUCT", "PRODUCTVARIANT", "COLLECTION", "PAGE", "ORDER"]
DEFAULT_PAGE_SIZE = 250

CORE_FIXED = [{'display_name': 'Collection GID',
  'entity_type': 'COLLECTION',
  'field_key': 'core.gid',
  'expr': 'collection.id',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Collection Handle',
  'entity_type': 'COLLECTION',
  'field_key': 'core.handle',
  'expr': 'collection.handle',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Collection Image',
  'entity_type': 'COLLECTION',
  'field_key': 'core.image_url',
  'expr': 'collection.image.url',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Collection ID (numeric)',
  'entity_type': 'COLLECTION',
  'field_key': 'core.legacy_id',
  'expr': 'collection.legacyResourceId',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Collection Name',
  'entity_type': 'COLLECTION',
  'field_key': 'core.title',
  'expr': 'collection.title',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Collection Description (HTML)',
  'entity_type': 'COLLECTION',
  'field_key': 'core.description_html',
  'expr': 'collection.descriptionHtml',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Collection Description (text)',
  'entity_type': 'COLLECTION',
  'field_key': 'core.description',
  'expr': 'collection.description',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Metaobject Entry Name',
  'entity_type': 'METAOBJECT_ENTRY',
  'field_key': 'core.display_name',
  'expr': 'metaobject.displayName',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Metaobject Entry GID',
  'entity_type': 'METAOBJECT_ENTRY',
  'field_key': 'core.gid',
  'expr': 'metaobject.id',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Metaobject Entry Handle',
  'entity_type': 'METAOBJECT_ENTRY',
  'field_key': 'core.handle',
  'expr': 'metaobject.handle',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Metaobject Entry ID (numeric)',
  'entity_type': 'METAOBJECT_ENTRY',
  'field_key': 'core.metaobject.id_numeric',
  'expr': 'GID_NUM({METAOBJECT_ENTRY|core.gid})',
  'field_type': 'CALC',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Metaobject Type',
  'entity_type': 'METAOBJECT_ENTRY',
  'field_key': 'core.type',
  'expr': 'metaobject.type',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Page GID',
  'entity_type': 'PAGE',
  'field_key': 'core.gid',
  'expr': 'page.id',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Page Handle',
  'entity_type': 'PAGE',
  'field_key': 'core.handle',
  'expr': 'page.handle',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Page ID (numeric)',
  'entity_type': 'PAGE',
  'field_key': 'core.page.id_numeric',
  'expr': 'GID_NUM({PAGE|core.gid})',
  'field_type': 'CALC',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Page Name',
  'entity_type': 'PAGE',
  'field_key': 'core.title',
  'expr': 'page.title',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Page Body (HTML)',
  'entity_type': 'PAGE',
  'field_key': 'core.body_html',
  'expr': 'page.body',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Created At',
  'entity_type': 'PRODUCT',
  'field_key': 'core.created_at',
  'expr': 'product.createdAt',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product GID',
  'entity_type': 'PRODUCT',
  'field_key': 'core.gid',
  'expr': 'product.id',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Handle',
  'entity_type': 'PRODUCT',
  'field_key': 'core.handle',
  'expr': 'product.handle',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product ID (numeric)',
  'entity_type': 'PRODUCT',
  'field_key': 'core.legacy_id',
  'expr': 'product.legacyResourceId',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Type',
  'entity_type': 'PRODUCT',
  'field_key': 'core.product_type',
  'expr': 'product.productType',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Description (HTML)',
  'entity_type': 'PRODUCT',
  'field_key': 'core.description_html',
  'expr': 'product.descriptionHtml',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Description (text)',
  'entity_type': 'PRODUCT',
  'field_key': 'core.description',
  'expr': 'product.description',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product SEO Title',
  'entity_type': 'PRODUCT',
  'field_key': 'core.seo_title',
  'expr': 'product.seo.title',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product SEO Description',
  'entity_type': 'PRODUCT',
  'field_key': 'core.seo_description',
  'expr': 'product.seo.description',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Weight',
  'entity_type': 'VARIANT',
  'field_key': 'core.weight',
  'expr': 'variant.weight',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Weight Unit',
  'entity_type': 'VARIANT',
  'field_key': 'core.weight_unit',
  'expr': 'variant.weightUnit',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'P_Admin URL',
  'entity_type': 'PRODUCT',
  'field_key': 'core.product.admin_url',
  'expr': '=IF(LEN({PRODUCT|core.legacy_id}&"")=0,"","https://admin.shopify.com/store/544104/products/"&{PRODUCT|core.legacy_id})',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Image',
  'entity_type': 'PRODUCT',
  'field_key': 'core.product.image_preview',
  'expr': '=IF(LEN({Product Image}&"")=0,"",IMAGE({Product Image}))',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Image',
  'entity_type': 'PRODUCT',
  'field_key': 'core.product.image_url',
  'expr': 'product.media[0].preview.image.url',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Options (JSON)',
  'entity_type': 'PRODUCT',
  'field_key': 'core.product.options_json',
  'expr': 'JSON({PRODUCT|raw.product.options})',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Storefront URL',
  'entity_type': 'PRODUCT',
  'field_key': 'core.product.storefront_url',
  'expr': '=IF(LEN({PRODUCT|core.handle}&"")=0,"","https://plumbingsell.com/products/"&{PRODUCT|core.handle})',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Status',
  'entity_type': 'PRODUCT',
  'field_key': 'core.status',
  'expr': 'product.status',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Synced At',
  'entity_type': 'PRODUCT',
  'field_key': 'core.synced_at',
  'expr': '',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Tags',
  'entity_type': 'PRODUCT',
  'field_key': 'core.tags',
  'expr': 'product.tags',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Title',
  'entity_type': 'PRODUCT',
  'field_key': 'core.title',
  'expr': 'product.title',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Updated At',
  'entity_type': 'PRODUCT',
  'field_key': 'core.updated_at',
  'expr': 'product.updatedAt',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': '(raw) Product featured image url',
  'entity_type': 'PRODUCT',
  'field_key': 'raw.product.featured_image_url',
  'expr': 'product.featuredImage.url',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': '(raw) Product media(0) preview url',
  'entity_type': 'PRODUCT',
  'field_key': 'raw.product.media0_preview_url',
  'expr': 'product.media.nodes[0].preview.image.url',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': '(raw) Product.options',
  'entity_type': 'PRODUCT',
  'field_key': 'raw.product.options',
  'expr': 'product.options',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Compare At Price',
  'entity_type': 'VARIANT',
  'field_key': 'core.compare_at_price',
  'expr': 'variant.compareAtPrice',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Variant Name',
  'entity_type': 'VARIANT',
  'field_key': 'core.display_name',
  'expr': 'variant.displayName',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Variant GID',
  'entity_type': 'VARIANT',
  'field_key': 'core.gid',
  'expr': 'variant.id',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Variant ID (numeric)',
  'entity_type': 'VARIANT',
  'field_key': 'core.legacy_id',
  'expr': 'variant.legacyResourceId',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Item_ID',
  'entity_type': 'VARIANT',
  'field_key': 'core.item_id',
  'expr': '="shopify_us_"&{PRODUCT|core.legacy_id}&"_"&{VARIANT|core.legacy_id}',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Price',
  'entity_type': 'VARIANT',
  'field_key': 'core.price',
  'expr': 'variant.price',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product GID (parent)',
  'entity_type': 'VARIANT',
  'field_key': 'core.product_gid',
  'expr': 'variant.product.id',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Product Handle (parent)',
  'entity_type': 'VARIANT',
  'field_key': 'core.product_handle',
  'expr': 'variant.product.handle',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'SKU',
  'entity_type': 'VARIANT',
  'field_key': 'core.sku',
  'expr': 'variant.sku',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'V_Admin URL',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.admin_url',
  'expr': '=IF(LEN({VARIANT|core.legacy_id}&"")=0,"","https://admin.shopify.com/store/544104/products/"&{PRODUCT|core.legacy_id}&"/variants/"&{VARIANT|core.legacy_id})',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Variant Image',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.image_url',
  'expr': 'COALESCE({VARIANT|raw.variant.image_url},{PRODUCT|core.product.image_url})',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Option1 Name',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.option1_name',
  'expr': 'GET({VARIANT|raw.variant.selected_options},1).name',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Option1 Value',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.option1_value',
  'expr': 'GET({VARIANT|raw.variant.selected_options},1).value',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Option2 Name',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.option2_name',
  'expr': 'GET({VARIANT|raw.variant.selected_options},2).name',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Option2 Value',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.option2_value',
  'expr': 'GET({VARIANT|raw.variant.selected_options},2).value',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Option3 Name',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.option3_name',
  'expr': 'GET({VARIANT|raw.variant.selected_options},3).name',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Option3 Value',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.option3_value',
  'expr': 'GET({VARIANT|raw.variant.selected_options},3).value',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Selected Options (JSON)',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.selected_options_json',
  'expr': 'JSON({VARIANT|raw.variant.selected_options})',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Storefront URL',
  'entity_type': 'VARIANT',
  'field_key': 'core.variant.storefront_url',
  'expr': '=IF(LEN({PRODUCT|core.handle}&"")=0,"","https://plumbingsell.com/products/"&{PRODUCT|core.handle})',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': '(raw) Variant image url',
  'entity_type': 'VARIANT',
  'field_key': 'raw.variant.image_url',
  'expr': 'variant.image.url',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': '(raw) Variant media(0) preview url',
  'entity_type': 'VARIANT',
  'field_key': 'raw.variant.media0_preview_url',
  'expr': 'variant.media.nodes[0].preview.image.url',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': '(raw) SelectedOptions',
  'entity_type': 'VARIANT',
  'field_key': 'raw.variant.selected_options',
  'expr': 'variant.selectedOptions',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Final Price-V',
  'entity_type': 'VARIANT',
  'field_key': 'core.final_price',
  'expr': '=IF(LEN({VARIANT|v_mf.custom.sku_unit_price_v}&"")=0,"",IFERROR(ROUND(VALUE({VARIANT|v_mf.custom.sku_unit_price_v})*IF(LEN({VARIANT|v_mf.custom.settlement_quantity}&"")=0,1,VALUE({VARIANT|v_mf.custom.settlement_quantity}))*IF(LEN({VARIANT|v_mf.custom.multiplier}&"")=0,1,VALUE({VARIANT|v_mf.custom.multiplier})),2),""))',
  'field_type': 'CALC',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Order GID',
  'entity_type': 'ORDER',
  'field_key': 'core.gid',
  'expr': 'order.id',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Order ID (numeric)',
  'entity_type': 'ORDER',
  'field_key': 'core.legacy_id',
  'expr': 'order.legacyResourceId',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Order Name',
  'entity_type': 'ORDER',
  'field_key': 'core.name',
  'expr': 'order.name',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Created At',
  'entity_type': 'ORDER',
  'field_key': 'core.created_at',
  'expr': 'order.createdAt',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Updated At',
  'entity_type': 'ORDER',
  'field_key': 'core.updated_at',
  'expr': 'order.updatedAt',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Financial Status',
  'entity_type': 'ORDER',
  'field_key': 'core.display_financial_status',
  'expr': 'order.displayFinancialStatus',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Fulfillment Status',
  'entity_type': 'ORDER',
  'field_key': 'core.display_fulfillment_status',
  'expr': 'order.displayFulfillmentStatus',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Currency Code',
  'entity_type': 'ORDER',
  'field_key': 'core.currency_code',
  'expr': 'order.currencyCode',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Total Price',
  'entity_type': 'ORDER',
  'field_key': 'core.total_price_amount',
  'expr': 'order.totalPriceSet.shopMoney.amount',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Subtotal Price',
  'entity_type': 'ORDER',
  'field_key': 'core.subtotal_price_amount',
  'expr': 'order.subtotalPriceSet.shopMoney.amount',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Total Tax',
  'entity_type': 'ORDER',
  'field_key': 'core.total_tax_amount',
  'expr': 'order.totalTaxSet.shopMoney.amount',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Total Discounts',
  'entity_type': 'ORDER',
  'field_key': 'core.total_discounts_amount',
  'expr': 'order.totalDiscountsSet.shopMoney.amount',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Total Shipping',
  'entity_type': 'ORDER',
  'field_key': 'core.total_shipping_amount',
  'expr': 'order.totalShippingPriceSet.shopMoney.amount',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Email',
  'entity_type': 'ORDER',
  'field_key': 'core.email',
  'expr': 'order.email',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Customer Display Name',
  'entity_type': 'ORDER',
  'field_key': 'core.customer_display_name',
  'expr': 'order.customer.displayName',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Tags',
  'entity_type': 'ORDER',
  'field_key': 'core.tags',
  'expr': 'order.tags',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Tax Lines (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.tax_lines_json',
  'expr': 'order.taxLines',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Discount Applications (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.discount_applications_json',
  'expr': 'order.discountApplications',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Lines (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_lines_json',
  'expr': 'order.shippingLines',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Refunds (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.refunds_json',
  'expr': 'order.refunds',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'LineItem Discount Allocations (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.lineitem_discount_allocations_json',
  'expr': 'order.lineItems[].discountAllocations',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'LineItem Discounted Total Amount',
  'entity_type': 'ORDER',
  'field_key': 'core.lineitem_discounted_total_amount',
  'expr': 'order.lineItems[].discountedTotalSet.shopMoney.amount',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Refund RefundLineItems (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.refund_refundlineitems_json',
  'expr': 'order.refunds[].refundLineItems',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Name',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_name',
  'expr': 'order.shippingAddress.name',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address First Name',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_first_name',
  'expr': 'order.shippingAddress.firstName',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Last Name',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_last_name',
  'expr': 'order.shippingAddress.lastName',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Company',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_company',
  'expr': 'order.shippingAddress.company',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Line 1',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_line1',
  'expr': 'order.shippingAddress.address1',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Line 2',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_line2',
  'expr': 'order.shippingAddress.address2',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address City',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_city',
  'expr': 'order.shippingAddress.city',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Province',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_province',
  'expr': 'order.shippingAddress.province',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Province Code',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_province_code',
  'expr': 'order.shippingAddress.provinceCode',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address ZIP',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_zip',
  'expr': 'order.shippingAddress.zip',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Country',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_country',
  'expr': 'order.shippingAddress.country',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Country Code',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_country_code',
  'expr': 'order.shippingAddress.countryCodeV2',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Address Phone',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_address_phone',
  'expr': 'order.shippingAddress.phone',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Status',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_status',
  'expr': 'order.displayFulfillmentStatus',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Fulfillments Count',
  'entity_type': 'ORDER',
  'field_key': 'core.fulfillments_count',
  'expr': 'order.fulfillmentsCount.count',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Fulfillments (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.fulfillments_json',
  'expr': 'order.fulfillments',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Status List (Fulfillment Display Status)',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_status_list',
  'expr': 'order.fulfillments[].displayStatus',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Status Raw List',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_status_raw_list',
  'expr': 'order.fulfillments[].status',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Tracking Companies (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.tracking_companies_json',
  'expr': 'order.fulfillments[].trackingInfo[].company',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Tracking Numbers (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.tracking_numbers_json',
  'expr': 'order.fulfillments[].trackingInfo[].number',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Tracking URLs (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.tracking_urls_json',
  'expr': 'order.fulfillments[].trackingInfo[].url',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Events (JSON)',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_events_json',
  'expr': 'order.fulfillments[].events',
  'field_type': 'RAW',
  'data_type': 'list.json',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Event Status List',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_event_status_list',
  'expr': 'order.fulfillments[].events[].status',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Shipping Event Happened At List',
  'entity_type': 'ORDER',
  'field_key': 'core.shipping_event_happened_at_list',
  'expr': 'order.fulfillments[].events[].happenedAt',
  'field_type': 'RAW',
  'data_type': 'list.string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Cost',
  'entity_type': 'VARIANT',
  'field_key': 'core.cost',
  'expr': 'variant.inventoryItem.unitCost.amount',
  'field_type': 'RAW',
  'data_type': 'number',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''},
 {'display_name': 'Cost Currency',
  'entity_type': 'VARIANT',
  'field_key': 'core.cost_currency',
  'expr': 'variant.inventoryItem.unitCost.currencyCode',
  'field_type': 'RAW',
  'data_type': 'string',
  'source_type': 'CORE',
  'namespace': '',
  'key': ''}]


class ConfigFieldsError(RuntimeError):
    pass


def namespace_blocked(namespace: str) -> bool:
    """
    Confirmed rule:
    If namespace contains "shopify" anywhere, case-insensitive, skip it.
    """
    return "shopify" in str(namespace or "").strip().lower()


def _now_cn_like() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _get_colab_secret(secret_name: str) -> str:
    if not secret_name:
        raise ConfigFieldsError("Secret name is empty.")
    try:
        from google.colab import userdata
    except Exception as e:
        raise ConfigFieldsError(
            "google.colab.userdata is not available. This job is designed to run in Colab with Colab Secrets."
        ) from e

    value = userdata.get(secret_name)
    if not value:
        raise ConfigFieldsError(f"Colab Secret not found or empty: {secret_name}")
    return value


def _build_gspread_client_from_b64_secret(secret_name: str) -> gspread.Client:
    sa_b64 = _get_colab_secret(secret_name)
    try:
        sa_info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    except Exception as e:
        raise ConfigFieldsError(f"Failed to decode Google SA base64 JSON from secret: {secret_name}") from e

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)


def _require_worksheet(sh: gspread.Spreadsheet, tab_name: str):
    try:
        return sh.worksheet(tab_name)
    except Exception as e:
        raise ConfigFieldsError(f"Worksheet not found: {tab_name} in spreadsheet: {getattr(sh, 'url', '')}") from e


def _rows_as_dicts(ws) -> List[Dict[str, str]]:
    values = ws.get_all_values()
    if not values:
        return []
    header = [str(h or "").strip() for h in values[0]]
    rows: List[Dict[str, str]] = []
    for raw in values[1:]:
        row = {}
        for i, h in enumerate(header):
            if not h:
                continue
            row[h] = str(raw[i] if i < len(raw) else "").strip()
        if any(v for v in row.values()):
            rows.append(row)
    return rows


# Cell — Replace function: _read_account_config

def _read_account_config(console_sh, tab_name: str, site_code: str) -> Dict[str, str]:
    """
    Read Cfg__account_id.

    Supported shapes:
    1) Header table:
       key | value

    2) Header table:
       site_code | key | value

    3) No-header two-column table:
       SHOP_DOMAIN              | 544104.myshopify.com
       SHOPIFY_API_VERSION      | 2026-01
       GSHEET_SA_B64_SECRET     | PBS_GSHEET
       SHOPIFY_TOKEN_SECRET     | PBS_SHOPIFY_ACCESS_TOKEN

    The user's current Console Core uses shape #3.
    """
    ws = _require_worksheet(console_sh, tab_name)
    values = ws.get_all_values()

    if not values:
        raise ConfigFieldsError(f"{tab_name} is empty.")

    cfg: Dict[str, str] = {}

    # ---------- Shape 1 / 2: header-based table ----------
    header = [str(x or "").strip() for x in values[0]]
    header_lc = [x.lower() for x in header]

    if "key" in header_lc and "value" in header_lc:
        key_idx = header_lc.index("key")
        value_idx = header_lc.index("value")
        site_idx = header_lc.index("site_code") if "site_code" in header_lc else None

        for row in values[1:]:
            if not any(str(x or "").strip() for x in row):
                continue

            row_pad = row + [""] * max(0, len(header) - len(row))

            if site_idx is not None:
                row_site = str(row_pad[site_idx] or "").strip()
                if row_site and row_site.upper() != str(site_code or "").strip().upper():
                    continue

            k = str(row_pad[key_idx] or "").strip()
            v = str(row_pad[value_idx] or "").strip()

            if k:
                cfg[k] = v

    # ---------- Shape 3: no-header A=key, B=value ----------
    else:
        for row in values:
            if len(row) < 2:
                continue

            k = str(row[0] or "").strip()
            v = str(row[1] or "").strip()

            if not k:
                continue

            # Skip accidental header-like rows, if any.
            if k.lower() in {"key", "site_code"}:
                continue

            cfg[k] = v

    required = [
        "SHOP_DOMAIN",
        "SHOPIFY_API_VERSION",
        "GSHEET_SA_B64_SECRET",
        "SHOPIFY_TOKEN_SECRET",
        "STOREFRONT_BASE_URL",
        "ADMIN_BASE_URL",
    ]

    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise ConfigFieldsError(
            f"{tab_name} missing required account config keys: {missing}"
        )

    return cfg


def _read_site_route(console_sh, tab_name: str, site_code: str, label: str) -> Dict[str, str]:
    ws = _require_worksheet(console_sh, tab_name)
    rows = _rows_as_dicts(ws)
    if not rows:
        raise ConfigFieldsError(f"{tab_name} is empty.")

    required_cols = ["site_code", "sheet_url", "label"]
    available = set(rows[0].keys())
    missing_cols = [c for c in required_cols if c not in available]
    if missing_cols:
        raise ConfigFieldsError(f"{tab_name} missing required columns: {missing_cols}")

    matches = [
        r for r in rows
        if (r.get("site_code") or "").strip().upper() == site_code.upper()
        and (r.get("label") or "").strip() == label
    ]
    if not matches:
        raise ConfigFieldsError(f"{tab_name} cannot find route: site_code={site_code}, label={label}")
    if len(matches) > 1:
        raise ConfigFieldsError(f"{tab_name} has duplicate routes: site_code={site_code}, label={label}")

    route = matches[0]
    if not route.get("sheet_url"):
        raise ConfigFieldsError(f"{tab_name} route has empty sheet_url: site_code={site_code}, label={label}")
    return route


def _build_shopify_client(shop_domain: str, api_version: str, token_secret_name: str):
    token = _get_colab_secret(token_secret_name)
    graphql_url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    def gql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = requests.post(
            graphql_url,
            headers=headers,
            json={"query": query, "variables": variables or {}},
            timeout=60,
        )
        try:
            data = resp.json()
        except Exception as e:
            raise ConfigFieldsError(f"Shopify GraphQL returned non-JSON response: HTTP {resp.status_code}") from e

        if resp.status_code >= 400:
            raise ConfigFieldsError(f"Shopify GraphQL HTTP {resp.status_code}: {data}")
        if data.get("errors"):
            raise ConfigFieldsError(f"Shopify GraphQL errors: {data['errors']}")
        if "data" not in data:
            raise ConfigFieldsError(f"Shopify GraphQL response missing data: {data}")
        return data["data"]

    return gql


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


def _fetch_all_metafield_definitions(gql, owner_type: str, page_size: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    after = None
    while True:
        d = gql(Q_METAFIELD_DEFS, {"ownerType": owner_type, "first": page_size, "after": after})
        conn = d["metafieldDefinitions"]
        out.extend(conn["nodes"])
        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
    return out


def _fetch_all_metaobject_definitions(gql, page_size: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    after = None
    while True:
        d = gql(Q_METAOBJECT_DEFS, {"first": page_size, "after": after})
        conn = d["metaobjectDefinitions"]
        out.extend(conn["nodes"])
        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
    return out


def _ensure_header(ws) -> List[str]:
    vals = ws.get_all_values()
    if not vals or len(vals[0]) == 0:
        ws.append_row(EXPECTED_HEADERS, value_input_option="RAW")
        return EXPECTED_HEADERS[:]

    header = [str(h or "").strip() for h in vals[0]]
    missing = [h for h in EXPECTED_HEADERS if h not in header]
    if missing:
        raise ConfigFieldsError(f"Cfg__Fields header missing columns: {missing}. Please fix header first.")
    return header


def _cfg_data_type_from_shopify_type_name(shopify_type_name: str) -> str:
    t = (shopify_type_name or "").strip()
    return t if t else "string"


def _make_append_row(payload: Dict[str, Any], header: List[str], col_idx: Dict[str, int]) -> List[str]:
    row = [""] * len(header)
    for k, v in payload.items():
        if k in col_idx:
            row[col_idx[k]] = "" if v is None else str(v)
    return row


def _col_letter(col_num_1_based: int) -> str:
    return rowcol_to_a1(1, col_num_1_based).replace("1", "")


def _sync_cfg_fields(ws, mf_defs: List[Dict[str, Any]], mo_defs: List[Dict[str, Any]]) -> Dict[str, Any]:
    header = _ensure_header(ws)
    col_idx = {h: i for i, h in enumerate(header)}

    def get_cell(row_list: List[str], col_name: str) -> str:
        i = col_idx.get(col_name)
        if i is None or i >= len(row_list):
            return ""
        return str(row_list[i] or "").strip()

    all_vals = ws.get_all_values()
    existing_pks = set()
    if len(all_vals) >= 2:
        for r in all_vals[1:]:
            et = get_cell(r, "entity_type")
            fk = get_cell(r, "field_key")
            if et and fk:
                existing_pks.add(f"{et}||{fk}")

    rows_to_append: List[List[str]] = []
    new_pks_in_this_run = set()

    def try_add_row(payload: Dict[str, Any]) -> None:
        et = str(payload.get("entity_type", "") or "").strip()
        fk = str(payload.get("field_key", "") or "").strip()
        if not et or not fk:
            return
        pk = f"{et}||{fk}"
        if pk in existing_pks or pk in new_pks_in_this_run:
            return
        slim = {k: payload.get(k, "") for k in ALLOWED_COLS}
        rows_to_append.append(_make_append_row(slim, header, col_idx))
        new_pks_in_this_run.add(pk)

    for x in CORE_FIXED:
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

    owner_to_entity = {
        "PRODUCT": "PRODUCT",
        "PRODUCTVARIANT": "VARIANT",
        "COLLECTION": "COLLECTION",
        "PAGE": "PAGE",
        "ORDER": "ORDER",
    }

    skipped_shopify_ns = 0
    for m in mf_defs:
        entity = owner_to_entity.get(m.get("ownerType", ""), m.get("ownerType", ""))
        ns = (m.get("namespace") or "").strip()
        k = (m.get("key") or "").strip()

        if namespace_blocked(ns):
            skipped_shopify_ns += 1
            continue

        shopify_t = ((m.get("type") or {}).get("name") or "").strip()
        internal = f"v_mf.{ns}.{k}" if entity == "VARIANT" else f"mf.{ns}.{k}"

        try_add_row({
            "display_name": m.get("name") or internal,
            "entity_type": entity,
            "field_key": internal,
            "expr": f'MF_VALUE("{ns}","{k}")',
            "field_type": "RAW",
            "data_type": _cfg_data_type_from_shopify_type_name(shopify_t),
            "source_type": "METAFIELD",
            "namespace": ns,
            "key": k,
        })

    skipped_shopify_mo_type = 0
    for d in mo_defs:
        mo_type = (d.get("type") or "").strip()
        mo_name = (d.get("name") or mo_type).strip()

        if namespace_blocked(mo_type):
            skipped_shopify_mo_type += 1
            continue

        for f in (d.get("fieldDefinitions") or []):
            f_key = (f.get("key") or "").strip()
            f_name = (f.get("name") or f_key).strip()
            shopify_t = ((f.get("type") or {}).get("name") or "").strip()
            internal = f"mo.{mo_type}.{f_key}"

            try_add_row({
                "display_name": f"{mo_name} · {f_name}",
                "entity_type": "METAOBJECT_ENTRY",
                "field_key": internal,
                "expr": f'MO_FIELD("{f_key}")',
                "field_type": shopify_t,
                "data_type": _cfg_data_type_from_shopify_type_name(shopify_t),
                "source_type": "METAOBJECT_REF",
                "namespace": mo_type,
                "key": f_key,
            })

    print(f"Existing PKs: {len(existing_pks)}")
    print(f"New rows to append: {len(rows_to_append)}")
    print(f"Skipped metafieldDefinitions due to namespace contains 'shopify': {skipped_shopify_ns}")
    print(f"Skipped metaobjectDefinitions due to type contains 'shopify': {skipped_shopify_mo_type}")

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="RAW", insert_data_option="INSERT_ROWS")

    all_vals_after = ws.get_all_values()
    last_row = len(all_vals_after)

    if last_row >= 2:
        col_field_handle = col_idx["field_handle"] + 1
        col_field_id = col_idx["field_id"] + 1
        col_entity = col_idx["entity_type"] + 1
        col_display = col_idx["display_name"] + 1
        col_field_key = col_idx["field_key"] + 1

        l_handle = _col_letter(col_field_handle)
        l_id = _col_letter(col_field_id)
        l_entity = _col_letter(col_entity)
        l_disp = _col_letter(col_display)
        l_fk = _col_letter(col_field_key)

        handle_formulas = []
        id_formulas = []
        for r in range(2, last_row + 1):
            handle_formulas.append([f'={l_entity}{r}&"|"&{l_disp}{r}'])
            id_formulas.append([f'={l_entity}{r}&"|"&{l_fk}{r}'])

        ws.update(f"{l_handle}2:{l_handle}{last_row}", handle_formulas, value_input_option="USER_ENTERED")
        ws.update(f"{l_id}2:{l_id}{last_row}", id_formulas, value_input_option="USER_ENTERED")

    print(f"Done. Sheet rows now: {last_row}")

    return {
        "existing_pks": len(existing_pks),
        "rows_appended": len(rows_to_append),
        "sheet_rows_now": last_row,
        "skipped_metafield_definitions_shopify_namespace": skipped_shopify_ns,
        "skipped_metaobject_definitions_shopify_type": skipped_shopify_mo_type,
    }


def run(
    SITE_CODE: str,
    JOB_NAME: str,
    CONSOLE_CORE_URL: str,
    BOOTSTRAP_GSHEET_SA_B64_SECRET: str,
    TAB_CFG_ACCOUNT_ID: str = "Cfg__account_id",
    TAB_CFG_SITES: str = "Cfg__Sites",
    CONFIG_SHEET_LABEL: str = "config",
    RUNLOG_SHEET_LABEL: str = "runlog_sheet",
    WORKSHEET_NAME: str = "Cfg__Fields",
    MF_OWNER_TYPES: Optional[List[str]] = None,
    TZ_NAME: str = "Asia/Shanghai",
    RUN_ID: Optional[str] = None,
) -> Dict[str, Any]:
    if not SITE_CODE:
        raise ConfigFieldsError("SITE_CODE is required.")
    if JOB_NAME != "config_fields":
        raise ConfigFieldsError(f"JOB_NAME must be config_fields, got: {JOB_NAME}")
    if not CONSOLE_CORE_URL:
        raise ConfigFieldsError("CONSOLE_CORE_URL is required.")
    if not BOOTSTRAP_GSHEET_SA_B64_SECRET:
        raise ConfigFieldsError("BOOTSTRAP_GSHEET_SA_B64_SECRET is required.")

    run_id = RUN_ID or str(uuid.uuid4())
    owner_types = MF_OWNER_TYPES or DEFAULT_MF_OWNER_TYPES[:]

    print(f"=== {JOB_NAME} start ===")
    print(f"SITE_CODE={SITE_CODE}")
    print(f"RUN_ID={run_id}")
    print(f"CONFIG_SHEET_LABEL={CONFIG_SHEET_LABEL}")
    print(f"WORKSHEET_NAME={WORKSHEET_NAME}")
    print(f"MF_OWNER_TYPES={owner_types}")

    gc_bootstrap = _build_gspread_client_from_b64_secret(BOOTSTRAP_GSHEET_SA_B64_SECRET)
    console_sh = gc_bootstrap.open_by_url(CONSOLE_CORE_URL)

    account_cfg = _read_account_config(console_sh, TAB_CFG_ACCOUNT_ID, SITE_CODE)
    account_secret = account_cfg["GSHEET_SA_B64_SECRET"]
    if BOOTSTRAP_GSHEET_SA_B64_SECRET != account_secret:
        raise ConfigFieldsError(
            "BOOTSTRAP_GSHEET_SA_B64_SECRET mismatch: "
            f"Cell1={BOOTSTRAP_GSHEET_SA_B64_SECRET}, "
            f"{TAB_CFG_ACCOUNT_ID}.GSHEET_SA_B64_SECRET={account_secret}"
        )

    config_route = _read_site_route(console_sh, TAB_CFG_SITES, SITE_CODE, CONFIG_SHEET_LABEL)
    config_sheet_url = config_route["sheet_url"]

    print("Console Core loaded.")
    print(f"SHOP_DOMAIN={account_cfg['SHOP_DOMAIN']}")
    print(f"SHOPIFY_API_VERSION={account_cfg['SHOPIFY_API_VERSION']}")
    print(f"Config sheet route: label={CONFIG_SHEET_LABEL}, url={config_sheet_url}")

    gc = _build_gspread_client_from_b64_secret(account_cfg["GSHEET_SA_B64_SECRET"])
    cfg_sh = gc.open_by_url(config_sheet_url)
    cfg_ws = _require_worksheet(cfg_sh, WORKSHEET_NAME)

    gql = _build_shopify_client(
        shop_domain=account_cfg["SHOP_DOMAIN"],
        api_version=account_cfg["SHOPIFY_API_VERSION"],
        token_secret_name=account_cfg["SHOPIFY_TOKEN_SECRET"],
    )

    mf_defs: List[Dict[str, Any]] = []
    mf_counts_by_owner = {}
    for ot in owner_types:
        rows = _fetch_all_metafield_definitions(gql, ot, DEFAULT_PAGE_SIZE)
        mf_counts_by_owner[ot] = len(rows)
        mf_defs.extend(rows)
        print(f"metafieldDefinitions ownerType={ot}: {len(rows)}")

    mo_defs = _fetch_all_metaobject_definitions(gql, DEFAULT_PAGE_SIZE)
    print(f"metafieldDefinitions total: {len(mf_defs)}")
    print(f"metaobjectDefinitions: {len(mo_defs)}")

    sync_summary = _sync_cfg_fields(cfg_ws, mf_defs, mo_defs)

    result = {
        "ok": True,
        "job_name": JOB_NAME,
        "site_code": SITE_CODE,
        "run_id": run_id,
        "ts": _now_cn_like(),
        "targets": {
            "console_core_url": CONSOLE_CORE_URL,
            "config_sheet_url": config_sheet_url,
            "worksheet_name": WORKSHEET_NAME,
            "runlog_sheet_label": RUNLOG_SHEET_LABEL,
        },
        "summary": {
            "metafield_definitions_total": len(mf_defs),
            "metafield_definitions_by_owner": mf_counts_by_owner,
            "metaobject_definitions": len(mo_defs),
            **sync_summary,
        },
        "warnings": [],
    }

    print("=== result summary ===")
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    print(f"=== {JOB_NAME} done ===")
    return result
