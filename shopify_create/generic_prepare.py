# -*- coding: utf-8 -*-
"""Prepare a generic Shopify product-creation plan from Google Sheets.

GitHub target: ``ecom/shopify_create/generic_prepare.py``
Import path: ``shopify_create.generic_prepare``

Scope
-----
- Read ``Input`` with two header rows: display name + optional field_key.
- Resolve missing Row-2 field_key values from system definitions and
  ``Cfg__Fields``, then optionally write the generated mapping row back.
- Read ``Defaults`` as configurable fallback/derivation rules.
- Resolve the target workbook through Console Core / ``Cfg__Sites`` label
  ``create_generic``.
- Resolve ``Cfg__Fields`` from the routed ``config`` workbook.
- Resolve the current site's default warehouse from Console Core /
  ``Cfg__Locations``.
- Normalize, validate, group variants by ``sys.product_key`` and overwrite
  ``Preview`` with a machine-readable two-row-header plan.
- ``core.handle`` is the Product duplicate identity. SKU and Barcode are
  business values and are not treated as duplicate identities.
- Product Category, default theme template, and all-channel publishing are
  driven by Defaults with Input nonblank values taking precedence.
- Write RunLog evidence.

This module never creates or changes Shopify products. ``Result`` is untouched.
It supports Colab and local Jupyter/CLI without interactive OAuth.

Default precedence
------------------
1. A non-empty value in Input.
2. The matching Defaults row.
3. No value.

Recognized rule tokens in Defaults:
- ``FROM_TITLE``
- ``FROM_DESCRIPTION_TEXT``
- ``FROM_CFG_LOCATIONS_DEFAULT``

Validation errors are written into Preview and returned as
``FAILED_VALIDATION``. Infrastructure and contract failures raise explicitly.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import json
import os
import platform
import re
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo


MODULE_VERSION = "1.6.1"
MODULE_PATH = "shopify_create.generic_prepare"
DEFAULT_JOB_NAME = "generic_create_prepare"


RUNLOG_HEADER_18 = [
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

SYSTEM_FIELD_DEFINITIONS: Dict[str, Dict[str, str]] = {
    "sys.action": {
        "display_name": "Action",
        "scope": "SYSTEM",
        "data_type": "string",
    },
    "sys.product_key": {
        "display_name": "Product Key",
        "scope": "SYSTEM",
        "data_type": "string",
    },
    "sys.variant_key": {
        "display_name": "Variant Key",
        "scope": "SYSTEM",
        "data_type": "string",
    },
    "core.title": {
        "display_name": "Title",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.handle": {
        "display_name": "Handle",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.description_html": {
        "display_name": "Description HTML",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.vendor": {
        "display_name": "Vendor",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.product_type": {
        "display_name": "Product Type",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.category_id": {
        "display_name": "Category",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.tags": {
        "display_name": "Tags",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.template_suffix": {
        "display_name": "Template Suffix",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "publish.all_channels": {
        "display_name": "Publish All Channels",
        "scope": "PRODUCT",
        "data_type": "boolean",
    },
    "core.status": {
        "display_name": "Status",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.seo_title": {
        "display_name": "SEO Title",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.seo_description": {
        "display_name": "SEO Description",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.option1_name": {
        "display_name": "Option 1 Name",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.option1_value": {
        "display_name": "Option 1 Value",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.option2_name": {
        "display_name": "Option 2 Name",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.option2_value": {
        "display_name": "Option 2 Value",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.option3_name": {
        "display_name": "Option 3 Name",
        "scope": "PRODUCT",
        "data_type": "string",
    },
    "core.option3_value": {
        "display_name": "Option 3 Value",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.sku": {
        "display_name": "SKU",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.barcode": {
        "display_name": "Barcode",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.price": {
        "display_name": "Price",
        "scope": "VARIANT",
        "data_type": "decimal",
    },
    "core.compare_at_price": {
        "display_name": "Compare-at Price",
        "scope": "VARIANT",
        "data_type": "decimal",
    },
    "core.cost": {
        "display_name": "Cost",
        "scope": "VARIANT",
        "data_type": "decimal",
    },
    "core.weight": {
        "display_name": "Weight",
        "scope": "VARIANT",
        "data_type": "decimal",
    },
    "core.weight_unit": {
        "display_name": "Weight Unit",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.inventory_policy": {
        "display_name": "Inventory Policy",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.inventory_tracker": {
        "display_name": "Inventory Tracker",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.fulfillment_service": {
        "display_name": "Fulfillment Service",
        "scope": "VARIANT",
        "data_type": "string",
    },
    "core.requires_shipping": {
        "display_name": "Requires Shipping",
        "scope": "VARIANT",
        "data_type": "boolean",
    },
    "core.taxable": {
        "display_name": "Taxable",
        "scope": "VARIANT",
        "data_type": "boolean",
    },
    "inventory.location_code": {
        "display_name": "Inventory Location",
        "scope": "INVENTORY",
        "data_type": "string",
    },
    "inventory.quantity": {
        "display_name": "Inventory Quantity",
        "scope": "INVENTORY",
        "data_type": "integer",
    },
}

SYSTEM_DISPLAY_ALIASES: Dict[str, str] = {
    # Generic Create human labels.
    "Action": "sys.action",
    "Product Key": "sys.product_key",
    "Variant Key": "sys.variant_key",
    "Title": "core.title",
    "Product Title": "core.title",
    "Handle": "core.handle",
    "Product Handle": "core.handle",
    "Description HTML": "core.description_html",
    "Product Description (HTML)": "core.description_html",
    "Vendor": "core.vendor",
    "Product Type": "core.product_type",
    "Category": "core.category_id",
    "Product Category": "core.category_id",
    "Category ID": "core.category_id",
    "Tags": "core.tags",
    "Template Suffix": "core.template_suffix",
    "Theme Template": "core.template_suffix",
    "Publish All Channels": "publish.all_channels",
    "All Channels": "publish.all_channels",
    "Status": "core.status",
    "Product Status": "core.status",
    "SEO Title": "core.seo_title",
    "Product SEO Title": "core.seo_title",
    "SEO Description": "core.seo_description",
    "Product SEO Description": "core.seo_description",
    "Option 1 Name": "core.option1_name",
    "Option1 Name": "core.option1_name",
    "Option 1 Value": "core.option1_value",
    "Option1 Value": "core.option1_value",
    "Option 2 Name": "core.option2_name",
    "Option2 Name": "core.option2_name",
    "Option 2 Value": "core.option2_value",
    "Option2 Value": "core.option2_value",
    "Option 3 Name": "core.option3_name",
    "Option3 Name": "core.option3_name",
    "Option 3 Value": "core.option3_value",
    "Option3 Value": "core.option3_value",
    "SKU": "core.sku",
    "Variant SKU": "core.sku",
    "Barcode": "core.barcode",
    "Variant Barcode": "core.barcode",
    "Price": "core.price",
    "Variant Price": "core.price",
    "Compare-at Price": "core.compare_at_price",
    "Compare At Price": "core.compare_at_price",
    "Variant Compare At Price": "core.compare_at_price",
    "Cost": "core.cost",
    "Cost per item": "core.cost",
    "Weight": "core.weight",
    "Variant Weight": "core.weight",
    "Weight Unit": "core.weight_unit",
    "Variant Weight Unit": "core.weight_unit",
    "Inventory Policy": "core.inventory_policy",
    "Variant Inventory Policy": "core.inventory_policy",
    "Inventory Tracker": "core.inventory_tracker",
    "Inventory Tracking": "core.inventory_tracker",
    "Variant Inventory Tracker": "core.inventory_tracker",
    "Fulfillment Service": "core.fulfillment_service",
    "Variant Fulfillment Service": "core.fulfillment_service",
    "Requires Shipping": "core.requires_shipping",
    "Variant Requires Shipping": "core.requires_shipping",
    "Taxable": "core.taxable",
    "Variant Taxable": "core.taxable",
    "Inventory Location": "inventory.location_code",
    "Inventory Quantity": "inventory.quantity",
}

AUTO_RESOLVE_ENTITY_PRIORITY = ("PRODUCT", "VARIANT")


PRODUCT_CORE_FIELDS = {
    key
    for key, definition in SYSTEM_FIELD_DEFINITIONS.items()
    if definition.get("scope") == "PRODUCT"
}
VARIANT_CORE_FIELDS = {
    key
    for key, definition in SYSTEM_FIELD_DEFINITIONS.items()
    if definition.get("scope") == "VARIANT"
}

REQUIRED_ACTIVE_FIELDS = {
    "sys.product_key",
    "sys.variant_key",
    "core.title",
    "core.sku",
    "core.price",
}

DEFAULT_RULE_TOKENS = {
    "FROM_TITLE",
    "FROM_DESCRIPTION_TEXT",
    "FROM_CFG_LOCATIONS_DEFAULT",
}

INFERRED_DEFAULT_VALUES = {
    "default_seo_title": "FROM_TITLE",
    "default_seo_description": "FROM_DESCRIPTION_TEXT",
    "default_inventory_location": "FROM_CFG_LOCATIONS_DEFAULT",
}

REQUIRED_DEFAULT_FIELDS = {
    "inventory.quantity",
    "core.weight_unit",
    "core.inventory_policy",
    "core.inventory_tracker",
    "core.fulfillment_service",
    "core.requires_shipping",
    "core.taxable",
    "core.status",
    "core.category_id",
    "core.template_suffix",
    "publish.all_channels",
    "core.seo_title",
    "core.seo_description",
    "core.barcode",
    "core.cost",
    "core.weight",
    "inventory.location_code",
}

PREVIEW_SYSTEM_FIELDS = [
    ("Plan Status", "sys.plan_status"),
    ("Source Row", "sys.source_row"),
    ("Error Count", "sys.error_count"),
    ("Warning Count", "sys.warning_count"),
    ("Validation Messages", "sys.validation_messages"),
    ("Defaulted Fields", "sys.defaulted_fields"),
    ("Inherited Fields", "sys.inherited_fields"),
    ("Product Variant Count", "sys.product_variant_count"),
]


@dataclass(frozen=True)
class SecretValue:
    value: str
    source_type: str
    source_detail: str


@dataclass(frozen=True)
class DefaultSpec:
    config_key: str
    config_value: str
    notes: str
    display_name: str
    field_key: str
    description: str
    source_row: int


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: List[str] = []

    def handle_data(self, data: str) -> None:
        value = str(data)
        if value.strip():
            self.parts.append(value.strip())


class RunLogger18:
    def __init__(
        self,
        worksheet: gspread.Worksheet,
        run_id: str,
        job_name: str,
        site_code: str,
        tz_name: str,
    ) -> None:
        self.worksheet = worksheet
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.tz_name = tz_name
        self.buffer: List[List[Any]] = []
        _ensure_runlog_header(worksheet)

    def log(
        self,
        *,
        phase: str,
        log_type: str,
        status: str,
        entity_type: str = "GENERIC_PRODUCT_CREATE",
        gid: str = "",
        field_key: str = "",
        rows_loaded: int = 0,
        rows_pending: int = 0,
        rows_recognized: int = 0,
        rows_planned: int = 0,
        rows_written: int = 0,
        rows_skipped: int = 0,
        message: str = "",
        error_reason: str = "",
    ) -> None:
        self.buffer.append(
            [
                self.run_id,
                _now_str(self.tz_name),
                self.job_name,
                phase,
                log_type,
                status,
                self.site_code,
                entity_type,
                gid,
                field_key,
                int(rows_loaded or 0),
                int(rows_pending or 0),
                int(rows_recognized or 0),
                int(rows_planned or 0),
                int(rows_written or 0),
                int(rows_skipped or 0),
                str(message),
                str(error_reason),
            ]
        )

    def flush(self) -> None:
        if not self.buffer:
            return
        values = self.worksheet.get_all_values()
        start_row = max(2, len(values) + 1)
        required_rows = start_row + len(self.buffer) - 1
        required_cols = len(RUNLOG_HEADER_18)
        if (
            self.worksheet.row_count < required_rows
            or self.worksheet.col_count < required_cols
        ):
            self.worksheet.resize(
                rows=max(self.worksheet.row_count, required_rows + 100),
                cols=max(self.worksheet.col_count, required_cols),
            )
        end_row = start_row + len(self.buffer) - 1
        self.worksheet.update(
            range_name=f"A{start_row}:R{end_row}",
            values=self.buffer,
            value_input_option="RAW",
        )
        self.buffer.clear()


def _runtime_mode() -> str:
    try:
        import google.colab  # type: ignore  # noqa: F401
        return "COLAB"
    except Exception:
        return "LOCAL"


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    return str(value).strip()


def _normalize_site_code(value: Any) -> str:
    return _safe_str(value).upper()


def _normalize_bool(value: Any) -> Optional[bool]:
    text = _safe_str(value).lower()
    if text in {"true", "1", "yes", "y", "是"}:
        return True
    if text in {"false", "0", "no", "n", "否"}:
        return False
    if not text:
        return None
    raise ValueError(f"Invalid boolean value: {value!r}")


def _bool_cell(value: Any) -> str:
    parsed = _normalize_bool(value)
    if parsed is None:
        return ""
    return "TRUE" if parsed else "FALSE"


def _now_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def _make_run_id(job_name: str, tz_name: str) -> str:
    stamp = dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d_%H%M%S")
    return f"{job_name}_{stamp}"


def _normalize_header(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_str(value)).strip()


def _normalize_display_lookup(value: Any) -> str:
    text = html.unescape(_safe_str(value)).casefold()
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[\s_]+", " ", text)
    text = re.sub(r"\s*-\s*", "-", text)
    return text.strip()


def _normalize_owner_entity(value: Any, field_key: str = "") -> str:
    key = _safe_str(field_key)
    if key.startswith("mf."):
        return "PRODUCT"
    if key.startswith("v_mf."):
        return "VARIANT"
    entity = _safe_str(value).upper().replace(" ", "")
    if entity in {"PRODUCTVARIANT", "VARIANT"}:
        return "VARIANT"
    return entity


def _normalize_registry_header(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", _safe_str(value).lower()).strip()


def _slugify(value: Any) -> str:
    text = html.unescape(_safe_str(value)).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def _html_to_text(value: Any) -> str:
    raw = _safe_str(value)
    if not raw:
        return ""
    parser = _HTMLTextExtractor()
    parser.feed(raw)
    parser.close()
    text = " ".join(parser.parts)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _decimal_text(value: Any) -> str:
    text = _safe_str(value)
    if not text:
        return ""
    try:
        number = Decimal(text.replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc
    normalized = format(number, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _integer_text(value: Any) -> str:
    text = _safe_str(value)
    if not text:
        return ""
    try:
        number = Decimal(text.replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid integer value: {value!r}") from exc
    if number != number.to_integral_value():
        raise ValueError(f"Inventory quantity must be an integer: {value!r}")
    return str(int(number))


def _project_code_for_secret(
    *,
    secret_name: str,
    project_code: Optional[str],
) -> str:
    """Resolve the project identity used by Workspace Secret Resolver."""
    explicit = _normalize_site_code(project_code)
    if explicit:
        return explicit

    normalized_name = _safe_str(secret_name).upper()
    for suffix in (
        "_GSHEET",
        "_SHOPIFY_ACCESS_TOKEN",
        "_SHOPIFY_TOKEN",
        "_SHOPIFY",
    ):
        if normalized_name.endswith(suffix):
            inferred = normalized_name[: -len(suffix)].strip("_")
            if inferred:
                return inferred

    raise RuntimeError(
        "PROJECT_CODE is required for Local Secret resolution. "
        "Pass project_code=SITE_CODE at the loader boundary."
    )


def _workspace_secret_result_to_value(result: Any) -> SecretValue:
    """Adapt Workspace Secret Resolver provenance to the legacy value type."""
    path = getattr(result, "path", None)
    key = _safe_str(getattr(result, "key", ""))
    resolved_name = _safe_str(
        getattr(result, "resolved_name", "")
    )
    if path is not None:
        source_detail = str(path)
        if key:
            source_detail += f":{key}"
    else:
        source_detail = key or resolved_name

    return SecretValue(
        value=str(result.value).strip(),
        source_type=_safe_str(result.source_type),
        source_detail=source_detail,
    )


def read_secret(
    name: str,
    *,
    project_code: Optional[str] = None,
    explicit_value: Optional[str] = None,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[
        Mapping[str, Mapping[str, str]]
    ] = None,
) -> SecretValue:
    """Read one Secret without printing its value.

    Colab continues to use ``google.colab.userdata`` with the existing
    logical Secret name. Local VSCode/CLI delegates all file and environment
    resolution to the independent ``workspace_secret_resolver`` package.
    """
    secret_name = _safe_str(name)
    if not secret_name:
        raise RuntimeError("Secret name is empty.")

    if explicit_value is not None and _safe_str(explicit_value):
        return SecretValue(
            str(explicit_value).strip(),
            "EXPLICIT_VALUE",
            "caller",
        )

    environment_value = os.environ.get(secret_name)
    if environment_value is not None and environment_value.strip():
        return SecretValue(
            environment_value.strip(),
            "ENVIRONMENT_VARIABLE",
            secret_name,
        )

    if _runtime_mode() == "COLAB":
        try:
            from google.colab import userdata  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "Colab Secret adapter is unavailable."
            ) from exc
        value = userdata.get(secret_name)
        if value is None or not str(value).strip():
            raise RuntimeError(
                f"Colab Secret {secret_name!r} is missing or not enabled."
            )
        return SecretValue(
            str(value).strip(),
            "COLAB_SECRETS",
            secret_name,
        )

    if local_secret_aliases:
        raise RuntimeError(
            "local_secret_aliases is no longer supported by Generic Product "
            "Creation. Use the Workspace Secret Resolver naming contract."
        )

    resolved_project_code = _project_code_for_secret(
        secret_name=secret_name,
        project_code=project_code,
    )
    try:
        from workspace_secret_resolver import WorkspaceSecretResolver
    except Exception as exc:
        raise RuntimeError(
            "Workspace Secret Resolver is required for Local execution. "
            "Install it once into the active Python environment with:\n"
            f"{sys.executable} -m pip install -e "
            "/Users/nikki/Documents/AI_Workspace/Projects/"
            "Workspace_Secret_Resolver"
        ) from exc

    resolver = WorkspaceSecretResolver(
        resolved_project_code,
        secret_home=secret_home,
    )
    aliases: Tuple[str, ...] = ()
    if secret_name.upper().endswith("_GSHEET"):
        canonical_name = f"{resolved_project_code}_GSHEET"
        if canonical_name != secret_name:
            aliases = (canonical_name,)

    result = resolver.read(secret_name, aliases=aliases)
    return _workspace_secret_result_to_value(result)


def _parse_service_account(secret: SecretValue) -> Dict[str, Any]:
    raw = secret.value.strip()
    try:
        info = json.loads(raw)
        secret_format = "RAW_JSON"
    except Exception:
        try:
            padded = raw + "=" * ((4 - len(raw) % 4) % 4)
            info = json.loads(
                base64.b64decode(padded).decode("utf-8")
            )
            secret_format = "BASE64_JSON"
        except Exception as exc:
            raise RuntimeError(
                "Google service-account Secret is neither raw JSON nor "
                "Base64 JSON."
            ) from exc

    required = {
        "type",
        "project_id",
        "private_key",
        "client_email",
        "token_uri",
    }
    missing = sorted(key for key in required if not info.get(key))
    if missing or info.get("type") != "service_account":
        raise RuntimeError(
            "Google Secret is not a complete service-account credential; "
            f"missing={missing}."
        )
    info["__secret_format"] = secret_format
    return info


def _build_gspread_client(
    secret: SecretValue,
) -> Tuple[gspread.Client, Dict[str, str]]:
    info = _parse_service_account(secret)
    secret_format = str(info.pop("__secret_format"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(
        info,
        scopes=scopes,
    )
    return gspread.authorize(credentials), {
        "source_type": secret.source_type,
        "source_detail": secret.source_detail,
        "secret_format": secret_format,
        "service_account_email": _safe_str(info.get("client_email")),
    }


def _load_account_values(
    spreadsheet: gspread.Spreadsheet,
    tab_name: str,
) -> Dict[str, str]:
    values = spreadsheet.worksheet(tab_name).get_all_values()
    if not values:
        raise ValueError(f"{tab_name} is empty.")
    result: Dict[str, str] = {}
    duplicates: List[str] = []
    for row_number, row in enumerate(values, start=1):
        key = _safe_str(row[0] if row else "").upper()
        value = _safe_str(row[1] if len(row) > 1 else "")
        if not key:
            continue
        if key in result:
            duplicates.append(f"{key}@row{row_number}")
        result[key] = value
    if duplicates:
        raise ValueError(f"Duplicated keys in {tab_name}: {duplicates}")
    return result


def _resolve_sheet_url_by_label(
    console: gspread.Spreadsheet,
    tab_cfg_sites: str,
    site_code: str,
    label: str,
) -> str:
    records = console.worksheet(tab_cfg_sites).get_all_records()
    matches = [
        row
        for row in records
        if _normalize_site_code(row.get("site_code"))
        == _normalize_site_code(site_code)
        and _safe_str(row.get("label")) == _safe_str(label)
    ]
    if not matches:
        raise ValueError(
            f"No route in {tab_cfg_sites} for "
            f"site_code={site_code}, label={label}."
        )
    if len(matches) > 1:
        raise ValueError(
            f"Duplicated route in {tab_cfg_sites} for "
            f"site_code={site_code}, label={label}."
        )
    url = _safe_str(matches[0].get("sheet_url"))
    if not url:
        raise ValueError(
            f"Empty sheet_url in {tab_cfg_sites} for "
            f"site_code={site_code}, label={label}."
        )
    return url


def _ensure_runlog_header(worksheet: gspread.Worksheet) -> None:
    values = worksheet.get_all_values()
    if not values:
        if worksheet.row_count < 2 or worksheet.col_count < 18:
            worksheet.resize(
                rows=max(worksheet.row_count, 100),
                cols=max(worksheet.col_count, 18),
            )
        worksheet.update(
            range_name="A1:R1",
            values=[RUNLOG_HEADER_18],
            value_input_option="RAW",
        )
        return
    current = [
        _safe_str(value)
        for value in values[0][: len(RUNLOG_HEADER_18)]
    ]
    if current != RUNLOG_HEADER_18:
        raise ValueError(
            f"RunLog header mismatch in {worksheet.title}. "
            f"Expected={RUNLOG_HEADER_18}; actual={current}"
        )


def _require_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound as exc:
        raise ValueError(
            f"Required worksheet {title!r} does not exist in "
            f"{spreadsheet.title!r}."
        ) from exc


def _get_or_create_preview_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
    rows: int,
    cols: int,
) -> gspread.Worksheet:
    try:
        worksheet = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=title,
            rows=max(100, int(rows)),
            cols=max(20, int(cols)),
        )
    return worksheet


def _write_matrix_overwrite(
    spreadsheet: gspread.Spreadsheet,
    tab_name: str,
    matrix: Sequence[Sequence[Any]],
) -> int:
    if not matrix:
        raise ValueError("Refusing to write an empty Preview matrix.")
    rows = len(matrix)
    cols = max(len(row) for row in matrix)
    worksheet = _get_or_create_preview_worksheet(
        spreadsheet,
        tab_name,
        rows + 50,
        cols + 5,
    )
    if worksheet.row_count < rows or worksheet.col_count < cols:
        worksheet.resize(
            rows=max(worksheet.row_count, rows + 50),
            cols=max(worksheet.col_count, cols + 5),
        )
    worksheet.clear()
    end_col = _a1_col(cols)
    worksheet.update(
        range_name=f"A1:{end_col}{rows}",
        values=[list(row) for row in matrix],
        value_input_option="RAW",
    )
    try:
        worksheet.freeze(rows=2)
    except Exception:
        pass
    return max(0, rows - 2)


def _a1_col(number: int) -> str:
    if number < 1:
        raise ValueError("Column number must be >= 1.")
    result = ""
    current = int(number)
    while current:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _build_system_display_map() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for field_key, definition in SYSTEM_FIELD_DEFINITIONS.items():
        display_name = _safe_str(definition.get("display_name"))
        if display_name:
            mapping[_normalize_display_lookup(display_name)] = field_key
    for display_name, field_key in SYSTEM_DISPLAY_ALIASES.items():
        normalized = _normalize_display_lookup(display_name)
        existing = mapping.get(normalized)
        if existing and existing != field_key:
            raise RuntimeError(
                "SYSTEM_DISPLAY_ALIASES conflict for "
                f"{display_name!r}: {existing!r} vs {field_key!r}."
            )
        mapping[normalized] = field_key
    return mapping


def _cfg_records(
    cfg_fields: Mapping[str, Any],
) -> List[Dict[str, str]]:
    records = cfg_fields.get("records", [])
    if not isinstance(records, list):
        raise TypeError("Cfg__Fields registry records must be a list.")
    return [
        {
            str(key): _safe_str(value)
            for key, value in record.items()
        }
        for record in records
    ]


def _cfg_get_exact(
    cfg_fields: Mapping[str, Any],
    *,
    entity_type: str,
    field_key: str,
) -> Optional[Dict[str, str]]:
    owner = _normalize_owner_entity(entity_type, field_key)
    canonical_field_id = f"{owner}|{_safe_str(field_key)}"
    record = cfg_fields.get("by_field_id", {}).get(
        canonical_field_id
    )
    if record is None:
        return None
    return {
        str(key): _safe_str(value)
        for key, value in record.items()
    }


def _cfg_records_for_field_key(
    cfg_fields: Mapping[str, Any],
    field_key: str,
) -> List[Dict[str, str]]:
    records = cfg_fields.get("by_field_key", {}).get(
        _safe_str(field_key),
        [],
    )
    return [
        {
            str(key): _safe_str(value)
            for key, value in record.items()
        }
        for record in records
    ]


def _build_cfg_display_index(
    cfg_fields: Mapping[str, Any],
) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    """Index writable metafields by owner and normalized display_name.

    Cfg__Fields identity is ``field_id = entity_type|field_key``.
    ``field_key`` may repeat across different entities.
    """
    index: Dict[str, Dict[str, List[Dict[str, str]]]] = {
        "PRODUCT": {},
        "VARIANT": {},
    }

    for raw in _cfg_records(cfg_fields):
        field_key = _safe_str(raw.get("field_key"))
        if not field_key.startswith(("mf.", "v_mf.")):
            continue

        display_name = _safe_str(raw.get("display_name"))
        if not display_name:
            continue

        owner = _normalize_owner_entity(
            raw.get("entity_type"),
            field_key,
        )
        if owner not in index:
            continue

        # Prefix and owner must agree. This prevents a PRODUCT mf.* field
        # from being confused with a VARIANT v_mf.* field.
        if field_key.startswith("mf.") and owner != "PRODUCT":
            continue
        if field_key.startswith("v_mf.") and owner != "VARIANT":
            continue

        display_key = _normalize_display_lookup(display_name)
        record = {
            "field_id": _safe_str(raw.get("field_id")),
            "display_name": display_name,
            "field_key": field_key,
            "entity_type": owner,
            "data_type": _safe_str(raw.get("data_type")),
            "source_type": _safe_str(raw.get("source_type")),
            "source_row": _safe_str(raw.get("source_row")),
        }
        index[owner].setdefault(display_key, []).append(record)

    for owner, by_display in index.items():
        for display_key, records in by_display.items():
            deduped: Dict[str, Dict[str, str]] = {}
            for record in records:
                deduped[record["field_id"]] = record
            by_display[display_key] = list(deduped.values())

    return index

def _resolve_input_field_keys(
    display_headers: Sequence[str],
    provided_field_keys: Sequence[str],
    cfg_fields: Mapping[str, Any],
    *,
    entity_priority: Sequence[str] = AUTO_RESOLVE_ENTITY_PRIORITY,
) -> Dict[str, Any]:
    system_map = _build_system_display_map()
    cfg_index = _build_cfg_display_index(cfg_fields)

    resolved: List[str] = []
    mapping_records: List[Dict[str, Any]] = []
    errors: List[str] = []

    for index, (display_name, provided_key) in enumerate(
        zip(display_headers, provided_field_keys),
        start=1,
    ):
        display_name = _normalize_header(display_name)
        provided_key = _safe_str(provided_key)

        if not display_name and not provided_key:
            resolved.append("")
            continue

        if provided_key:
            resolved.append(provided_key)
            mapping_records.append(
                {
                    "column_number": index,
                    "display_name": display_name,
                    "field_key": provided_key,
                    "mapping_source": "EXPLICIT_ROW_2",
                    "entity_type": _safe_str(
                        (_field_definition(provided_key, cfg_fields) or {}).get(
                            "scope"
                        )
                    ),
                    "field_id": _safe_str(
                        (_field_definition(provided_key, cfg_fields) or {}).get(
                            "field_id"
                        )
                    ),
                }
            )
            continue

        if not display_name:
            errors.append(
                f"column={index}: Row 2 has no field_key and Row 1 has no "
                "display name."
            )
            resolved.append("")
            continue

        display_key = _normalize_display_lookup(display_name)
        system_key = system_map.get(display_key)
        if system_key:
            resolved.append(system_key)
            mapping_records.append(
                {
                    "column_number": index,
                    "display_name": display_name,
                    "field_key": system_key,
                    "mapping_source": "SYSTEM_DISPLAY_NAME",
                    "entity_type": _safe_str(
                        SYSTEM_FIELD_DEFINITIONS[system_key].get("scope")
                    ),
                    "field_id": (
                        f"{_safe_str(SYSTEM_FIELD_DEFINITIONS[system_key].get('scope'))}"
                        f"|{system_key}"
                    ),
                }
            )
            continue

        matched = False
        for raw_owner in entity_priority:
            owner = _normalize_owner_entity(raw_owner)
            candidates = cfg_index.get(owner, {}).get(display_key, [])
            if not candidates:
                continue
            if len(candidates) == 1:
                candidate = candidates[0]
                field_key = candidate["field_key"]
                resolved.append(field_key)
                mapping_records.append(
                    {
                        "column_number": index,
                        "display_name": display_name,
                        "field_key": field_key,
                        "mapping_source": f"CFG_FIELDS_{owner}",
                        "entity_type": owner,
                        "field_id": candidate["field_id"],
                    }
                )
                matched = True
                break

            candidate_text = [
                {
                    "field_id": candidate["field_id"],
                    "field_key": candidate["field_key"],
                    "display_name": candidate["display_name"],
                    "entity_type": candidate["entity_type"],
                    "source_row": candidate["source_row"],
                }
                for candidate in candidates
            ]
            errors.append(
                f"column={index}, display_name={display_name!r}: "
                f"Cfg__Fields has multiple {owner} candidates="
                f"{candidate_text}. Fill Row 2 explicitly for this column."
            )
            resolved.append("")
            matched = True
            break

        if matched:
            continue

        errors.append(
            f"column={index}, display_name={display_name!r}: no matching "
            "Generic Create system field or writable PRODUCT/VARIANT "
            "metafield was found in Cfg__Fields."
        )
        resolved.append("")

    if errors:
        raise ValueError(
            "Input header resolution failed. " + " | ".join(errors)
        )

    active_keys = [field_key for field_key in resolved if field_key]
    duplicate_keys = sorted(
        field_key
        for field_key in set(active_keys)
        if active_keys.count(field_key) > 1
    )
    if duplicate_keys:
        duplicate_columns = {
            field_key: [
                record["column_number"]
                for record in mapping_records
                if record["field_key"] == field_key
            ]
            for field_key in duplicate_keys
        }
        raise ValueError(
            "Input resolves multiple columns to the same field_key. "
            f"duplicates={duplicate_columns}"
        )

    source_counts: Dict[str, int] = {}
    for record in mapping_records:
        source = str(record["mapping_source"])
        source_counts[source] = source_counts.get(source, 0) + 1

    return {
        "resolved_field_keys": resolved,
        "mapping_records": mapping_records,
        "mapping_source_counts": source_counts,
    }


def _read_input_matrix(
    values: Sequence[Sequence[Any]],
    cfg_fields: Mapping[str, Any],
    *,
    entity_priority: Sequence[str] = AUTO_RESOLVE_ENTITY_PRIORITY,
) -> Dict[str, Any]:
    if len(values) < 2:
        raise ValueError(
            "Input requires two header rows: Row 1 display name and "
            "Row 2 optional/generated field_key."
        )

    max_cols = max(len(values[0]), len(values[1]))
    display_headers = [
        _normalize_header(
            values[0][index] if index < len(values[0]) else ""
        )
        for index in range(max_cols)
    ]
    provided_field_keys = [
        _safe_str(values[1][index] if index < len(values[1]) else "")
        for index in range(max_cols)
    ]

    while (
        max_cols
        and not display_headers[-1]
        and not provided_field_keys[-1]
    ):
        display_headers.pop()
        provided_field_keys.pop()
        max_cols -= 1

    if not display_headers:
        raise ValueError("Input Row 1 display-name header is empty.")

    resolution = _resolve_input_field_keys(
        display_headers,
        provided_field_keys,
        cfg_fields,
        entity_priority=entity_priority,
    )
    resolved_field_keys = resolution["resolved_field_keys"]

    columns = [
        {
            "display_name": display_headers[index]
            or SYSTEM_FIELD_DEFINITIONS.get(
                resolved_field_keys[index],
                {},
            ).get("display_name", resolved_field_keys[index]),
            "field_key": resolved_field_keys[index],
            "provided_field_key": provided_field_keys[index],
            "column_number": index + 1,
        }
        for index in range(max_cols)
        if resolved_field_keys[index]
    ]

    rows: List[Dict[str, Any]] = []
    for sheet_row, raw_row in enumerate(values[2:], start=3):
        padded = list(raw_row) + [""] * max(
            0,
            max_cols - len(raw_row),
        )
        row_values = {
            column["field_key"]: _safe_str(
                padded[column["column_number"] - 1]
            )
            for column in columns
        }
        if not any(row_values.values()):
            continue
        rows.append(
            {
                "source_row": sheet_row,
                "values": row_values,
            }
        )

    changed_columns = [
        index + 1
        for index, (before, after) in enumerate(
            zip(provided_field_keys, resolved_field_keys)
        )
        if _safe_str(before) != _safe_str(after)
    ]

    return {
        "display_headers": display_headers,
        "provided_field_keys": provided_field_keys,
        "resolved_field_keys": resolved_field_keys,
        "field_keys": [column["field_key"] for column in columns],
        "columns": columns,
        "rows": rows,
        "mapping_records": resolution["mapping_records"],
        "mapping_source_counts": resolution[
            "mapping_source_counts"
        ],
        "mapping_changed_columns": changed_columns,
    }


def _write_input_field_key_row_if_changed(
    worksheet: gspread.Worksheet,
    *,
    provided_field_keys: Sequence[str],
    resolved_field_keys: Sequence[str],
) -> Dict[str, Any]:
    before = [_safe_str(value) for value in provided_field_keys]
    after = [_safe_str(value) for value in resolved_field_keys]
    if len(before) != len(after):
        raise ValueError(
            "Input mapping row length mismatch: "
            f"before={len(before)}, after={len(after)}."
        )
    changed_columns = [
        index + 1
        for index, (old, new) in enumerate(zip(before, after))
        if old != new
    ]
    if not changed_columns:
        return {
            "field_key_row_written": False,
            "mapping_cells_changed": 0,
            "changed_columns": [],
        }
    if worksheet.col_count < len(after):
        worksheet.resize(
            rows=max(worksheet.row_count, 3),
            cols=len(after),
        )
    worksheet.update(
        range_name=f"A2:{_a1_col(len(after))}2",
        values=[after],
        value_input_option="RAW",
    )
    return {
        "field_key_row_written": True,
        "mapping_cells_changed": len(changed_columns),
        "changed_columns": changed_columns,
    }


def _normalize_defaults_header(value: Any) -> str:
    text = _safe_str(value).lower()
    text = text.replace("filed_key", "field_key")
    text = re.sub(r"[\s_]+", "_", text).strip("_")
    aliases = {
        "对应字段": "display_name",
        "對應字段": "display_name",
        "field": "display_name",
        "说明": "description",
        "說明": "description",
    }
    return aliases.get(text, text)


def _read_defaults_matrix(
    values: Sequence[Sequence[Any]],
) -> Dict[str, DefaultSpec]:
    if not values:
        raise ValueError("Defaults is empty.")
    headers = [
        _normalize_defaults_header(value)
        for value in values[0]
    ]
    header_map = {
        header: index
        for index, header in enumerate(headers)
        if header
    }
    required = {"config_key", "config_value", "field_key"}
    missing = sorted(required - set(header_map))
    if missing:
        raise ValueError(
            f"Defaults missing required columns: {missing}"
        )

    specs: Dict[str, DefaultSpec] = {}
    config_keys: Dict[str, int] = {}
    for source_row, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        field_key = _safe_str(
            padded[header_map["field_key"]]
        )
        config_key = _safe_str(
            padded[header_map["config_key"]]
        )
        config_value = _safe_str(
            padded[header_map["config_value"]]
        )
        if not field_key and not config_key and not config_value:
            continue
        if not field_key:
            raise ValueError(
                f"Defaults row {source_row} has no field_key."
            )
        if not config_key:
            config_key = "default_" + re.sub(
                r"[^a-z0-9]+",
                "_",
                field_key.lower(),
            ).strip("_")
        if not config_value:
            config_value = INFERRED_DEFAULT_VALUES.get(
                config_key,
                "",
            )
        if field_key in specs:
            raise ValueError(
                f"Defaults has duplicate field_key {field_key!r} "
                f"at rows {specs[field_key].source_row} and {source_row}."
            )
        if config_key in config_keys:
            raise ValueError(
                f"Defaults has duplicate config_key {config_key!r} "
                f"at rows {config_keys[config_key]} and {source_row}."
            )

        notes = _safe_str(
            padded[header_map["notes"]]
        ) if "notes" in header_map else ""
        display_name = _safe_str(
            padded[header_map["display_name"]]
        ) if "display_name" in header_map else ""
        description = _safe_str(
            padded[header_map["description"]]
        ) if "description" in header_map else ""

        specs[field_key] = DefaultSpec(
            config_key=config_key,
            config_value=config_value,
            notes=notes,
            display_name=display_name
            or SYSTEM_FIELD_DEFINITIONS.get(field_key, {}).get(
                "display_name",
                field_key,
            ),
            field_key=field_key,
            description=description,
            source_row=source_row,
        )
        config_keys[config_key] = source_row

    if not specs:
        raise ValueError("Defaults contains no usable rows.")

    missing_required = sorted(
        REQUIRED_DEFAULT_FIELDS - set(specs)
    )
    if missing_required:
        raise ValueError(
            "Defaults is missing required field_key rows: "
            f"{missing_required}"
        )
    return specs


def _read_cfg_fields(
    values: Sequence[Sequence[Any]],
) -> Dict[str, Any]:
    """Read Cfg__Fields using field_id as the primary identity.

    Formal identity:
        field_id = entity_type + "|" + field_key

    ``field_key`` is intentionally allowed to repeat across entities.
    """
    if not values:
        raise ValueError("Cfg__Fields is empty.")

    headers = [_safe_str(value) for value in values[0]]
    header_map = {
        header: index
        for index, header in enumerate(headers)
        if header
    }
    required = {
        "field_id",
        "display_name",
        "entity_type",
        "field_key",
        "data_type",
    }
    missing = sorted(required - set(header_map))
    if missing:
        raise ValueError(
            f"Cfg__Fields missing required columns: {missing}"
        )

    records: List[Dict[str, str]] = []
    by_field_id: Dict[str, Dict[str, str]] = {}
    by_field_key: Dict[str, List[Dict[str, str]]] = {}
    by_entity_field_key: Dict[
        Tuple[str, str],
        Dict[str, str],
    ] = {}

    for source_row, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(
            0,
            len(headers) - len(row),
        )
        record = {
            header: _safe_str(padded[index])
            for header, index in header_map.items()
        }

        raw_field_id = _safe_str(record.get("field_id"))
        field_key = _safe_str(record.get("field_key"))
        raw_entity = _safe_str(record.get("entity_type"))

        if not raw_field_id and not field_key and not raw_entity:
            continue
        if not raw_field_id:
            raise ValueError(
                f"Cfg__Fields row {source_row} has no field_id."
            )
        if not field_key:
            raise ValueError(
                f"Cfg__Fields row {source_row} has no field_key."
            )
        if not raw_entity:
            raise ValueError(
                f"Cfg__Fields row {source_row} has no entity_type."
            )

        entity_type = _normalize_owner_entity(
            raw_entity,
            field_key,
        )
        if not entity_type:
            raise ValueError(
                f"Cfg__Fields row {source_row} has an invalid "
                f"entity_type={raw_entity!r}."
            )

        if "|" not in raw_field_id:
            raise ValueError(
                f"Cfg__Fields row {source_row} field_id must be "
                f"entity_type|field_key; got={raw_field_id!r}."
            )

        field_id_entity, field_id_key = raw_field_id.split("|", 1)
        normalized_field_id_entity = _normalize_owner_entity(
            field_id_entity,
            field_key,
        )
        if (
            normalized_field_id_entity != entity_type
            or _safe_str(field_id_key) != field_key
        ):
            raise ValueError(
                f"Cfg__Fields row {source_row} field_id mismatch. "
                f"field_id={raw_field_id!r}; "
                f"entity_type={raw_entity!r}; "
                f"field_key={field_key!r}."
            )

        canonical_field_id = f"{entity_type}|{field_key}"
        if canonical_field_id in by_field_id:
            previous = by_field_id[canonical_field_id]
            raise ValueError(
                "Cfg__Fields duplicate field_id "
                f"{canonical_field_id!r} at rows "
                f"{previous['source_row']} and {source_row}."
            )

        record["raw_field_id"] = raw_field_id
        record["field_id"] = canonical_field_id
        record["entity_type"] = entity_type
        record["field_key"] = field_key
        record["source_row"] = str(source_row)

        records.append(record)
        by_field_id[canonical_field_id] = record
        by_field_key.setdefault(field_key, []).append(record)
        by_entity_field_key[(entity_type, field_key)] = record

    if not records:
        raise ValueError("Cfg__Fields contains no usable rows.")

    return {
        "records": records,
        "by_field_id": by_field_id,
        "by_field_key": by_field_key,
        "by_entity_field_key": by_entity_field_key,
        "stats": {
            "records": len(records),
            "unique_field_ids": len(by_field_id),
            "distinct_field_keys": len(by_field_key),
            "repeated_field_keys": sum(
                1
                for matches in by_field_key.values()
                if len(matches) > 1
            ),
        },
    }

def _read_locations(
    values: Sequence[Sequence[Any]],
    site_code: str,
) -> Dict[str, Any]:
    if not values:
        raise ValueError("Cfg__Locations is empty.")
    headers = [_safe_str(value) for value in values[0]]
    header_map = {
        header: index for index, header in enumerate(headers) if header
    }
    required = {
        "site_code",
        "location_code",
        "location_gid",
        "active",
        "is_default",
    }
    missing = sorted(required - set(header_map))
    if missing:
        raise ValueError(
            f"Cfg__Locations missing required columns: {missing}"
        )

    active_by_code: Dict[str, Dict[str, str]] = {}
    defaults: List[Dict[str, str]] = []
    for source_row, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        record = {
            header: _safe_str(padded[index])
            for header, index in header_map.items()
        }
        if _normalize_site_code(record.get("site_code")) != site_code:
            continue
        try:
            is_active = _normalize_bool(record.get("active"))
            is_default = _normalize_bool(record.get("is_default"))
        except ValueError as exc:
            raise ValueError(
                f"Cfg__Locations row {source_row}: {exc}"
            ) from exc
        if not is_active:
            continue
        code = _safe_str(record.get("location_code"))
        gid = _safe_str(record.get("location_gid"))
        if not code or not gid:
            raise ValueError(
                f"Cfg__Locations row {source_row} has empty active "
                "location_code or location_gid."
            )
        if code in active_by_code:
            raise ValueError(
                f"Cfg__Locations duplicate active location_code={code!r}."
            )
        record["source_row"] = str(source_row)
        active_by_code[code] = record
        if is_default:
            defaults.append(record)

    if not active_by_code:
        raise ValueError(
            f"Cfg__Locations has no active row for site_code={site_code}."
        )
    if len(defaults) != 1:
        raise ValueError(
            "Cfg__Locations must have exactly one active default row for "
            f"site_code={site_code}; found={len(defaults)}."
        )
    return {
        "active_by_code": active_by_code,
        "default": defaults[0],
    }


def _field_definition(
    field_key: str,
    cfg_fields: Mapping[str, Any],
) -> Optional[Dict[str, str]]:
    field_key = _safe_str(field_key)

    if field_key in SYSTEM_FIELD_DEFINITIONS:
        definition = dict(SYSTEM_FIELD_DEFINITIONS[field_key])
        owner = _safe_str(definition.get("scope")).upper()
        definition["field_id"] = f"{owner}|{field_key}"

        exact = _cfg_get_exact(
            cfg_fields,
            entity_type=owner,
            field_key=field_key,
        )
        if exact:
            # Cfg__Fields can enrich the built-in writable contract with
            # data_type/source metadata, but cannot change its owner.
            definition.update(exact)
            definition["scope"] = owner
            definition["field_id"] = f"{owner}|{field_key}"
        return definition

    if field_key.startswith("mf."):
        owner = "PRODUCT"
    elif field_key.startswith("v_mf."):
        owner = "VARIANT"
    else:
        # Generic Create accepts only the explicit system contract plus
        # Config-driven Product/Variant metafields.
        return None

    cfg = _cfg_get_exact(
        cfg_fields,
        entity_type=owner,
        field_key=field_key,
    )
    if not cfg:
        return None

    definition = {
        key: _safe_str(value)
        for key, value in cfg.items()
    }
    definition["scope"] = owner
    definition["field_id"] = f"{owner}|{field_key}"

    actual_entity = _normalize_owner_entity(
        definition.get("entity_type"),
        field_key,
    )
    if actual_entity != owner:
        definition["scope_error"] = (
            f"{definition['field_id']} owner mismatch: "
            f"expected={owner}; actual={actual_entity}."
        )
    return definition

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
            "Unrecognized field_key values. Add them to Cfg__Fields or "
            f"correct the Input/Defaults header: {sorted(unknown)}"
        )
    if scope_errors:
        raise ValueError("Field scope mismatch: " + " | ".join(scope_errors))


def _evaluate_default(
    spec: DefaultSpec,
    values: Mapping[str, str],
    locations: Mapping[str, Any],
) -> str:
    token = _safe_str(spec.config_value).upper()
    if token == "FROM_TITLE":
        return _safe_str(values.get("core.title"))
    if token == "FROM_DESCRIPTION_TEXT":
        return _html_to_text(values.get("core.description_html"))
    if token == "FROM_CFG_LOCATIONS_DEFAULT":
        return _safe_str(locations["default"].get("location_code"))
    return _safe_str(spec.config_value)


def _field_scope(
    field_key: str,
    cfg_fields: Mapping[str, Any],
) -> str:
    definition = _field_definition(field_key, cfg_fields)
    return _safe_str((definition or {}).get("scope")).upper()


def _normalize_typed_value(
    field_key: str,
    value: str,
    cfg_fields: Mapping[str, Any],
) -> str:
    text = _safe_str(value)
    if not text:
        return ""
    definition = _field_definition(field_key, cfg_fields) or {}
    data_type = _safe_str(definition.get("data_type")).lower()

    if field_key == "inventory.quantity":
        normalized = _integer_text(text)
        if int(normalized) < 0:
            raise ValueError("Inventory Quantity cannot be negative.")
        return normalized

    if field_key in {
        "core.price",
        "core.compare_at_price",
        "core.cost",
        "core.weight",
    }:
        normalized = _decimal_text(text)
        if Decimal(normalized) < 0:
            raise ValueError(f"{field_key} cannot be negative.")
        return normalized

    if field_key in {
        "core.requires_shipping",
        "core.taxable",
    } or "boolean" in data_type:
        return _bool_cell(text)

    if field_key == "core.category_id":
        if not text.startswith(
            "gid://shopify/TaxonomyCategory/"
        ):
            raise ValueError(
                "Category must be a Shopify TaxonomyCategory GID."
            )
        return text

    if field_key == "core.status":
        normalized = text.lower()
        if normalized not in {"draft", "active", "archived"}:
            raise ValueError(
                "Status must be draft, active, or archived."
            )
        return normalized

    if field_key == "core.inventory_policy":
        normalized = text.lower()
        if normalized not in {"deny", "continue"}:
            raise ValueError(
                "Inventory Policy must be deny or continue."
            )
        return normalized

    if field_key == "core.inventory_tracker":
        normalized = text.lower()
        aliases = {
            "shopify": "shopify",
            "tracked": "shopify",
            "true": "shopify",
            "yes": "shopify",
            "1": "shopify",
        }
        if normalized not in aliases:
            raise ValueError(
                "Inventory Tracker currently supports only shopify."
            )
        return aliases[normalized]

    if field_key == "core.fulfillment_service":
        normalized = text.lower()
        if normalized != "manual":
            raise ValueError(
                "Fulfillment Service currently supports only manual. "
                "Shopify derives fulfillment ownership from the "
                "selected inventory Location."
            )
        return normalized

    if field_key == "core.weight_unit":
        normalized = text.lower()
        if normalized not in {"g", "kg", "oz", "lb"}:
            raise ValueError(
                "Weight Unit must be g, kg, oz, or lb."
            )
        return normalized

    if any(
        marker in data_type
        for marker in {
            "number_integer",
            "integer",
        }
    ):
        return _integer_text(text)

    if any(
        marker in data_type
        for marker in {
            "number_decimal",
            "decimal",
            "float",
        }
    ):
        return _decimal_text(text)

    return text


def _add_issue(
    row_state: Dict[str, Any],
    level: str,
    code: str,
    message: str,
) -> None:
    issue = {
        "level": level.upper(),
        "code": code,
        "message": message,
    }
    target = (
        row_state["errors"]
        if issue["level"] == "ERROR"
        else row_state["warnings"]
    )
    if issue not in target:
        target.append(issue)


def _product_level_fields(
    all_field_keys: Iterable[str],
    cfg_fields: Mapping[str, Any],
) -> List[str]:
    result = []
    for field_key in all_field_keys:
        if field_key in {
            "sys.product_key",
            "sys.variant_key",
            "sys.action",
        }:
            continue
        if _field_scope(field_key, cfg_fields) == "PRODUCT":
            result.append(field_key)
    return result


def _apply_product_inheritance(
    active_rows: Sequence[Dict[str, Any]],
    product_fields: Sequence[str],
) -> None:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row_state in active_rows:
        product_key = _safe_str(
            row_state["values"].get("sys.product_key")
        )
        if product_key:
            groups.setdefault(product_key, []).append(row_state)

    for product_key, group in groups.items():
        for field_key in product_fields:
            nonblank = {
                _safe_str(row["values"].get(field_key))
                for row in group
                if _safe_str(row["values"].get(field_key))
            }
            if len(nonblank) > 1:
                message = (
                    f"Product field {field_key} has conflicting values "
                    f"within product_key={product_key}: "
                    f"{sorted(nonblank)}"
                )
                for row in group:
                    _add_issue(
                        row,
                        "ERROR",
                        "PRODUCT_FIELD_CONFLICT",
                        message,
                    )
                continue
            if len(nonblank) == 1:
                inherited = next(iter(nonblank))
                for row in group:
                    if not _safe_str(row["values"].get(field_key)):
                        row["values"][field_key] = inherited
                        row["inherited_fields"].append(field_key)


def _build_prepare_plan(
    *,
    input_contract: Mapping[str, Any],
    defaults: Mapping[str, DefaultSpec],
    cfg_fields: Mapping[str, Any],
    locations: Mapping[str, Any],
) -> Dict[str, Any]:
    input_field_keys = list(input_contract["field_keys"])
    all_field_keys: List[str] = list(input_field_keys)
    for field_key in defaults:
        if field_key not in all_field_keys:
            all_field_keys.append(field_key)

    _validate_known_fields(all_field_keys, cfg_fields)

    row_states: List[Dict[str, Any]] = []
    active_rows: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []

    for row in input_contract["rows"]:
        values = {
            key: _safe_str(value)
            for key, value in row["values"].items()
        }
        action = _safe_str(values.get("sys.action")).upper() or "CREATE"
        values["sys.action"] = action
        row_state = {
            "source_row": int(row["source_row"]),
            "values": values,
            "errors": [],
            "warnings": [],
            "defaulted_fields": [],
            "inherited_fields": [],
            "status": "",
        }
        row_states.append(row_state)

        if action == "SKIP":
            row_state["status"] = "SKIPPED"
            skipped_rows.append(row_state)
            continue
        if action != "CREATE":
            _add_issue(
                row_state,
                "ERROR",
                "INVALID_ACTION",
                f"sys.action must be CREATE, SKIP, or blank; got={action!r}.",
            )
        active_rows.append(row_state)

    product_fields = _product_level_fields(
        all_field_keys,
        cfg_fields,
    )
    _apply_product_inheritance(active_rows, product_fields)

    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row_state in active_rows:
        product_key = _safe_str(
            row_state["values"].get("sys.product_key")
        )
        if product_key:
            groups.setdefault(product_key, []).append(row_state)

    for product_key, group in groups.items():
        handles = {
            _safe_str(row["values"].get("core.handle"))
            for row in group
            if _safe_str(row["values"].get("core.handle"))
        }
        if not handles:
            title = next(
                (
                    _safe_str(row["values"].get("core.title"))
                    for row in group
                    if _safe_str(row["values"].get("core.title"))
                ),
                "",
            )
            generated = _slugify(title)
            if generated:
                for row in group:
                    row["values"]["core.handle"] = generated
                    if "core.handle" not in row["defaulted_fields"]:
                        row["defaulted_fields"].append("core.handle")
                if "core.handle" not in all_field_keys:
                    all_field_keys.append("core.handle")
        elif len(handles) == 1:
            handle = next(iter(handles))
            for row in group:
                if not _safe_str(row["values"].get("core.handle")):
                    row["values"]["core.handle"] = handle
                    row["inherited_fields"].append("core.handle")

    for row_state in active_rows:
        for field_key, spec in defaults.items():
            if not _safe_str(row_state["values"].get(field_key)):
                resolved = _evaluate_default(
                    spec,
                    row_state["values"],
                    locations,
                )
                row_state["values"][field_key] = resolved
                row_state["defaulted_fields"].append(field_key)

    # Product values can now include derived Defaults, so run consistency again.
    product_fields = _product_level_fields(
        all_field_keys,
        cfg_fields,
    )
    _apply_product_inheritance(active_rows, product_fields)

    variant_key_rows: Dict[str, List[Dict[str, Any]]] = {}

    for row_state in active_rows:
        values = row_state["values"]

        for required_field in REQUIRED_ACTIVE_FIELDS:
            if not _safe_str(values.get(required_field)):
                _add_issue(
                    row_state,
                    "ERROR",
                    "MISSING_REQUIRED_FIELD",
                    f"Required field is empty: {required_field}",
                )

        for field_key in all_field_keys:
            raw = _safe_str(values.get(field_key))
            if not raw:
                continue
            try:
                values[field_key] = _normalize_typed_value(
                    field_key,
                    raw,
                    cfg_fields,
                )
            except ValueError as exc:
                _add_issue(
                    row_state,
                    "ERROR",
                    "INVALID_FIELD_VALUE",
                    f"{field_key}: {exc}",
                )

        publish_all_channels = _normalize_bool(
            values.get("publish.all_channels")
        )
        if (
            publish_all_channels is True
            and _safe_str(values.get("core.status")).lower()
            != "active"
        ):
            _add_issue(
                row_state,
                "ERROR",
                "PUBLISH_ALL_CHANNELS_REQUIRES_ACTIVE",
                "publish.all_channels=TRUE requires "
                "core.status=active. Draft products are not "
                "visible on sales channels.",
            )

        location_code = _safe_str(
            values.get("inventory.location_code")
        )
        if location_code and location_code not in locations["active_by_code"]:
            _add_issue(
                row_state,
                "ERROR",
                "UNKNOWN_LOCATION_CODE",
                f"inventory.location_code={location_code!r} is not an "
                "active Cfg__Locations code for this site.",
            )

        price = _safe_str(values.get("core.price"))
        compare_at = _safe_str(values.get("core.compare_at_price"))
        if price and compare_at:
            try:
                if Decimal(compare_at) < Decimal(price):
                    _add_issue(
                        row_state,
                        "WARNING",
                        "COMPARE_AT_BELOW_PRICE",
                        "Compare-at Price is lower than Price.",
                    )
            except InvalidOperation:
                pass

        for option_number in (1, 2, 3):
            name_key = f"core.option{option_number}_name"
            value_key = f"core.option{option_number}_value"
            name = _safe_str(values.get(name_key))
            option_value = _safe_str(values.get(value_key))
            if name and not option_value:
                _add_issue(
                    row_state,
                    "ERROR",
                    "OPTION_VALUE_MISSING",
                    f"{name_key} has a value but {value_key} is empty.",
                )
            if option_value and not name:
                _add_issue(
                    row_state,
                    "ERROR",
                    "OPTION_NAME_MISSING",
                    f"{value_key} has a value but {name_key} is empty.",
                )

        variant_key = _safe_str(values.get("sys.variant_key"))
        if variant_key:
            variant_key_rows.setdefault(variant_key, []).append(row_state)

    for value, rows in variant_key_rows.items():
        if len(rows) > 1:
            for row_state in rows:
                _add_issue(
                    row_state,
                    "ERROR",
                    "DUPLICATE_VARIANT_KEY",
                    f"Duplicate sys.variant_key={value!r}.",
                )

    handle_groups: Dict[str, List[str]] = {}
    for product_key, group in groups.items():
        handles = {
            _safe_str(row["values"].get("core.handle"))
            for row in group
            if _safe_str(row["values"].get("core.handle"))
        }
        for handle in handles:
            handle_groups.setdefault(handle, []).append(product_key)

    for handle, product_keys in handle_groups.items():
        unique_product_keys = sorted(set(product_keys))
        if len(unique_product_keys) > 1:
            message = (
                f"Duplicate core.handle={handle!r} across "
                f"product_key values={unique_product_keys}."
            )
            for product_key in unique_product_keys:
                for row_state in groups[product_key]:
                    _add_issue(
                        row_state,
                        "ERROR",
                        "DUPLICATE_HANDLE",
                        message,
                    )

    # Check option names and option-value combinations within each Product.
    for product_key, group in groups.items():
        for option_number in (1, 2, 3):
            name_key = f"core.option{option_number}_name"
            names = {
                _safe_str(row["values"].get(name_key))
                for row in group
                if _safe_str(row["values"].get(name_key))
            }
            if len(names) > 1:
                message = (
                    f"{name_key} is inconsistent within "
                    f"product_key={product_key}: {sorted(names)}"
                )
                for row_state in group:
                    _add_issue(
                        row_state,
                        "ERROR",
                        "OPTION_NAME_CONFLICT",
                        message,
                    )

        combinations: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        for row_state in group:
            combination = tuple(
                _safe_str(
                    row_state["values"].get(
                        f"core.option{number}_value"
                    )
                )
                for number in (1, 2, 3)
            )
            if any(combination):
                combinations.setdefault(combination, []).append(row_state)
        for combination, rows in combinations.items():
            if len(rows) > 1:
                for row_state in rows:
                    _add_issue(
                        row_state,
                        "ERROR",
                        "DUPLICATE_OPTION_COMBINATION",
                        "Duplicate option-value combination within "
                        f"product_key={product_key}: {combination}",
                    )

    for product_key, group in groups.items():
        if any(row_state["errors"] for row_state in group):
            message = (
                "The Product group is blocked because at least one "
                f"Variant row has an error: product_key={product_key}."
            )
            for row_state in group:
                if not row_state["errors"]:
                    _add_issue(
                        row_state,
                        "ERROR",
                        "PRODUCT_GROUP_BLOCKED",
                        message,
                    )

    product_variant_counts = {
        product_key: len(group)
        for product_key, group in groups.items()
    }

    for row_state in active_rows:
        if row_state["errors"]:
            row_state["status"] = "ERROR"
        elif row_state["warnings"]:
            row_state["status"] = "READY_WITH_WARNINGS"
        else:
            row_state["status"] = "READY"

    display_by_key: Dict[str, str] = {
        column["field_key"]: column["display_name"]
        for column in input_contract["columns"]
    }
    for field_key, spec in defaults.items():
        display_by_key.setdefault(
            field_key,
            spec.display_name
            or SYSTEM_FIELD_DEFINITIONS.get(field_key, {}).get(
                "display_name",
                field_key,
            ),
        )
    for field_key in all_field_keys:
        definition = _field_definition(
            field_key,
            cfg_fields,
        ) or {}
        display_by_key.setdefault(
            field_key,
            _safe_str(definition.get("display_name"))
            or SYSTEM_FIELD_DEFINITIONS.get(field_key, {}).get(
                "display_name",
                field_key,
            ),
        )

    ordered_field_keys = list(input_field_keys)
    for field_key in all_field_keys:
        if field_key not in ordered_field_keys:
            ordered_field_keys.append(field_key)

    preview_display_headers = [
        item[0] for item in PREVIEW_SYSTEM_FIELDS
    ] + [display_by_key[field_key] for field_key in ordered_field_keys]
    preview_field_keys = [
        item[1] for item in PREVIEW_SYSTEM_FIELDS
    ] + ordered_field_keys

    preview_rows: List[List[str]] = []
    preview_records: List[Dict[str, str]] = []
    for row_state in row_states:
        product_key = _safe_str(
            row_state["values"].get("sys.product_key")
        )
        messages = row_state["errors"] + row_state["warnings"]
        message_text = json.dumps(
            messages,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        defaulted_text = ";".join(
            sorted(set(row_state["defaulted_fields"]))
        )
        inherited_text = ";".join(
            sorted(set(row_state["inherited_fields"]))
        )
        system_values = [
            row_state["status"],
            str(row_state["source_row"]),
            str(len(row_state["errors"])),
            str(len(row_state["warnings"])),
            message_text,
            defaulted_text,
            inherited_text,
            str(product_variant_counts.get(product_key, 0)),
        ]
        normalized_values = [
            _safe_str(row_state["values"].get(field_key))
            for field_key in ordered_field_keys
        ]
        preview_rows.append(system_values + normalized_values)
        preview_records.append(
            dict(
                zip(
                    preview_field_keys,
                    system_values + normalized_values,
                )
            )
        )

    errors = [
        issue
        for row_state in row_states
        for issue in row_state["errors"]
    ]
    warnings = [
        issue
        for row_state in row_states
        for issue in row_state["warnings"]
    ]
    ready_rows = [
        row_state
        for row_state in active_rows
        if not row_state["errors"]
    ]
    ready_product_keys = {
        _safe_str(row_state["values"].get("sys.product_key"))
        for row_state in ready_rows
        if _safe_str(row_state["values"].get("sys.product_key"))
        and all(
            not member["errors"]
            for member in groups.get(
                _safe_str(
                    row_state["values"].get("sys.product_key")
                ),
                [],
            )
        )
    }

    if not active_rows:
        status = "FAILED_VALIDATION"
        errors.append(
            {
                "level": "ERROR",
                "code": "NO_ACTIVE_ROWS",
                "message": "Input has no CREATE rows.",
            }
        )
    elif errors:
        status = "FAILED_VALIDATION"
    elif warnings:
        status = "READY_WITH_WARNINGS"
    else:
        status = "READY"

    return {
        "status": status,
        "ready_for_apply": status in {
            "READY",
            "READY_WITH_WARNINGS",
        },
        "preview_matrix": [
            preview_display_headers,
            preview_field_keys,
            *preview_rows,
        ],
        "preview_records": preview_records,
        "row_states": row_states,
        "warnings": warnings,
        "errors": errors,
        "ordered_field_keys": ordered_field_keys,
        "stats": {
            "rows_loaded": len(row_states),
            "rows_pending": len(active_rows),
            "rows_recognized": len(active_rows),
            "rows_planned": len(ready_rows),
            "rows_skipped": len(skipped_rows),
            "product_groups": len(groups),
            "business_objects_planned": len(ready_product_keys),
            "variant_objects_planned": len(ready_rows),
            "warning_count": len(warnings),
            "error_count": len(errors),
        },
    }


def update_existing_notebook_registry_row(
    *,
    registry_mode: str,
    console_core_url: str,
    bootstrap_gsheet_secret_name: str,
    registry_tab: str,
    job_name: str,
    sheet_label: str,
    tab_name: str,
    current_colab_url: str = "",
    current_colab_name: str = "",
    project_code: Optional[str] = None,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    explicit_sa_value: Optional[str] = None,
    print_progress: bool = True,
) -> Dict[str, Any]:
    """Check or update one existing registry row; never append."""
    mode = _safe_str(registry_mode).upper() or "OFF"
    allowed = {"OFF", "CHECK", "UPDATE_URL", "UPDATE_URL_AND_NAME"}
    if mode not in allowed:
        raise ValueError(
            f"registry_mode must be one of {sorted(allowed)}."
        )
    if mode == "OFF":
        if print_progress:
            print("[Registry] mode=OFF; no registry read or write")
        return {
            "status": "OFF",
            "changed_fields": [],
            "target_row": None,
        }

    if mode in {"UPDATE_URL", "UPDATE_URL_AND_NAME"} and not _safe_str(
        current_colab_url
    ):
        raise ValueError(
            f"registry_mode={mode} requires current_colab_url."
        )
    if mode == "UPDATE_URL_AND_NAME" and not _safe_str(
        current_colab_name
    ):
        raise ValueError(
            "UPDATE_URL_AND_NAME requires current_colab_name."
        )

    if print_progress:
        print(
            "[Registry] resolving target | "
            f"job_name={job_name} | sheet_label={sheet_label} | "
            f"tab_name={tab_name}"
        )

    secret = read_secret(
        bootstrap_gsheet_secret_name,
        project_code=project_code,
        explicit_value=explicit_sa_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, auth_meta = _build_gspread_client(secret)
    worksheet = gc.open_by_url(
        console_core_url
    ).worksheet(registry_tab)
    values = worksheet.get_all_values()
    if not values:
        raise ValueError(f"Registry tab {registry_tab!r} is empty.")

    header_map: Dict[str, int] = {}
    for index, raw_header in enumerate(values[0]):
        normalized = _normalize_registry_header(raw_header)
        if normalized:
            header_map[normalized] = index

    def require_column(*aliases: str) -> int:
        for alias in aliases:
            key = _normalize_registry_header(alias)
            if key in header_map:
                return header_map[key]
        raise ValueError(
            "Registry tab is missing a required column; "
            f"accepted aliases={aliases}."
        )

    job_col = require_column("job_name", "job name")
    label_col = require_column("sheet_label", "sheet label")
    tab_col = require_column(
        "Tab name",
        "sheet name",
        "sheet_name",
    )
    url_col = require_column("colab_url", "colab url")
    name_col = require_column("colab_name", "colab name")

    wanted = (
        _safe_str(job_name).lower(),
        _safe_str(sheet_label).lower(),
        _safe_str(tab_name).lower(),
    )
    matches: List[int] = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = list(row) + [""] * max(
            0,
            len(values[0]) - len(row),
        )
        logical_key = (
            _safe_str(padded[job_col]).lower(),
            _safe_str(padded[label_col]).lower(),
            _safe_str(padded[tab_col]).lower(),
        )
        if logical_key == wanted:
            matches.append(row_index)

    if not matches:
        raise ValueError(
            "Registry target row was not found. This function never "
            f"appends. logical_key={wanted}"
        )
    if len(matches) > 1:
        raise ValueError(
            "Registry logical key is duplicated at "
            f"rows={matches}; no row was changed."
        )

    row_number = matches[0]
    current_row = values[row_number - 1] + [""] * max(
        0,
        len(values[0]) - len(values[row_number - 1]),
    )
    changes: List[Tuple[str, int, str, str]] = []

    provided_url = _safe_str(current_colab_url)
    provided_name = _safe_str(current_colab_name)
    if (
        provided_url
        and _safe_str(current_row[url_col]) != provided_url
    ):
        changes.append(
            (
                "colab_url",
                url_col + 1,
                _safe_str(current_row[url_col]),
                provided_url,
            )
        )
    if (
        provided_name
        and _safe_str(current_row[name_col]) != provided_name
    ):
        changes.append(
            (
                "colab_name",
                name_col + 1,
                _safe_str(current_row[name_col]),
                provided_name,
            )
        )

    if mode == "CHECK":
        status = "CHANGE_DETECTED" if changes else "NO_CHANGE"
    else:
        permitted = (
            {"colab_url"}
            if mode == "UPDATE_URL"
            else {"colab_url", "colab_name"}
        )
        applied = [
            change for change in changes if change[0] in permitted
        ]
        for _, column_number, _, new_value in applied:
            worksheet.update_cell(
                row_number,
                column_number,
                new_value,
            )
        changes = applied
        status = "UPDATED" if changes else "NO_CHANGE"

    if print_progress:
        print(
            "[Registry] "
            f"row={row_number} | status={status} | "
            f"changed_fields={[item[0] for item in changes]}"
        )

    return {
        "status": status,
        "target_row": row_number,
        "changed_fields": [item[0] for item in changes],
        "auth_source_type": auth_meta["source_type"],
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
    tab_result: str = "Result",
    tab_runlog: str = "Ops__RunLog",
    write_input_field_key_row: bool = True,
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
    """Build and optionally write the Generic Product Creation Preview."""
    site_code = _normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not _safe_str(console_core_url):
        raise ValueError("console_core_url is required.")
    if not _safe_str(bootstrap_gsheet_sa_b64_secret):
        raise ValueError(
            "bootstrap_gsheet_sa_b64_secret is required."
        )

    run_id = run_id or _make_run_id(job_name, tz_name)
    phase = "prepare"
    started = time.monotonic()

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(
        1,
        10,
        f"Resolve Google Secret | site={site_code} | phase={phase}",
    )
    secret = read_secret(
        bootstrap_gsheet_sa_b64_secret,
        project_code=site_code,
        explicit_value=sa_b64_value,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, auth_meta = _build_gspread_client(secret)
    console = gc.open_by_url(console_core_url)

    progress(
        2,
        10,
        "Google access ready | "
        f"source={auth_meta['source_type']} | "
        f"format={auth_meta['secret_format']}",
    )

    account = _load_account_values(
        console,
        tab_cfg_account_id,
    )
    configured_secret = _safe_str(
        account.get("GSHEET_SA_B64_SECRET")
    )
    if (
        configured_secret
        and configured_secret
        != bootstrap_gsheet_sa_b64_secret
    ):
        raise ValueError(
            "Bootstrap Google Secret does not match "
            f"{tab_cfg_account_id}. bootstrap="
            f"{bootstrap_gsheet_sa_b64_secret}; "
            f"cfg={configured_secret}"
        )

    progress(
        3,
        10,
        "Resolve routed workbooks | "
        f"create={create_sheet_label} | config={config_sheet_label}",
    )
    create_url = _resolve_sheet_url_by_label(
        console,
        tab_cfg_sites,
        site_code,
        create_sheet_label,
    )
    config_url = _resolve_sheet_url_by_label(
        console,
        tab_cfg_sites,
        site_code,
        config_sheet_label,
    )
    runlog_url = _resolve_sheet_url_by_label(
        console,
        tab_cfg_sites,
        site_code,
        runlog_sheet_label,
    )

    create_book = gc.open_by_url(create_url)
    config_book = gc.open_by_url(config_url)
    runlog_ws = gc.open_by_url(runlog_url).worksheet(tab_runlog)
    logger = RunLogger18(
        worksheet=runlog_ws,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )

    try:
        progress(
            4,
            10,
            f"Read Input and Defaults | tabs={tab_input}, {tab_defaults}",
        )
        input_ws = _require_worksheet(
            create_book,
            tab_input,
        )
        input_values = input_ws.get_all_values()
        defaults_values = _require_worksheet(
            create_book,
            tab_defaults,
        ).get_all_values()
        defaults = _read_defaults_matrix(defaults_values)

        progress(
            5,
            10,
            f"Read field dictionary | tab={tab_cfg_fields}",
        )
        cfg_fields = _read_cfg_fields(
            _require_worksheet(
                config_book,
                tab_cfg_fields,
            ).get_all_values()
        )
        print(
            "[Cfg__Fields] "
            f"records={cfg_fields['stats']['records']} | "
            f"unique_field_ids="
            f"{cfg_fields['stats']['unique_field_ids']} | "
            f"distinct_field_keys="
            f"{cfg_fields['stats']['distinct_field_keys']} | "
            f"repeated_field_keys="
            f"{cfg_fields['stats']['repeated_field_keys']}"
        )

        progress(
            6,
            10,
            "Resolve Input Row-2 field_key mapping from Row 1 / Cfg__Fields",
        )
        input_contract = _read_input_matrix(
            input_values,
            cfg_fields,
        )
        if not input_contract["rows"]:
            raise ValueError("Input contains no data rows.")
        mapping_counts = input_contract["mapping_source_counts"]
        print(
            "[Field Mapping] "
            f"columns={len(input_contract['field_keys'])} | "
            f"explicit={mapping_counts.get('EXPLICIT_ROW_2', 0)} | "
            f"system={mapping_counts.get('SYSTEM_DISPLAY_NAME', 0)} | "
            f"cfg_product={mapping_counts.get('CFG_FIELDS_PRODUCT', 0)} | "
            f"cfg_variant={mapping_counts.get('CFG_FIELDS_VARIANT', 0)} | "
            f"changed_columns="
            f"{input_contract['mapping_changed_columns']}"
        )

        mapping_write = {
            "field_key_row_written": False,
            "mapping_cells_changed": 0,
            "changed_columns": [],
        }
        if write_input_field_key_row:
            mapping_write = _write_input_field_key_row_if_changed(
                input_ws,
                provided_field_keys=input_contract[
                    "provided_field_keys"
                ],
                resolved_field_keys=input_contract[
                    "resolved_field_keys"
                ],
            )
        print(
            "[Field Mapping Write] "
            f"enabled={write_input_field_key_row} | "
            f"written={mapping_write['field_key_row_written']} | "
            f"cells_changed={mapping_write['mapping_cells_changed']}"
        )

        progress(
            7,
            10,
            f"Resolve default Location | tab={tab_cfg_locations}",
        )
        locations = _read_locations(
            _require_worksheet(
                console,
                tab_cfg_locations,
            ).get_all_values(),
            site_code,
        )
        default_location = locations["default"]
        print(
            "[Location] "
            f"code={default_location.get('location_code')} | "
            f"name={default_location.get('location_name')} | "
            f"gid={default_location.get('location_gid')}"
        )

        progress(
            8,
            10,
            "Normalize, apply Defaults, group Products, and validate",
        )
        plan = _build_prepare_plan(
            input_contract=input_contract,
            defaults=defaults,
            cfg_fields=cfg_fields,
            locations=locations,
        )
        stats = plan["stats"]
        print(
            "[Plan] "
            f"rows_loaded={stats['rows_loaded']} | "
            f"rows_pending={stats['rows_pending']} | "
            f"rows_planned={stats['rows_planned']} | "
            f"products={stats['product_groups']} | "
            f"warnings={stats['warning_count']} | "
            f"errors={stats['error_count']}"
        )

        preview_rows_written = 0
        if write_preview:
            progress(
                9,
                10,
                f"Overwrite Preview | tab={tab_preview}",
            )
            preview_rows_written = _write_matrix_overwrite(
                create_book,
                tab_preview,
                plan["preview_matrix"],
            )
        else:
            progress(
                9,
                10,
                "Preview write disabled; no Sheet change",
            )

        status = plan["status"]
        logger.log(
            phase=phase,
            log_type="summary",
            status=status,
            rows_loaded=stats["rows_loaded"],
            rows_pending=stats["rows_pending"],
            rows_recognized=stats["rows_recognized"],
            rows_planned=stats["rows_planned"],
            rows_written=preview_rows_written,
            rows_skipped=stats["rows_skipped"],
            message=(
                f"prepare | ready_for_apply={plan['ready_for_apply']} | "
                f"products={stats['product_groups']} | "
                f"business_objects_planned="
                f"{stats['business_objects_planned']} | "
                f"warnings={stats['warning_count']} | "
                f"errors={stats['error_count']} | "
                f"preview_rows_written={preview_rows_written} | "
                f"mapping_row_written="
                f"{mapping_write['field_key_row_written']} | "
                f"mapping_cells_changed="
                f"{mapping_write['mapping_cells_changed']}"
            ),
            error_reason=(
                "INPUT_VALIDATION_FAILED"
                if status == "FAILED_VALIDATION"
                else ""
            ),
        )
        for issue in (plan["errors"] + plan["warnings"])[:20]:
            logger.log(
                phase=phase,
                log_type="detail",
                status=issue["level"],
                message=issue["message"],
                error_reason=issue["code"],
            )
        logger.flush()

        elapsed = round(time.monotonic() - started, 2)
        progress(
            10,
            10,
            f"Completed | status={status} | "
            f"ready_for_apply={plan['ready_for_apply']} | "
            f"elapsed={elapsed}s",
        )

        preview_df = pd.DataFrame(plan["preview_records"])
        if preview_rows >= 0:
            preview_df = preview_df.head(int(preview_rows))

        return {
            "ok": plan["ready_for_apply"],
            "status": status,
            "phase": phase,
            "ready_for_apply": plan["ready_for_apply"],
            "run_id": run_id,
            "job_name": job_name,
            "site_code": site_code,
            "summary": {
                **stats,
                "input_columns": len(input_contract["field_keys"]),
                "field_keys_explicit": mapping_counts.get(
                    "EXPLICIT_ROW_2",
                    0,
                ),
                "field_keys_resolved_system": mapping_counts.get(
                    "SYSTEM_DISPLAY_NAME",
                    0,
                ),
                "field_keys_resolved_cfg_product": mapping_counts.get(
                    "CFG_FIELDS_PRODUCT",
                    0,
                ),
                "field_keys_resolved_cfg_variant": mapping_counts.get(
                    "CFG_FIELDS_VARIANT",
                    0,
                ),
                "field_key_row_written": mapping_write[
                    "field_key_row_written"
                ],
                "mapping_cells_changed": mapping_write[
                    "mapping_cells_changed"
                ],
                "preview_rows_written": preview_rows_written,
                "api_operations_planned": 0,
                "api_operations_succeeded": 0,
                "api_operations_failed": 0,
                "elapsed_seconds": elapsed,
            },
            "preview": preview_df,
            "field_mapping": pd.DataFrame(
                input_contract["mapping_records"]
            ),
            "warnings": plan["warnings"],
            "errors": plan["errors"],
            "targets": {
                "console_core_url": console_core_url,
                "create_sheet_url": create_url,
                "config_sheet_url": config_url,
                "preview_tab": tab_preview,
                "result_tab": (
                    f"{tab_result} (untouched)"
                ),
                "runlog_sheet_url": runlog_url,
                "runlog_tab": tab_runlog,
                "module_path": MODULE_PATH,
                "module_version": MODULE_VERSION,
            },
            "runtime": {
                "runtime_mode": _runtime_mode(),
                "auth_type": "SERVICE_ACCOUNT",
                "interactive_auth_used": False,
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "google_secret_source": auth_meta["source_type"],
                "google_secret_format": auth_meta["secret_format"],
                "service_account_email": auth_meta[
                    "service_account_email"
                ],
            },
            "defaults": {
                field_key: {
                    "config_key": spec.config_key,
                    "config_value": spec.config_value,
                }
                for field_key, spec in defaults.items()
            },
            "default_location": {
                "location_code": _safe_str(
                    default_location.get("location_code")
                ),
                "location_name": _safe_str(
                    default_location.get("location_name")
                ),
                "location_gid": _safe_str(
                    default_location.get("location_gid")
                ),
            },
        }

    except BaseException as exc:
        try:
            logger.log(
                phase=phase,
                log_type="summary",
                status="FAILED",
                message=str(exc),
                error_reason=type(exc).__name__,
            )
            logger.flush()
        except Exception as log_exc:
            if print_progress:
                print(
                    "[RunLog warning] failed to write failure log: "
                    f"{log_exc}"
                )
        if print_progress:
            print(f"[FAILED] {type(exc).__name__}: {exc}")
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare Generic Shopify Product Creation Preview."
        )
    )
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument(
        "--bootstrap-gsheet-secret",
        required=True,
    )
    parser.add_argument(
        "--create-sheet-label",
        default="create_generic",
    )
    parser.add_argument(
        "--config-sheet-label",
        default="config",
    )
    parser.add_argument(
        "--no-write-input-field-key-row",
        action="store_true",
    )
    parser.add_argument(
        "--no-write-preview",
        action="store_true",
    )
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
        create_sheet_label=args.create_sheet_label,
        config_sheet_label=args.config_sheet_label,
        write_input_field_key_row=(
            not args.no_write_input_field_key_row
        ),
        write_preview=not args.no_write_preview,
        secret_home=args.secret_home or None,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "ready_for_apply": result["ready_for_apply"],
                "summary": result["summary"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if result["ready_for_apply"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
