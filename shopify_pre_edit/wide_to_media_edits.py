# shopify_pre_edit/wide_to_media_edits_v1.py

from __future__ import annotations

import base64
import datetime as dt
import json
import re
from typing import Any, Optional

import gspread
import pandas as pd
from google.oauth2 import service_account

try:
    from google.colab import userdata
except Exception:
    userdata = None


# =========================================================
# Constants
# =========================================================

CFG_SITES_TAB_DEFAULT = "Cfg__Sites"
CFG_ACCOUNT_TAB_DEFAULT = "Cfg__account_id"

JOB_NAME = "wide_to_media_edits"

WIDE_INPUT_TAB_DEFAULT = "Wide_Media"
LONG_OUTPUT_TAB_DEFAULT = "Long_Media"

LONG_HEADER = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "desired_value",
    "action",
]

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "VARIANT"}
SUPPORTED_ACTIONS = {"SET", "CLEAR", "SKIP"}

PRODUCT_FIELD_KEY = "core.product.images_urls"
VARIANT_FIELD_KEY = "core.variant.image_url"

PRODUCT_ID_HEADERS = (
    "Product ID (numeric)",
    "Product ID",
    "Product GID",
    "Product Handle",
    "Handle",
    "handle",
)
VARIANT_ID_HEADERS = (
    "Variant ID (numeric)",
    "Variant ID",
    "Variant GID",
    "SKU",
    "sku",
)

ENTITY_TYPE_HEADER = "entity_type"
ACTION_HEADER = "action"
VARIANT_IMAGE_HEADER = "Variant Image URL"

PRODUCT_IMAGE_HEADER_RE = re.compile(r"^Product\s+Image\s+URL(?:\s*[-_]\s*|\s+)(\d+)$", re.I)
HTTP_URL_RE = re.compile(r"^https?://", re.I)


# =========================================================
# Small helpers
# =========================================================

def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _utc_run_id(prefix: str = JOB_NAME) -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _norm_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        value = _norm_str(item)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _col_to_a1(col_num: int) -> str:
    if col_num <= 0:
        raise ValueError(f"Invalid column number: {col_num}")
    result = ""
    n = col_num
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _first_nonempty(row: dict[str, Any], headers: tuple[str, ...]) -> str:
    for header in headers:
        value = _norm_str(row.get(header))
        if value:
            return value
    return ""


def _is_http_url(value: str) -> bool:
    return bool(HTTP_URL_RE.match(_norm_str(value)))


# =========================================================
# Google auth / Sheet routing
# =========================================================

def _get_secret(secret_name: str) -> str:
    if userdata is None:
        raise RuntimeError(
            "google.colab.userdata is unavailable. This module is intended for Colab runner use."
        )
    value = userdata.get(secret_name)
    if not value:
        raise ValueError(f"Missing Colab Secret: {secret_name}")
    return value


def build_gsheet_client(gsheet_sa_b64_secret: str) -> gspread.Client:
    sa_b64 = _get_secret(gsheet_sa_b64_secret)
    sa_info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    sh = gc.open_by_url(console_core_url)
    ws = sh.worksheet(cfg_sites_tab)
    df = pd.DataFrame(ws.get_all_records())

    if df.empty:
        raise ValueError(f"{cfg_sites_tab} is empty")

    required = ["site_code", "label", "sheet_url"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{cfg_sites_tab} missing required columns: {missing}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    matched = df[
        (df["site_code"] == _norm_str(site_code).upper())
        & (df["label"] == _norm_str(label))
        & (df["sheet_url"] != "")
    ].copy()

    if matched.empty:
        raise ValueError(
            f"Cannot find sheet_url for site_code={site_code}, label={label} in {cfg_sites_tab}"
        )

    return _norm_str(matched.iloc[0]["sheet_url"])


def open_ws_by_label_and_title(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    worksheet_title: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    create_if_missing: bool = False,
    default_rows: int = 1000,
    default_cols: int = 20,
):
    sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=label,
        cfg_sites_tab=cfg_sites_tab,
    )
    sh = gc.open_by_url(sheet_url)

    try:
        ws = sh.worksheet(worksheet_title)
    except gspread.WorksheetNotFound:
        if not create_if_missing:
            raise
        ws = sh.add_worksheet(
            title=worksheet_title,
            rows=max(1, int(default_rows)),
            cols=max(1, int(default_cols)),
        )

    return sh, ws, sheet_url


def load_account_config(
    gc_console: gspread.Client,
    console_core_url: str,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
) -> dict[str, str]:
    """Read account/runtime settings from Console Core / Cfg__account_id."""
    sh = gc_console.open_by_url(console_core_url)
    ws = sh.worksheet(cfg_account_tab)
    values = ws.get_all_values()

    values = [row for row in values if any(_norm_str(x) for x in row)]
    if not values:
        raise ValueError(f"{cfg_account_tab} is empty in Console Core")

    rows: list[tuple[str, str]] = []
    for raw in values:
        padded = list(raw) + ["", ""]
        rows.append((_norm_str(padded[0]), _norm_str(padded[1])))

    first = (rows[0][0].lower(), rows[0][1].lower())
    if first in {("key", "value"), ("config_key", "config_value")}:
        rows = rows[1:]

    out: dict[str, str] = {}
    for key, value in rows:
        if key:
            out[key] = value

    if not out:
        raise ValueError(f"{cfg_account_tab} has no usable key/value rows")

    return out


# =========================================================
# Wide sheet parsing
# =========================================================

def load_wide_sheet(
    ws_wide,
    header_row: int = 1,
    field_key_row: int = 2,
    data_start_row: int = 3,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Row 1: human-friendly headers.
    Row 2: generated field_key mapping row.
    Row 3+: data.
    """
    values = ws_wide.get_all_values()
    if not values or len(values) < header_row:
        return pd.DataFrame(), [], []

    header_idx = header_row - 1
    mapping_idx = field_key_row - 1
    data_idx = data_start_row - 1

    headers = [_norm_str(x) for x in values[header_idx]]
    while headers and headers[-1] == "":
        headers.pop()

    if not headers:
        return pd.DataFrame(), [], []

    duplicate_headers = sorted({h for h in headers if h and headers.count(h) > 1})
    if duplicate_headers:
        raise ValueError(f"Wide_Media has duplicate headers: {duplicate_headers}")

    existing_mapping: list[str] = []
    if len(values) > mapping_idx:
        existing_mapping = [_norm_str(x) for x in values[mapping_idx][: len(headers)]]
        existing_mapping += [""] * (len(headers) - len(existing_mapping))

    data_rows = values[data_idx:] if len(values) > data_idx else []
    normalized_rows: list[list[str]] = []
    for raw in data_rows:
        row = list(raw[: len(headers)]) + [""] * max(0, len(headers) - len(raw))
        normalized_rows.append([_norm_str(x) for x in row])

    df = pd.DataFrame(normalized_rows, columns=headers)
    if df.empty:
        return df, headers, existing_mapping

    not_blank = df.apply(lambda r: any(_norm_str(x) for x in r.tolist()), axis=1)
    df = df[not_blank].copy()

    for col in df.columns:
        df[col] = df[col].apply(_norm_str)

    return df, headers, existing_mapping


def parse_media_columns(headers: list[str]) -> dict[str, Any]:
    product_columns: list[tuple[int, str]] = []
    variant_columns: list[str] = []
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for header in headers:
        h = _norm_str(header)
        if not h:
            continue

        match = PRODUCT_IMAGE_HEADER_RE.match(h)
        if match:
            seq = int(match.group(1))
            if seq <= 0:
                errors.append(
                    {
                        "source_col": h,
                        "error_reason": "invalid_product_image_sequence",
                        "message": "Product Image URL sequence must start at 1.",
                    }
                )
            else:
                product_columns.append((seq, h))
            continue

        if h.lower() == VARIANT_IMAGE_HEADER.lower():
            variant_columns.append(h)

    seq_to_columns: dict[int, list[str]] = {}
    for seq, col in product_columns:
        seq_to_columns.setdefault(seq, []).append(col)

    duplicate_sequences = {seq: cols for seq, cols in seq_to_columns.items() if len(cols) > 1}
    if duplicate_sequences:
        errors.append(
            {
                "error_reason": "duplicate_product_image_sequence",
                "message": f"Duplicate Product Image URL sequence numbers: {duplicate_sequences}",
            }
        )

    if len(variant_columns) > 1:
        errors.append(
            {
                "error_reason": "duplicate_variant_image_column",
                "message": f"Multiple Variant Image URL columns found: {variant_columns}",
            }
        )

    product_columns = sorted(product_columns, key=lambda item: item[0])

    if not product_columns:
        warnings.append(
            {
                "warning_type": "no_product_image_columns",
                "message": "No Product Image URL-N columns were found.",
            }
        )
    if not variant_columns:
        warnings.append(
            {
                "warning_type": "no_variant_image_column",
                "message": "No Variant Image URL column was found.",
            }
        )

    return {
        "product_columns": product_columns,
        "variant_image_column": variant_columns[0] if variant_columns else "",
        "errors": errors,
        "warnings": warnings,
    }


def build_field_key_mapping_row(headers: list[str]) -> list[str]:
    mapping: list[str] = []
    for header in headers:
        h = _norm_str(header)
        if PRODUCT_IMAGE_HEADER_RE.match(h):
            mapping.append(PRODUCT_FIELD_KEY)
        elif h.lower() == VARIANT_IMAGE_HEADER.lower():
            mapping.append(VARIANT_FIELD_KEY)
        else:
            mapping.append("")
    return mapping


def write_field_key_mapping_row(
    ws_wide,
    mapping_values: list[str],
    field_key_row: int = 2,
) -> dict[str, int]:
    if not mapping_values:
        return {"mapping_row_written": 0, "mapping_cols_written": 0}

    last_col = _col_to_a1(len(mapping_values))
    ws_wide.update(
        range_name=f"A{field_key_row}:{last_col}{field_key_row}",
        values=[mapping_values],
        value_input_option="RAW",
    )
    return {
        "mapping_row_written": 1,
        "mapping_cols_written": len(mapping_values),
    }


# =========================================================
# Long row builder
# =========================================================

def _infer_entity_type(row: dict[str, Any]) -> str:
    explicit = _norm_str(row.get(ENTITY_TYPE_HEADER)).upper()
    if explicit:
        return explicit

    product_id = _first_nonempty(row, PRODUCT_ID_HEADERS)
    variant_id = _first_nonempty(row, VARIANT_ID_HEADERS)

    if product_id and not variant_id:
        return "PRODUCT"
    if variant_id and not product_id:
        return "VARIANT"
    return ""


def build_long_rows(
    df_wide: pd.DataFrame,
    product_columns: list[tuple[int, str]],
    variant_image_column: str,
    *,
    data_start_row: int = 3,
    action_default: str = "SET",
    dedupe_product_urls: bool = True,
    strict_url_validation: bool = True,
) -> tuple[list[dict[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    out: list[dict[str, str]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    if df_wide.empty:
        return out, errors, warnings

    action_default = _norm_str(action_default).upper() or "SET"
    if action_default not in SUPPORTED_ACTIONS:
        raise ValueError(f"Invalid action_default: {action_default}")

    for sheet_row, row in enumerate(df_wide.to_dict("records"), start=data_start_row):
        entity_type = _infer_entity_type(row)
        action = _norm_str(row.get(ACTION_HEADER)).upper() or action_default

        product_id = _first_nonempty(row, PRODUCT_ID_HEADERS)
        variant_id = _first_nonempty(row, VARIANT_ID_HEADERS)
        variant_url = _norm_str(row.get(variant_image_column)) if variant_image_column else ""
        product_urls_raw = [_norm_str(row.get(col)) for _, col in product_columns]
        product_urls = [url for url in product_urls_raw if url]

        if action not in SUPPORTED_ACTIONS:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "error_reason": "invalid_action",
                    "message": f"action={action}; supported={sorted(SUPPORTED_ACTIONS)}",
                }
            )
            continue

        if action == "SKIP":
            continue

        if entity_type not in SUPPORTED_ENTITY_TYPES:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "error_reason": "invalid_or_ambiguous_entity_type",
                    "message": (
                        "entity_type must be PRODUCT or VARIANT. If blank, exactly one of "
                        "Product ID / Variant ID must be filled."
                    ),
                }
            )
            continue

        if entity_type == "PRODUCT":
            if not product_id:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "error_reason": "missing_product_id",
                        "message": "PRODUCT row requires Product ID (numeric), Product GID, or Product Handle.",
                    }
                )
                continue

            if variant_id or variant_url:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "gid_or_handle": product_id,
                        "error_reason": "product_row_contains_variant_values",
                        "message": "PRODUCT row must leave Variant ID and Variant Image URL blank.",
                    }
                )
                continue

            if action == "SET" and not product_urls:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "gid_or_handle": product_id,
                        "field_key": PRODUCT_FIELD_KEY,
                        "error_reason": "missing_product_image_urls",
                        "message": "PRODUCT SET requires at least one Product Image URL-N value.",
                    }
                )
                continue

            invalid_urls = [url for url in product_urls if strict_url_validation and not _is_http_url(url)]
            if invalid_urls:
                errors.append(
                    {
                        "sheet_row": sheet_row,
                        "entity_type": entity_type,
                        "gid_or_handle": product_id,
                        "field_key": PRODUCT_FIELD_KEY,
                        "error_reason": "invalid_product_image_url",
                        "message": f"Non-HTTP(S) Product Image URL values: {invalid_urls[:5]}",
                    }
                )
                continue

            if dedupe_product_urls:
                deduped = _dedupe_keep_order(product_urls)
                if len(deduped) != len(product_urls):
                    warnings.append(
                        {
                            "sheet_row": sheet_row,
                            "entity_type": entity_type,
                            "gid_or_handle": product_id,
                            "warning_type": "duplicate_product_image_url_removed",
                            "message": f"Removed {len(product_urls) - len(deduped)} duplicate Product Image URL value(s).",
                        }
                    )
                product_urls = deduped

            desired_value = "[]" if action == "CLEAR" else _json_dumps(product_urls)
            out.append(
                {
                    "entity_type": "PRODUCT",
                    "gid_or_handle": product_id,
                    "field_key": PRODUCT_FIELD_KEY,
                    "desired_value": desired_value,
                    "action": action,
                }
            )
            continue

        # VARIANT
        if not variant_id:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "error_reason": "missing_variant_id",
                    "message": "VARIANT row requires Variant ID (numeric), Variant GID, or SKU.",
                }
            )
            continue

        if product_id or product_urls:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": variant_id,
                    "error_reason": "variant_row_contains_product_values",
                    "message": "VARIANT row must leave Product ID and Product Image URL-N columns blank.",
                }
            )
            continue

        if action == "SET" and not variant_url:
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": variant_id,
                    "field_key": VARIANT_FIELD_KEY,
                    "error_reason": "missing_variant_image_url",
                    "message": "VARIANT SET requires Variant Image URL.",
                }
            )
            continue

        if strict_url_validation and variant_url and not _is_http_url(variant_url):
            errors.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": variant_id,
                    "field_key": VARIANT_FIELD_KEY,
                    "error_reason": "invalid_variant_image_url",
                    "message": f"Variant Image URL is not HTTP(S): {variant_url}",
                }
            )
            continue

        out.append(
            {
                "entity_type": "VARIANT",
                "gid_or_handle": variant_id,
                "field_key": VARIANT_FIELD_KEY,
                "desired_value": "" if action == "CLEAR" else variant_url,
                "action": action,
            }
        )

    return out, errors, warnings


# =========================================================
# Output
# =========================================================

def write_long_output(
    ws_output,
    long_rows: list[dict[str, Any]],
    clear_output_first: bool = True,
) -> dict[str, int]:
    df = pd.DataFrame(long_rows)
    if df.empty:
        df = pd.DataFrame(columns=LONG_HEADER)

    for col in LONG_HEADER:
        if col not in df.columns:
            df[col] = ""

    values = [LONG_HEADER] + df[LONG_HEADER].fillna("").astype(str).values.tolist()

    required_rows = max(1, len(values))
    required_cols = len(LONG_HEADER)
    if ws_output.row_count < required_rows or ws_output.col_count < required_cols:
        ws_output.resize(
            rows=max(ws_output.row_count, required_rows),
            cols=max(ws_output.col_count, required_cols),
        )

    if clear_output_first:
        ws_output.clear()

    ws_output.update(
        range_name=f"A1:E{required_rows}",
        values=values,
        value_input_option="RAW",
    )

    return {"rows_written": int(len(df))}


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    console_gsheet_sa_b64_secret: str,

    input_sheet_label: str = "pre_edit",
    output_sheet_label: str = "pre_edit",

    input_worksheet_title: str = WIDE_INPUT_TAB_DEFAULT,
    output_worksheet_title: str = LONG_OUTPUT_TAB_DEFAULT,

    wide_header_row: int = 1,
    wide_field_key_row: int = 2,
    wide_data_start_row: int = 3,
    write_wide_field_key_row: bool = True,

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,

    action_default: str = "SET",
    preview_only: bool = True,
    create_output_tab_if_missing: bool = True,
    clear_output_first: bool = True,

    dedupe_product_urls: bool = True,
    strict_url_validation: bool = True,
    preview_limit: int = 50,
) -> dict[str, Any]:
    """
    Convert human-friendly Wide_Media into Long_Media.

    Wide_Media standard:
      Row 1: entity_type, Product ID (numeric), Variant ID (numeric),
             Product Image URL-1 ... Product Image URL-N,
             Variant Image URL, action
      Row 2: generated field_key mapping row
      Row 3+: data

    Conversion:
      PRODUCT -> one Long_Media row with core.product.images_urls and an ordered JSON array.
      VARIANT -> one Long_Media row with core.variant.image_url and a scalar URL.

    Product Image URL-N suffix controls final image order. No separate sort field is needed.
    """
    run_id = _utc_run_id(JOB_NAME)

    gc_console = build_gsheet_client(console_gsheet_sa_b64_secret)
    account_cfg = load_account_config(
        gc_console=gc_console,
        console_core_url=console_core_url,
        cfg_account_tab=cfg_account_tab,
    )

    site_gsheet_secret = (
        account_cfg.get("GSHEET_SA_B64_SECRET")
        or account_cfg.get("gsheet_sa_b64_secret")
        or console_gsheet_sa_b64_secret
    )
    if not site_gsheet_secret:
        raise ValueError(
            "Missing GSHEET_SA_B64_SECRET in Cfg__account_id and no console secret fallback."
        )

    gc_site = build_gsheet_client(site_gsheet_secret)

    _, ws_wide, input_sheet_url = open_ws_by_label_and_title(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
        create_if_missing=False,
    )

    _, ws_output, output_sheet_url = open_ws_by_label_and_title(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        label=output_sheet_label,
        worksheet_title=output_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
        create_if_missing=create_output_tab_if_missing,
        default_rows=1000,
        default_cols=len(LONG_HEADER),
    )

    df_wide, wide_headers, existing_mapping = load_wide_sheet(
        ws_wide,
        header_row=wide_header_row,
        field_key_row=wide_field_key_row,
        data_start_row=wide_data_start_row,
    )

    parsed = parse_media_columns(wide_headers)
    parse_errors = parsed["errors"]
    warnings = list(parsed["warnings"])

    mapping_values = build_field_key_mapping_row(wide_headers)
    mapping_write_result = {"mapping_row_written": 0, "mapping_cols_written": 0}
    if write_wide_field_key_row:
        mapping_write_result = write_field_key_mapping_row(
            ws_wide,
            mapping_values=mapping_values,
            field_key_row=wide_field_key_row,
        )

    base_meta = {
        "input_sheet_label": input_sheet_label,
        "output_sheet_label": output_sheet_label,
        "input_sheet_url": input_sheet_url,
        "output_sheet_url": output_sheet_url,
        "input_worksheet_title": input_worksheet_title,
        "output_worksheet_title": output_worksheet_title,
        "site_gsheet_secret": site_gsheet_secret,
        "wide_header_row": wide_header_row,
        "wide_field_key_row": wide_field_key_row,
        "wide_data_start_row": wide_data_start_row,
        "generated_at_cn": _now_cn_str(),
        "product_field_key": PRODUCT_FIELD_KEY,
        "variant_field_key": VARIANT_FIELD_KEY,
    }

    if parse_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "product_image_columns": int(len(parsed["product_columns"])),
                "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
                "parse_errors": int(len(parse_errors)),
                "warnings": int(len(warnings)),
                "rows_generated": 0,
                "written": 0,
                **mapping_write_result,
            },
            "errors": parse_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": [],
            "meta": base_meta,
        }

    long_rows, build_errors, build_warnings = build_long_rows(
        df_wide=df_wide,
        product_columns=parsed["product_columns"],
        variant_image_column=parsed["variant_image_column"],
        data_start_row=wide_data_start_row,
        action_default=action_default,
        dedupe_product_urls=dedupe_product_urls,
        strict_url_validation=strict_url_validation,
    )
    warnings.extend(build_warnings)

    if build_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "product_image_columns": int(len(parsed["product_columns"])),
                "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
                "build_errors": int(len(build_errors)),
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "errors": build_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "meta": base_meta,
        }

    if preview_only:
        return {
            "status": "preview",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "product_image_columns": int(len(parsed["product_columns"])),
                "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "errors": [],
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "parsed_columns": {
                "product": [
                    {"sequence": seq, "source_col": col, "field_key": PRODUCT_FIELD_KEY}
                    for seq, col in parsed["product_columns"]
                ],
                "variant": (
                    {
                        "source_col": parsed["variant_image_column"],
                        "field_key": VARIANT_FIELD_KEY,
                    }
                    if parsed["variant_image_column"]
                    else None
                ),
            },
            "meta": base_meta,
        }

    write_result = write_long_output(
        ws_output=ws_output,
        long_rows=long_rows,
        clear_output_first=clear_output_first,
    )

    return {
        "status": "written",
        "job_name": JOB_NAME,
        "run_id": run_id,
        "site_code": site_code,
        "summary": {
            "rows_loaded": int(len(df_wide)),
            "product_image_columns": int(len(parsed["product_columns"])),
            "variant_image_columns": 1 if parsed["variant_image_column"] else 0,
            "warnings": int(len(warnings)),
            "rows_generated": int(len(long_rows)),
            **write_result,
            **mapping_write_result,
        },
        "errors": [],
        "warnings": warnings[:preview_limit],
        "preview": long_rows[:preview_limit],
        "parsed_columns": {
            "product": [
                {"sequence": seq, "source_col": col, "field_key": PRODUCT_FIELD_KEY}
                for seq, col in parsed["product_columns"]
            ],
            "variant": (
                {
                    "source_col": parsed["variant_image_column"],
                    "field_key": VARIANT_FIELD_KEY,
                }
                if parsed["variant_image_column"]
                else None
            ),
        },
        "meta": base_meta,
    }
