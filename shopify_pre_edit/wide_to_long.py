# shopify_pre_edit/wide_to_metafield_edits.py

from __future__ import annotations

import base64
import datetime as dt
import json
import re
from collections import defaultdict
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

JOB_NAME = "wide_to_metafield_edits"

WIDE_INPUT_TAB_DEFAULT = "Wide"
LONG_OUTPUT_TAB_DEFAULT = "Long"

# Matches the Long layout shown in the user's sheet.
LONG_HEADER = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "desired_value",
    "note",
    "error_reason",
]

SUPPORTED_ENTITY_TYPES = {
    "PRODUCT",
    "VARIANT",
    "COLLECTION",
    "PAGE",
    "CUSTOMER",
}

# Entity type is inferred directly from the populated ID column.
# Numeric IDs are kept numeric in Long; the downstream edit script converts
# them to Shopify GIDs.
ENTITY_ID_HEADERS: dict[str, tuple[str, ...]] = {
    "PRODUCT": (
        "Product ID (numeric)",
        "Product ID",
        "Product GID",
        "Product Handle",
    ),
    "VARIANT": (
        "Variant ID (numeric)",
        "Variant ID",
        "Variant GID",
        "SKU",
    ),
    "COLLECTION": (
        "Collection ID (numeric)",
        "Collection ID",
        "Collection GID",
        "Collection Handle",
    ),
    "PAGE": (
        "Page ID (numeric)",
        "Page ID",
        "Page GID",
        "Page Handle",
    ),
    "CUSTOMER": (
        "Customer ID (numeric)",
        "Customer ID",
        "Customer GID",
    ),
}

ENTITY_TYPE_HEADERS = ("entity_type", "Entity Type")
NOTE_HEADERS = ("note", "Note")

# Columns that are controls/identifiers rather than editable fields.
CONTROL_HEADER_NAMES = {
    "entity_type",
    "entity type",
    "note",
    "action",
    "mode",
    "run_id",
    "error_reason",
}

FIELD_KEY_RE = re.compile(r"^(?:v_)?mf\.[^.]+\..+$", re.I)


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


def _norm_header(value: Any) -> str:
    return re.sub(r"\s+", " ", _norm_str(value)).strip().lower()


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


def _all_entity_id_header_names() -> set[str]:
    out: set[str] = set()
    for headers in ENTITY_ID_HEADERS.values():
        out.update(_norm_header(x) for x in headers)
    return out


ENTITY_ID_HEADER_NAMES_NORMALIZED = _all_entity_id_header_names()


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
    Row 2: field_key mapping, e.g. mf.custom.ab_test_group.
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
        raise ValueError(f"Wide sheet has duplicate headers: {duplicate_headers}")

    mappings: list[str] = []
    if len(values) > mapping_idx:
        mappings = [_norm_str(x) for x in values[mapping_idx][: len(headers)]]
    mappings += [""] * max(0, len(headers) - len(mappings))

    data_rows = values[data_idx:] if len(values) > data_idx else []
    normalized_rows: list[list[str]] = []
    for raw in data_rows:
        row = list(raw[: len(headers)]) + [""] * max(0, len(headers) - len(raw))
        normalized_rows.append([_norm_str(x) for x in row])

    df = pd.DataFrame(normalized_rows, columns=headers)
    if df.empty:
        return df, headers, mappings

    not_blank = df.apply(lambda r: any(_norm_str(x) for x in r.tolist()), axis=1)
    df = df[not_blank].copy()

    for col in df.columns:
        df[col] = df[col].apply(_norm_str)

    return df, headers, mappings


def parse_mapped_columns(
    headers: list[str],
    mappings: list[str],
    *,
    require_metafield_prefix: bool = True,
) -> dict[str, Any]:
    mapped_columns: list[dict[str, str]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for index, header in enumerate(headers):
        field_key = _norm_str(mappings[index] if index < len(mappings) else "")
        header_norm = _norm_header(header)

        is_id = header_norm in ENTITY_ID_HEADER_NAMES_NORMALIZED
        is_control = header_norm in CONTROL_HEADER_NAMES

        if is_id or is_control:
            if field_key:
                warnings.append(
                    {
                        "source_col": header,
                        "warning_type": "mapping_ignored_for_control_column",
                        "message": f"field_key={field_key} is ignored for an ID/control column.",
                    }
                )
            continue

        if not field_key:
            # A human-facing column without a row-2 mapping is intentionally ignored.
            continue

        if require_metafield_prefix and not FIELD_KEY_RE.match(field_key):
            errors.append(
                {
                    "source_col": header,
                    "field_key": field_key,
                    "error_reason": "invalid_field_key",
                    "message": "field_key must look like mf.namespace.key or v_mf.namespace.key.",
                }
            )
            continue

        mapped_columns.append(
            {
                "source_col": header,
                "field_key": field_key,
            }
        )

    if not mapped_columns:
        warnings.append(
            {
                "warning_type": "no_mapped_field_columns",
                "message": "No editable columns with a field_key in the mapping row were found.",
            }
        )

    duplicate_map: dict[str, list[str]] = defaultdict(list)
    for item in mapped_columns:
        duplicate_map[item["field_key"]].append(item["source_col"])
    for field_key, source_cols in duplicate_map.items():
        if len(source_cols) > 1:
            warnings.append(
                {
                    "field_key": field_key,
                    "source_cols": source_cols,
                    "warning_type": "duplicate_field_key_mapping",
                    "message": (
                        "The same field_key appears under multiple Wide columns. "
                        "Only one non-empty value per input row is allowed for this field_key."
                    ),
                }
            )

    return {
        "mapped_columns": mapped_columns,
        "errors": errors,
        "warnings": warnings,
    }


# =========================================================
# Entity detection and Long row builder
# =========================================================

def _explicit_entity_type(row: dict[str, Any]) -> str:
    return _first_nonempty(row, ENTITY_TYPE_HEADERS).upper()


def _note_value(row: dict[str, Any]) -> str:
    return _first_nonempty(row, NOTE_HEADERS)


def _find_entity_candidates(row: dict[str, Any]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for entity_type, headers in ENTITY_ID_HEADERS.items():
        for header in headers:
            value = _norm_str(row.get(header))
            if value:
                candidates.append(
                    {
                        "entity_type": entity_type,
                        "gid_or_handle": value,
                        "source_col": header,
                    }
                )
                break
    return candidates


def _resolve_entity(row: dict[str, Any]) -> tuple[str, str, str]:
    """Return (entity_type, gid_or_handle, error_reason)."""
    explicit = _explicit_entity_type(row)
    candidates = _find_entity_candidates(row)

    if explicit:
        if explicit not in SUPPORTED_ENTITY_TYPES:
            return explicit, "", "unsupported_entity_type"

        matching = [x for x in candidates if x["entity_type"] == explicit]
        other = [x for x in candidates if x["entity_type"] != explicit]

        if not matching:
            return explicit, "", f"missing_{explicit.lower()}_id"
        if len(matching) > 1 or other:
            return explicit, matching[0]["gid_or_handle"], "multiple_entity_ids"
        return explicit, matching[0]["gid_or_handle"], ""

    if not candidates:
        return "", "", "missing_entity_id"
    if len(candidates) > 1:
        joined = " | ".join(
            f"{x['entity_type']}:{x['gid_or_handle']}" for x in candidates
        )
        return "", joined, "multiple_entity_ids"

    return candidates[0]["entity_type"], candidates[0]["gid_or_handle"], ""


def _field_prefix_entity_error(entity_type: str, field_key: str) -> str:
    fk = _norm_str(field_key).lower()
    if entity_type == "VARIANT" and fk.startswith("mf."):
        return "field_key_entity_mismatch"
    if entity_type != "VARIANT" and fk.startswith("v_mf."):
        return "field_key_entity_mismatch"
    return ""


def build_long_rows(
    df_wide: pd.DataFrame,
    mapped_columns: list[dict[str, str]],
    *,
    data_start_row: int = 3,
    include_error_rows: bool = True,
    validate_field_key_entity: bool = True,
) -> tuple[list[dict[str, str]], list[dict[str, Any]], list[dict[str, Any]]]:
    out: list[dict[str, str]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    if df_wide.empty:
        return out, errors, warnings

    # Group duplicate row-2 mappings so we can prevent duplicate writes.
    by_field_key: dict[str, list[str]] = defaultdict(list)
    for item in mapped_columns:
        by_field_key[item["field_key"]].append(item["source_col"])

    for sheet_row, row in enumerate(df_wide.to_dict("records"), start=data_start_row):
        entity_type, gid_or_handle, entity_error = _resolve_entity(row)
        note = _note_value(row)

        if entity_error:
            error = {
                "sheet_row": sheet_row,
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": "",
                "error_reason": entity_error,
                "message": (
                    "Exactly one supported Entity ID column must be populated: "
                    "Product, Variant, Collection, Page, or Customer."
                ),
            }
            errors.append(error)
            if include_error_rows:
                out.append(
                    {
                        "entity_type": entity_type,
                        "gid_or_handle": gid_or_handle,
                        "field_key": "",
                        "desired_value": "",
                        "note": note,
                        "error_reason": entity_error,
                    }
                )
            continue

        generated_for_row = 0

        for field_key, source_cols in by_field_key.items():
            populated = [
                (source_col, _norm_str(row.get(source_col)))
                for source_col in source_cols
                if _norm_str(row.get(source_col)) != ""
            ]

            if not populated:
                continue

            if len(populated) > 1:
                error_reason = "multiple_values_for_same_field_key"
                error = {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": field_key,
                    "error_reason": error_reason,
                    "message": (
                        f"field_key={field_key} has multiple populated source columns: "
                        f"{[x[0] for x in populated]}"
                    ),
                }
                errors.append(error)
                if include_error_rows:
                    out.append(
                        {
                            "entity_type": entity_type,
                            "gid_or_handle": gid_or_handle,
                            "field_key": field_key,
                            "desired_value": " | ".join(x[1] for x in populated),
                            "note": note,
                            "error_reason": error_reason,
                        }
                    )
                continue

            source_col, desired_value = populated[0]
            mismatch = (
                _field_prefix_entity_error(entity_type, field_key)
                if validate_field_key_entity
                else ""
            )

            if mismatch:
                error = {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": field_key,
                    "source_col": source_col,
                    "error_reason": mismatch,
                    "message": (
                        "VARIANT fields must use v_mf.; PRODUCT/COLLECTION/PAGE/CUSTOMER "
                        "fields must use mf."
                    ),
                }
                errors.append(error)
                if include_error_rows:
                    out.append(
                        {
                            "entity_type": entity_type,
                            "gid_or_handle": gid_or_handle,
                            "field_key": field_key,
                            "desired_value": desired_value,
                            "note": note,
                            "error_reason": mismatch,
                        }
                    )
                continue

            out.append(
                {
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": field_key,
                    "desired_value": desired_value,
                    "note": note,
                    "error_reason": "",
                }
            )
            generated_for_row += 1

        if generated_for_row == 0:
            warnings.append(
                {
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "warning_type": "no_mapped_values",
                    "message": "The row has an Entity ID but no populated mapped field values.",
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

    last_col = _col_to_a1(required_cols)
    ws_output.update(
        range_name=f"A1:{last_col}{required_rows}",
        values=values,
        value_input_option="RAW",
    )

    valid_count = int(sum(not _norm_str(x.get("error_reason")) for x in long_rows))
    error_count = int(len(long_rows) - valid_count)
    return {
        "rows_written": int(len(df)),
        "valid_rows_written": valid_count,
        "error_rows_written": error_count,
    }


# =========================================================
# Main entry
# =========================================================

def _run_impl(
    *,
    site_code: str,
    console_core_url: str,
    console_gsheet_sa_b64_secret: str,

    job_name: str = JOB_NAME,
    run_id: Optional[str] = None,

    input_sheet_label: str = "pre_edit",
    output_sheet_label: str = "pre_edit",

    input_worksheet_title: str = WIDE_INPUT_TAB_DEFAULT,
    output_worksheet_title: str = LONG_OUTPUT_TAB_DEFAULT,

    wide_header_row: int = 1,
    wide_field_key_row: int = 2,
    wide_data_start_row: int = 3,

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,

    preview_only: bool = True,
    create_output_tab_if_missing: bool = True,
    clear_output_first: bool = True,

    include_error_rows: bool = True,
    require_metafield_prefix: bool = True,
    validate_field_key_entity: bool = True,
    preview_limit: int = 50,

    # Backward-compatible arguments retained so an older Colab runner that
    # called wide_to_media_edits.run(...) does not fail with unexpected kwargs.
    write_wide_field_key_row: bool = False,
    action_default: str = "SET",
    dedupe_product_urls: bool = True,
    strict_url_validation: bool = True,
) -> dict[str, Any]:
    """
    Convert a generic Wide metafield sheet into Long rows.

    Wide standard:
      Row 1: human-friendly headers.
      Row 2: field_key under each editable column, e.g. mf.custom.ab_test_group.
      Row 3+: data.

    Entity inference:
      Customer ID (numeric)   -> CUSTOMER
      Product ID (numeric)    -> PRODUCT
      Variant ID (numeric)    -> VARIANT
      Collection ID (numeric) -> COLLECTION
      Page ID (numeric)       -> PAGE

    One Wide row may produce multiple Long rows, one for each populated mapped
    metafield column. Exactly one Entity ID type may be populated per Wide row.
    """
    del write_wide_field_key_row, action_default, dedupe_product_urls, strict_url_validation

    job_name = _norm_str(job_name) or JOB_NAME
    run_id = _norm_str(run_id) or _utc_run_id(job_name)
    print(
        f"=== {job_name} start === site_code={site_code} run_id={run_id} "
        f"preview_only={preview_only}",
        flush=True,
    )

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

    df_wide, wide_headers, mappings = load_wide_sheet(
        ws_wide,
        header_row=wide_header_row,
        field_key_row=wide_field_key_row,
        data_start_row=wide_data_start_row,
    )

    parsed = parse_mapped_columns(
        headers=wide_headers,
        mappings=mappings,
        require_metafield_prefix=require_metafield_prefix,
    )
    parse_errors = parsed["errors"]
    warnings = list(parsed["warnings"])

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
        "supported_entity_types": sorted(SUPPORTED_ENTITY_TYPES),
    }

    if parse_errors:
        print(
            f"=== parse failed === rows_loaded={len(df_wide)} "
            f"parse_errors={len(parse_errors)} warnings={len(warnings)}",
            flush=True,
        )
        return {
            "status": "error",
            "job_name": job_name,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "mapped_columns": int(len(parsed["mapped_columns"])),
                "parse_errors": int(len(parse_errors)),
                "warnings": int(len(warnings)),
                "rows_generated": 0,
                "written": 0,
            },
            "errors": parse_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": [],
            "meta": base_meta,
        }

    long_rows, build_errors, build_warnings = build_long_rows(
        df_wide=df_wide,
        mapped_columns=parsed["mapped_columns"],
        data_start_row=wide_data_start_row,
        include_error_rows=include_error_rows,
        validate_field_key_entity=validate_field_key_entity,
    )
    warnings.extend(build_warnings)

    valid_rows = [x for x in long_rows if not _norm_str(x.get("error_reason"))]
    error_rows = [x for x in long_rows if _norm_str(x.get("error_reason"))]

    summary = {
        "rows_loaded": int(len(df_wide)),
        "mapped_columns": int(len(parsed["mapped_columns"])),
        "build_errors": int(len(build_errors)),
        "warnings": int(len(warnings)),
        "rows_generated": int(len(long_rows)),
        "valid_rows_generated": int(len(valid_rows)),
        "error_rows_generated": int(len(error_rows)),
        "written": 0,
    }

    print(
        f"=== build done === rows_loaded={summary['rows_loaded']} "
        f"mapped_columns={summary['mapped_columns']} "
        f"rows_generated={summary['rows_generated']} "
        f"valid={summary['valid_rows_generated']} errors={summary['build_errors']} "
        f"warnings={summary['warnings']}",
        flush=True,
    )

    if preview_only:
        return {
            "status": "preview" if not build_errors else "preview_with_errors",
            "job_name": job_name,
            "run_id": run_id,
            "site_code": site_code,
            "summary": summary,
            "errors": build_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "parsed_columns": parsed["mapped_columns"],
            "meta": base_meta,
        }

    write_result = write_long_output(
        ws_output=ws_output,
        long_rows=long_rows,
        clear_output_first=clear_output_first,
    )
    summary.update(write_result)
    summary["written"] = write_result["rows_written"]

    print(
        f"=== write done === output={output_worksheet_title} "
        f"rows_written={write_result['rows_written']} "
        f"valid_rows_written={write_result['valid_rows_written']} "
        f"error_rows_written={write_result['error_rows_written']}",
        flush=True,
    )

    return {
        "status": "written" if not build_errors else "written_with_errors",
        "job_name": job_name,
        "run_id": run_id,
        "site_code": site_code,
        "summary": summary,
        "errors": build_errors[:preview_limit],
        "warnings": warnings[:preview_limit],
        "preview": long_rows[:preview_limit],
        "parsed_columns": parsed["mapped_columns"],
        "meta": base_meta,
    }

# =========================================================
# Colab-compatible public entry
# =========================================================

def run(*args, **kwargs) -> dict[str, Any]:
    """
    Public runner compatible with both call styles:

      run(site_code=SITE_CODE, ...)
      run(SITE_CODE=SITE_CODE, JOB_NAME=JOB_NAME, ...)

    The Colab does not need to be changed. Uppercase notebook parameter names
    are normalized to the lowercase implementation parameters here.
    """
    if args:
        raise TypeError(
            "run() accepts keyword arguments only. "
            "Example: run(SITE_CODE=SITE_CODE, CONSOLE_CORE_URL=CONSOLE_CORE_URL, ...)"
        )

    # Aliases whose notebook name is not simply the uppercase form of the
    # implementation parameter.
    alias_map = {
        "CONSOLE_GSHEET_SA_B64": "console_gsheet_sa_b64_secret",
        "BOOTSTRAP_GSHEET_SA_B64_SECRET": "console_gsheet_sa_b64_secret",
        "GSHEET_SA_B64_SECRET": "console_gsheet_sa_b64_secret",

        "INPUT_TAB": "input_worksheet_title",
        "TAB_WIDE": "input_worksheet_title",
        "WIDE_TAB": "input_worksheet_title",
        "OUTPUT_TAB": "output_worksheet_title",
        "TAB_LONG": "output_worksheet_title",
        "LONG_TAB": "output_worksheet_title",

        "INPUT_LABEL": "input_sheet_label",
        "OUTPUT_LABEL": "output_sheet_label",

        "HEADER_ROW": "wide_header_row",
        "FIELD_KEY_ROW": "wide_field_key_row",
        "DATA_START_ROW": "wide_data_start_row",
    }

    # One shared pre-edit label may be used by older runners for both tabs.
    shared_label_keys = {
        "SHEET_LABEL",
        "PRE_EDIT_SHEET_LABEL",
        "LABEL_PRE_EDIT",
    }

    normalized: dict[str, Any] = {}
    source_for_target: dict[str, str] = {}

    def put(target: str, value: Any, source: str) -> None:
        if target in normalized and normalized[target] != value:
            raise TypeError(
                f"run() received conflicting values for '{target}' "
                f"from '{source_for_target[target]}' and '{source}'."
            )
        normalized[target] = value
        source_for_target[target] = source

    for raw_key, value in kwargs.items():
        key = str(raw_key)

        if key in shared_label_keys:
            put("input_sheet_label", value, key)
            put("output_sheet_label", value, key)
            continue

        target = alias_map.get(key)
        if target is None:
            # SITE_CODE -> site_code, JOB_NAME -> job_name, etc.
            target = key.lower() if key.isupper() else key

        put(target, value, key)

    import inspect

    accepted = set(inspect.signature(_run_impl).parameters)
    unknown = sorted(k for k in normalized if k not in accepted)
    if unknown:
        original_names = [source_for_target.get(k, k) for k in unknown]
        raise TypeError(
            "run() received unsupported parameter(s): "
            + ", ".join(original_names)
            + ". The Python module and Colab config names may be from different versions."
        )

    return _run_impl(**normalized)

