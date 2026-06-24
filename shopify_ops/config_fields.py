"""Apply the complete Media + Files field configuration to Cfg__Fields.

This module is intended to be imported and called from a Google Colab runner.
It updates only the existing Cfg__Fields worksheet, preserves unrelated rows and
extra columns, and never creates a new worksheet.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from dataclasses import dataclass
from typing import Any

import gspread
import pandas as pd
from google.oauth2 import service_account

try:
    from google.colab import userdata
except Exception:  # pragma: no cover
    userdata = None


# =========================================================
# Constants
# =========================================================

JOB_NAME = "apply_cfg_media_files_full_v2"
SCRIPT_VERSION = "2026-06-24-v2"
CFG_SITES_TAB_DEFAULT = "Cfg__Sites"
CFG_FIELDS_TAB_DEFAULT = "Cfg__Fields"
CONFIG_SHEET_LABEL_DEFAULT = "config"

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


# These rows cover the agreed requirements:
# 1) Product image URLs and order.
# 2) Variant image URL assignment.
# 3) Export all Shopify Files.
# 4) Filter Files by updatedAt in the future Files export job.
#
# Product Image URL-1 / -2 / -3 are Wide_Media column names. They are not
# separate Cfg__Fields rows. wide_to_media_edits converts them into the single
# ordered field core.product.images_urls.
REQUIRED_FIELD_ROWS: list[dict[str, str]] = [
    {
        "field_handle": "PRODUCT|Product Image",
        "field_id": "PRODUCT|core.product.image_url",
        "display_name": "Product Image",
        "entity_type": "PRODUCT",
        "field_key": "core.product.image_url",
        "expr": "product.media.nodes[0].preview.image.url",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "Media",
        "notes": "First product image URL. Read value; the first item of core.product.images_urls becomes the leading image after media ordering.",
    },
    {
        "field_handle": "PRODUCT|Product Images URLs",
        "field_id": "PRODUCT|core.product.images_urls",
        "display_name": "Product Images URLs",
        "entity_type": "PRODUCT",
        "field_key": "core.product.images_urls",
        "expr": "product.media.nodes[].preview.image.url",
        "field_type": "RAW",
        "data_type": "list.string",
        "source_type": "CORE",
        "group": "Media",
        "notes": "Writable ordered product image URL list. Wide_Media columns Product Image URL-1/-2/-3 define the final image order; no separate position field is required.",
    },
    {
        "field_handle": "PRODUCT|Product Images JSON",
        "field_id": "PRODUCT|core.product.images_json",
        "display_name": "Product Images JSON",
        "entity_type": "PRODUCT",
        "field_key": "core.product.images_json",
        "expr": "product.media.nodes[].preview.image.url",
        "field_type": "RAW",
        "data_type": "list.json",
        "source_type": "CORE",
        "group": "Media",
        "notes": "Read/export compatibility field. wide_to_media_edits generates the ordered JSON value automatically; humans do not fill JSON manually.",
    },
    {
        "field_handle": "VARIANT|Variant Image",
        "field_id": "VARIANT|core.variant.image_url",
        "display_name": "Variant Image",
        "entity_type": "VARIANT",
        "field_key": "core.variant.image_url",
        "expr": "variant.media.nodes[0].preview.image.url",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "Media",
        "notes": "Writable variant image URL. The media edit job resolves or attaches the product media and assigns it to the target Variant ID.",
    },
    {
        "field_handle": "VARIANT|(raw) Variant image url",
        "field_id": "VARIANT|raw.variant.image_url",
        "display_name": "(raw) Variant image url",
        "entity_type": "VARIANT",
        "field_key": "raw.variant.image_url",
        "expr": "variant.media.nodes[0].preview.image.url",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "Media",
        "notes": "Raw export field for the current variant image URL.",
    },
    {
        "field_handle": "VARIANT|(raw) Variant media(0) preview url",
        "field_id": "VARIANT|raw.variant.media0_preview_url",
        "display_name": "(raw) Variant media(0) preview url",
        "entity_type": "VARIANT",
        "field_key": "raw.variant.media0_preview_url",
        "expr": "variant.media.nodes[0].preview.image.url",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "Media",
        "notes": "Raw compatibility field used when exporting the current first variant media preview URL.",
    },
    {
        "field_handle": "FILE|File GID",
        "field_id": "FILE|core.gid",
        "display_name": "File GID",
        "entity_type": "FILE",
        "field_key": "core.gid",
        "expr": "file.id",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Shopify File global ID.",
    },
    {
        "field_handle": "FILE|File Type",
        "field_id": "FILE|core.file_type",
        "display_name": "File Type",
        "entity_type": "FILE",
        "field_key": "core.file_type",
        "expr": "file.__typename",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Concrete Shopify File type, for example MediaImage or GenericFile.",
    },
    {
        "field_handle": "FILE|File URL",
        "field_id": "FILE|core.file_url",
        "display_name": "File URL",
        "entity_type": "FILE",
        "field_key": "core.file_url",
        "expr": "FILE_PRIMARY_URL(file)",
        "field_type": "CALC",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Stable exported URL selected by file type: MediaImage.image.url, GenericFile.url, or the applicable primary source URL for other supported file types.",
    },
    {
        "field_handle": "FILE|Preview Image URL",
        "field_id": "FILE|core.preview_image_url",
        "display_name": "Preview Image URL",
        "entity_type": "FILE",
        "field_key": "core.preview_image_url",
        "expr": "file.preview.image.url",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Preview image URL when Shopify provides one.",
    },
    {
        "field_handle": "FILE|Original Source URL",
        "field_id": "FILE|core.original_source_url",
        "display_name": "Original Source URL",
        "entity_type": "FILE",
        "field_key": "core.original_source_url",
        "expr": "FILE_ORIGINAL_SOURCE_URL(file)",
        "field_type": "CALC",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Type-specific original source URL when available. Some original-source URLs can be temporary; core.file_url is the main reusable export URL.",
    },
    {
        "field_handle": "FILE|Alt",
        "field_id": "FILE|core.alt",
        "display_name": "Alt",
        "entity_type": "FILE",
        "field_key": "core.alt",
        "expr": "file.alt",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "File alt text when available.",
    },
    {
        "field_handle": "FILE|Created At",
        "field_id": "FILE|core.created_at",
        "display_name": "Created At",
        "entity_type": "FILE",
        "field_key": "core.created_at",
        "expr": "file.createdAt",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "File creation timestamp in ISO 8601 format.",
    },
    {
        "field_handle": "FILE|Updated At",
        "field_id": "FILE|core.updated_at",
        "display_name": "Updated At",
        "entity_type": "FILE",
        "field_key": "core.updated_at",
        "expr": "file.updatedAt",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "File last-updated timestamp. The Files export Colab uses updated_at from/to filters against this Shopify value.",
    },
    {
        "field_handle": "FILE|File Status",
        "field_id": "FILE|core.file_status",
        "display_name": "File Status",
        "entity_type": "FILE",
        "field_key": "core.file_status",
        "expr": "file.fileStatus",
        "field_type": "RAW",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Shopify file processing status.",
    },
    {
        "field_handle": "FILE|MIME Type",
        "field_id": "FILE|core.mime_type",
        "display_name": "MIME Type",
        "entity_type": "FILE",
        "field_key": "core.mime_type",
        "expr": "FILE_MIME_TYPE(file)",
        "field_type": "CALC",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Type-specific MIME type when Shopify exposes it.",
    },
    {
        "field_handle": "FILE|File Size",
        "field_id": "FILE|core.file_size",
        "display_name": "File Size",
        "entity_type": "FILE",
        "field_key": "core.file_size",
        "expr": "FILE_SIZE(file)",
        "field_type": "CALC",
        "data_type": "number_integer",
        "source_type": "CORE",
        "unit": "bytes",
        "group": "File",
        "notes": "Type-specific original file size in bytes when Shopify exposes it.",
    },
    {
        "field_handle": "FILE|Filename",
        "field_id": "FILE|derived.filename",
        "display_name": "Filename",
        "entity_type": "FILE",
        "field_key": "derived.filename",
        "expr": "URL_BASENAME({FILE|core.file_url})",
        "field_type": "CALC",
        "data_type": "string",
        "source_type": "CORE",
        "group": "File",
        "notes": "Filename derived from the exported file URL.",
    },
]


# =========================================================
# Helpers
# =========================================================


def _norm_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo

        return dt.datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _get_secret(secret_name: str) -> str:
    if userdata is None:
        raise RuntimeError("google.colab.userdata is unavailable. Run this module from Google Colab.")
    value = userdata.get(secret_name)
    if not value:
        raise ValueError(f"Missing Colab Secret: {secret_name}")
    return str(value).strip()


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
    missing = [c for c in required if c not in df.columns]
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


def _complete_row(row: dict[str, Any]) -> dict[str, str]:
    return {column: _norm_str(row.get(column)) for column in CFG_FIELDS_COLUMNS}


def _ensure_cfg_columns(df: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in CFG_FIELDS_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Cfg__Fields missing required columns: {missing}")

    # Preserve any future extra columns after the required canonical columns.
    extra = [column for column in df.columns if column not in CFG_FIELDS_COLUMNS]
    return df[CFG_FIELDS_COLUMNS + extra].copy()


def build_upsert_plan(df_existing: pd.DataFrame) -> dict[str, Any]:
    df = _ensure_cfg_columns(df_existing).fillna("").astype(str)

    # One config row per entity_type + field_key.
    duplicate_mask = df.duplicated(["entity_type", "field_key"], keep=False)
    duplicates = df.loc[duplicate_mask, ["entity_type", "field_key", "field_id"]].to_dict("records")
    if duplicates:
        raise ValueError(
            {
                "message": "Cfg__Fields contains duplicate entity_type + field_key rows. Resolve these before media/file config upsert.",
                "examples": duplicates[:30],
            }
        )

    row_index_by_key = {
        (_norm_str(row["entity_type"]).upper(), _norm_str(row["field_key"])): idx
        for idx, row in df.iterrows()
        if _norm_str(row["entity_type"]) and _norm_str(row["field_key"])
    }

    inserted: list[dict[str, str]] = []
    updated: list[dict[str, Any]] = []
    unchanged: list[dict[str, str]] = []

    for raw_required in REQUIRED_FIELD_ROWS:
        required = _complete_row(raw_required)
        key = (required["entity_type"].upper(), required["field_key"])
        existing_idx = row_index_by_key.get(key)

        if existing_idx is None:
            new_row = {column: "" for column in df.columns}
            for column in CFG_FIELDS_COLUMNS:
                new_row[column] = required[column]
            df.loc[len(df)] = new_row
            row_index_by_key[key] = len(df) - 1
            inserted.append(required)
            continue

        changes: dict[str, dict[str, str]] = {}
        for column in CFG_FIELDS_COLUMNS:
            desired = required[column]

            # Do not erase unrelated existing metadata with an empty template value.
            if desired == "":
                continue

            before = _norm_str(df.at[existing_idx, column])
            if before != desired:
                df.at[existing_idx, column] = desired
                changes[column] = {"before": before, "after": desired}

        if changes:
            updated.append(
                {
                    "entity_type": key[0],
                    "field_key": key[1],
                    "changes": changes,
                }
            )
        else:
            unchanged.append(
                {
                    "entity_type": key[0],
                    "field_key": key[1],
                }
            )

    return {
        "dataframe": df,
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
    }


def _write_full_table(ws, df: pd.DataFrame) -> None:
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    required_rows = max(len(values), 2)
    required_cols = max(len(df.columns), 1)

    if ws.row_count < required_rows:
        ws.add_rows(required_rows - ws.row_count)
    if ws.col_count < required_cols:
        ws.add_cols(required_cols - ws.col_count)

    ws.clear()
    ws.update(range_name="A1", values=values, value_input_option="RAW")


# =========================================================
# Public entry point
# =========================================================


def run(
    *,
    site_code: str,
    gsheet_sa_b64_secret: str,
    console_core_url: str,
    config_sheet_label: str = CONFIG_SHEET_LABEL_DEFAULT,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_fields_tab: str = CFG_FIELDS_TAB_DEFAULT,
    dry_run: bool = True,
    confirmed: bool = False,
) -> dict[str, Any]:
    """Upsert Media and File rows into Cfg__Fields.

    This job changes only Cfg__Fields. It does not call Shopify and it does not
    create additional worksheets.
    """
    run_id = dt.datetime.utcnow().strftime("apply_cfg_media_files_%Y%m%d_%H%M%S")

    print("====================================")
    print(JOB_NAME)
    print("====================================")
    print("script_version:", SCRIPT_VERSION)
    print("run_id:", run_id)
    print("site_code:", site_code)
    print("dry_run:", dry_run)
    print("confirmed:", confirmed)

    gc = build_gsheet_client(gsheet_sa_b64_secret)
    config_sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=config_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )

    sh = gc.open_by_url(config_sheet_url)
    ws = sh.worksheet(cfg_fields_tab)

    records = ws.get_all_records()
    df_existing = pd.DataFrame(records)
    if df_existing.empty:
        # Preserve the expected schema even when a brand-new table only has headers.
        header = ws.row_values(1)
        if not header:
            raise ValueError(f"{cfg_fields_tab} is empty and has no header row")
        df_existing = pd.DataFrame(columns=header)

    plan = build_upsert_plan(df_existing)
    df_final: pd.DataFrame = plan["dataframe"]

    inserted = plan["inserted"]
    updated = plan["updated"]
    unchanged = plan["unchanged"]

    print("rows_loaded:", len(df_existing))
    print("required_config_rows:", len(REQUIRED_FIELD_ROWS))
    print("rows_inserted:", len(inserted))
    print("rows_updated:", len(updated))
    print("rows_unchanged:", len(unchanged))

    preview = []
    for row in inserted:
        preview.append(
            {
                "change_type": "INSERT",
                "entity_type": row["entity_type"],
                "field_key": row["field_key"],
                "display_name": row["display_name"],
                "data_type": row["data_type"],
            }
        )
    for row in updated:
        preview.append(
            {
                "change_type": "UPDATE",
                "entity_type": row["entity_type"],
                "field_key": row["field_key"],
                "changed_columns": ", ".join(row["changes"].keys()),
            }
        )

    if preview:
        print("\nPreview")
        print(pd.DataFrame(preview).to_string(index=False))
    else:
        print("\nPreview: no changes required")

    should_write = (not dry_run) and confirmed
    if should_write:
        _write_full_table(ws, df_final)
        status = "SUCCESS"
        print("\nCfg__Fields written successfully")
    else:
        status = "DRY_RUN"
        print("\nNo write performed. Set dry_run=False and confirmed=True to apply.")

    return {
        "status": status,
        "meta": {
            "job_name": JOB_NAME,
            "script_version": SCRIPT_VERSION,
            "run_id": run_id,
            "ts_cn": _now_cn_str(),
            "site_code": site_code,
            "config_sheet_url": config_sheet_url,
            "cfg_fields_tab": cfg_fields_tab,
        },
        "summary": {
            "rows_loaded": len(df_existing),
            "required_config_rows": len(REQUIRED_FIELD_ROWS),
            "rows_inserted": len(inserted),
            "rows_updated": len(updated),
            "rows_unchanged": len(unchanged),
            "rows_final": len(df_final),
            "written": bool(should_write),
        },
        "preview": preview,
    }
