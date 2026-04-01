# shopify_pre_edit/wide_to_long_product.py

from __future__ import annotations

import base64
import json
from typing import Any, Optional

import gspread
import pandas as pd
from google.oauth2 import service_account

try:
    from google.colab import userdata
except Exception:
    userdata = None


CFG_SITES_TAB_DEFAULT = "Cfg__Sites"


# =========================================================
# Helpers
# =========================================================

def _norm(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def _is_blank(x: Any) -> bool:
    return _norm(x) == ""


def _get_secret(secret_name: str) -> str:
    if userdata is None:
        raise RuntimeError("google.colab.userdata is unavailable. This module is intended for Colab runner use.")
    v = userdata.get(secret_name)
    if not v:
        raise ValueError(f"Missing Colab Secret: {secret_name}")
    return v


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


# =========================================================
# Sheet locating
# =========================================================

def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    sh = gc.open_by_url(console_core_url)
    ws = sh.worksheet(cfg_sites_tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"{cfg_sites_tab} is empty")

    for c in ["site_code", "label", "sheet_url"]:
        if c not in df.columns:
            raise ValueError(f"{cfg_sites_tab} missing required column: {c}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    m = df[(df["site_code"] == site_code.strip().upper()) & (df["label"] == label.strip())].copy()
    m = m[m["sheet_url"] != ""]

    if m.empty:
        raise ValueError(f"Cannot find sheet_url for site_code={site_code}, label={label}")

    return m.iloc[0]["sheet_url"]


def open_ws_by_label_and_title(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    worksheet_title: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
):
    sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=label,
        cfg_sites_tab=cfg_sites_tab,
    )
    sh = gc.open_by_url(sheet_url)
    ws = sh.worksheet(worksheet_title)
    return sh, ws, sheet_url


# =========================================================
# Load cfg fields
# =========================================================

def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = ws_cfg_fields.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    for c in ["field_key", "entity_type", "display_name", "name", "字段名"]:
        if c not in df.columns:
            df[c] = ""

    for c in ["field_key", "entity_type", "display_name", "name", "字段名"]:
        df[c] = df[c].astype(str).fillna("").str.strip()

    df["entity_type"] = df["entity_type"].str.upper()
    return df


def build_header_map(cfg_fields: pd.DataFrame) -> dict[str, dict[str, str]]:
    """
    Return:
      {
        normalized_header: {
            "field_key": "...",
            "entity_type": "PRODUCT/VARIANT/..."
        }
      }
    """
    out: dict[str, dict[str, str]] = {}

    for r in cfg_fields.to_dict("records"):
        field_key = _norm(r.get("field_key"))
        entity_type = _norm(r.get("entity_type")).upper()

        if not field_key:
            continue

        aliases = [
            field_key,
            _norm(r.get("display_name")),
            _norm(r.get("name")),
            _norm(r.get("字段名")),
        ]

        for a in aliases:
            if not a:
                continue
            k = a.strip().lower()
            if k not in out:
                out[k] = {
                    "field_key": field_key,
                    "entity_type": entity_type,
                }

    return out


# =========================================================
# Wide loading
# =========================================================

def load_wide_sheet(ws_wide) -> pd.DataFrame:
    values = ws_wide.get_all_values()

    if not values or len(values) == 0:
        raise ValueError("❌ Wide 表为空")

    header = values[0]
    if not any(_norm(x) for x in header):
        raise ValueError("❌ Wide 第1行为空")

    body = values[1:]
    df = pd.DataFrame(body, columns=header)
    df["_sheet_row"] = range(2, 2 + len(df))

    for c in df.columns:
        if c != "_sheet_row":
            df[c] = df[c].astype(str).fillna("").replace("nan", "").str.strip()

    return df


# =========================================================
# Column / ID detection
# =========================================================

ID_COL_PRODUCT = "Product ID (numeric)"
ID_COL_VARIANT = "Variant ID (numeric)"
ID_COL_COLLECTION = "Collection ID (numeric)"
ID_COL_PAGE = "Page ID (numeric)"
ID_COL_HANDLE = "Handle"
ID_COL_SKU = "SKU"


def detect_known_id_columns(df_wide: pd.DataFrame) -> dict[str, Optional[str]]:
    cols = set(df_wide.columns)

    return {
        "product_id_col": ID_COL_PRODUCT if ID_COL_PRODUCT in cols else None,
        "variant_id_col": ID_COL_VARIANT if ID_COL_VARIANT in cols else None,
        "collection_id_col": ID_COL_COLLECTION if ID_COL_COLLECTION in cols else None,
        "page_id_col": ID_COL_PAGE if ID_COL_PAGE in cols else None,
        "handle_col": ID_COL_HANDLE if ID_COL_HANDLE in cols else None,
        "sku_col": ID_COL_SKU if ID_COL_SKU in cols else None,
    }


def pick_gid_or_handle(row: dict[str, Any], entity_type: str, id_cols: dict[str, Optional[str]]) -> str:
    entity_type = _norm(entity_type).upper()

    if entity_type == "PRODUCT":
        if id_cols["product_id_col"]:
            v = _norm(row.get(id_cols["product_id_col"]))
            if v:
                return v
        if id_cols["handle_col"]:
            v = _norm(row.get(id_cols["handle_col"]))
            if v:
                return v

    if entity_type == "VARIANT":
        if id_cols["variant_id_col"]:
            v = _norm(row.get(id_cols["variant_id_col"]))
            if v:
                return v
        if id_cols["sku_col"]:
            v = _norm(row.get(id_cols["sku_col"]))
            if v:
                return v

    if entity_type == "COLLECTION":
        if id_cols["collection_id_col"]:
            v = _norm(row.get(id_cols["collection_id_col"]))
            if v:
                return v
        if id_cols["handle_col"]:
            v = _norm(row.get(id_cols["handle_col"]))
            if v:
                return v

    if entity_type == "PAGE":
        if id_cols["page_id_col"]:
            v = _norm(row.get(id_cols["page_id_col"]))
            if v:
                return v
        if id_cols["handle_col"]:
            v = _norm(row.get(id_cols["handle_col"]))
            if v:
                return v

    return ""


def infer_entity_type_for_field(field_key: str, cfg_entity_type: str, row: dict[str, Any], id_cols: dict[str, Optional[str]]) -> str:
    fk = _norm(field_key)
    et = _norm(cfg_entity_type).upper()

    # 规则 1：v_mf.* 一律 VARIANT
    if fk.startswith("v_mf."):
        return "VARIANT"

    # 规则 2：mf.* 且当前行同时有 Product ID / Variant ID，则默认 PRODUCT
    if fk.startswith("mf."):
        if id_cols.get("product_id_col") and _norm(row.get(id_cols["product_id_col"])):
            return "PRODUCT"
        if et:
            return et
        return "PRODUCT"

    # 规则 3：其它字段按 cfg.entity_type
    if et:
        return et

    # 兜底：有 product id 就 PRODUCT
    if id_cols.get("product_id_col") and _norm(row.get(id_cols["product_id_col"])):
        return "PRODUCT"

    return ""


# =========================================================
# Transform
# =========================================================

def build_transform_columns(
    df_wide: pd.DataFrame,
    header_map: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    """
    Return:
      [
        {
          "source_col": "...",
          "field_key": "...",
          "cfg_entity_type": "...",
        },
        ...
      ]
    """
    out = []

    skip_cols = {
        "_sheet_row",
        ID_COL_PRODUCT,
        ID_COL_VARIANT,
        ID_COL_COLLECTION,
        ID_COL_PAGE,
        ID_COL_HANDLE,
        ID_COL_SKU,
    }

    for col in df_wide.columns:
        if col in skip_cols:
            continue

        k = _norm(col).lower()
        meta = header_map.get(k)
        if not meta:
            continue

        out.append({
            "source_col": col,
            "field_key": meta["field_key"],
            "cfg_entity_type": meta["entity_type"],
        })

    return out


def wide_to_long_rows(
    df_wide: pd.DataFrame,
    transform_cols: list[dict[str, str]],
    id_cols: dict[str, Optional[str]],
    run_id: str,
    include_empty: bool,
    default_action: str,
    default_mode: str,
    default_note: str,
    only_entity_types: Optional[set[str]],
) -> list[list[str]]:
    rows_out: list[list[str]] = []

    only_types_upper = {x.upper() for x in only_entity_types} if only_entity_types else None

    for r in df_wide.to_dict("records"):
        for tc in transform_cols:
            source_col = tc["source_col"]
            field_key = tc["field_key"]
            cfg_entity_type = tc["cfg_entity_type"]

            desired_value = _norm(r.get(source_col))

            if not include_empty and desired_value == "":
                continue

            entity_type = infer_entity_type_for_field(
                field_key=field_key,
                cfg_entity_type=cfg_entity_type,
                row=r,
                id_cols=id_cols,
            )

            if not entity_type:
                continue

            if only_types_upper and entity_type.upper() not in only_types_upper:
                continue

            gid_or_handle = pick_gid_or_handle(r, entity_type, id_cols)
            if not gid_or_handle:
                continue

            rows_out.append([
                entity_type,
                gid_or_handle,
                field_key,
                desired_value,
                default_action,
                default_mode,
                default_note,
                run_id,
            ])

    return rows_out


def dedupe_long_rows(rows_out: list[list[str]]) -> list[list[str]]:
    seen = set()
    deduped = []

    for row in rows_out:
        key = tuple(row[:4])  # entity_type, gid_or_handle, field_key, desired_value
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


# =========================================================
# Output
# =========================================================

LONG_HEADER = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "desired_value",
    "action",
    "mode",
    "note",
    "run_id",
]


def write_long_output(
    ws_long,
    rows_out: list[list[str]],
    clear_output_first: bool,
):
    values = [LONG_HEADER] + rows_out

    if clear_output_first:
        ws_long.clear()

    if not rows_out:
        ws_long.update("A1:H1", [LONG_HEADER])
        return

    end_row = len(values)
    ws_long.update(f"A1:H{end_row}", values)


# =========================================================
# Main
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = "wide_to_long_product",

    gsheet_sa_b64_secret: str,

    console_core_url: str,

    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Wide",

    output_sheet_label: str = "pre_edit",
    output_worksheet_title: str = "Long",

    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = "Cfg__Fields",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,

    run_id: str,

    include_empty: bool = False,
    clear_output_first: bool = True,

    default_action: str = "SET",
    default_mode: str = "",
    default_note: str = "",

    only_entity_types: Optional[set[str]] = None,
) -> dict[str, Any]:
    gc = build_gsheet_client(gsheet_sa_b64_secret)

    _, ws_wide, wide_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_long, long_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=output_sheet_label,
        worksheet_title=output_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_cfg_fields, cfg_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_sheet_label,
        worksheet_title=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
    )

    df_wide = load_wide_sheet(ws_wide)
    cfg_fields = load_cfg_fields(ws_cfg_fields)
    header_map = build_header_map(cfg_fields)
    id_cols = detect_known_id_columns(df_wide)
    transform_cols = build_transform_columns(df_wide, header_map)

    if len(transform_cols) == 0:
        raise ValueError("No transformable field columns found. Check Wide headers and Cfg__Fields mapping.")

    rows_out = wide_to_long_rows(
        df_wide=df_wide,
        transform_cols=transform_cols,
        id_cols=id_cols,
        run_id=run_id,
        include_empty=include_empty,
        default_action=default_action,
        default_mode=default_mode,
        default_note=default_note,
        only_entity_types=only_entity_types,
    )
    rows_out = dedupe_long_rows(rows_out)

    write_long_output(
        ws_long=ws_long,
        rows_out=rows_out,
        clear_output_first=clear_output_first,
    )

    preview_dicts = []
    for row in rows_out[:50]:
        preview_dicts.append(dict(zip(LONG_HEADER, row)))

    return {
        "status": "success",
        "summary": {
            "job_name": job_name,
            "site_code": site_code,
            "rows_read": int(len(df_wide)),
            "rows_written": int(len(rows_out)),
            "mapped_columns": int(len(transform_cols)),
            "include_empty": bool(include_empty),
            "clear_output_first": bool(clear_output_first),
        },
        "preview": preview_dicts,
        "meta": {
            "run_id": run_id,
            "wide_sheet_url": wide_sheet_url,
            "long_sheet_url": long_sheet_url,
            "cfg_sheet_url": cfg_sheet_url,
        },
    }