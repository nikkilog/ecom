# shopify_sync/edit_metafieldblocks.py

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
except Exception:  # pragma: no cover
    userdata = None


# =========================================================
# Constants
# =========================================================

CFG_SITES_TAB_DEFAULT = "Cfg__Sites"
CFG_ACCOUNT_TAB_DEFAULT = "Cfg__account_id"

INPUT_HEADER = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "block_type",
    "block_seq",
    "title",
    "body",
    "value",
    "action",
    "mode",
    "note",
]

OUTPUT_HEADER = [
    "entity_type",
    "gid_or_handle",
    "field_key",
    "desired_value",
    "action",
    "mode",
    "note",
    "run_id",
]

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "VARIANT", "COLLECTION", "PAGE"}
SUPPORTED_ACTIONS = {"SET", "CLEAR", "SKIP"}
SUPPORTED_BLOCK_TYPES = {
    "list_item",
    "bullet",
    "feature",
    "paragraph",
}

GENERATOR_MARKER = "[generated_by=edit_metafieldblocks]"
GOOGLE_SHEETS_CELL_LIMIT = 50000


# =========================================================
# Generic utils
# =========================================================

def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _utc_run_id(prefix: str = "metafieldblocks") -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    return s


def _safe_int_or_none(x: Any) -> Optional[int]:
    s = _norm_str(x)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        k = item.strip()
        if not k:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def _is_blank_row(row: dict[str, Any]) -> bool:
    return all(_norm_str(row.get(c)) == "" for c in INPUT_HEADER)


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# =========================================================
# Colab Secrets / Google Sheets auth
# =========================================================

def _get_secret(secret_name: str) -> str:
    if userdata is None:
        raise RuntimeError("google.colab.userdata is unavailable. This module is intended for Colab runner use.")
    v = userdata.get(secret_name)
    if not v:
        raise ValueError(f"Missing Colab Secret: {secret_name}")
    return v


def build_gsheet_client(gsheet_sa_b64_secret: str) -> gspread.Client:
    secret_name = _norm_str(gsheet_sa_b64_secret)
    if not secret_name:
        raise ValueError("gsheet_sa_b64_secret is required")

    sa_b64 = _get_secret(secret_name)
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
# Console Core routing / account config
# =========================================================

def read_cfg_account_id(
    gc: gspread.Client,
    console_core_url: str,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
) -> dict[str, str]:
    """Read key-value config from Console Core / Cfg__account_id.

    Supports both:
    - 2-column no-header layout: key | value
    - get_all_records-style layout if the first row is header-like
    """
    sh = gc.open_by_url(console_core_url)
    ws = sh.worksheet(cfg_account_tab)
    values = ws.get_all_values()

    out: dict[str, str] = {}
    for row in values:
        if not row:
            continue
        key = _norm_str(row[0]) if len(row) >= 1 else ""
        val = _norm_str(row[1]) if len(row) >= 2 else ""
        if not key:
            continue
        # Ignore obvious header rows if present.
        if key.lower() in {"key", "config_key", "name"}:
            continue
        out[key] = val

    if not out:
        raise ValueError(f"{cfg_account_tab} is empty or not a key-value table")

    return out


def get_required_account_value(account_cfg: dict[str, str], key: str) -> str:
    v = _norm_str(account_cfg.get(key))
    if not v:
        raise ValueError(f"Cfg__account_id missing required key or value: {key}")
    return v


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

    required = ["site_code", "label", "sheet_url"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{cfg_sites_tab} missing required columns: {missing}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    m = df[
        (df["site_code"] == site_code.strip().upper())
        & (df["label"] == label.strip())
        & (df["sheet_url"] != "")
    ].copy()

    if m.empty:
        raise ValueError(f"Cannot find sheet_url for site_code={site_code}, label={label} in {cfg_sites_tab}")

    if len(m) > 1:
        # Keep deterministic behavior: first non-empty row in the sheet wins.
        m = m.head(1)

    return m.iloc[0]["sheet_url"]


def open_ws_by_url_and_title(
    gc: gspread.Client,
    sheet_url: str,
    worksheet_title: str,
    create_if_missing: bool = False,
    default_rows: int = 1000,
    default_cols: int = 20,
):
    sh = gc.open_by_url(sheet_url)
    try:
        ws = sh.worksheet(worksheet_title)
    except gspread.WorksheetNotFound:
        if not create_if_missing:
            raise
        ws = sh.add_worksheet(
            title=worksheet_title,
            rows=default_rows,
            cols=default_cols,
        )
    return sh, ws


# =========================================================
# Input loading / validation
# =========================================================

def load_blocks_sheet(ws_blocks) -> pd.DataFrame:
    rows = ws_blocks.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=INPUT_HEADER + ["_sheet_row", "_source_order"])

    missing = [c for c in INPUT_HEADER if c not in df.columns]
    if missing:
        raise ValueError(f"Edit__MetafieldBlocks missing required columns: {missing}")

    df = df[INPUT_HEADER].copy()
    df["_sheet_row"] = range(2, 2 + len(df))
    df["_source_order"] = range(len(df))

    for c in INPUT_HEADER:
        df[c] = df[c].apply(_norm_str)

    df = df[~df.apply(lambda r: _is_blank_row(r.to_dict()), axis=1)].copy()

    df["entity_type"] = df["entity_type"].str.upper().str.strip()
    df["block_type"] = df["block_type"].str.lower().str.strip()
    df["action"] = df["action"].str.upper().str.strip()
    df["mode"] = df["mode"].str.upper().str.strip()

    return df


def normalize_blocks_df(
    df: pd.DataFrame,
    mode_default: str = "STRICT",
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if df.empty:
        return df.copy(), []

    d = df.copy()
    errors: list[dict[str, Any]] = []

    d["mode"] = d["mode"].replace("", mode_default.upper())
    d["action"] = d["action"].replace("", "SET")

    for r in d.to_dict("records"):
        sheet_row = r.get("_sheet_row")

        if r["entity_type"] not in SUPPORTED_ENTITY_TYPES:
            errors.append({
                "sheet_row": sheet_row,
                "error_reason": "invalid_entity_type",
                "message": f"entity_type={r['entity_type']}",
            })

        if not r["gid_or_handle"]:
            errors.append({
                "sheet_row": sheet_row,
                "error_reason": "missing_gid_or_handle",
                "message": "gid_or_handle is required",
            })

        if not r["field_key"]:
            errors.append({
                "sheet_row": sheet_row,
                "error_reason": "missing_field_key",
                "message": "field_key is required",
            })

        if r["action"] not in SUPPORTED_ACTIONS:
            errors.append({
                "sheet_row": sheet_row,
                "error_reason": "invalid_action",
                "message": f"action={r['action']}",
            })

        if r["action"] != "CLEAR":
            if r["block_type"] not in SUPPORTED_BLOCK_TYPES:
                errors.append({
                    "sheet_row": sheet_row,
                    "error_reason": "invalid_block_type",
                    "message": f"block_type={r['block_type']}",
                })

    if errors:
        return d, errors

    d = d[d["action"] != "SKIP"].copy()
    return d, []


# =========================================================
# Shopify rich text builders
# =========================================================

def build_rich_text_bullet(items: list[str]) -> str:
    clean_items = [x.strip() for x in items if x and x.strip()]

    root = {
        "type": "root",
        "children": [
            {
                "type": "list",
                "listType": "unordered",
                "children": [
                    {
                        "type": "list-item",
                        "children": [
                            {
                                "type": "text",
                                "value": item,
                            }
                        ],
                    }
                    for item in clean_items
                ],
            }
        ],
    }

    return _json_dumps(root)


def build_rich_text_feature(rows: list[dict[str, Any]]) -> str:
    children = []

    for r in rows:
        title = _norm_str(r.get("title"))
        body = _norm_str(r.get("body"))

        if not title and not body:
            continue

        paragraph_children = []

        if title:
            paragraph_children.append({
                "type": "text",
                "value": title,
                "bold": True,
            })

        if body:
            paragraph_children.append({
                "type": "text",
                "value": f"\n{body}" if title else body,
            })

        children.append({
            "type": "paragraph",
            "children": paragraph_children,
        })

    return _json_dumps({"type": "root", "children": children})


def build_rich_text_paragraph(rows: list[dict[str, Any]]) -> str:
    children = []

    for r in rows:
        title = _norm_str(r.get("title"))
        body = _norm_str(r.get("body") or r.get("value"))

        if not title and not body:
            continue

        paragraph_children = []

        if title:
            paragraph_children.append({
                "type": "text",
                "value": title,
                "bold": True,
            })

        if body:
            paragraph_children.append({
                "type": "text",
                "value": f"\n{body}" if title else body,
            })

        children.append({
            "type": "paragraph",
            "children": paragraph_children,
        })

    return _json_dumps({"type": "root", "children": children})


# =========================================================
# Build ValuesLong rows
# =========================================================

def _sort_group_rows(g: pd.DataFrame) -> pd.DataFrame:
    d = g.copy()
    d["_seq_sort"] = d["block_seq"].apply(_safe_int_or_none)
    d["_seq_sort"] = d["_seq_sort"].apply(lambda x: 999999999 if x is None else x)
    return d.sort_values(["_seq_sort", "_source_order"], kind="stable").copy()


def _combine_note(notes: list[str], run_id: str) -> str:
    clean_notes = [n.strip() for n in notes if n and n.strip()]
    base = " | ".join(clean_notes[:3])
    marker = f"{GENERATOR_MARKER} run_id={run_id}"
    if base:
        return f"{base} | {marker}"
    return marker


def build_valueslong_rows(
    df_blocks: pd.DataFrame,
    run_id: str,
    dedupe_list_values: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    out_rows: list[dict[str, Any]] = []

    if df_blocks.empty:
        return out_rows, errors

    group_cols = ["entity_type", "gid_or_handle", "field_key"]

    for group_key, g0 in df_blocks.groupby(group_cols, dropna=False, sort=False):
        entity_type, gid_or_handle, field_key = group_key
        g = _sort_group_rows(g0)

        actions = sorted(set(g["action"].astype(str).str.upper().str.strip().tolist()))
        actions = [a for a in actions if a]

        if len(actions) > 1:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "mixed_actions_in_group",
                "message": f"actions={actions}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        action = actions[0] if actions else "SET"
        mode = _norm_str(g["mode"].dropna().iloc[0]) or "STRICT"
        note = _combine_note(g["note"].astype(str).tolist(), run_id)

        if action == "CLEAR":
            out_rows.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "desired_value": "",
                "action": "CLEAR",
                "mode": mode,
                "note": note,
                "run_id": "",
            })
            continue

        block_types = [
            x for x in g["block_type"].astype(str).str.lower().str.strip().tolist()
            if x
        ]
        unique_block_types = sorted(set(block_types))

        if len(unique_block_types) != 1:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "mixed_block_types_in_group",
                "message": f"block_types={unique_block_types}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        block_type = unique_block_types[0]
        records = g.to_dict("records")

        if block_type == "list_item":
            values = [_norm_str(r.get("value")) for r in records]
            values = [v for v in values if v]
            if dedupe_list_values:
                values = _dedupe_keep_order(values)
            desired_value = _json_dumps(values)

        elif block_type == "bullet":
            items = [_norm_str(r.get("body") or r.get("value")) for r in records]
            desired_value = build_rich_text_bullet(items)

        elif block_type == "feature":
            desired_value = build_rich_text_feature(records)

        elif block_type == "paragraph":
            desired_value = build_rich_text_paragraph(records)

        else:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "unsupported_block_type",
                "message": f"block_type={block_type}",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        if len(desired_value) >= GOOGLE_SHEETS_CELL_LIMIT:
            errors.append({
                "entity_type": entity_type,
                "gid_or_handle": gid_or_handle,
                "field_key": field_key,
                "error_reason": "desired_value_too_long",
                "message": f"desired_value length={len(desired_value)} exceeds Google Sheets cell limit",
                "sheet_rows": g["_sheet_row"].tolist(),
            })
            continue

        out_rows.append({
            "entity_type": entity_type,
            "gid_or_handle": gid_or_handle,
            "field_key": field_key,
            "desired_value": desired_value,
            "action": action,
            "mode": mode,
            "note": note,
            "run_id": "",
        })

    return out_rows, errors


# =========================================================
# Output write
# =========================================================

def _worksheet_values_to_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=OUTPUT_HEADER)

    header = values[0]
    body = values[1:]
    if not header:
        return pd.DataFrame(columns=OUTPUT_HEADER)

    df = pd.DataFrame(body, columns=header)
    for c in OUTPUT_HEADER:
        if c not in df.columns:
            df[c] = ""
    return df[OUTPUT_HEADER].copy()


def ensure_output_header(ws_output):
    values = ws_output.get_all_values()
    if not values:
        ws_output.update(range_name="A1:H1", values=[OUTPUT_HEADER])
        return

    header = values[0]
    if header[:len(OUTPUT_HEADER)] != OUTPUT_HEADER:
        ws_output.update(range_name="A1:H1", values=[OUTPUT_HEADER])


def write_valueslong_output(
    ws_output,
    generated_rows: list[dict[str, Any]],
    replace_generated: bool = True,
    clear_output_first: bool = False,
) -> dict[str, Any]:
    ensure_output_header(ws_output)

    generated_df = pd.DataFrame(generated_rows)
    if generated_df.empty:
        generated_df = pd.DataFrame(columns=OUTPUT_HEADER)

    for c in OUTPUT_HEADER:
        if c not in generated_df.columns:
            generated_df[c] = ""
    generated_df = generated_df[OUTPUT_HEADER].fillna("").astype(str)

    if clear_output_first:
        final_df = generated_df.copy()
        preserved_count = 0
        removed_generated_count = 0
    else:
        existing_df = _worksheet_values_to_df(ws_output).fillna("").astype(str)

        if replace_generated:
            mask_generated = existing_df["note"].astype(str).str.contains(
                re.escape(GENERATOR_MARKER),
                regex=True,
                na=False,
            )
            preserved_df = existing_df[~mask_generated].copy()
            removed_generated_count = int(mask_generated.sum())
        else:
            preserved_df = existing_df.copy()
            removed_generated_count = 0

        preserved_count = int(len(preserved_df))
        final_df = pd.concat([preserved_df, generated_df], ignore_index=True)

    values = [OUTPUT_HEADER] + final_df[OUTPUT_HEADER].fillna("").astype(str).values.tolist()

    ws_output.clear()
    if values:
        ws_output.update(
            range_name=f"A1:H{len(values)}",
            values=values,
            value_input_option="RAW",
        )

    return {
        "rows_preserved_existing": preserved_count,
        "rows_removed_previous_generated": removed_generated_count,
        "rows_generated_written": int(len(generated_df)),
        "rows_output_total": int(len(final_df)),
    }


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    console_gsheet_sa_b64_secret: str,
    job_name: str = "edit_metafieldblocks",

    input_sheet_label: str = "edit",
    output_sheet_label: str = "edit",
    input_worksheet_title: str = "Edit__MetafieldBlocks",
    output_worksheet_title: str = "Edit__ValuesLong",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    account_gsheet_secret_key: str = "GSHEET_SA_B64_SECRET",

    mode_default: str = "STRICT",
    preview_only: bool = True,
    replace_generated: bool = True,
    clear_output_first: bool = False,
    dedupe_list_values: bool = True,
    create_output_tab_if_missing: bool = True,
    preview_limit: int = 30,
) -> dict[str, Any]:
    """Build standard Edit__ValuesLong rows from human-friendly Edit__MetafieldBlocks.

    Responsibility:
      Console Core / Cfg__Sites + Cfg__account_id
      -> route edit sheet
      -> Edit__MetafieldBlocks
      -> Edit__ValuesLong

    This module does NOT write Shopify.
    Final Shopify write remains handled by shopify_sync/edit_metafields.py.
    """

    run_id = _utc_run_id("metafieldblocks")

    # Bootstrap client only opens Console Core.
    gc_console = build_gsheet_client(console_gsheet_sa_b64_secret)

    account_cfg = read_cfg_account_id(
        gc=gc_console,
        console_core_url=console_core_url,
        cfg_account_tab=cfg_account_tab,
    )
    gsheet_sa_b64_secret = get_required_account_value(account_cfg, account_gsheet_secret_key)

    # Target client opens site/edit/config sheets. Usually same as bootstrap for one site,
    # but intentionally routed from Cfg__account_id rather than Cell 1.
    if gsheet_sa_b64_secret == console_gsheet_sa_b64_secret:
        gc_target = gc_console
    else:
        gc_target = build_gsheet_client(gsheet_sa_b64_secret)

    input_sheet_url = get_sheet_url_by_label(
        gc=gc_console,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )
    output_sheet_url = get_sheet_url_by_label(
        gc=gc_console,
        console_core_url=console_core_url,
        site_code=site_code,
        label=output_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_blocks = open_ws_by_url_and_title(
        gc=gc_target,
        sheet_url=input_sheet_url,
        worksheet_title=input_worksheet_title,
        create_if_missing=False,
    )

    _, ws_output = open_ws_by_url_and_title(
        gc=gc_target,
        sheet_url=output_sheet_url,
        worksheet_title=output_worksheet_title,
        create_if_missing=create_output_tab_if_missing,
        default_rows=1000,
        default_cols=len(OUTPUT_HEADER),
    )

    df_raw = load_blocks_sheet(ws_blocks)
    df_blocks, validation_errors = normalize_blocks_df(
        df=df_raw,
        mode_default=mode_default,
    )

    base_meta = {
        "site_code": site_code,
        "job_name": job_name,
        "run_id": run_id,
        "console_core_url": console_core_url,
        "input_sheet_url": input_sheet_url,
        "output_sheet_url": output_sheet_url,
        "input_sheet_label": input_sheet_label,
        "output_sheet_label": output_sheet_label,
        "input_worksheet_title": input_worksheet_title,
        "output_worksheet_title": output_worksheet_title,
        "cfg_sites_tab": cfg_sites_tab,
        "cfg_account_tab": cfg_account_tab,
        "account_gsheet_secret_key": account_gsheet_secret_key,
        "gsheet_sa_b64_secret_from_cfg": gsheet_sa_b64_secret,
        "replace_generated": replace_generated,
        "clear_output_first": clear_output_first,
        "generated_at_cn": _now_cn_str(),
    }

    if validation_errors:
        return {
            "status": "error",
            "run_id": run_id,
            "site_code": site_code,
            "job_name": job_name,
            "summary": {
                "rows_loaded": int(len(df_raw)),
                "rows_valid": 0,
                "rows_generated": 0,
                "errors": int(len(validation_errors)),
            },
            "errors": validation_errors[:preview_limit],
            "meta": base_meta,
        }

    generated_rows, build_errors = build_valueslong_rows(
        df_blocks=df_blocks,
        run_id=run_id,
        dedupe_list_values=dedupe_list_values,
    )

    if build_errors:
        return {
            "status": "error",
            "run_id": run_id,
            "site_code": site_code,
            "job_name": job_name,
            "summary": {
                "rows_loaded": int(len(df_raw)),
                "rows_valid": int(len(df_blocks)),
                "rows_generated": int(len(generated_rows)),
                "errors": int(len(build_errors)),
            },
            "errors": build_errors[:preview_limit],
            "preview": generated_rows[:preview_limit],
            "meta": base_meta,
        }

    if preview_only:
        return {
            "status": "preview",
            "run_id": run_id,
            "site_code": site_code,
            "job_name": job_name,
            "summary": {
                "rows_loaded": int(len(df_raw)),
                "rows_valid": int(len(df_blocks)),
                "rows_generated": int(len(generated_rows)),
                "errors": 0,
                "written": 0,
            },
            "preview": generated_rows[:preview_limit],
            "meta": base_meta,
        }

    write_result = write_valueslong_output(
        ws_output=ws_output,
        generated_rows=generated_rows,
        replace_generated=replace_generated,
        clear_output_first=clear_output_first,
    )

    return {
        "status": "written",
        "run_id": run_id,
        "site_code": site_code,
        "job_name": job_name,
        "summary": {
            "rows_loaded": int(len(df_raw)),
            "rows_valid": int(len(df_blocks)),
            "rows_generated": int(len(generated_rows)),
            "errors": 0,
            **write_result,
        },
        "preview": generated_rows[:preview_limit],
        "meta": base_meta,
    }
