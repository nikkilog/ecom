
# shopify_pre_edit/entries_update_wide_to_long.py

from __future__ import annotations

import base64
import datetime as dt
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

RUNLOG_HEADER = [
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
# basic
# =========================================================

def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def _safe_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _make_run_id(prefix: str = "entries_update_w2l") -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}_%Y%m%d_%H%M%S")


# =========================================================
# auth / clients
# =========================================================

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
# sheet locating
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
        raise ValueError(f"Cannot find sheet_url for site_code={site_code}, label={label} in {cfg_sites_tab}")

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
# runlog
# =========================================================

class RunLogger:
    def __init__(
        self,
        gc: gspread.Client,
        runlog_sheet_url: str,
        runlog_tab_name: str,
        run_id: str,
        job_name: str,
        site_code: str,
    ):
        self.gc = gc
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code

        sh = gc.open_by_url(runlog_sheet_url)
        self.ws = sh.worksheet(runlog_tab_name)
        self.ws.update(range_name="A1:R1", values=[RUNLOG_HEADER])

    def log_summary(
        self,
        *,
        phase: str,
        status: str,
        rows_loaded: int = 0,
        rows_pending: int = 0,
        rows_recognized: int = 0,
        rows_planned: int = 0,
        rows_written: int = 0,
        rows_skipped: int = 0,
        message: str = "",
        error_reason: str = "",
    ):
        row = [[
            self.run_id,
            _now_cn_str(),
            self.job_name,
            phase,
            "summary",
            status,
            self.site_code,
            "",
            "",
            "",
            rows_loaded,
            rows_pending,
            rows_recognized,
            rows_planned,
            rows_written,
            rows_skipped,
            message,
            error_reason,
        ]]
        self.ws.append_rows(row, value_input_option="RAW", table_range="A:R")


# =========================================================
# load config fields
# =========================================================

def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = ws_cfg_fields.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    for c in ["field_id", "entity_type", "field_key"]:
        if c not in df.columns:
            raise ValueError(f"Cfg__Fields missing required column: {c}")

    df["field_id"] = df["field_id"].astype(str).str.strip()
    df["entity_type"] = df["entity_type"].astype(str).str.strip().str.upper()
    df["field_key"] = df["field_key"].astype(str).str.strip()

    return df


def build_field_maps(cfg_fields_df: pd.DataFrame):
    fid_to_key = {}
    fid_to_entity = {}

    for r in cfg_fields_df.to_dict("records"):
        fid = _norm_str(r.get("field_id"))
        fk = _norm_str(r.get("field_key"))
        et = _norm_str(r.get("entity_type")).upper()
        if fid:
            fid_to_key[fid] = fk
            fid_to_entity[fid] = et

    return fid_to_key, fid_to_entity


# =========================================================
# wide load / header parse
# =========================================================

def load_wide_sheet(ws_input) -> pd.DataFrame:
    values = ws_input.get_all_values()

    if not values:
        raise ValueError("❌ Wide sheet is empty")

    header = values[0]
    if not header or all(_norm_str(x) == "" for x in header):
        raise ValueError("❌ Wide 第1行为空")

    data = values[1:]
    max_len = len(header)
    data = [row + [""] * (max_len - len(row)) if len(row) < max_len else row[:max_len] for row in data]

    df = pd.DataFrame(data, columns=header)
    df["_sheet_row"] = range(2, 2 + len(df))
    return df


def detect_base_columns(df_wide: pd.DataFrame) -> tuple[str, str]:
    cols = [str(c).strip() for c in df_wide.columns]

    candidates_owner = [
        "entry_gid",
        "gid",
        "gid_or_handle",
        "lookup_handle",
        "handle",
    ]
    candidates_action = [
        "action",
    ]

    owner_col = next((c for c in candidates_owner if c in cols), "")
    action_col = next((c for c in candidates_action if c in cols), "")

    if owner_col == "":
        raise ValueError("❌ 未找到 owner 列。需要存在 entry_gid / gid / gid_or_handle / lookup_handle / handle 之一")
    if action_col == "":
        raise ValueError("❌ 未找到 action 列")

    return owner_col, action_col


# =========================================================
# transform
# =========================================================

def transform_wide_to_long(
    df_wide: pd.DataFrame,
    fid_to_key: dict[str, str],
    fid_to_entity: dict[str, str],
    owner_col: str,
    action_col: str,
    include_empty: bool = False,
) -> tuple[pd.DataFrame, dict[str, int]]:
    out_rows = []

    fixed_keep_cols = [
        c for c in [
            owner_col,
            action_col,
            "mode",
            "note",
            "run_id",
            "lookup_handle",
            "handle",
            "entry_gid",
            "gid",
            "gid_or_handle",
        ]
        if c in df_wide.columns
    ]

    field_id_cols = [c for c in df_wide.columns if _norm_str(c).startswith("f.")]
    if not field_id_cols:
        # 兼容有人直接把 field_id 放成表头，而不是 f.xxx
        field_id_cols = [c for c in df_wide.columns if _norm_str(c) in fid_to_key]

    rows_loaded = len(df_wide)
    rows_pending = 0
    rows_recognized = 0
    rows_planned = 0
    rows_skipped = 0

    for r in df_wide.to_dict("records"):
        owner_value = _norm_str(r.get(owner_col))
        action_value = _norm_str(r.get(action_col)).upper()
        mode_value = _norm_str(r.get("mode")) or "STRICT"
        note_value = _norm_str(r.get("note"))
        run_id_value = _norm_str(r.get("run_id"))
        sheet_row = _safe_int(r.get("_sheet_row"))

        if action_value in ("", "SKIP"):
            continue

        rows_pending += 1

        for col in field_id_cols:
            raw_value = r.get(col, "")
            desired_value = _norm_str(raw_value)

            field_id = _norm_str(col)
            if field_id.startswith("f."):
                field_id = field_id

            field_key = fid_to_key.get(field_id, "")
            entity_type = fid_to_entity.get(field_id, "")

            if not field_key or not entity_type:
                rows_skipped += 1
                continue

            rows_recognized += 1

            if (desired_value == "") and (not include_empty):
                continue

            out_rows.append({
                "entity_type": entity_type,
                "gid_or_handle": owner_value,
                "field_key": field_key,
                "desired_value": desired_value,
                "action": action_value,
                "mode": mode_value,
                "note": note_value,
                "run_id": run_id_value,
                "_sheet_row": sheet_row,
            })
            rows_planned += 1

    df_long = pd.DataFrame(out_rows)

    if not df_long.empty:
        df_long = df_long.drop_duplicates(subset=[
            "entity_type", "gid_or_handle", "field_key", "desired_value", "action", "mode"
        ], keep="first").reset_index(drop=True)

    summary = {
        "rows_loaded": rows_loaded,
        "rows_pending": rows_pending,
        "rows_recognized": rows_recognized,
        "rows_planned": rows_planned,
        "rows_written": len(df_long),
        "rows_skipped": rows_skipped,
    }
    return df_long, summary


# =========================================================
# write
# =========================================================

def clear_and_write_ws(ws_output, df_long: pd.DataFrame, clear_output_first: bool = True):
    header = ["entity_type", "gid_or_handle", "field_key", "desired_value", "action", "mode", "note", "run_id"]

    if clear_output_first:
        ws_output.clear()

    if df_long.empty:
        ws_output.update("A1:H1", [header])
        return

    values = [header] + df_long[header].fillna("").astype(str).values.tolist()
    ws_output.update(values=values, range_name=f"A1:H{len(values)}")


# =========================================================
# main
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = "entries_update_wide_to_long",

    gsheet_sa_b64_secret: str,
    console_core_url: str,

    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Entries_Update-Wide",
    output_worksheet_title: str = "Entries_Update-Long",

    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = "Cfg__Fields",

    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,

    include_empty: bool = False,
    clear_output_first: bool = True,

    run_id: Optional[str] = None,
) -> dict[str, Any]:
    run_id = run_id or _make_run_id()

    gc = build_gsheet_client(gsheet_sa_b64_secret)

    _, ws_input, input_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    sh_output, ws_output, output_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
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

    runlog_sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=runlog_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )

    logger = RunLogger(
        gc=gc,
        runlog_sheet_url=runlog_sheet_url,
        runlog_tab_name=runlog_tab_name,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
    )

    try:
        cfg_fields_df = load_cfg_fields(ws_cfg_fields)
        fid_to_key, fid_to_entity = build_field_maps(cfg_fields_df)

        df_wide = load_wide_sheet(ws_input)
        owner_col, action_col = detect_base_columns(df_wide)

        df_long, summary = transform_wide_to_long(
            df_wide=df_wide,
            fid_to_key=fid_to_key,
            fid_to_entity=fid_to_entity,
            owner_col=owner_col,
            action_col=action_col,
            include_empty=include_empty,
        )

        clear_and_write_ws(
            ws_output=ws_output,
            df_long=df_long,
            clear_output_first=clear_output_first,
        )

        logger.log_summary(
            phase="transform",
            status="SUCCESS",
            rows_loaded=summary["rows_loaded"],
            rows_pending=summary["rows_pending"],
            rows_recognized=summary["rows_recognized"],
            rows_planned=summary["rows_planned"],
            rows_written=summary["rows_written"],
            rows_skipped=summary["rows_skipped"],
            message=(
                f"Entries_Update wide->long completed | "
                f"owner_col={owner_col} | "
                f"input_ws={input_worksheet_title} | output_ws={output_worksheet_title}"
            ),
            error_reason="",
        )

        preview = df_long.head(50).to_dict("records") if not df_long.empty else []

        return {
            "status": "success",
            "summary": summary,
            "preview": preview,
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "owner_col": owner_col,
                "action_col": action_col,
            },
        }

    except Exception as e:
        logger.log_summary(
            phase="transform",
            status="ERROR",
            rows_loaded=0,
            rows_pending=0,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=0,
            message=str(e),
            error_reason=type(e).__name__,
        )
        raise
