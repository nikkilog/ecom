# shopify_pre_edit/entries_create_wide_to_long.py

from __future__ import annotations

import base64
import datetime as dt
import json
import random
import time
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
# Utils
# =========================================================

def _utc_run_id(prefix: str = "wide_to_long") -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}__%Y%m%d_%H%M%S")


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


# =========================================================
# Secrets / clients
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
# Sheets locating
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


def open_ws_optional_by_label_and_title(
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
    try:
        ws = sh.worksheet(worksheet_title)
    except Exception as e:
        raise ValueError(f"Cannot open config worksheet: label={label}, title={worksheet_title}") from e
    return sh, ws, sheet_url


def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = ws_cfg_fields.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    required = ["field_id", "field_key"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Cfg__Fields missing required column: {c}")

    for c in ["field_id", "field_key", "field_handle", "display_name", "entity_type", "source_type", "namespace", "key"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].map(_norm_str)

    df = df[df["field_id"] != ""].copy()
    return df


def _norm_lookup_key(x: Any) -> str:
    s = _norm_str(x).lower()
    s = s.replace("｜", "|").replace("–", "-").replace("—", "-")
    s = " ".join(s.split())
    return s


def build_cfg_fields_lookup(cfg_fields_df: pd.DataFrame, entity_type: str = "METAOBJECT_ENTRY") -> dict[str, str]:
    df = cfg_fields_df.copy()
    if entity_type:
        m = df["entity_type"].map(_norm_str).str.upper() == entity_type.strip().upper()
        if m.any():
            df = df[m].copy()

    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    candidate_cols = ["field_id", "field_key", "field_handle", "display_name", "key"]
    for _, r in df.iterrows():
        fid = _norm_str(r.get("field_id"))
        if not fid:
            continue
        for c in candidate_cols:
            k = _norm_lookup_key(r.get(c))
            if not k:
                continue
            if k in lookup and lookup[k] != fid:
                collisions[k].update([lookup[k], fid])
            else:
                lookup[k] = fid

    for k in collisions:
        lookup.pop(k, None)

    return lookup


def load_cfg_metaobject_defs(ws_cfg_metaobject_defs) -> pd.DataFrame:
    rows = ws_cfg_metaobject_defs.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("Cfg__MetaobjectDefs is empty")

    required = ["type", "field_key"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Cfg__MetaobjectDefs missing required column: {c}")

    for c in ["type", "type_name", "field_key", "field_name", "field_type", "gid"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].map(_norm_str)

    df = df[(df["type"] != "") & (df["field_key"] != "")].copy()
    return df


def build_cfg_metaobject_defs_lookup(
    cfg_metaobject_defs_df: pd.DataFrame,
    entity_type: str = "METAOBJECT_ENTRY",
) -> dict[str, str]:
    """
    Build row-level lookup for metaobject entry fields.

    Key format: normalized metaobject_type + "|" + normalized header candidate.
    Value format: METAOBJECT_ENTRY|mo.{type}.{field_key}

    Example:
      type=card_spec, field_key=icon
      card_spec|icon -> METAOBJECT_ENTRY|mo.card_spec.icon
    """
    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    df = cfg_metaobject_defs_df.copy()
    for _, r in df.iterrows():
        mo_type = _norm_str(r.get("type"))
        field_key = _norm_str(r.get("field_key"))
        if not mo_type or not field_key:
            continue

        fid = f"{entity_type}|mo.{mo_type}.{field_key}"
        candidates = [
            field_key,
            r.get("field_name"),
            f"mo.{mo_type}.{field_key}",
            fid,
        ]

        for cand in candidates:
            ck = _norm_lookup_key(cand)
            if not ck:
                continue
            lk = f"{_norm_lookup_key(mo_type)}|{ck}"
            if lk in lookup and lookup[lk] != fid:
                collisions[lk].update([lookup[lk], fid])
            else:
                lookup[lk] = fid

    for k in collisions:
        lookup.pop(k, None)

    return lookup


def resolve_blank_field_id_row_from_cfg_fields(
    *,
    header_row: list[str],
    field_id_row: list[str],
    cfg_fields_df: pd.DataFrame,
    value_start_col_idx: int = 4,
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[str]:
    """
    Resolve only globally unique/full headers from Cfg__Fields.

    Ambiguous short headers such as icon/label/value are intentionally left blank here.
    They are resolved row-by-row later using entry_type + Cfg__MetaobjectDefs.
    """
    max_cols = max(len(header_row), len(field_id_row))
    headers = header_row + [""] * (max_cols - len(header_row))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    lookup = build_cfg_fields_lookup(cfg_fields_df, entity_type=entity_type)

    for col_idx in range(value_start_col_idx, max_cols):
        existing_fid = _norm_str(field_ids[col_idx])
        if existing_fid:
            continue

        header = _norm_str(headers[col_idx])
        if not header:
            continue

        matched = lookup.get(_norm_lookup_key(header))
        if matched:
            field_ids[col_idx] = matched

    return field_ids


def resolve_field_id_for_cell(
    *,
    header: Any,
    entry_type: Any,
    existing_field_id: Any,
    cfg_metaobject_lookup: dict[str, str],
    entity_type: str = "METAOBJECT_ENTRY",
) -> str:
    existing = _norm_str(existing_field_id)
    if existing:
        return existing

    h = _norm_str(header)
    et = _norm_str(entry_type)
    if not h or not et:
        return ""

    # First use explicit Cfg__MetaobjectDefs lookup.
    lk = f"{_norm_lookup_key(et)}|{_norm_lookup_key(h)}"
    matched = cfg_metaobject_lookup.get(lk, "")
    if matched:
        return matched

    # Safe deterministic fallback only for simple field keys.
    # This keeps the job usable when Cfg__MetaobjectDefs has not yet synced a newly-created field,
    # while still avoiding display-name guesses with spaces or punctuation.
    simple = _norm_str(h)
    if simple and all(ch.isalnum() or ch == "_" for ch in simple):
        return f"{entity_type}|mo.{et}.{simple}"

    return ""


def validate_resolved_field_ids_for_values(
    *,
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    cfg_metaobject_lookup: dict[str, str],
    value_start_col_idx: int = 4,
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []

    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(r) for _, r in effective_rows), default=0),
    )
    headers = header_row + [""] * (max_cols - len(header_row))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    for sheet_row, row in effective_rows:
        row_pad = row + [""] * (max_cols - len(row))
        entry_type = _norm_str(row_pad[1]) if max_cols > 1 else ""
        for col_idx in range(value_start_col_idx, max_cols):
            value = _norm_str(row_pad[col_idx])
            if value == "":
                continue

            header = _norm_str(headers[col_idx])
            fid = resolve_field_id_for_cell(
                header=header,
                entry_type=entry_type,
                existing_field_id=field_ids[col_idx],
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )
            if not fid:
                errors.append({
                    "entity_type": entity_type,
                    "gid": "",
                    "field_key": header,
                    "error_reason": "unmatched_field_header",
                    "message": (
                        f"sheet_row={sheet_row} | col={col_idx + 1} | "
                        f"entry_type={entry_type} | header={header} | "
                        f"cannot resolve field_id from row2 / Cfg__Fields / Cfg__MetaobjectDefs"
                    ),
                })

    return errors


# =========================================================
# Runlog
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
        flush_every: int = 200,
    ):
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.flush_every = flush_every
        self._buf: list[list[Any]] = []

        sh = gc.open_by_url(runlog_sheet_url)
        self.ws = sh.worksheet(runlog_tab_name)
        self.ws.update(range_name="A1:R1", values=[RUNLOG_HEADER])

    def log_row(
        self,
        *,
        phase: str,
        log_type: str,
        status: str,
        entity_type: str = "",
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
    ):
        self._buf.append([
            self.run_id,
            _now_cn_str(),
            self.job_name,
            phase,
            log_type,
            status,
            self.site_code,
            entity_type,
            gid,
            field_key,
            rows_loaded,
            rows_pending,
            rows_recognized,
            rows_planned,
            rows_written,
            rows_skipped,
            message,
            error_reason,
        ])
        if len(self._buf) >= self.flush_every:
            self.flush()

    def flush(self):
        if not self._buf:
            return
        for i in range(6):
            try:
                self.ws.append_rows(self._buf, value_input_option="RAW", table_range="A:R")
                self._buf = []
                return
            except Exception:
                time.sleep(min(2**i, 20) + random.random())
        raise RuntimeError("Failed to write RunLog after retries")


def log_grouped_details(
    logger: RunLogger,
    *,
    phase: str,
    status: str,
    rows_loaded: int,
    rows_pending: int,
    rows_recognized: int,
    rows_planned: int,
    rows_written: int,
    rows_skipped: int,
    detail_rows: list[dict[str, Any]],
    max_per_reason: int = 2,
):
    grouped = defaultdict(list)
    for r in detail_rows:
        reason = _norm_str(r.get("error_reason")) or "unknown"
        grouped[reason].append(r)

    for reason, items in grouped.items():
        for row in items[:max_per_reason]:
            logger.log_row(
                phase=phase,
                log_type="detail",
                status=status,
                entity_type=_norm_str(row.get("entity_type")),
                gid=_norm_str(row.get("gid")),
                field_key=_norm_str(row.get("field_key")),
                rows_loaded=rows_loaded,
                rows_pending=rows_pending,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                message=_norm_str(row.get("message")),
                error_reason=reason,
            )


# =========================================================
# Core transform
# =========================================================

def _row_has_any_value(values: list[Any]) -> bool:
    return any(_norm_str(v) != "" for v in values)


def _make_header_unique(cols: list[str]) -> list[str]:
    seen = {}
    out = []
    for c in cols:
        base = _norm_str(c)
        if base == "":
            base = "unnamed"
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__dup{seen[base]}")
    return out


def load_wide_sheet(ws_wide) -> tuple[list[list[Any]], list[str], list[str], list[list[Any]]]:
    values = ws_wide.get_all_values()
    if not values:
        raise ValueError("❌ Wide sheet is empty")
    if len(values) < 2:
        raise ValueError("❌ Wide sheet must have at least 2 rows (header row + optional field_id row)")

    row1 = values[0]
    row2 = values[1]
    if not _row_has_any_value(row1):
        raise ValueError("❌ Wide 第1行为空")

    # Row 2 is allowed to be blank.
    # When blank, run() will resolve field_id from Cfg__Fields using Row 1 headers.
    max_cols = max(len(r) for r in values)
    padded = [r + [""] * (max_cols - len(r)) for r in values]

    header_row = padded[0]
    field_id_row = padded[1]
    data_rows = padded[2:]

    return padded, header_row, field_id_row, data_rows


def find_effective_data_rows(data_rows: list[list[Any]]) -> list[tuple[int, list[Any]]]:
    out = []
    for i, row in enumerate(data_rows, start=3):  # actual sheet row number
        if _row_has_any_value(row):
            out.append((i, row))
    return out


def validate_field_id_row(
    effective_rows: list[tuple[int, list[Any]]],
    field_id_row: list[str],
    value_start_col_idx: int = 4,
) -> list[dict[str, Any]]:
    errors = []

    max_cols = max(len(field_id_row), max((len(r) for _, r in effective_rows), default=0))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    for sheet_row, row in effective_rows:
        row_pad = row + [""] * (max_cols - len(row))
        for col_idx in range(value_start_col_idx, max_cols):
            value = _norm_str(row_pad[col_idx])
            fid = _norm_str(field_ids[col_idx])
            if value != "" and fid == "":
                errors.append({
                    "entity_type": "METAOBJECT_ENTRY",
                    "gid": "",
                    "field_key": "",
                    "error_reason": "missing_field_id",
                    "message": f"sheet_row={sheet_row} | col={col_idx + 1} | value exists but row2 field_id is blank",
                })
    return errors


def transform_wide_to_long(
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    cfg_metaobject_lookup: dict[str, str],
    value_start_col_idx: int = 4,
    entity_type: str = "METAOBJECT_ENTRY",
) -> tuple[pd.DataFrame, int]:
    records = []
    input_value_cells = 0

    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(r) for _, r in effective_rows), default=0),
    )
    headers = header_row + [""] * (max_cols - len(header_row))
    field_ids = field_id_row + [""] * (max_cols - len(field_id_row))

    for sheet_row, row in effective_rows:
        row_pad = row + [""] * (max_cols - len(row))

        op = _norm_str(row_pad[0]) if max_cols > 0 else ""
        entry_type = _norm_str(row_pad[1]) if max_cols > 1 else ""
        handle = _norm_str(row_pad[2]) if max_cols > 2 else ""
        mode = _norm_str(row_pad[3]) if max_cols > 3 else ""

        for col_idx in range(value_start_col_idx, max_cols):
            value = _norm_str(row_pad[col_idx])
            if value == "":
                continue

            field_id = resolve_field_id_for_cell(
                header=headers[col_idx],
                entry_type=entry_type,
                existing_field_id=field_ids[col_idx],
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )

            input_value_cells += 1
            records.append({
                "op": op,
                "entry_type": entry_type,
                "handle": handle,
                "mode": mode,
                "field_id": field_id,
                "value": value,
                "slot": "",
                "note": "",
                "_source_sheet_row": sheet_row,
            })

    df_long = pd.DataFrame(records)

    if df_long.empty:
        return pd.DataFrame(columns=["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"]), input_value_cells

    df_long = df_long.drop_duplicates(
        subset=["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"],
        keep="first",
    ).copy()

    df_long = df_long[["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"]].reset_index(drop=True)
    return df_long, input_value_cells


def overwrite_long_sheet(ws_long, df_long: pd.DataFrame):
    header = ["op", "entry_type", "handle", "mode", "field_id", "value", "slot", "note"]
    values = [header] + df_long.fillna("").astype(str).values.tolist()

    ws_long.clear()
    ws_long.update("A1:H1", [header])

    if len(values) > 1:
        ws_long.update(f"A2:H{len(values)}", values[1:])


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = "entries_create_wide_to_long",

    gsheet_sa_b64_secret: str,
    console_core_url: str,

    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Entries_Create-Wide",

    output_sheet_label: str = "pre_edit",
    output_worksheet_title: str = "Entries_Create-Long",

    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",

    cfg_fields_sheet_label: str = "config",
    cfg_fields_worksheet_title: str = "Cfg__Fields",
    cfg_field_match_entity_type: str = "METAOBJECT_ENTRY",

    cfg_metaobject_defs_sheet_label: str = "config",
    cfg_metaobject_defs_worksheet_title: str = "Cfg__MetaobjectDefs",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    run_id: Optional[str] = None,
    detail_max_per_reason: int = 2,
) -> dict[str, Any]:
    run_id = run_id or _utc_run_id("entries_create_wide_to_long")

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

    runlog_sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=runlog_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_cfg_fields, cfg_fields_sheet_url = open_ws_optional_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_fields_sheet_label,
        worksheet_title=cfg_fields_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )
    cfg_fields_df = load_cfg_fields(ws_cfg_fields)

    _, ws_cfg_metaobject_defs, cfg_metaobject_defs_sheet_url = open_ws_optional_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_metaobject_defs_sheet_label,
        worksheet_title=cfg_metaobject_defs_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )
    cfg_metaobject_defs_df = load_cfg_metaobject_defs(ws_cfg_metaobject_defs)
    cfg_metaobject_lookup = build_cfg_metaobject_defs_lookup(
        cfg_metaobject_defs_df,
        entity_type=cfg_field_match_entity_type,
    )

    logger = RunLogger(
        gc=gc,
        runlog_sheet_url=runlog_sheet_url,
        runlog_tab_name=runlog_tab_name,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
    )

    padded, header_row, field_id_row, data_rows = load_wide_sheet(ws_wide)

    if not _row_has_any_value(field_id_row):
        field_id_row = resolve_blank_field_id_row_from_cfg_fields(
            header_row=header_row,
            field_id_row=field_id_row,
            cfg_fields_df=cfg_fields_df,
            value_start_col_idx=4,
            entity_type=cfg_field_match_entity_type,
        )

    effective_rows = find_effective_data_rows(data_rows)

    rows_loaded = len(effective_rows)
    rows_pending = rows_loaded

    field_id_errors = validate_resolved_field_ids_for_values(
        effective_rows=effective_rows,
        header_row=header_row,
        field_id_row=field_id_row,
        cfg_metaobject_lookup=cfg_metaobject_lookup,
        value_start_col_idx=4,
        entity_type=cfg_field_match_entity_type,
    )

    if field_id_errors:
        rows_recognized = 0
        rows_planned = 0
        rows_written = 0
        rows_skipped = len(field_id_errors)

        logger.log_row(
            phase="transform",
            log_type="summary",
            status="ERROR",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            message=f"Field_id validation failed | errors={len(field_id_errors)}",
            error_reason="unmatched_field_header",
        )
        log_grouped_details(
            logger,
            phase="transform",
            status="FAIL",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            detail_rows=field_id_errors,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()

        return {
            "status": "ERROR",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": 0,
                "rows_planned": 0,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
                "error_count": len(field_id_errors),
            },
            "warnings": [
                {
                    "type": "unmatched_field_header",
                    "count": len(field_id_errors),
                    "examples": field_id_errors[: min(10, len(field_id_errors))],
                }
            ],
            "preview": [],
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "wide_sheet_url": wide_sheet_url,
                "long_sheet_url": long_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "cfg_fields_sheet_url": cfg_fields_sheet_url,
                "cfg_metaobject_defs_sheet_url": cfg_metaobject_defs_sheet_url,
            },
        }

    df_long, input_value_cells = transform_wide_to_long(
        effective_rows=effective_rows,
        header_row=header_row,
        field_id_row=field_id_row,
        cfg_metaobject_lookup=cfg_metaobject_lookup,
        value_start_col_idx=4,
        entity_type=cfg_field_match_entity_type,
    )

    rows_recognized = rows_loaded
    rows_planned = len(df_long)
    rows_written = len(df_long)
    rows_skipped = max(0, input_value_cells - len(df_long))

    overwrite_long_sheet(ws_long, df_long)

    logger.log_row(
        phase="transform",
        log_type="summary",
        status="SUCCESS",
        rows_loaded=rows_loaded,
        rows_pending=rows_pending,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message=(
            f"Wide to Long completed | "
            f"rows_loaded={rows_loaded} | "
            f"input_value_cells={input_value_cells} | "
            f"rows_written={rows_written} | "
            f"dedup_skipped={rows_skipped}"
        ),
        error_reason="",
    )
    logger.flush()

    preview = df_long.head(20).to_dict("records") if not df_long.empty else []

    return {
        "status": "SUCCESS",
        "summary": {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_pending,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_written": rows_written,
            "rows_skipped": rows_skipped,
            "error_count": 0,
        },
        "warnings": [],
        "preview": preview,
        "meta": {
            "site_code": site_code,
            "job_name": job_name,
            "run_id": run_id,
            "wide_sheet_url": wide_sheet_url,
            "long_sheet_url": long_sheet_url,
            "runlog_sheet_url": runlog_sheet_url,
        },
    }
