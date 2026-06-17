# shopify_pre_edit/entries_update_wide_to_long.py

from __future__ import annotations

import base64
import datetime as dt
import json
import random
import re
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

OUTPUT_HEADER = [
    "op",
    "entry_type",
    "entry_gid",
    "mode",
    "field_id",
    "value",
    "new_handle",
    "slot",
    "note",
]

OWNER_COLUMN_CANDIDATES = [
    "entry_gid",
    "gid",
    "gid_or_handle",
    "lookup_handle",
    "handle",
]

CONTROL_COLUMN_NAMES = {
    "op",
    "action",
    "entry_type",
    "mode",
    "note",
    "run_id",
    "new_handle",
    *OWNER_COLUMN_CANDIDATES,
}


# =========================================================
# Utils
# =========================================================

def _make_run_id(prefix: str = "entries_update_w2l") -> str:
    return dt.datetime.now(dt.timezone.utc).strftime(f"{prefix}_%Y%m%d_%H%M%S")


def _now_cn_str() -> str:
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def _norm_header(x: Any) -> str:
    return _norm_str(x).lower()


def _norm_lookup_key(x: Any) -> str:
    s = _norm_str(x).lower()
    s = s.replace("｜", "|").replace("–", "-").replace("—", "-")
    return " ".join(s.split())


def _row_has_any_value(values: list[Any]) -> bool:
    return any(_norm_str(v) != "" for v in values)


def _pad_row(row: list[Any], length: int) -> list[Any]:
    return list(row) + [""] * max(0, length - len(row))


def _safe_cell(row: list[Any], idx: Optional[int]) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return _norm_str(row[idx])


# =========================================================
# Auth / clients
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


# =========================================================
# Sheet routing
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

    for col in ["site_code", "label", "sheet_url"]:
        if col not in df.columns:
            raise ValueError(f"{cfg_sites_tab} missing required column: {col}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    matched = df[
        (df["site_code"] == site_code.strip().upper())
        & (df["label"] == label.strip())
    ].copy()
    matched = matched[matched["sheet_url"] != ""]

    if matched.empty:
        raise ValueError(
            f"Cannot find sheet_url for site_code={site_code}, label={label} in {cfg_sites_tab}"
        )

    return matched.iloc[0]["sheet_url"]


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
# RunLog
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
    ) -> None:
        self._buf.append(
            [
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
                str(message)[:1000],
                str(error_reason)[:250],
            ]
        )
        if len(self._buf) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return

        for attempt in range(8):
            try:
                self.ws.append_rows(
                    self._buf,
                    value_input_option="RAW",
                    table_range="A:R",
                )
                self._buf = []
                return
            except Exception:
                time.sleep(min(2**attempt, 20) + random.random())

        raise RuntimeError("Failed to write Ops__RunLog after retries")


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
) -> None:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        reason = _norm_str(row.get("error_reason")) or "unknown"
        grouped[reason].append(row)

    for reason, items in grouped.items():
        for row in items[:max_per_reason]:
            logger.log_row(
                phase=phase,
                log_type="detail",
                status=status,
                entity_type=_norm_str(row.get("entity_type")) or "METAOBJECT_ENTRY",
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
# Config: Cfg__Fields
# =========================================================

def load_cfg_fields(ws_cfg_fields) -> pd.DataFrame:
    rows = ws_cfg_fields.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    for col in ["field_id", "field_key"]:
        if col not in df.columns:
            raise ValueError(f"Cfg__Fields missing required column: {col}")

    optional_cols = [
        "field_handle",
        "display_name",
        "entity_type",
        "source_type",
        "namespace",
        "key",
        "data_type",
        "field_type",
    ]
    for col in optional_cols:
        if col not in df.columns:
            df[col] = ""

    for col in ["field_id", "field_key", *optional_cols]:
        df[col] = df[col].map(_norm_str)

    df = df[df["field_id"] != ""].copy()
    return df


def build_cfg_fields_lookup(
    cfg_fields_df: pd.DataFrame,
    entity_type: str = "METAOBJECT_ENTRY",
) -> dict[str, str]:
    df = cfg_fields_df.copy()

    if entity_type:
        entity_match = (
            df["entity_type"].map(_norm_str).str.upper()
            == entity_type.strip().upper()
        )
        if entity_match.any():
            df = df[entity_match].copy()

    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    candidate_cols = [
        "field_id",
        "field_key",
        "field_handle",
        "display_name",
        "key",
    ]

    for _, row in df.iterrows():
        field_id = _norm_str(row.get("field_id"))
        if not field_id:
            continue

        for col in candidate_cols:
            key = _norm_lookup_key(row.get(col))
            if not key:
                continue

            if key in lookup and lookup[key] != field_id:
                collisions[key].update([lookup[key], field_id])
            else:
                lookup[key] = field_id

    # Ambiguous short names are not guessed globally.
    for key in collisions:
        lookup.pop(key, None)

    return lookup


def build_field_maps(
    cfg_fields_df: pd.DataFrame,
) -> tuple[dict[str, str], dict[str, str]]:
    field_id_to_key: dict[str, str] = {}
    field_id_to_entity: dict[str, str] = {}

    for row in cfg_fields_df.to_dict("records"):
        field_id = _norm_str(row.get("field_id"))
        field_key = _norm_str(row.get("field_key"))
        entity_type = _norm_str(row.get("entity_type")).upper()

        if field_id:
            field_id_to_key[field_id] = field_key
            field_id_to_entity[field_id] = entity_type

    return field_id_to_key, field_id_to_entity


# =========================================================
# Config: Cfg__MetaobjectDefs
# =========================================================

def load_cfg_metaobject_defs(ws_cfg_metaobject_defs) -> pd.DataFrame:
    rows = ws_cfg_metaobject_defs.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Cfg__MetaobjectDefs is empty")

    for col in ["type", "field_key"]:
        if col not in df.columns:
            raise ValueError(f"Cfg__MetaobjectDefs missing required column: {col}")

    for col in ["type", "type_name", "field_key", "field_name", "field_type", "gid"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(_norm_str)

    df = df[(df["type"] != "") & (df["field_key"] != "")].copy()
    return df


def build_cfg_metaobject_defs_lookup(
    cfg_metaobject_defs_df: pd.DataFrame,
    entity_type: str = "METAOBJECT_ENTRY",
) -> dict[str, str]:
    """
    Key: normalized metaobject type + "|" + normalized field header candidate.
    Value: METAOBJECT_ENTRY|mo.{type}.{field_key}
    """
    lookup: dict[str, str] = {}
    collisions: dict[str, set[str]] = defaultdict(set)

    for _, row in cfg_metaobject_defs_df.iterrows():
        metaobject_type = _norm_str(row.get("type"))
        field_key = _norm_str(row.get("field_key"))
        if not metaobject_type or not field_key:
            continue

        field_id = f"{entity_type}|mo.{metaobject_type}.{field_key}"
        candidates = [
            field_key,
            row.get("field_name"),
            f"mo.{metaobject_type}.{field_key}",
            field_id,
        ]

        for candidate in candidates:
            candidate_key = _norm_lookup_key(candidate)
            if not candidate_key:
                continue

            lookup_key = f"{_norm_lookup_key(metaobject_type)}|{candidate_key}"
            if lookup_key in lookup and lookup[lookup_key] != field_id:
                collisions[lookup_key].update([lookup[lookup_key], field_id])
            else:
                lookup[lookup_key] = field_id

    for key in collisions:
        lookup.pop(key, None)

    return lookup


# =========================================================
# Wide sheet parsing
# =========================================================

def load_wide_sheet(
    ws_input,
) -> tuple[list[list[Any]], list[str], list[str], list[list[Any]]]:
    values = ws_input.get_all_values()

    if not values:
        raise ValueError("❌ Wide sheet is empty")
    if len(values) < 2:
        raise ValueError(
            "❌ Wide sheet must have at least 2 rows: header row + field_id row"
        )

    if not _row_has_any_value(values[0]):
        raise ValueError("❌ Wide 第1行为空")

    max_cols = max(len(row) for row in values)
    padded = [_pad_row(row, max_cols) for row in values]

    header_row = [_norm_str(x) for x in padded[0]]
    field_id_row = [_norm_str(x) for x in padded[1]]
    data_rows = padded[2:]

    return padded, header_row, field_id_row, data_rows


def find_effective_data_rows(
    data_rows: list[list[Any]],
) -> list[tuple[int, list[Any]]]:
    result: list[tuple[int, list[Any]]] = []

    for sheet_row, row in enumerate(data_rows, start=3):
        if _row_has_any_value(row):
            result.append((sheet_row, row))

    return result


def _build_header_positions(header_row: list[str]) -> dict[str, list[int]]:
    positions: dict[str, list[int]] = defaultdict(list)
    for idx, header in enumerate(header_row):
        normalized = _norm_header(header)
        if normalized:
            positions[normalized].append(idx)
    return dict(positions)


def detect_layout(header_row: list[str]) -> dict[str, Any]:
    positions = _build_header_positions(header_row)

    duplicated_headers = {
        name: indexes
        for name, indexes in positions.items()
        if len(indexes) > 1
    }
    if duplicated_headers:
        raise ValueError(
            "❌ Wide 第1行存在重复表头: "
            + ", ".join(
                f"{name} at columns {[i + 1 for i in indexes]}"
                for name, indexes in sorted(duplicated_headers.items())
            )
        )

    op_idx = positions.get("op", [None])[0]
    action_idx = positions.get("action", [None])[0]
    if op_idx is None and action_idx is None:
        raise ValueError("❌ 未找到 op/action 列。需要存在 op 或 action 之一")

    owner_columns: list[tuple[str, int]] = []
    for candidate in OWNER_COLUMN_CANDIDATES:
        indexes = positions.get(candidate, [])
        if indexes:
            owner_columns.append((candidate, indexes[0]))

    if not owner_columns:
        raise ValueError(
            "❌ 未找到 owner 列。需要存在 entry_gid / gid / gid_or_handle / lookup_handle / handle 之一"
        )

    fixed_indexes: set[int] = set()
    for name in CONTROL_COLUMN_NAMES:
        indexes = positions.get(name, [])
        fixed_indexes.update(indexes)

    field_col_indexes = [
        idx
        for idx, header in enumerate(header_row)
        if _norm_str(header) != "" and idx not in fixed_indexes
    ]

    if not field_col_indexes:
        raise ValueError(
            "❌ 未找到可转成长表的字段列。控制列之外至少需要一个字段列"
        )

    return {
        "op_idx": op_idx,
        "action_idx": action_idx,
        "op_input_columns": [
            name
            for name, idx in [("op", op_idx), ("action", action_idx)]
            if idx is not None
        ],
        "owner_columns": owner_columns,
        "owner_col": owner_columns[0][0],
        "entry_type_idx": positions.get("entry_type", [None])[0],
        "mode_idx": positions.get("mode", [None])[0],
        "note_idx": positions.get("note", [None])[0],
        "run_id_idx": positions.get("run_id", [None])[0],
        "new_handle_idx": positions.get("new_handle", [None])[0],
        "fixed_indexes": fixed_indexes,
        "field_col_indexes": field_col_indexes,
    }


def resolve_input_op(
    row: list[Any],
    *,
    sheet_row: int,
    op_idx: Optional[int],
    action_idx: Optional[int],
) -> tuple[str, Optional[dict[str, Any]]]:
    op_value = _safe_cell(row, op_idx).upper()
    action_value = _safe_cell(row, action_idx).upper()

    if op_value and action_value and op_value != action_value:
        return "", {
            "entity_type": "METAOBJECT_ENTRY",
            "gid": "",
            "field_key": "op",
            "error_reason": "op_action_conflict",
            "message": (
                f"sheet_row={sheet_row} | op={op_value} | action={action_value} | "
                "op and action conflict"
            ),
        }

    return op_value or action_value, None


def resolve_owner_value(
    row: list[Any],
    owner_columns: list[tuple[str, int]],
) -> tuple[str, str]:
    for name, idx in owner_columns:
        value = _safe_cell(row, idx)
        if value:
            return value, name
    return "", ""


# =========================================================
# Field-id resolution
# =========================================================

def resolve_blank_field_id_row_from_cfg_fields(
    *,
    header_row: list[str],
    field_id_row: list[str],
    cfg_fields_df: pd.DataFrame,
    field_col_indexes: list[int],
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[str]:
    max_cols = max(len(header_row), len(field_id_row))
    headers = _pad_row(header_row, max_cols)
    field_ids = _pad_row(field_id_row, max_cols)

    lookup = build_cfg_fields_lookup(
        cfg_fields_df,
        entity_type=entity_type,
    )

    for col_idx in field_col_indexes:
        if _norm_str(field_ids[col_idx]):
            continue

        header = _norm_str(headers[col_idx])
        if not header:
            continue

        matched = lookup.get(_norm_lookup_key(header), "")
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

    header_value = _norm_str(header)
    entry_type_value = _norm_str(entry_type)
    if not header_value or not entry_type_value:
        return ""

    lookup_key = (
        f"{_norm_lookup_key(entry_type_value)}|"
        f"{_norm_lookup_key(header_value)}"
    )
    matched = cfg_metaobject_lookup.get(lookup_key, "")
    if matched:
        return matched

    # Same safe deterministic fallback used by Entries_Create.
    simple = _norm_str(header_value)
    if simple and all(ch.isalnum() or ch == "_" for ch in simple):
        return f"{entity_type}|mo.{entry_type_value}.{simple}"

    return ""


def resolve_field_id_row_from_pending_rows(
    *,
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    layout: dict[str, Any],
    cfg_metaobject_lookup: dict[str, str],
    include_empty: bool,
    entity_type: str = "METAOBJECT_ENTRY",
) -> list[str]:
    """
    Fill Row 2 only when all relevant pending rows resolve a column to one field_id.
    Mixed entry types may legitimately require different field_ids, in which case Row 2
    remains blank and each cell is resolved row-by-row during transformation.
    """
    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(row) for _, row in effective_rows), default=0),
    )
    headers = _pad_row(header_row, max_cols)
    field_ids = _pad_row(field_id_row, max_cols)

    pending_rows: list[tuple[int, list[Any]]] = []
    for sheet_row, raw_row in effective_rows:
        row = _pad_row(raw_row, max_cols)
        op_value, conflict = resolve_input_op(
            row,
            sheet_row=sheet_row,
            op_idx=layout["op_idx"],
            action_idx=layout["action_idx"],
        )
        if conflict or op_value in ("", "SKIP"):
            continue
        pending_rows.append((sheet_row, row))

    for col_idx in layout["field_col_indexes"]:
        if _norm_str(field_ids[col_idx]):
            continue

        resolved_ids: set[str] = set()
        for _, row in pending_rows:
            value = _norm_str(row[col_idx])
            if value == "" and not include_empty:
                continue

            entry_type_value = _safe_cell(row, layout["entry_type_idx"])
            resolved = resolve_field_id_for_cell(
                header=headers[col_idx],
                entry_type=entry_type_value,
                existing_field_id="",
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )
            if resolved:
                resolved_ids.add(resolved)

        if len(resolved_ids) == 1:
            field_ids[col_idx] = next(iter(resolved_ids))

    return field_ids


def write_field_id_row_if_changed(
    ws_input,
    *,
    original_field_id_row: list[str],
    resolved_field_id_row: list[str],
) -> int:
    max_cols = max(len(original_field_id_row), len(resolved_field_id_row))
    original = _pad_row(original_field_id_row, max_cols)
    resolved = _pad_row(resolved_field_id_row, max_cols)

    changed = sum(
        1
        for before, after in zip(original, resolved)
        if _norm_str(before) != _norm_str(after)
    )
    if changed == 0:
        return 0

    end_a1 = gspread.utils.rowcol_to_a1(2, max_cols)
    ws_input.update(
        range_name=f"A2:{end_a1}",
        values=[resolved],
        value_input_option="RAW",
    )
    return changed


# =========================================================
# Validation / transformation
# =========================================================

def validate_and_transform(
    *,
    effective_rows: list[tuple[int, list[Any]]],
    header_row: list[str],
    field_id_row: list[str],
    layout: dict[str, Any],
    cfg_metaobject_lookup: dict[str, str],
    cfg_field_ids: set[str],
    include_empty: bool,
    default_mode: str = "STRICT",
    entity_type: str = "METAOBJECT_ENTRY",
) -> tuple[pd.DataFrame, dict[str, int], list[dict[str, Any]]]:
    max_cols = max(
        len(header_row),
        len(field_id_row),
        max((len(row) for _, row in effective_rows), default=0),
    )
    headers = _pad_row(header_row, max_cols)
    field_ids = _pad_row(field_id_row, max_cols)

    output_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    rows_loaded = len(effective_rows)
    rows_pending = 0
    resolved_field_cells = 0
    rows_skipped_input = 0

    for sheet_row, raw_row in effective_rows:
        row = _pad_row(raw_row, max_cols)

        op_value, op_error = resolve_input_op(
            row,
            sheet_row=sheet_row,
            op_idx=layout["op_idx"],
            action_idx=layout["action_idx"],
        )
        if op_error:
            errors.append(op_error)
            continue

        if op_value in ("", "SKIP"):
            rows_skipped_input += 1
            continue

        rows_pending += 1

        if op_value != "UPDATE":
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": "",
                    "field_key": "op",
                    "error_reason": "invalid_op",
                    "message": (
                        f"sheet_row={sheet_row} | op={op_value} | "
                        "Entries_Update only accepts UPDATE, blank, or SKIP"
                    ),
                }
            )
            continue

        owner_value, owner_source = resolve_owner_value(
            row,
            layout["owner_columns"],
        )
        if not owner_value:
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": "",
                    "field_key": "entry_gid",
                    "error_reason": "missing_owner",
                    "message": (
                        f"sheet_row={sheet_row} | no value found in owner columns "
                        f"{[name for name, _ in layout['owner_columns']]}"
                    ),
                }
            )
            continue

        entry_type_value = _safe_cell(row, layout["entry_type_idx"])
        mode_value = _safe_cell(row, layout["mode_idx"]).upper() or default_mode
        note_value = _safe_cell(row, layout["note_idx"])
        new_handle_value = _safe_cell(row, layout["new_handle_idx"])

        if mode_value not in {"STRICT", "LOOSE"}:
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": owner_value,
                    "field_key": "mode",
                    "error_reason": "invalid_mode",
                    "message": (
                        f"sheet_row={sheet_row} | mode={mode_value} | "
                        "mode must be STRICT or LOOSE"
                    ),
                }
            )
            continue

        generated_for_row: list[dict[str, Any]] = []

        for col_idx in layout["field_col_indexes"]:
            desired_value = _norm_str(row[col_idx])
            if desired_value == "" and not include_empty:
                continue

            header = _norm_str(headers[col_idx])
            field_id = resolve_field_id_for_cell(
                header=header,
                entry_type=entry_type_value,
                existing_field_id=field_ids[col_idx],
                cfg_metaobject_lookup=cfg_metaobject_lookup,
                entity_type=entity_type,
            )

            if not field_id:
                errors.append(
                    {
                        "entity_type": entity_type,
                        "gid": owner_value,
                        "field_key": header,
                        "error_reason": "unmatched_field_header",
                        "message": (
                            f"sheet_row={sheet_row} | col={col_idx + 1} | "
                            f"entry_type={entry_type_value} | header={header} | "
                            "cannot resolve field_id from Row 2 / Cfg__Fields / Cfg__MetaobjectDefs"
                        ),
                    }
                )
                continue

            if field_id not in cfg_field_ids:
                errors.append(
                    {
                        "entity_type": entity_type,
                        "gid": owner_value,
                        "field_key": header,
                        "error_reason": "field_id_not_in_cfg_fields",
                        "message": (
                            f"sheet_row={sheet_row} | col={col_idx + 1} | "
                            f"header={header} | field_id={field_id} | "
                            "resolved field_id does not exist in Cfg__Fields"
                        ),
                    }
                )
                continue

            match = re.match(r"^METAOBJECT_ENTRY\|mo\.([^.]+)\..+$", field_id)
            field_id_entry_type = match.group(1).strip() if match else ""
            if (
                entry_type_value
                and field_id_entry_type
                and entry_type_value != field_id_entry_type
            ):
                errors.append(
                    {
                        "entity_type": entity_type,
                        "gid": owner_value,
                        "field_key": header,
                        "error_reason": "field_id_entry_type_mismatch",
                        "message": (
                            f"sheet_row={sheet_row} | col={col_idx + 1} | "
                            f"entry_type={entry_type_value} | field_id={field_id} | "
                            f"field_id_entry_type={field_id_entry_type}"
                        ),
                    }
                )
                continue

            resolved_field_cells += 1
            generated_for_row.append(
                {
                    "op": "UPDATE",
                    "entry_type": entry_type_value,
                    "entry_gid": owner_value,
                    "mode": mode_value,
                    "field_id": field_id,
                    "value": desired_value,
                    "new_handle": "",
                    "slot": "",
                    "note": note_value,
                    "_source_sheet_row": sheet_row,
                    "_owner_source": owner_source,
                }
            )

        # Put new_handle on one generated row only. If no field value exists,
        # create a handle-only row accepted by edit_entries_update.py.
        if new_handle_value:
            if generated_for_row:
                generated_for_row[0]["new_handle"] = new_handle_value
            else:
                generated_for_row.append(
                    {
                        "op": "UPDATE",
                        "entry_type": entry_type_value,
                        "entry_gid": owner_value,
                        "mode": mode_value,
                        "field_id": "",
                        "value": "",
                        "new_handle": new_handle_value,
                        "slot": "",
                        "note": note_value,
                        "_source_sheet_row": sheet_row,
                        "_owner_source": owner_source,
                    }
                )

        if not generated_for_row:
            errors.append(
                {
                    "entity_type": entity_type,
                    "gid": owner_value,
                    "field_key": "",
                    "error_reason": "nothing_to_update",
                    "message": (
                        f"sheet_row={sheet_row} | UPDATE row has no field value "
                        "and no new_handle"
                    ),
                }
            )
            continue

        output_rows.extend(generated_for_row)

    df_long = pd.DataFrame(output_rows)
    rows_planned_before_dedupe = len(df_long)

    if df_long.empty:
        df_long = pd.DataFrame(columns=OUTPUT_HEADER)
        duplicate_rows = 0
    else:
        dedupe_cols = OUTPUT_HEADER
        before = len(df_long)
        df_long = (
            df_long.drop_duplicates(subset=dedupe_cols, keep="first")
            .reset_index(drop=True)
        )
        duplicate_rows = before - len(df_long)
        df_long = df_long[OUTPUT_HEADER]

    summary = {
        "rows_loaded": rows_loaded,
        "rows_pending": rows_pending,
        "rows_recognized": resolved_field_cells,
        "rows_planned": rows_planned_before_dedupe,
        "rows_written": len(df_long),
        "rows_skipped": rows_skipped_input + duplicate_rows,
        "error_count": len(errors),
    }

    return df_long, summary, errors


# =========================================================
# Sheet writes
# =========================================================

def _ensure_grid_size(ws, *, rows: int, cols: int) -> None:
    target_rows = max(rows, 1)
    target_cols = max(cols, 1)

    current_rows = int(getattr(ws, "row_count", 0) or 0)
    current_cols = int(getattr(ws, "col_count", 0) or 0)

    if current_rows < target_rows or current_cols < target_cols:
        ws.resize(
            rows=max(current_rows, target_rows),
            cols=max(current_cols, target_cols),
        )


def overwrite_long_sheet(
    ws_output,
    df_long: pd.DataFrame,
    *,
    clear_output_first: bool = True,
) -> None:
    values = [OUTPUT_HEADER] + df_long[OUTPUT_HEADER].fillna("").astype(str).values.tolist()

    _ensure_grid_size(
        ws_output,
        rows=len(values),
        cols=len(OUTPUT_HEADER),
    )

    if clear_output_first:
        ws_output.clear()

    end_a1 = gspread.utils.rowcol_to_a1(len(values), len(OUTPUT_HEADER))
    ws_output.update(
        range_name=f"A1:{end_a1}",
        values=values,
        value_input_option="RAW",
    )


# =========================================================
# Main
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = "entries_update_wide_to_long",

    gsheet_sa_b64_secret: str,
    console_core_url: str,

    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Entries_Update-Wide",

    output_sheet_label: Optional[str] = None,
    output_worksheet_title: str = "Entries_Update-Long",

    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = "Cfg__Fields",
    cfg_tab_metaobject_defs: str = "Cfg__MetaobjectDefs",
    cfg_field_match_entity_type: str = "METAOBJECT_ENTRY",

    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,

    include_empty: bool = False,
    clear_output_first: bool = True,
    write_resolved_field_ids: bool = True,
    default_mode: str = "STRICT",
    detail_max_per_reason: int = 2,

    run_id: Optional[str] = None,
) -> dict[str, Any]:
    run_id = run_id or _make_run_id()
    output_sheet_label = output_sheet_label or input_sheet_label
    default_mode = _norm_str(default_mode).upper() or "STRICT"

    if default_mode not in {"STRICT", "LOOSE"}:
        raise ValueError("default_mode must be STRICT or LOOSE")

    gc = build_gsheet_client(gsheet_sa_b64_secret)

    _, ws_input, input_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )

    _, ws_output, output_sheet_url = open_ws_by_label_and_title(
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

    _, ws_cfg_metaobject_defs, cfg_metaobject_defs_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_sheet_label,
        worksheet_title=cfg_tab_metaobject_defs,
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

    rows_loaded = 0
    rows_pending = 0
    rows_recognized = 0
    rows_planned = 0
    rows_written = 0
    rows_skipped = 0

    try:
        cfg_fields_df = load_cfg_fields(ws_cfg_fields)
        cfg_field_ids = set(cfg_fields_df["field_id"].map(_norm_str).tolist())

        cfg_metaobject_defs_df = load_cfg_metaobject_defs(ws_cfg_metaobject_defs)
        cfg_metaobject_lookup = build_cfg_metaobject_defs_lookup(
            cfg_metaobject_defs_df,
            entity_type=cfg_field_match_entity_type,
        )

        _, header_row, original_field_id_row, data_rows = load_wide_sheet(ws_input)
        layout = detect_layout(header_row)
        effective_rows = find_effective_data_rows(data_rows)

        resolved_field_id_row = resolve_blank_field_id_row_from_cfg_fields(
            header_row=header_row,
            field_id_row=original_field_id_row,
            cfg_fields_df=cfg_fields_df,
            field_col_indexes=layout["field_col_indexes"],
            entity_type=cfg_field_match_entity_type,
        )
        resolved_field_id_row = resolve_field_id_row_from_pending_rows(
            effective_rows=effective_rows,
            header_row=header_row,
            field_id_row=resolved_field_id_row,
            layout=layout,
            cfg_metaobject_lookup=cfg_metaobject_lookup,
            include_empty=include_empty,
            entity_type=cfg_field_match_entity_type,
        )

        df_long, summary, validation_errors = validate_and_transform(
            effective_rows=effective_rows,
            header_row=header_row,
            field_id_row=resolved_field_id_row,
            layout=layout,
            cfg_metaobject_lookup=cfg_metaobject_lookup,
            cfg_field_ids=cfg_field_ids,
            include_empty=include_empty,
            default_mode=default_mode,
            entity_type=cfg_field_match_entity_type,
        )

        rows_loaded = summary["rows_loaded"]
        rows_pending = summary["rows_pending"]
        rows_recognized = summary["rows_recognized"]
        rows_planned = summary["rows_planned"]
        rows_written = 0
        rows_skipped = summary["rows_skipped"]

        if validation_errors:
            rows_skipped += len(validation_errors)
            summary["rows_written"] = 0
            summary["rows_skipped"] = rows_skipped

            logger.log_row(
                phase="transform",
                log_type="summary",
                status="ERROR",
                rows_loaded=rows_loaded,
                rows_pending=rows_pending,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=0,
                rows_skipped=rows_skipped,
                message=f"Validation failed | errors={len(validation_errors)}",
                error_reason="validation_failed",
            )
            log_grouped_details(
                logger,
                phase="transform",
                status="FAIL",
                rows_loaded=rows_loaded,
                rows_pending=rows_pending,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=0,
                rows_skipped=rows_skipped,
                detail_rows=validation_errors,
                max_per_reason=detail_max_per_reason,
            )
            logger.flush()

            return {
                "status": "ERROR",
                "summary": summary,
                "warnings": [
                    {
                        "type": "validation_failed",
                        "count": len(validation_errors),
                        "examples": validation_errors[: min(20, len(validation_errors))],
                    }
                ],
                "preview": [],
                "meta": {
                    "site_code": site_code,
                    "job_name": job_name,
                    "run_id": run_id,
                    "input_sheet_url": input_sheet_url,
                    "output_sheet_url": output_sheet_url,
                    "cfg_sheet_url": cfg_sheet_url,
                    "cfg_metaobject_defs_sheet_url": cfg_metaobject_defs_sheet_url,
                    "runlog_sheet_url": runlog_sheet_url,
                    "op_input_columns": layout["op_input_columns"],
                    "owner_col": layout["owner_col"],
                    "owner_columns": [name for name, _ in layout["owner_columns"]],
                    "field_id_row_written": 0,
                },
            }

        field_id_row_written = 0
        if write_resolved_field_ids:
            field_id_row_written = write_field_id_row_if_changed(
                ws_input,
                original_field_id_row=original_field_id_row,
                resolved_field_id_row=resolved_field_id_row,
            )

        overwrite_long_sheet(
            ws_output,
            df_long,
            clear_output_first=clear_output_first,
        )

        rows_written = len(df_long)
        summary["rows_written"] = rows_written
        summary["field_ids_written_to_row2"] = field_id_row_written

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
                "Entries_Update wide->long completed | "
                f"op_inputs={layout['op_input_columns']} | "
                f"owner_col={layout['owner_col']} | "
                f"field_ids_written_to_row2={field_id_row_written} | "
                f"input_ws={input_worksheet_title} | "
                f"output_ws={output_worksheet_title}"
            ),
            error_reason="",
        )
        logger.flush()

        preview = df_long.head(50).to_dict("records") if not df_long.empty else []

        return {
            "status": "SUCCESS",
            "summary": summary,
            "warnings": [],
            "preview": preview,
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "cfg_metaobject_defs_sheet_url": cfg_metaobject_defs_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "op_input_columns": layout["op_input_columns"],
                "owner_col": layout["owner_col"],
                "owner_columns": [name for name, _ in layout["owner_columns"]],
                "field_id_row_written": field_id_row_written,
                "output_columns": OUTPUT_HEADER,
            },
        }

    except Exception as exc:
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
            message=str(exc),
            error_reason=type(exc).__name__,
        )
        logger.flush()
        raise
