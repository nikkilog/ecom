# shopify_pre_edit/wide_to_metafieldblocks.py

from __future__ import annotations

import base64
import datetime as dt
import json
import re
from dataclasses import dataclass
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
CFG_FIELDS_TAB_DEFAULT = "Cfg__Fields"

JOB_NAME = "wide_to_metafieldblocks"

WIDE_INPUT_TAB_DEFAULT = "Wide_MFBs"
LONG_OUTPUT_TAB_DEFAULT = "Long_MFBs"

LONG_HEADER = [
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

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "VARIANT", "COLLECTION", "PAGE"}

DEFAULT_IGNORE_COLUMNS = {
    "",
    "来源",
    "source",
    "SKU",
    "sku",
    "Product ID (numeric)",
    "Product ID",
    "Product GID",
    "gid_or_handle",
    "entity_type",
    "Variant ID (numeric)",
    "Variant ID",
    "Collection ID (numeric)",
    "Page ID (numeric)",
    "Variant Base",
    "Variant Root",
    "Product Title",
    "Handle",
    "handle",
    "note",
    "Note",
    "备注",
}

SPLIT_COMPAT_SEPARATORS = (";", "|", "\n")


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


def _norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    return s


def _norm_key(x: Any) -> str:
    return re.sub(r"\s+", " ", _norm_str(x)).strip().lower()


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _safe_int(x: Any) -> Optional[int]:
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
        v = _norm_str(item)
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _split_list_cell(value: Any) -> list[str]:
    s = _norm_str(value)
    if not s:
        return []

    # JSON array compatibility, only for legacy/automation input.
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [_norm_str(x) for x in arr if _norm_str(x)]
        except Exception:
            pass

    parts = [s]
    for sep in SPLIT_COMPAT_SEPARATORS:
        new_parts = []
        for p in parts:
            new_parts.extend(p.split(sep))
        parts = new_parts

    return [_norm_str(p) for p in parts if _norm_str(p)]


def _has_legacy_separator(value: Any) -> bool:
    s = _norm_str(value)
    return any(sep in s for sep in SPLIT_COMPAT_SEPARATORS)


def _col_to_a1(col_num: int) -> str:
    if col_num <= 0:
        raise ValueError(f"Invalid column number: {col_num}")
    result = ""
    n = col_num
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


# =========================================================
# Google auth / routing
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

    return m.iloc[0]["sheet_url"]


def open_ws_by_label_and_title(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    worksheet_title: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    create_if_missing: bool = False,
    default_rows: int = 1000,
    default_cols: int = 30,
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
            rows=default_rows,
            cols=default_cols,
        )

    return sh, ws, sheet_url


def load_account_config(
    gc_console: gspread.Client,
    console_core_url: str,
    site_code: str,
    config_sheet_label: str = "config",  # kept for backward-compatible function signature; not used
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
) -> dict[str, str]:
    """
    Read account/runtime secrets from Console Core / Cfg__account_id.

    Important:
      Cfg__account_id belongs to Console Core itself.
      Do NOT route it through sheet_label=config.
      sheet_label=config is only for business config tabs such as Cfg__Fields.
    """
    sh_console = gc_console.open_by_url(console_core_url)
    ws = sh_console.worksheet(cfg_account_tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"{cfg_account_tab} is empty in Console Core")

    lower_cols = {str(c).lower().strip(): c for c in df.columns}

    # key/value style
    if "key" in lower_cols and "value" in lower_cols:
        k_col = lower_cols["key"]
        v_col = lower_cols["value"]
        out = {}
        for r in df.to_dict("records"):
            k = _norm_str(r.get(k_col))
            v = _norm_str(r.get(v_col))
            if k:
                out[k] = v
        return out

    # config_key/config_value style
    if "config_key" in lower_cols and "config_value" in lower_cols:
        k_col = lower_cols["config_key"]
        v_col = lower_cols["config_value"]
        out = {}
        for r in df.to_dict("records"):
            k = _norm_str(r.get(k_col))
            v = _norm_str(r.get(v_col))
            if k:
                out[k] = v
        return out

    # fallback: first two columns, but explicitly report the assumption
    if len(df.columns) >= 2:
        k_col, v_col = df.columns[:2]
        out = {}
        for r in df.to_dict("records"):
            k = _norm_str(r.get(k_col))
            v = _norm_str(r.get(v_col))
            if k:
                out[k] = v
        if out:
            return out

    raise ValueError(
        f"{cfg_account_tab} in Console Core must contain key/value or config_key/config_value columns. "
        f"Found columns: {list(df.columns)}"
    )

# =========================================================
# Cfg__Fields dictionary
# =========================================================

@dataclass
class FieldDef:
    display_name: str
    field_key: str
    data_type: str
    entity_type: str
    source_type: str


def load_cfg_fields(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    config_sheet_label: str = "config",
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> pd.DataFrame:
    _, ws, _ = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=config_sheet_label,
        worksheet_title=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
        create_if_missing=False,
    )

    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(f"{cfg_tab_fields} is empty")

    for c in ["display_name", "field_key", "data_type", "entity_type", "source_type"]:
        if c not in df.columns:
            raise ValueError(f"{cfg_tab_fields} missing required column: {c}")

    d = df.copy()
    d["display_name"] = d["display_name"].astype(str).str.strip()
    d["field_key"] = d["field_key"].astype(str).str.strip()
    d["data_type"] = d["data_type"].astype(str).str.strip().str.lower()
    d["entity_type"] = d["entity_type"].astype(str).str.strip().str.upper()
    d["source_type"] = d["source_type"].astype(str).str.strip().str.upper()

    d = d[
        (d["display_name"] != "")
        & (d["field_key"] != "")
        & (
            d["source_type"].eq("METAFIELD")
            | d["field_key"].str.startswith("mf.")
            | d["field_key"].str.startswith("v_mf.")
        )
    ].copy()

    return d


def build_cfg_display_map(
    cfg_fields: pd.DataFrame,
    target_entity_type: str = "PRODUCT",
) -> dict[str, FieldDef]:
    """
    Build display_name -> FieldDef mapping for the current owner entity type.

    Cfg__Fields may legitimately contain the same display_name for different owners,
    for example:
      PRODUCT / Variant Base -> mf.custom.variant_base
      VARIANT / Variant Base -> v_mf.custom.variant_base

    That is not an error. For Wide_MFBs, the mapping must be unique only within
    the target entity_type currently being generated.
    """
    d = cfg_fields.copy()

    target_entity_type = _norm_str(target_entity_type).upper() or "PRODUCT"

    if "entity_type" not in d.columns:
        raise ValueError("Cfg__Fields missing required column: entity_type")

    d["entity_type"] = d["entity_type"].astype(str).str.strip().str.upper()
    d = d[d["entity_type"].eq(target_entity_type)].copy()

    if d.empty:
        raise ValueError(f"Cfg__Fields has no rows for entity_type={target_entity_type}")

    d["_display_key"] = d["display_name"].apply(_norm_key)

    dup = d[d.duplicated("_display_key", keep=False)].copy()
    if not dup.empty:
        examples = dup[["display_name", "field_key", "data_type", "entity_type"]].head(50).to_dict("records")
        raise ValueError({
            "message": (
                "Cfg__Fields has duplicate display_name within the same entity_type. "
                "For Wide_MFBs mapping, display_name must be unique after filtering by entity_type."
            ),
            "target_entity_type": target_entity_type,
            "examples": examples,
        })

    out = {}
    for r in d.to_dict("records"):
        out[r["_display_key"]] = FieldDef(
            display_name=_norm_str(r.get("display_name")),
            field_key=_norm_str(r.get("field_key")),
            data_type=_norm_str(r.get("data_type")).lower(),
            entity_type=_norm_str(r.get("entity_type")).upper(),
            source_type=_norm_str(r.get("source_type")).upper(),
        )
    return out


def lookup_field_def(
    cfg_map: dict[str, FieldDef],
    display_name: str,
) -> Optional[FieldDef]:
    return cfg_map.get(_norm_key(display_name))


# =========================================================
# Wide header parsing
# =========================================================

@dataclass
class ParsedWideColumn:
    source_col: str
    display_name: str
    block_seq: int
    role: str  # "", "title", "body"
    field_def: FieldDef


def parse_number_suffix(header: str) -> tuple[str, Optional[int]]:
    """
    Display Name-1 / Display Name 1 -> (Display Name, 1)
    Suffix number is block_seq, never part of Cfg__Fields.display_name.
    """
    h = _norm_str(header)
    if not h:
        return "", None

    m = re.match(r"^(.*?)[\s_-]+(\d+)$", h)
    if not m:
        return h, None

    base = _norm_str(m.group(1))
    seq = _safe_int(m.group(2))
    return base, seq


def parse_role_from_display(base: str) -> tuple[str, str]:
    """
    PDP Feature Title -> (PDP Feature, title)
    PDP Feature Body  -> (PDP Feature, body)

    Exact Cfg__Fields display_name is tried before this role stripping.
    """
    b = _norm_str(base)
    m = re.match(r"^(.*?)[\s_-]+(Title|Body)$", b, flags=re.I)
    if not m:
        return b, ""

    parent = _norm_str(m.group(1))
    role = _norm_str(m.group(2)).lower()
    return parent, role


def classify_block_type(
    data_type: str,
    role: str,
    rich_text_default_block_type: str = "bullet",
) -> str:
    dt_ = _norm_str(data_type).lower()
    role = _norm_str(role).lower()

    if dt_.startswith("list."):
        return "list_item"

    if dt_ == "rich_text_field":
        if role in {"title", "body"}:
            return "feature"
        return rich_text_default_block_type

    return ""


def parse_wide_columns(
    columns: list[str],
    cfg_map: dict[str, FieldDef],
    ignore_columns: Optional[set[str]] = None,
    rich_text_default_block_type: str = "bullet",
    error_on_unsupported_datatype: bool = True,
) -> tuple[list[ParsedWideColumn], list[dict[str, Any]], list[dict[str, Any]]]:
    ignore = set(DEFAULT_IGNORE_COLUMNS)
    if ignore_columns:
        ignore.update(ignore_columns)

    parsed = []
    errors = []
    warnings = []

    for col in columns:
        col_s = _norm_str(col)
        if not col_s or col_s in ignore:
            continue

        base, seq = parse_number_suffix(col_s)
        if seq is None:
            # This flow is for block/list fields. Single fields should not silently enter this flow.
            if col_s in ignore:
                continue
            # Try exact config only to give a clearer unsupported/single error.
            fd_exact = lookup_field_def(cfg_map, col_s)
            if fd_exact:
                block_type = classify_block_type(
                    fd_exact.data_type,
                    role="",
                    rich_text_default_block_type=rich_text_default_block_type,
                )
                if block_type:
                    errors.append({
                        "source_col": col_s,
                        "error_reason": "missing_block_seq_suffix",
                        "message": "MFB wide columns must use -1/-2/-3 suffix. This column maps to a block/list field but has no sequence suffix.",
                    })
                elif error_on_unsupported_datatype:
                    errors.append({
                        "source_col": col_s,
                        "display_name": fd_exact.display_name,
                        "field_key": fd_exact.field_key,
                        "data_type": fd_exact.data_type,
                        "error_reason": "unsupported_data_type_for_mfb",
                        "message": "This field is not list.* or rich_text_field; use the normal single-value flow.",
                    })
            else:
                errors.append({
                    "source_col": col_s,
                    "error_reason": "unknown_wide_column",
                    "message": "Column is not ignored and cannot be found in Cfg__Fields.display_name. If it is metadata, add it to ignore_columns.",
                })
            continue

        # Try exact display name first.
        role = ""
        fd = lookup_field_def(cfg_map, base)
        display_for_lookup = base

        # If exact is not found, try stripping Title/Body role.
        if not fd:
            parent, role_candidate = parse_role_from_display(base)
            if role_candidate:
                fd2 = lookup_field_def(cfg_map, parent)
                if fd2:
                    fd = fd2
                    role = role_candidate
                    display_for_lookup = parent

        if not fd:
            errors.append({
                "source_col": col_s,
                "base_display_name": base,
                "error_reason": "display_name_not_found_in_cfg_fields",
                "message": "Base display_name after removing sequence suffix was not found in Cfg__Fields.",
            })
            continue

        # If exact match exists and role not yet set, still detect Title/Body for rich_text feature columns
        # only when exact display name is not itself the intended standalone config key.
        if not role:
            parent, role_candidate = parse_role_from_display(base)
            if role_candidate and _norm_key(parent) == _norm_key(fd.display_name):
                role = role_candidate

        block_type = classify_block_type(
            fd.data_type,
            role=role,
            rich_text_default_block_type=rich_text_default_block_type,
        )

        if not block_type:
            if error_on_unsupported_datatype:
                errors.append({
                    "source_col": col_s,
                    "display_name": display_for_lookup,
                    "field_key": fd.field_key,
                    "data_type": fd.data_type,
                    "error_reason": "unsupported_data_type_for_mfb",
                    "message": "Wide_MFBs only supports list.* and rich_text_field fields.",
                })
            continue

        if fd.data_type.startswith("list.") and role:
            errors.append({
                "source_col": col_s,
                "display_name": display_for_lookup,
                "field_key": fd.field_key,
                "data_type": fd.data_type,
                "error_reason": "role_column_used_for_list_field",
                "message": "Title/Body role columns are only valid for rich_text_field.",
            })
            continue

        parsed.append(ParsedWideColumn(
            source_col=col_s,
            display_name=fd.display_name,
            block_seq=seq,
            role=role,
            field_def=fd,
        ))

    return parsed, errors, warnings


# =========================================================
# Wide loading / long building
# =========================================================

def load_wide_sheet(
    ws_wide,
    header_row: int = 1,
    field_key_row: int = 2,
    data_start_row: int = 3,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Read Wide_MFBs with a two-row header convention.

    Row 1: human wide column names, for example:
      Product ID (numeric), Compatible Brand-1, Compatible Models-1

    Row 2: generated field_key mapping row. It is NOT a data row.
      This row is written by this job as a process record.

    Row 3+: product data rows.
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

    # Existing mapping row is returned only for diagnostics; the job regenerates it.
    existing_mapping = []
    if len(values) > mapping_idx:
        existing_mapping = [_norm_str(x) for x in values[mapping_idx][:len(headers)]]
        existing_mapping += [""] * (len(headers) - len(existing_mapping))

    data_rows = values[data_idx:] if len(values) > data_idx else []
    normalized_rows = []
    for row in data_rows:
        padded = list(row[:len(headers)]) + [""] * max(0, len(headers) - len(row))
        normalized_rows.append([_norm_str(x) for x in padded])

    df = pd.DataFrame(normalized_rows, columns=headers)
    if df.empty:
        return df, headers, existing_mapping

    # Drop rows that are completely blank across the known wide columns.
    mask_not_blank = df.apply(lambda r: any(_norm_str(x) for x in r.tolist()), axis=1)
    df = df[mask_not_blank].copy()

    for c in df.columns:
        df[c] = df[c].apply(_norm_str)

    return df, headers, existing_mapping


def build_field_key_mapping_row(
    columns: list[str],
    parsed_cols: list[ParsedWideColumn],
) -> list[str]:
    """Build the row-2 field_key process record for Wide_MFBs."""
    by_col = {p.source_col: p.field_def.field_key for p in parsed_cols}
    return [by_col.get(_norm_str(c), "") for c in columns]


def write_field_key_mapping_row(
    ws_wide,
    mapping_values: list[str],
    field_key_row: int = 2,
) -> dict[str, Any]:
    if not mapping_values:
        return {"mapping_row_written": 0}
    last_col = _col_to_a1(len(mapping_values))
    ws_wide.update(
        range_name=f"A{field_key_row}:{last_col}{field_key_row}",
        values=[mapping_values],
        value_input_option="RAW",
    )
    return {"mapping_row_written": 1, "mapping_cols_written": len(mapping_values)}


def get_owner_from_wide_row(
    row: dict[str, Any],
    default_entity_type: str = "PRODUCT",
) -> tuple[str, str]:
    entity_type = _norm_str(row.get("entity_type") or default_entity_type).upper()
    if entity_type not in SUPPORTED_ENTITY_TYPES:
        raise ValueError(f"Unsupported entity_type: {entity_type}")

    # Prefer explicit gid_or_handle.
    gid_or_handle = _norm_str(row.get("gid_or_handle"))

    if not gid_or_handle:
        if entity_type == "PRODUCT":
            gid_or_handle = (
                _norm_str(row.get("Product ID (numeric)"))
                or _norm_str(row.get("Product ID"))
                or _norm_str(row.get("Product GID"))
                or _norm_str(row.get("Handle"))
                or _norm_str(row.get("handle"))
            )
        elif entity_type == "VARIANT":
            gid_or_handle = (
                _norm_str(row.get("Variant ID (numeric)"))
                or _norm_str(row.get("Variant ID"))
                or _norm_str(row.get("SKU"))
            )
        elif entity_type == "COLLECTION":
            gid_or_handle = (
                _norm_str(row.get("Collection ID (numeric)"))
                or _norm_str(row.get("Collection ID"))
                or _norm_str(row.get("Handle"))
                or _norm_str(row.get("handle"))
            )
        elif entity_type == "PAGE":
            gid_or_handle = (
                _norm_str(row.get("Page ID (numeric)"))
                or _norm_str(row.get("Page ID"))
                or _norm_str(row.get("Handle"))
                or _norm_str(row.get("handle"))
            )

    return entity_type, gid_or_handle


def build_long_rows(
    df_wide: pd.DataFrame,
    parsed_cols: list[ParsedWideColumn],
    *,
    data_start_row: int = 3,
    default_entity_type: str = "PRODUCT",
    action_default: str = "SET",
    mode_default: str = "STRICT",
    add_source_note: bool = True,
    dedupe_list_values_per_field: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    out = []
    errors = []
    warnings = []

    if df_wide.empty:
        return out, errors, warnings

    # Group parsed columns by source_col for lookup.
    parsed_by_col = {p.source_col: p for p in parsed_cols}

    for row_idx, row in enumerate(df_wide.to_dict("records"), start=data_start_row):
        try:
            entity_type, gid_or_handle = get_owner_from_wide_row(row, default_entity_type=default_entity_type)
        except Exception as e:
            errors.append({
                "sheet_row": row_idx,
                "error_reason": "invalid_owner",
                "message": str(e),
            })
            continue

        if not gid_or_handle:
            errors.append({
                "sheet_row": row_idx,
                "error_reason": "missing_gid_or_handle",
                "message": "Cannot determine gid_or_handle from wide row.",
            })
            continue

        base_note = _norm_str(row.get("note") or row.get("Note") or row.get("备注"))

        # Feature rows need combining title/body cells into a single long row.
        feature_groups: dict[tuple[str, int], dict[str, Any]] = {}
        list_seen: dict[str, set[str]] = {}

        for col_name, value in row.items():
            col_name = _norm_str(col_name)
            if col_name not in parsed_by_col:
                continue

            p = parsed_by_col[col_name]
            v = _norm_str(value)
            if not v:
                continue

            fd = p.field_def
            block_type = classify_block_type(fd.data_type, p.role)

            note_parts = []
            if base_note:
                note_parts.append(base_note)
            if add_source_note:
                note_parts.append(f"source_col={col_name}")
            note = " | ".join(note_parts)

            if block_type == "list_item":
                values = _split_list_cell(v)
                if _has_legacy_separator(v) and len(values) > 1:
                    warnings.append({
                        "sheet_row": row_idx,
                        "source_col": col_name,
                        "field_key": fd.field_key,
                        "warning_type": "legacy_separator_split",
                        "message": "List cell contains separator. Standard input should use Display Name-1/-2/-3 columns.",
                        "split_values": values,
                    })

                if dedupe_list_values_per_field:
                    seen_key = fd.field_key
                    list_seen.setdefault(seen_key, set())

                for item in values:
                    if dedupe_list_values_per_field:
                        if item in list_seen[fd.field_key]:
                            continue
                        list_seen[fd.field_key].add(item)

                    out.append({
                        "entity_type": entity_type,
                        "gid_or_handle": gid_or_handle,
                        "field_key": fd.field_key,
                        "block_type": "list_item",
                        "block_seq": str(p.block_seq),
                        "title": "",
                        "body": "",
                        "value": item,
                        "action": action_default,
                        "mode": mode_default,
                        "note": note,
                    })

            elif block_type == "bullet":
                out.append({
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": fd.field_key,
                    "block_type": "bullet",
                    "block_seq": str(p.block_seq),
                    "title": "",
                    "body": v,
                    "value": "",
                    "action": action_default,
                    "mode": mode_default,
                    "note": note,
                })

            elif block_type == "feature":
                key = (fd.field_key, p.block_seq)
                g = feature_groups.setdefault(key, {
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": fd.field_key,
                    "block_type": "feature",
                    "block_seq": str(p.block_seq),
                    "title": "",
                    "body": "",
                    "value": "",
                    "action": action_default,
                    "mode": mode_default,
                    "note": note,
                    "_source_cols": [],
                })

                if p.role == "title":
                    g["title"] = v
                elif p.role == "body":
                    g["body"] = v
                else:
                    # Rich text field with no Title/Body role uses bullet by default, so this should not happen.
                    g["body"] = v

                g["_source_cols"].append(col_name)

            elif block_type == "paragraph":
                out.append({
                    "entity_type": entity_type,
                    "gid_or_handle": gid_or_handle,
                    "field_key": fd.field_key,
                    "block_type": "paragraph",
                    "block_seq": str(p.block_seq),
                    "title": "",
                    "body": v,
                    "value": "",
                    "action": action_default,
                    "mode": mode_default,
                    "note": note,
                })

        for (_, _), g in sorted(feature_groups.items(), key=lambda kv: kv[0]):
            title = _norm_str(g.get("title"))
            body = _norm_str(g.get("body"))

            if title and not body:
                warnings.append({
                    "sheet_row": row_idx,
                    "field_key": g["field_key"],
                    "block_seq": g["block_seq"],
                    "warning_type": "feature_title_without_body",
                    "message": "Feature title exists but body is empty.",
                })
            if body and not title:
                warnings.append({
                    "sheet_row": row_idx,
                    "field_key": g["field_key"],
                    "block_seq": g["block_seq"],
                    "warning_type": "feature_body_without_title",
                    "message": "Feature body exists but title is empty.",
                })

            g.pop("_source_cols", None)
            if title or body:
                out.append(g)

    out = sorted(
        out,
        key=lambda r: (
            _norm_str(r.get("entity_type")),
            _norm_str(r.get("gid_or_handle")),
            _norm_str(r.get("field_key")),
            _safe_int(r.get("block_seq")) or 999999999,
            _norm_str(r.get("block_type")),
        ),
    )

    return out, errors, warnings


# =========================================================
# Output write
# =========================================================

def ensure_long_header(ws_output):
    values = ws_output.get_all_values()
    if not values:
        ws_output.update(range_name=f"A1:K1", values=[LONG_HEADER])
        return

    header = values[0]
    if header[:len(LONG_HEADER)] != LONG_HEADER:
        ws_output.update(range_name=f"A1:K1", values=[LONG_HEADER])


def write_long_output(
    ws_output,
    long_rows: list[dict[str, Any]],
    clear_output_first: bool = True,
) -> dict[str, Any]:
    df = pd.DataFrame(long_rows)
    if df.empty:
        df = pd.DataFrame(columns=LONG_HEADER)

    for c in LONG_HEADER:
        if c not in df.columns:
            df[c] = ""

    values = [LONG_HEADER] + df[LONG_HEADER].fillna("").astype(str).values.tolist()

    if clear_output_first:
        ws_output.clear()

    ws_output.update(
        range_name=f"A1:K{len(values)}",
        values=values,
        value_input_option="RAW",
    )

    return {
        "rows_written": int(len(df)),
    }


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
    config_sheet_label: str = "config",

    input_worksheet_title: str = WIDE_INPUT_TAB_DEFAULT,
    output_worksheet_title: str = LONG_OUTPUT_TAB_DEFAULT,

    wide_header_row: int = 1,
    wide_field_key_row: int = 2,
    wide_data_start_row: int = 3,
    write_wide_field_key_row: bool = True,

    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,

    default_entity_type: str = "PRODUCT",
    action_default: str = "SET",
    mode_default: str = "STRICT",

    preview_only: bool = True,
    create_output_tab_if_missing: bool = True,
    clear_output_first: bool = True,

    rich_text_default_block_type: str = "bullet",
    ignore_columns: Optional[set[str]] = None,
    error_on_unsupported_datatype: bool = True,

    dedupe_list_values_per_field: bool = True,
    add_source_note: bool = True,

    preview_limit: int = 50,
) -> dict[str, Any]:
    """
    Convert human-friendly Wide_MFBs into Long_MFBs / Edit__MetafieldBlocks-compatible rows.

    Flow:
      pre_edit / Wide_MFBs
        -> wide_to_metafieldblocks.py
      pre_edit / Long_MFBs

    Important:
      - field_key and data_type come only from config / Cfg__Fields.
      - Wide_MFBs row 1 is human header; row 2 is generated field_key mapping; row 3+ is data.
      - Wide column suffix -1/-2/-3 is block_seq, not display_name.
      - Standard list input is Display Name-1 / Display Name-2 / Display Name-3.
      - Legacy cell separators ;, |, newline are supported with warning.
    """

    run_id = _utc_run_id(JOB_NAME)

    # Bootstrap access to Console Core.
    gc_console = build_gsheet_client(console_gsheet_sa_b64_secret)

    # Read account config from routed config sheet; use site GSHEET SA for business sheets.
    account_cfg = load_account_config(
        gc_console=gc_console,
        console_core_url=console_core_url,
        site_code=site_code,
        config_sheet_label=config_sheet_label,
        cfg_sites_tab=cfg_sites_tab,
        cfg_account_tab=cfg_account_tab,
    )

    site_gsheet_secret = (
        account_cfg.get("GSHEET_SA_B64_SECRET")
        or account_cfg.get("gsheet_sa_b64_secret")
        or console_gsheet_sa_b64_secret
    )
    if not site_gsheet_secret:
        raise ValueError("Missing GSHEET_SA_B64_SECRET in Cfg__account_id and no console secret fallback.")

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

    cfg_fields = load_cfg_fields(
        gc=gc_site,
        console_core_url=console_core_url,
        site_code=site_code,
        config_sheet_label=config_sheet_label,
        cfg_tab_fields=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
    )
    cfg_map = build_cfg_display_map(cfg_fields, target_entity_type=default_entity_type)

    df_wide, wide_columns, existing_field_key_row = load_wide_sheet(
        ws_wide,
        header_row=wide_header_row,
        field_key_row=wide_field_key_row,
        data_start_row=wide_data_start_row,
    )

    parsed_cols, parse_errors, parse_warnings = parse_wide_columns(
        columns=wide_columns,
        cfg_map=cfg_map,
        ignore_columns=ignore_columns,
        rich_text_default_block_type=rich_text_default_block_type,
        error_on_unsupported_datatype=error_on_unsupported_datatype,
    )

    mapping_values = build_field_key_mapping_row(wide_columns, parsed_cols)
    mapping_write_result = {"mapping_row_written": 0, "mapping_cols_written": 0}
    if write_wide_field_key_row:
        mapping_write_result = write_field_key_mapping_row(
            ws_wide,
            mapping_values=mapping_values,
            field_key_row=wide_field_key_row,
        )

    if parse_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "parsed_columns": int(len(parsed_cols)),
                "parse_errors": int(len(parse_errors)),
                "warnings": int(len(parse_warnings)),
                "rows_generated": 0,
                "written": 0,
                **mapping_write_result,
            },
            "errors": parse_errors[:preview_limit],
            "warnings": parse_warnings[:preview_limit],
            "meta": {
                "input_sheet_label": input_sheet_label,
                "output_sheet_label": output_sheet_label,
                "config_sheet_label": config_sheet_label,
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "input_worksheet_title": input_worksheet_title,
                "output_worksheet_title": output_worksheet_title,
                "cfg_tab_fields": cfg_tab_fields,
                "site_gsheet_secret": site_gsheet_secret,
                "wide_header_row": wide_header_row,
                "wide_field_key_row": wide_field_key_row,
                "wide_data_start_row": wide_data_start_row,
            },
        }

    long_rows, build_errors, build_warnings = build_long_rows(
        df_wide=df_wide,
        parsed_cols=parsed_cols,
        data_start_row=wide_data_start_row,
        default_entity_type=default_entity_type,
        action_default=action_default,
        mode_default=mode_default,
        add_source_note=add_source_note,
        dedupe_list_values_per_field=dedupe_list_values_per_field,
    )

    warnings = parse_warnings + build_warnings

    if build_errors:
        return {
            "status": "error",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "parsed_columns": int(len(parsed_cols)),
                "build_errors": int(len(build_errors)),
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "errors": build_errors[:preview_limit],
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "meta": {
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "input_worksheet_title": input_worksheet_title,
                "output_worksheet_title": output_worksheet_title,
                "cfg_tab_fields": cfg_tab_fields,
                "site_gsheet_secret": site_gsheet_secret,
                "wide_header_row": wide_header_row,
                "wide_field_key_row": wide_field_key_row,
                "wide_data_start_row": wide_data_start_row,
            },
        }

    if preview_only:
        return {
            "status": "preview",
            "job_name": JOB_NAME,
            "run_id": run_id,
            "site_code": site_code,
            "summary": {
                "rows_loaded": int(len(df_wide)),
                "parsed_columns": int(len(parsed_cols)),
                "warnings": int(len(warnings)),
                "rows_generated": int(len(long_rows)),
                "written": 0,
                **mapping_write_result,
            },
            "warnings": warnings[:preview_limit],
            "preview": long_rows[:preview_limit],
            "parsed_columns": [
                {
                    "source_col": p.source_col,
                    "display_name": p.field_def.display_name,
                    "field_key": p.field_def.field_key,
                    "data_type": p.field_def.data_type,
                    "block_seq": p.block_seq,
                    "role": p.role,
                    "block_type": classify_block_type(p.field_def.data_type, p.role, rich_text_default_block_type),
                }
                for p in parsed_cols[:preview_limit]
            ],
            "meta": {
                "input_sheet_url": input_sheet_url,
                "output_sheet_url": output_sheet_url,
                "input_worksheet_title": input_worksheet_title,
                "output_worksheet_title": output_worksheet_title,
                "cfg_tab_fields": cfg_tab_fields,
                "generated_at_cn": _now_cn_str(),
                "site_gsheet_secret": site_gsheet_secret,
                "wide_header_row": wide_header_row,
                "wide_field_key_row": wide_field_key_row,
                "wide_data_start_row": wide_data_start_row,
            },
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
            "parsed_columns": int(len(parsed_cols)),
            "warnings": int(len(warnings)),
            "rows_generated": int(len(long_rows)),
            **write_result,
            **mapping_write_result,
        },
        "warnings": warnings[:preview_limit],
        "preview": long_rows[:preview_limit],
        "parsed_columns": [
            {
                "source_col": p.source_col,
                "display_name": p.field_def.display_name,
                "field_key": p.field_def.field_key,
                "data_type": p.field_def.data_type,
                "block_seq": p.block_seq,
                "role": p.role,
                "block_type": classify_block_type(p.field_def.data_type, p.role, rich_text_default_block_type),
            }
            for p in parsed_cols[:preview_limit]
        ],
        "meta": {
            "input_sheet_url": input_sheet_url,
            "output_sheet_url": output_sheet_url,
            "input_worksheet_title": input_worksheet_title,
            "output_worksheet_title": output_worksheet_title,
            "cfg_tab_fields": cfg_tab_fields,
            "generated_at_cn": _now_cn_str(),
            "site_gsheet_secret": site_gsheet_secret,
            "wide_header_row": wide_header_row,
            "wide_field_key_row": wide_field_key_row,
            "wide_data_start_row": wide_data_start_row,
        },
    }
