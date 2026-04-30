from __future__ import annotations

import base64
import json
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import gspread
from google.oauth2 import service_account
from gspread.exceptions import APIError, WorksheetNotFound

try:
    from google.colab import userdata
except Exception:  # pragma: no cover
    userdata = None


# ============================================================
# Defaults
# ============================================================

CFG_ACCOUNT_TAB_DEFAULT = "Cfg__account_id"
CFG_SITES_TAB_DEFAULT = "Cfg__Sites"

CFG_MATCH_COLUMN_CANDIDATES_DEFAULT = ["display_name", "field_name", "name", "field", "字段名"]
CFG_KEY_COLUMN_CANDIDATES_DEFAULT = ["field_key", "key", "api_key", "字段key", "字段_key", "field_id"]
CFG_ENTITY_COLUMN_CANDIDATES_DEFAULT = ["entity_type", "entity type", "Entity Type", "实体类型"]
CFG_SOURCE_TYPE_COLUMN_CANDIDATES_DEFAULT = ["source_type", "source type"]
CFG_FIELD_TYPE_COLUMN_CANDIDATES_DEFAULT = ["field_type", "field type"]
CFG_DATA_TYPE_COLUMN_CANDIDATES_DEFAULT = ["data_type", "data type"]
CFG_PURPOSE_COLUMN_CANDIDATES_DEFAULT = ["purpose_1", "purpose", "usage", "用途"]

WIDE_HEADER_ROW_DEFAULT = 1
WIDE_WRITE_ROW_DEFAULT = 2

TARGET_WRITES_PER_MIN_DEFAULT = 40
MAX_RETRIES_DEFAULT = 8
BASE_BACKOFF_DEFAULT = 1.2
JITTER_DEFAULT = 0.25

LONG_HEADER = ["entity_type", "gid_or_handle", "field_key", "desired_value", "note", "error_reason"]
RUNLOG_HEADER = [
    "run_id", "ts_cn", "job_name", "phase", "log_type", "status",
    "site_code", "entity_type", "gid", "field_key",
    "rows_loaded", "rows_pending", "rows_recognized", "rows_planned",
    "rows_written", "rows_skipped", "message", "error_reason",
]

# wide_to_long 只读写 Google Sheets，不访问 Shopify / Ads。
# 本 job 不读取 Cfg__account_id，也不校验站点账号字段。
REQUIRED_ACCOUNT_FIELDS: list[str] = []

OWNER_ID_KEYS = {
    "PRODUCT": "product_id",
    "VARIANT": "variant_id",
    "PRODUCTVARIANT": "variant_id",
    "COLLECTION": "collection_id",
    "PAGE": "page_id",
}

OWNER_HEADER_TO_KEY = {
    "PRODUCT": "product_id",
    "VARIANT": "variant_id",
    "COLLECTION": "collection_id",
    "PAGE": "page_id",
}

OWNER_TYPE_WORDS = {
    "PRODUCT": re.compile(r"(?<![a-z0-9])product(?![a-z0-9])", re.I),
    "VARIANT": re.compile(r"(?<![a-z0-9])variant(?![a-z0-9])", re.I),
    "COLLECTION": re.compile(r"(?<![a-z0-9])collection(?![a-z0-9])", re.I),
    "PAGE": re.compile(r"(?<![a-z0-9])page(?![a-z0-9])", re.I),
}

ID_WORD_RE = re.compile(
    r"(?<![a-z0-9])("
    r"id|gid|legacy[\s_\-]*id|numeric[\s_\-]*id"
    r")(?![a-z0-9])",
    re.I,
)

GENERIC_OWNER_HEADERS_BLOCKLIST = {
    "id",
    "gid",
    "legacy id",
    "legacy_id",
    "core.legacy_id",
    "numeric id",
    "numeric_id",
    "gid_or_handle",
    "shopify id",
    "shopify_id",
}

_last_write_ts = 0.0


# ============================================================
# Basic helpers
# ============================================================

def _is_blank(x: Any) -> bool:
    return x is None or str(x).strip() == ""


def norm_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip().lower()


def norm_header(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def norm_key(x: Any) -> str:
    return "" if x is None else str(x).strip()


def normalize_entity_type(x: Any) -> str:
    s = str(x or "").strip().upper()
    if s in {"PRODUCTVARIANT", "PRODUCT_VARIANT", "VARIATION", "VARIANT"}:
        return "VARIANT"
    if s in {"PRODUCT"}:
        return "PRODUCT"
    if s in {"COLLECTION", "SMART_COLLECTION", "CUSTOM_COLLECTION"}:
        return "COLLECTION"
    if s in {"PAGE"}:
        return "PAGE"
    return s


def trim_text(x: Any, max_len: int = 50000) -> str:
    s = "" if x is None else str(x)
    return s if len(s) <= max_len else s[:max_len]


def summarize_error(e: Exception, max_len: int = 1000) -> str:
    return trim_text(repr(e), max_len=max_len)


def cn_now_str() -> str:
    tz_cn = timezone(timedelta(hours=8))
    return datetime.now(tz_cn).strftime("%Y-%m-%d %H:%M:%S")


def make_run_id(job_name: str) -> str:
    tz_cn = timezone(timedelta(hours=8))
    ts = datetime.now(tz_cn).strftime("%Y%m%d_%H%M%S")
    tail = uuid.uuid4().hex[:8]
    return f"{job_name}__{ts}__{tail}"


def _get_secret(secret_name: str) -> str:
    if userdata is None:
        raise RuntimeError("google.colab.userdata is unavailable. This module is intended for Colab runner use.")
    v = userdata.get(secret_name)
    if not v:
        raise ValueError(f"❌ Colab Secrets 里找不到：{secret_name}")
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


def open_ss_by_url(gc: gspread.Client, url: str):
    if _is_blank(url):
        raise ValueError("❌ spreadsheet url 为空")
    return gc.open_by_url(str(url).strip())


def pick_col_index(header_row: list[Any], candidates: list[str]) -> Optional[int]:
    norm = [norm_header(h) for h in header_row]
    cand_norm = [norm_header(c) for c in candidates]
    for c in cand_norm:
        if c in norm:
            return norm.index(c)
    return None


def _pick_required_idx(header_norm: list[str], candidates: list[str], col_desc: str, tab_name: str) -> int:
    cand_norm = [norm_header(c) for c in candidates]
    for c in cand_norm:
        if c in header_norm:
            return header_norm.index(c)
    raise ValueError(f"❌ {tab_name} 找不到列：{col_desc}，候选={candidates}")


# ============================================================
# Console Core config
# ============================================================

def read_cfg_account(
    gc: gspread.Client,
    console_core_url: str,
    *,
    site_code: str,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    required_fields: Optional[list[str]] = None,
) -> dict[str, str]:
    sh = open_ss_by_url(gc, console_core_url)
    ws = sh.worksheet(cfg_account_tab)
    values = ws.get_all_values()

    if not values or len(values) < 2:
        raise ValueError(f"❌ {cfg_account_tab} 为空或不可读")

    required_fields = required_fields or REQUIRED_ACCOUNT_FIELDS

    header = values[0]
    header_norm = [norm_header(x) for x in header]

    # Structure 1: horizontal table with site_code column
    if "site code" in header_norm or "site_code" in [str(x).strip().lower() for x in header]:
        site_idx = None
        for i, h in enumerate(header):
            if norm_header(h) == "site code" or str(h).strip().lower() == "site_code":
                site_idx = i
                break
        if site_idx is None:
            raise ValueError(f"❌ {cfg_account_tab} 检测到横向表，但找不到 site_code 列")

        target = norm_text(site_code)
        matched_row = None
        for r in values[1:]:
            v = r[site_idx] if site_idx < len(r) else ""
            if norm_text(v) == target:
                matched_row = r
                break

        if matched_row is None:
            raise ValueError(f"❌ {cfg_account_tab} 未找到 site_code={site_code} 的账号配置")

        cfg: dict[str, str] = {}
        for i, col in enumerate(header):
            key = str(col).strip()
            if not key:
                continue
            cfg[key] = (matched_row[i].strip() if i < len(matched_row) else "")

    # Structure 2: key / value table, no site_code
    else:
        key_idx = pick_col_index(header, ["key", "name", "field", "config_key", "配置项"])
        val_idx = pick_col_index(header, ["value", "val", "config_value", "配置值"])
        if key_idx is None or val_idx is None:
            # Allow first two columns as key/value.
            key_idx, val_idx = 0, 1

        cfg = {}
        for r in values[1:]:
            k = r[key_idx].strip() if key_idx < len(r) else ""
            v = r[val_idx].strip() if val_idx < len(r) else ""
            if k:
                cfg[k] = v

    missing = [k for k in required_fields if _is_blank(cfg.get(k, ""))]
    if missing:
        raise ValueError(f"❌ {cfg_account_tab} 缺少必填字段或值为空：{missing}")

    return cfg


def assert_bootstrap_secret_matches_account(
    *,
    bootstrap_gsheet_sa_b64_secret: str,
    account_cfg: dict[str, str],
) -> None:
    expected = account_cfg.get("GSHEET_SA_B64_SECRET", "").strip()
    actual = str(bootstrap_gsheet_sa_b64_secret).strip()
    if not expected:
        raise ValueError("❌ Cfg__account_id.GSHEET_SA_B64_SECRET 为空")
    if actual != expected:
        raise ValueError(
            "❌ BOOTSTRAP_GSHEET_SA_B64_SECRET 与 Cfg__account_id.GSHEET_SA_B64_SECRET 不一致。\n"
            f"Cell1={actual}\n"
            f"Cfg__account_id={expected}\n"
            "不允许 fallback，不自动替换。"
        )


def build_runtime_context(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
) -> tuple[gspread.Client, dict[str, str]]:
    del site_code, console_core_url, cfg_account_tab  # kept for run() compatibility

    # wide_to_long 是 Google Sheets 内部整理 job：
    # 只需要用 Cell 1 的 BOOTSTRAP_GSHEET_SA_B64_SECRET 读取 Console Core / Cfg__Sites / 业务表。
    # 不读取 Cfg__account_id，不校验 Shopify / Ads / Storefront / Admin 字段。
    runtime_gc = build_gsheet_client(bootstrap_gsheet_sa_b64_secret)
    return runtime_gc, {}


def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    cfg_sites_sh = open_ss_by_url(gc, console_core_url)
    ws_cfg_sites = cfg_sites_sh.worksheet(cfg_sites_tab)

    values = ws_cfg_sites.get_all_values()
    if not values or len(values) < 2:
        raise ValueError(f"❌ {cfg_sites_tab} 为空或不可读")

    header = values[0]
    header_norm = [norm_header(x) for x in header]

    site_code_idx = _pick_required_idx(header_norm, ["site_code", "site code"], "site_code", cfg_sites_tab)
    label_idx = _pick_required_idx(header_norm, ["label"], "label", cfg_sites_tab)
    sheet_url_idx = _pick_required_idx(header_norm, ["sheet_url", "sheet url"], "sheet_url", cfg_sites_tab)

    site_code_n = norm_text(site_code)
    label_n = norm_text(label)

    for r in values[1:]:
        sc = norm_text(r[site_code_idx] if site_code_idx < len(r) else "")
        lb = norm_text(r[label_idx] if label_idx < len(r) else "")
        su = (r[sheet_url_idx] if sheet_url_idx < len(r) else "").strip()
        if sc == site_code_n and lb == label_n:
            if _is_blank(su):
                raise ValueError(f"❌ {cfg_sites_tab} 命中 site_code={site_code}, label={label}，但 sheet_url 为空")
            return su

    raise ValueError(f"❌ {cfg_sites_tab} 未找到 site_code={site_code}, label={label} 的记录")


def open_ws_by_label_and_title(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    worksheet_title: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    create_if_missing: bool = False,
    rows: int = 1000,
    cols: int = 6,
):
    sheet_url = get_sheet_url_by_label(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=label,
        cfg_sites_tab=cfg_sites_tab,
    )
    sh = open_ss_by_url(gc, sheet_url)
    try:
        ws = sh.worksheet(worksheet_title)
    except WorksheetNotFound:
        if not create_if_missing:
            raise
        ws = sh.add_worksheet(title=worksheet_title, rows=rows, cols=cols)
    return sh, ws, sheet_url


# ============================================================
# Google Sheets write helpers
# ============================================================

def _throttle(target_writes_per_min: int) -> None:
    global _last_write_ts
    min_interval = 60.0 / max(1, target_writes_per_min)
    now = time.time()
    wait = min_interval - (now - _last_write_ts)
    if wait > 0:
        time.sleep(wait)
    _last_write_ts = time.time()


def _is_quota_429(err: Exception) -> bool:
    s = str(err)
    return ("[429]" in s) or ("Quota exceeded" in s) or ("RATE_LIMIT_EXCEEDED" in s)


def safe_clear(
    ws,
    *,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.clear()
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit (clear). retry in {backoff:.1f}s")
                time.sleep(backoff)
                continue
            raise


def safe_resize(
    ws,
    rows: int,
    cols: int,
    *,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.resize(rows=rows, cols=cols)
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit (resize). retry in {backoff:.1f}s")
                time.sleep(backoff)
                continue
            raise


def safe_update_range(
    ws,
    a1_range: str,
    values: list[list[Any]],
    *,
    value_input_option: str = "RAW",
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.update(range_name=a1_range, values=values, value_input_option=value_input_option)
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit. retry in {backoff:.1f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff)
                continue
            raise


def append_rows_safe(
    ws,
    rows: list[list[Any]],
    *,
    value_input_option: str = "RAW",
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    if not rows:
        return
    for attempt in range(max_retries + 1):
        try:
            _throttle(target_writes_per_min)
            ws.append_rows(rows, value_input_option=value_input_option)
            return
        except APIError as e:
            if _is_quota_429(e) and attempt < max_retries:
                backoff = base_backoff * (2 ** attempt)
                backoff *= (1 + random.uniform(-jitter, jitter))
                print(f"⚠️ 429 quota hit (append_rows). retry in {backoff:.1f}s")
                time.sleep(backoff)
                continue
            raise


def write_chunk(
    ws,
    start_row: int,
    rows: list[list[Any]],
    *,
    n_cols: int,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    if not rows:
        return
    end_row = start_row + len(rows) - 1
    end_col = gspread.utils.rowcol_to_a1(1, n_cols).replace("1", "")
    safe_update_range(
        ws,
        f"A{start_row}:{end_col}{end_row}",
        rows,
        value_input_option="RAW",
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )


# ============================================================
# RunLog
# ============================================================

def ensure_runlog_header(
    ws,
    *,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    row1 = ws.row_values(1)
    if row1[:len(RUNLOG_HEADER)] != RUNLOG_HEADER:
        end_col = gspread.utils.rowcol_to_a1(1, len(RUNLOG_HEADER)).replace("1", "")
        safe_update_range(
            ws,
            f"A1:{end_col}1",
            [RUNLOG_HEADER],
            value_input_option="RAW",
            target_writes_per_min=target_writes_per_min,
            max_retries=max_retries,
            base_backoff=base_backoff,
            jitter=jitter,
        )


def write_runlog(
    ws,
    *,
    run_id: str,
    ts_cn: str,
    job_name: str,
    phase: str,
    log_type: str,
    status: str,
    site_code: str,
    entity_type: str = "",
    gid: str = "",
    field_key: str = "",
    rows_loaded: Any = "",
    rows_pending: Any = "",
    rows_recognized: Any = "",
    rows_planned: Any = "",
    rows_written: Any = "",
    rows_skipped: Any = "",
    message: str = "",
    error_reason: str = "",
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    ensure_runlog_header(
        ws,
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )
    row = [[
        run_id,
        ts_cn,
        job_name,
        phase,
        log_type,
        status,
        site_code,
        entity_type,
        gid,
        field_key,
        rows_loaded,
        rows_pending,
        rows_recognized,
        rows_planned,
        rows_written,
        rows_skipped,
        trim_text(message, 1000),
        trim_text(error_reason, 1000),
    ]]
    append_rows_safe(
        ws,
        row,
        value_input_option="RAW",
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )


# ============================================================
# Cfg__Fields maps
# ============================================================

def build_cfg_maps(
    ws_cfg,
    *,
    cfg_match_column_candidates: list[str],
    cfg_key_column_candidates: list[str],
    cfg_entity_column_candidates: list[str],
) -> tuple[dict[str, str], dict[str, str], dict[str, dict[str, str]], dict[str, str]]:
    values = ws_cfg.get_all_values()
    if not values or len(values) < 2:
        raise ValueError("❌ Cfg__Fields 为空或不可读")

    header = values[0]
    match_idx = pick_col_index(header, cfg_match_column_candidates)
    key_idx = pick_col_index(header, cfg_key_column_candidates)
    entity_idx = pick_col_index(header, cfg_entity_column_candidates)

    if match_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到匹配列：{cfg_match_column_candidates}")
    if key_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到 field_key/key 列：{cfg_key_column_candidates}")
    if entity_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到 entity_type 列：{cfg_entity_column_candidates}")

    optional_idxs = {
        "source_type": pick_col_index(header, CFG_SOURCE_TYPE_COLUMN_CANDIDATES_DEFAULT),
        "field_type": pick_col_index(header, CFG_FIELD_TYPE_COLUMN_CANDIDATES_DEFAULT),
        "data_type": pick_col_index(header, CFG_DATA_TYPE_COLUMN_CANDIDATES_DEFAULT),
        "purpose": pick_col_index(header, CFG_PURPOSE_COLUMN_CANDIDATES_DEFAULT),
    }

    map_name_to_key: dict[str, str] = {}
    map_key_to_entity: dict[str, str] = {}
    map_key_meta: dict[str, dict[str, str]] = {}

    for r in values[1:]:
        key = r[key_idx].strip() if key_idx < len(r) else ""
        if _is_blank(key):
            continue

        entity = normalize_entity_type(r[entity_idx] if entity_idx < len(r) else "")
        name = r[match_idx] if match_idx < len(r) else ""

        if not _is_blank(name):
            map_name_to_key[norm_header(name)] = key

        # Also allow header to be the field_key directly.
        map_name_to_key[norm_header(key)] = key

        if not _is_blank(entity):
            map_key_to_entity[key] = entity

        meta = {
            "field_key": key,
            "entity_type": entity,
            "display_name": str(name).strip(),
        }
        for meta_name, idx in optional_idxs.items():
            meta[meta_name] = r[idx].strip() if idx is not None and idx < len(r) else ""
        map_key_meta[key] = meta

    cfg_meta = {
        "match_col": header[match_idx],
        "key_col": header[key_idx],
        "entity_col": header[entity_idx],
    }

    print(
        "✅ CFG maps loaded:"
        f" name/key->field_key={len(map_name_to_key)}"
        f" | field_key->entity={len(map_key_to_entity)}"
        f" | match_col={cfg_meta['match_col']}"
        f" | key_col={cfg_meta['key_col']}"
        f" | entity_col={cfg_meta['entity_col']}"
    )

    return map_name_to_key, map_key_to_entity, map_key_meta, cfg_meta


def should_output_field(field_key: str, meta: dict[str, str], *, exclude_display_only: bool = True) -> bool:
    k = str(field_key or "").strip().lower()
    if not k:
        return False
    if k.startswith("core.") or k.startswith("calc."):
        return False

    if exclude_display_only:
        joined = " ".join([
            meta.get("source_type", ""),
            meta.get("field_type", ""),
            meta.get("data_type", ""),
            meta.get("purpose", ""),
        ]).lower()
        if "display-only" in joined or "display only" in joined or "display_only" in joined:
            return False

    return True


# ============================================================
# Owner column recognition
# ============================================================

def detect_owner_header(header_value: Any) -> Optional[str]:
    raw = "" if header_value is None else str(header_value).strip()
    if not raw:
        return None

    nh = norm_header(raw)
    if nh in GENERIC_OWNER_HEADERS_BLOCKLIST:
        return None

    has_id_word = bool(ID_WORD_RE.search(nh))
    if not has_id_word:
        return None

    matched_owner_types = [owner_type for owner_type, pattern in OWNER_TYPE_WORDS.items() if pattern.search(nh)]
    if len(matched_owner_types) != 1:
        return None

    return OWNER_HEADER_TO_KEY[matched_owner_types[0]]


def scan_owner_columns(header_row: list[Any], *, last_col: int) -> dict[str, int]:
    owner_cols: dict[str, int] = {}
    duplicates: dict[str, list[int]] = {}

    for idx in range(last_col):
        owner_key = detect_owner_header(header_row[idx] if idx < len(header_row) else "")
        if not owner_key:
            continue

        if owner_key in owner_cols:
            duplicates.setdefault(owner_key, [owner_cols[owner_key] + 1]).append(idx + 1)
        else:
            owner_cols[owner_key] = idx

    if duplicates:
        raise ValueError(f"❌ owner 身份列重复，无法安全判断：{duplicates}")

    return owner_cols


# ============================================================
# Wide row2 field_key write
# ============================================================

def write_wide_keys_from_cfg(
    ws_wide,
    map_name_to_key: dict[str, str],
    *,
    wide_header_row: int,
    wide_write_row: int,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
) -> dict[str, Any]:
    values = ws_wide.get_all_values()
    if not values:
        raise ValueError("❌ Wide 为空")

    header_row = values[wide_header_row - 1] if len(values) >= wide_header_row else []
    if not header_row:
        raise ValueError("❌ Wide 第1行为空")

    last_non_empty = 0
    for i, v in enumerate(header_row):
        if not _is_blank(v):
            last_non_empty = i + 1
    if last_non_empty == 0:
        raise ValueError("❌ Wide 第1行没有任何字段名")

    out: list[str] = []
    hit = 0
    miss = 0
    owner_cols = 0
    misses: list[dict[str, Any]] = []

    for i in range(last_non_empty):
        h = header_row[i]
        if detect_owner_header(h):
            out.append("")
            owner_cols += 1
            continue

        nh = norm_header(h)
        if nh and nh in map_name_to_key:
            out.append(map_name_to_key[nh])
            hit += 1
        else:
            out.append("")
            miss += 1
            misses.append({"col_num": i + 1, "header": h})

    start = gspread.utils.rowcol_to_a1(wide_write_row, 1)
    end = gspread.utils.rowcol_to_a1(wide_write_row, last_non_empty)
    safe_update_range(
        ws_wide,
        f"{start}:{end}",
        [out],
        value_input_option="RAW",
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )

    print(
        f"✅ Wide row{wide_write_row} written:"
        f" cols=1..{last_non_empty} | mapped={hit} | unmapped={miss} | owner_cols={owner_cols}"
    )

    return {
        "last_non_empty_col": last_non_empty,
        "hit": hit,
        "miss": miss,
        "owner_cols": owner_cols,
        "misses": misses,
    }


# ============================================================
# Build Long
# ============================================================

def build_long_rows(
    values_2d: list[list[Any]],
    map_key_to_entity: dict[str, str],
    map_key_meta: dict[str, dict[str, str]],
    *,
    include_empty: bool,
    dedup_output: bool,
    exclude_display_only: bool,
) -> tuple[list[list[str]], dict[str, Any]]:
    if len(values_2d) < 3:
        raise ValueError("❌ 宽表至少需要：第1行(header) + 第2行(field_key) + 第3行起(数据)")

    header1 = values_2d[0]
    key_row = values_2d[1]
    data_rows = values_2d[2:]

    last_col = 0
    for r in values_2d[: min(len(values_2d), 2000)]:
        last_col = max(last_col, len(r))
    if last_col < 1:
        raise ValueError("❌ Wide 没有任何列")

    header1 = header1 + [""] * max(0, last_col - len(header1))
    key_row = key_row + [""] * max(0, last_col - len(key_row))

    owner_cols = scan_owner_columns(header1, last_col=last_col)
    owner_col_indexes = set(owner_cols.values())

    if not owner_cols:
        raise ValueError(
            "❌ Wide 没有识别到任何 owner 身份列。\n"
            "owner 列表头必须同时包含 owner 类型词(Product/Variant/Collection/Page)"
            " 和 ID 类型词(ID/GID/Legacy ID/Numeric ID)。"
        )

    out: list[list[str]] = []
    seen = set() if dedup_output else None
    skipped_missing_owner = 0
    skipped_empty_value = 0
    skipped_excluded_field = 0
    skipped_blank_key = 0
    planned_fields = 0

    field_cols: list[dict[str, Any]] = []

    for col_idx in range(last_col):
        if col_idx in owner_col_indexes:
            continue

        k = norm_key(key_row[col_idx])
        if _is_blank(k):
            skipped_blank_key += 1
            continue

        meta = map_key_meta.get(k, {"field_key": k, "entity_type": map_key_to_entity.get(k, "")})
        if not should_output_field(k, meta, exclude_display_only=exclude_display_only):
            skipped_excluded_field += 1
            continue

        entity = normalize_entity_type(map_key_to_entity.get(k, meta.get("entity_type", "")))
        owner_id_key = OWNER_ID_KEYS.get(entity)
        if not owner_id_key:
            # Unknown entity_type is not safe for Shopify write long.
            skipped_excluded_field += 1
            continue

        field_cols.append({
            "col_idx": col_idx,
            "field_key": k,
            "entity_type": entity,
            "owner_id_key": owner_id_key,
        })
        planned_fields += 1

    if not field_cols:
        raise ValueError("❌ 没有可输出字段列。请检查 Wide 第2行 field_key 或 Cfg__Fields。")

    def emit(entity: str, owner_id: Any, field_key: str, value: Any, note: str = "", error_reason: str = ""):
        row = [
            entity,
            str(owner_id).strip(),
            field_key,
            "" if value is None else str(value),
            note,
            error_reason,
        ]
        if seen is not None:
            t = tuple(row)
            if t in seen:
                return
            seen.add(t)
        out.append(row)

    for r in data_rows:
        if not r:
            continue
        if len(r) < last_col:
            r = r + [""] * (last_col - len(r))

        row_owner_values: dict[str, str] = {}
        for owner_id_key, col_idx in owner_cols.items():
            row_owner_values[owner_id_key] = str(r[col_idx]).strip() if col_idx < len(r) else ""

        for fc in field_cols:
            v = r[fc["col_idx"]] if fc["col_idx"] < len(r) else ""
            if (not include_empty) and _is_blank(v):
                skipped_empty_value += 1
                continue

            owner_id_key = fc["owner_id_key"]
            owner_id = row_owner_values.get(owner_id_key, "")

            if _is_blank(owner_id):
                skipped_missing_owner += 1
                # Do not emit invalid long row because there is no safe gid_or_handle.
                continue

            emit(fc["entity_type"], owner_id, fc["field_key"], v)

    meta = {
        "mode": "OWNER_HEADER",
        "last_col": last_col,
        "owner_cols": {k: v + 1 for k, v in owner_cols.items()},
        "planned_fields": planned_fields,
        "skipped_missing_owner": skipped_missing_owner,
        "skipped_empty_value": skipped_empty_value,
        "skipped_excluded_field": skipped_excluded_field,
        "skipped_blank_key": skipped_blank_key,
        "rows_to_write": len(out),
    }

    return out, meta


# ============================================================
# Main entry
# ============================================================

def run(
    *,
    SITE_CODE: Optional[str] = None,
    JOB_NAME: str = "wide_to_long",
    CONSOLE_CORE_URL: Optional[str] = None,
    BOOTSTRAP_GSHEET_SA_B64_SECRET: Optional[str] = None,

    # Lowercase aliases kept for compatibility.
    site_code: Optional[str] = None,
    job_name: Optional[str] = None,
    console_core_url: Optional[str] = None,
    bootstrap_gsheet_sa_b64_secret: Optional[str] = None,

    # Console Core tabs
    cfg_account_tab: str = CFG_ACCOUNT_TAB_DEFAULT,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,

    # Business sheet labels / worksheet titles
    input_sheet_label: str = "pre_edit",
    input_worksheet_title: str = "Wide",
    output_sheet_label: str = "pre_edit",
    output_worksheet_title: str = "Edit__ValuesLong",
    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = "Cfg__Fields",
    runlog_sheet_label: str = "runlog_sheet",
    runlog_worksheet_title: str = "Ops__RunLog",

    # Runtime options
    run_id: Optional[str] = None,
    include_empty: bool = False,
    clear_output_first: bool = True,
    do_write_wide_keys: bool = True,
    do_build_long: bool = True,
    write_runlog_enabled: bool = True,
    dedup_output: bool = True,
    exclude_display_only: bool = True,
    out_chunk_rows: int = 20000,
    wide_header_row: int = WIDE_HEADER_ROW_DEFAULT,
    wide_write_row: int = WIDE_WRITE_ROW_DEFAULT,

    # Cfg__Fields column candidates
    cfg_match_column_candidates: Optional[list[str]] = None,
    cfg_key_column_candidates: Optional[list[str]] = None,
    cfg_entity_column_candidates: Optional[list[str]] = None,

    # Write control
    target_writes_per_min: int = TARGET_WRITES_PER_MIN_DEFAULT,
    max_retries: int = MAX_RETRIES_DEFAULT,
    base_backoff: float = BASE_BACKOFF_DEFAULT,
    jitter: float = JITTER_DEFAULT,
) -> dict[str, Any]:
    site_code_final = SITE_CODE or site_code
    job_name_final = job_name or JOB_NAME or "wide_to_long"
    console_core_url_final = CONSOLE_CORE_URL or console_core_url
    bootstrap_secret_final = BOOTSTRAP_GSHEET_SA_B64_SECRET or bootstrap_gsheet_sa_b64_secret

    if _is_blank(site_code_final):
        raise ValueError("❌ 缺少 SITE_CODE")
    if _is_blank(console_core_url_final):
        raise ValueError("❌ 缺少 CONSOLE_CORE_URL")
    if _is_blank(bootstrap_secret_final):
        raise ValueError("❌ 缺少 BOOTSTRAP_GSHEET_SA_B64_SECRET")

    cfg_match_column_candidates = cfg_match_column_candidates or CFG_MATCH_COLUMN_CANDIDATES_DEFAULT
    cfg_key_column_candidates = cfg_key_column_candidates or CFG_KEY_COLUMN_CANDIDATES_DEFAULT
    cfg_entity_column_candidates = cfg_entity_column_candidates or CFG_ENTITY_COLUMN_CANDIDATES_DEFAULT

    actual_run_id = run_id or make_run_id(job_name_final)
    ts_cn = cn_now_str()

    rows_read = 0
    rows_written = 0
    rows_recognized = 0
    rows_planned = 0
    rows_skipped = 0

    wide_key_write_meta: dict[str, Any] = {}
    cfg_meta: dict[str, Any] = {}
    long_meta: dict[str, Any] = {}
    preview: list[dict[str, str]] = []

    ws_runlog = None
    runlog_sheet_url = ""

    try:
        gc, account_cfg = build_runtime_context(
            site_code=str(site_code_final),
            console_core_url=str(console_core_url_final),
            bootstrap_gsheet_sa_b64_secret=str(bootstrap_secret_final),
            cfg_account_tab=cfg_account_tab,
        )

        sh_wide, ws_wide, wide_sheet_url = open_ws_by_label_and_title(
            gc=gc,
            console_core_url=str(console_core_url_final),
            site_code=str(site_code_final),
            label=input_sheet_label,
            worksheet_title=input_worksheet_title,
            cfg_sites_tab=cfg_sites_tab,
        )
        sh_long, ws_long, long_sheet_url = open_ws_by_label_and_title(
            gc=gc,
            console_core_url=str(console_core_url_final),
            site_code=str(site_code_final),
            label=output_sheet_label,
            worksheet_title=output_worksheet_title,
            cfg_sites_tab=cfg_sites_tab,
            create_if_missing=True,
            rows=1000,
            cols=len(LONG_HEADER),
        )
        sh_cfg, ws_cfg, cfg_sheet_url = open_ws_by_label_and_title(
            gc=gc,
            console_core_url=str(console_core_url_final),
            site_code=str(site_code_final),
            label=cfg_sheet_label,
            worksheet_title=cfg_tab_fields,
            cfg_sites_tab=cfg_sites_tab,
        )

        if write_runlog_enabled:
            _, ws_runlog, runlog_sheet_url = open_ws_by_label_and_title(
                gc=gc,
                console_core_url=str(console_core_url_final),
                site_code=str(site_code_final),
                label=runlog_sheet_label,
                worksheet_title=runlog_worksheet_title,
                cfg_sites_tab=cfg_sites_tab,
                create_if_missing=True,
                rows=1000,
                cols=len(RUNLOG_HEADER),
            )

        print("✅ Runtime config ready")
        print("  site_code :", site_code_final)
        print("  job_name  :", job_name_final)
        print("  config    : Cfg__account_id not used by this job")
        print("✅ Sheets ready")
        print("  wide      :", sh_wide.url, "|", ws_wide.title)
        print("  long      :", sh_long.url, "|", ws_long.title)
        print("  config    :", sh_cfg.url, "|", ws_cfg.title)
        if ws_runlog is not None:
            print("  runlog    :", runlog_sheet_url, "|", ws_runlog.title)

        map_name_to_key: dict[str, str] = {}
        map_key_to_entity: dict[str, str] = {}
        map_key_meta: dict[str, dict[str, str]] = {}

        if do_write_wide_keys or do_build_long:
            map_name_to_key, map_key_to_entity, map_key_meta, cfg_meta = build_cfg_maps(
                ws_cfg,
                cfg_match_column_candidates=cfg_match_column_candidates,
                cfg_key_column_candidates=cfg_key_column_candidates,
                cfg_entity_column_candidates=cfg_entity_column_candidates,
            )

        if do_write_wide_keys:
            wide_key_write_meta = write_wide_keys_from_cfg(
                ws_wide,
                map_name_to_key,
                wide_header_row=wide_header_row,
                wide_write_row=wide_write_row,
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )
            rows_recognized = int(wide_key_write_meta.get("hit", 0) or 0)

        if do_build_long:
            values = ws_wide.get_all_values()
            rows_read = max(0, len(values) - 2)

            long_rows, long_meta = build_long_rows(
                values,
                map_key_to_entity,
                map_key_meta,
                include_empty=include_empty,
                dedup_output=dedup_output,
                exclude_display_only=exclude_display_only,
            )
            rows_planned = len(long_rows)
            rows_skipped = int(long_meta.get("skipped_missing_owner", 0) or 0) + int(long_meta.get("skipped_empty_value", 0) or 0)

            print(
                f"✅ Mode={long_meta.get('mode')}"
                f" | owner_cols={long_meta.get('owner_cols')}"
                f" | planned_fields={long_meta.get('planned_fields')}"
                f" | rows_to_write={len(long_rows):,}"
                f" | skipped_missing_owner={long_meta.get('skipped_missing_owner')}"
            )

            if clear_output_first:
                safe_clear(
                    ws_long,
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )

            end_col = gspread.utils.rowcol_to_a1(1, len(LONG_HEADER)).replace("1", "")
            safe_update_range(
                ws_long,
                f"A1:{end_col}1",
                [LONG_HEADER],
                value_input_option="RAW",
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

            est_rows = 1 + len(long_rows)
            est_cells = est_rows * len(LONG_HEADER)
            if est_cells > 9_500_000:
                print("⚠️ 警告：预计接近/超过单表 1000万 cell 上限。建议拆分 Long 表。")

            safe_resize(
                ws_long,
                rows=max(ws_long.row_count, min(est_rows + 50, 2_300_000)),
                cols=len(LONG_HEADER),
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

            out_row_cursor = 2
            written = 0
            t0 = time.time()
            buf: list[list[str]] = []

            for row in long_rows:
                buf.append(row)
                if len(buf) >= out_chunk_rows:
                    write_chunk(
                        ws_long,
                        out_row_cursor,
                        buf,
                        n_cols=len(LONG_HEADER),
                        target_writes_per_min=target_writes_per_min,
                        max_retries=max_retries,
                        base_backoff=base_backoff,
                        jitter=jitter,
                    )
                    out_row_cursor += len(buf)
                    written += len(buf)
                    buf = []
                    print(f"… wrote {written:,} rows")

            if buf:
                write_chunk(
                    ws_long,
                    out_row_cursor,
                    buf,
                    n_cols=len(LONG_HEADER),
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )
                written += len(buf)

            rows_written = written
            dt = time.time() - t0
            print(
                f"✅ Done. rows_written={written:,}"
                f" | time={dt:.1f}s"
                f" | INCLUDE_EMPTY={include_empty}"
                f" | DEDUP={dedup_output}"
                f" | EXCLUDE_DISPLAY_ONLY={exclude_display_only}"
            )
            preview = [dict(zip(LONG_HEADER, row)) for row in long_rows[:50]]

        if write_runlog_enabled and ws_runlog is not None:
            write_runlog(
                ws_runlog,
                run_id=actual_run_id,
                ts_cn=ts_cn,
                job_name=job_name_final,
                phase="run",
                log_type="summary",
                status="SUCCESS",
                site_code=str(site_code_final),
                rows_loaded=rows_read,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                message="wide_to_long completed",
                error_reason="",
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

        return {
            "status": "SUCCESS",
            "run_id": actual_run_id,
            "ts_cn": ts_cn,
            "job_name": job_name_final,
            "summary": {
                "job_name": job_name_final,
                "site_code": site_code_final,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_skipped": rows_skipped,
                "mapped_columns": wide_key_write_meta.get("hit", 0),
                "unmapped_columns": wide_key_write_meta.get("miss", 0),
                "owner_columns": wide_key_write_meta.get("owner_cols", 0),
                "include_empty": include_empty,
                "clear_output_first": clear_output_first,
                "do_write_wide_keys": do_write_wide_keys,
                "do_build_long": do_build_long,
                "dedup_output": dedup_output,
                "exclude_display_only": exclude_display_only,
            },
            "preview": preview,
            "meta": {
                "run_id": actual_run_id,
                "wide_sheet_url": wide_sheet_url,
                "long_sheet_url": long_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
                "cfg_match_col": cfg_meta.get("match_col", ""),
                "cfg_key_col": cfg_meta.get("key_col", ""),
                "cfg_entity_col": cfg_meta.get("entity_col", ""),
                "wide_key_write_meta": wide_key_write_meta,
                "long_meta": long_meta,
            },
        }

    except Exception as e:
        error_message = trim_text(str(e), max_len=50000)
        error_summary = summarize_error(e, max_len=1000)
        print(f"❌ Run failed: {error_message}")

        if write_runlog_enabled and ws_runlog is not None:
            try:
                write_runlog(
                    ws_runlog,
                    run_id=actual_run_id,
                    ts_cn=ts_cn,
                    job_name=job_name_final,
                    phase="run",
                    log_type="error",
                    status="ERROR",
                    site_code=str(site_code_final or ""),
                    rows_loaded=rows_read,
                    rows_recognized=rows_recognized,
                    rows_planned=rows_planned,
                    rows_written=rows_written,
                    rows_skipped=rows_skipped,
                    message=error_message,
                    error_reason=error_summary,
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )
            except Exception as e2:
                print(f"⚠️ RunLog 写入失败：{e2}")
        raise
