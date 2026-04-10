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


CFG_SITES_TAB_DEFAULT = "Cfg__Sites"

# notebook-gold defaults
CFG_MATCH_COLUMN_CANDIDATES_DEFAULT = ["field_name", "name", "display_name", "field", "字段名"]
CFG_KEY_COLUMN_CANDIDATES_DEFAULT = ["field_key", "key", "api_key", "字段key", "字段_key", "field_id"]
CFG_ENTITY_COLUMN_CANDIDATES_DEFAULT = ["entity_type", "entity type", "Entity Type", "实体类型"]

WIDE_HEADER_ROW_DEFAULT = 1
WIDE_WRITE_ROW_DEFAULT = 2

TARGET_WRITES_PER_MIN_DEFAULT = 40
MAX_RETRIES_DEFAULT = 8
BASE_BACKOFF_DEFAULT = 1.2
JITTER_DEFAULT = 0.25

LONG_HEADER = ["entity_type", "gid_or_handle", "field_key", "desired_value"]
RUNLOG_HEADER = [
    "run_id", "ts_cn", "job_id", "entity_type", "gid", "field_key",
    "result", "error_message", "rows_read", "rows_written",
    "error_count", "error_summary",
]

_last_write_ts = 0.0


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
    s = re.sub(r"\s+", " ", s)
    return s


def trim_text(x: Any, max_len: int = 50000) -> str:
    s = "" if x is None else str(x)
    return s if len(s) <= max_len else s[:max_len]


def summarize_error(e: Exception, max_len: int = 1000) -> str:
    return trim_text(repr(e), max_len=max_len)


def cn_now_str() -> str:
    tz_cn = timezone(timedelta(hours=8))
    return datetime.now(tz_cn).strftime("%Y-%m-%d %H:%M:%S")


def make_run_id(job_id: str) -> str:
    tz_cn = timezone(timedelta(hours=8))
    ts = datetime.now(tz_cn).strftime("%Y%m%d_%H%M%S")
    tail = uuid.uuid4().hex[:8]
    return f"{job_id}__{ts}__{tail}"


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


def _pick_required_idx(header_norm: list[str], candidates: list[str], col_desc: str) -> int:
    cand_norm = [norm_text(c) for c in candidates]
    for c in cand_norm:
        if c in header_norm:
            return header_norm.index(c)
    raise ValueError(f"❌ Cfg__Sites 找不到列：{col_desc}，候选={candidates}")


def get_sheet_url_by_label(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
) -> str:
    cfg_sites_sh = open_ss_by_url(gc, console_core_url)
    ws_cfg_sites = cfg_sites_sh.worksheet(cfg_sites_tab)

    cfg_sites_values = ws_cfg_sites.get_all_values()
    if not cfg_sites_values or len(cfg_sites_values) < 2:
        raise ValueError("❌ Cfg__Sites 为空或不可读")

    cfg_sites_header = cfg_sites_values[0]
    cfg_sites_norm_header = [norm_text(x) for x in cfg_sites_header]

    site_code_idx = _pick_required_idx(cfg_sites_norm_header, ["site_code", "site code"], "site_code")
    label_idx = _pick_required_idx(cfg_sites_norm_header, ["label"], "label")
    sheet_url_idx = _pick_required_idx(cfg_sites_norm_header, ["sheet_url", "sheet url"], "sheet_url")

    site_code_n = norm_text(site_code)
    label_n = norm_text(label)

    for r in cfg_sites_values[1:]:
        sc = norm_text(r[site_code_idx] if site_code_idx < len(r) else "")
        lb = norm_text(r[label_idx] if label_idx < len(r) else "")
        su = (r[sheet_url_idx] if sheet_url_idx < len(r) else "").strip()
        if sc == site_code_n and lb == label_n:
            if _is_blank(su):
                raise ValueError(f"❌ Cfg__Sites 命中 site_code={site_code}, label={label}，但 sheet_url 为空")
            return su

    raise ValueError(f"❌ Cfg__Sites 未找到 site_code={site_code}, label={label} 的记录")


def open_ws_by_label_and_title(
    gc: gspread.Client,
    console_core_url: str,
    site_code: str,
    label: str,
    worksheet_title: str,
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    create_if_missing: bool = False,
    rows: int = 1000,
    cols: int = 4,
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
                print(f"⚠️ 429 quota hit. retry in {backoff:.1f}s (attempt {attempt+1}/{max_retries})")
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


def write_chunk_4cols(
    ws,
    start_row: int,
    rows_4cols: list[list[Any]],
    *,
    target_writes_per_min: int,
    max_retries: int,
    base_backoff: float,
    jitter: float,
):
    if not rows_4cols:
        return
    end_row = start_row + len(rows_4cols) - 1
    safe_update_range(
        ws,
        f"A{start_row}:D{end_row}",
        rows_4cols,
        value_input_option="RAW",
        target_writes_per_min=target_writes_per_min,
        max_retries=max_retries,
        base_backoff=base_backoff,
        jitter=jitter,
    )


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
        safe_update_range(
            ws,
            "A1:L1",
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
    job_id: str,
    entity_type: str = "",
    gid: str = "",
    field_key: str = "",
    result: str = "SUCCESS",
    error_message: str = "",
    rows_read: Any = "",
    rows_written: Any = "",
    error_count: Any = "",
    error_summary: str = "",
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
        job_id,
        entity_type,
        gid,
        field_key,
        result,
        trim_text(error_message),
        rows_read,
        rows_written,
        error_count,
        trim_text(error_summary, max_len=1000),
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


def pick_col_index(header_row: list[Any], candidates: list[str]) -> Optional[int]:
    norm = [norm_header(h) for h in header_row]
    cand_norm = [norm_header(c) for c in candidates]
    for c in cand_norm:
        if c in norm:
            return norm.index(c)
    return None


def build_cfg_maps(
    ws_cfg,
    *,
    cfg_match_column_candidates: list[str],
    cfg_key_column_candidates: list[str],
    cfg_entity_column_candidates: list[str],
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    cfg_values = ws_cfg.get_all_values()
    if not cfg_values or len(cfg_values) < 2:
        raise ValueError("❌ Cfg__Fields 为空或不可读")

    header = cfg_values[0]
    match_idx = pick_col_index(header, cfg_match_column_candidates)
    key_idx = pick_col_index(header, cfg_key_column_candidates)
    entity_idx = pick_col_index(header, cfg_entity_column_candidates)

    if match_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到匹配列：{cfg_match_column_candidates}")
    if key_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到 key 列：{cfg_key_column_candidates}")
    if entity_idx is None:
        raise ValueError(f"❌ Cfg__Fields 找不到 entity_type 列：{cfg_entity_column_candidates}")

    map_name_to_key: dict[str, str] = {}
    map_key_to_entity: dict[str, str] = {}

    for r in cfg_values[1:]:
        if match_idx < len(r):
            name = r[match_idx]
            if not _is_blank(name):
                key = r[key_idx] if key_idx < len(r) else ""
                if not _is_blank(key):
                    map_name_to_key[norm_header(name)] = str(key).strip()

        key2 = r[key_idx] if key_idx < len(r) else ""
        ent = r[entity_idx] if entity_idx < len(r) else ""
        if (not _is_blank(key2)) and (not _is_blank(ent)):
            map_key_to_entity[str(key2).strip()] = str(ent).strip().upper()

    meta = {
        "match_col": header[match_idx],
        "key_col": header[key_idx],
        "entity_col": header[entity_idx],
    }
    print(
        "✅ CFG maps loaded:"
        f" name->key={len(map_name_to_key)} | key->entity={len(map_key_to_entity)}"
        f" | match_col={meta['match_col']} | key_col={meta['key_col']} | entity_col={meta['entity_col']}"
    )
    return map_name_to_key, map_key_to_entity, meta


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
    if len(values) < 1:
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
    misses: list[dict[str, Any]] = []

    for i in range(last_non_empty):
        h = header_row[i]
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
    print(f"✅ Wide row{wide_write_row} written: cols=1..{last_non_empty} (hit={hit}, miss={miss})")
    return {
        "last_non_empty_col": last_non_empty,
        "hit": hit,
        "miss": miss,
        "misses": misses,
    }


def build_long_rows(
    values_2d: list[list[Any]],
    map_key_to_entity: dict[str, str],
    *,
    include_empty: bool,
    dedup_output: bool,
) -> tuple[list[list[str]], bool, int]:
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

    a1 = str(header1[0]).strip()
    b1 = str(header1[1]).strip() if last_col >= 2 else ""
    special_split = (a1 == "Product ID (numeric)" and b1 == "Variant ID (numeric)")

    out: list[list[str]] = []
    seen = set() if dedup_output else None

    def infer_entity_from_key(k: str) -> str:
        if k in map_key_to_entity:
            return map_key_to_entity[k]
        return "VARIANT" if str(k).startswith("v_mf.") else "PRODUCT"

    def emit(_id: Any, _k: Any, _v: Any):
        if _is_blank(_id) or _is_blank(_k):
            return
        if (not include_empty) and _is_blank(_v):
            return

        k = str(_k).strip()
        ent = infer_entity_from_key(k)
        row = [ent, str(_id).strip(), k, "" if _v is None else str(_v)]

        if seen is not None:
            t = (row[0], row[1], row[2], row[3])
            if t in seen:
                return
            seen.add(t)
        out.append(row)

    if special_split:
        for r in data_rows:
            if not r:
                continue
            if len(r) < last_col:
                r = r + [""] * (last_col - len(r))

            product_id = r[0]
            variant_id = r[1] if last_col >= 2 else ""

            for j in range(2, last_col):
                k = key_row[j]
                v = r[j]
                if _is_blank(k):
                    continue
                ks = str(k).strip()
                if ks.startswith("v_mf."):
                    emit(variant_id, ks, v)
                else:
                    emit(product_id, ks, v)
    else:
        keys = key_row[1:last_col]
        empty_key_positions = [i for i, k in enumerate(keys, start=2) if _is_blank(k)]
        if empty_key_positions:
            show = empty_key_positions[:20]
            raise ValueError(
                "❌ 第2行 field_key 存在空值（会导致映射不可信）。\n"
                f"请补齐这些列的 key 或删除这些列。空 key 的列号(从B=2开始)：{show}"
                + (" ..." if len(empty_key_positions) > 20 else "")
            )

        n_fields = len(keys)
        for r in data_rows:
            if not r:
                continue
            if len(r) < last_col:
                r = r + [""] * (last_col - len(r))

            idv = r[0]
            if _is_blank(idv):
                continue

            row_vals = r[1:last_col]
            for j in range(n_fields):
                emit(idv, keys[j], row_vals[j])

    return out, special_split, last_col


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
    runlog_sheet_label: str = "runlog_sheet",
    runlog_worksheet_title: str = "Ops__RunLog",
    run_id: Optional[str] = None,
    include_empty: bool = False,
    clear_output_first: bool = True,
    default_action: str = "SET",
    default_mode: str = "",
    default_note: str = "",
    only_entity_types: Optional[set[str]] = None,
    do_write_wide_keys: bool = True,
    do_build_long: bool = True,
    write_runlog_enabled: bool = True,
    dedup_output: bool = True,
    out_chunk_rows: int = 20000,
    wide_header_row: int = WIDE_HEADER_ROW_DEFAULT,
    wide_write_row: int = WIDE_WRITE_ROW_DEFAULT,
    cfg_match_column_candidates: Optional[list[str]] = None,
    cfg_key_column_candidates: Optional[list[str]] = None,
    cfg_entity_column_candidates: Optional[list[str]] = None,
    target_writes_per_min: int = TARGET_WRITES_PER_MIN_DEFAULT,
    max_retries: int = MAX_RETRIES_DEFAULT,
    base_backoff: float = BASE_BACKOFF_DEFAULT,
    jitter: float = JITTER_DEFAULT,
) -> dict[str, Any]:
    del default_action, default_mode, default_note, only_entity_types  # kept for call compatibility

    cfg_match_column_candidates = cfg_match_column_candidates or CFG_MATCH_COLUMN_CANDIDATES_DEFAULT
    cfg_key_column_candidates = cfg_key_column_candidates or CFG_KEY_COLUMN_CANDIDATES_DEFAULT
    cfg_entity_column_candidates = cfg_entity_column_candidates or CFG_ENTITY_COLUMN_CANDIDATES_DEFAULT

    gc = build_gsheet_client(gsheet_sa_b64_secret)

    sh_wide, ws_wide, wide_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=input_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
    )
    sh_long, ws_long, long_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=output_sheet_label,
        worksheet_title=output_worksheet_title,
        cfg_sites_tab=cfg_sites_tab,
        create_if_missing=True,
        rows=1000,
        cols=4,
    )
    sh_cfg, ws_cfg, cfg_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=cfg_sheet_label,
        worksheet_title=cfg_tab_fields,
        cfg_sites_tab=cfg_sites_tab,
    )

    ws_runlog = None
    runlog_sheet_url = ""
    if write_runlog_enabled:
        _, ws_runlog, runlog_sheet_url = open_ws_by_label_and_title(
            gc=gc,
            console_core_url=console_core_url,
            site_code=site_code,
            label=runlog_sheet_label,
            worksheet_title=runlog_worksheet_title,
            cfg_sites_tab=cfg_sites_tab,
        )

    print("✅ Sheets ready")
    print("  pre_edit :", sh_wide.url, "|", ws_wide.title, "->", ws_long.title)
    print("  config   :", sh_cfg.url, "|", ws_cfg.title)
    if ws_runlog is not None:
        print("  runlog   :", runlog_sheet_url, "|", ws_runlog.title)

    actual_run_id = run_id or make_run_id(job_name)
    ts_cn = cn_now_str()

    rows_read = 0
    rows_written = 0
    rows_mapped = 0
    error_count = 0

    wide_key_write_meta: dict[str, Any] = {}
    cfg_meta: dict[str, Any] = {}
    preview: list[dict[str, str]] = []

    try:
        map_name_to_key: dict[str, str] = {}
        map_key_to_entity: dict[str, str] = {}

        if do_write_wide_keys or do_build_long:
            map_name_to_key, map_key_to_entity, cfg_meta = build_cfg_maps(
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

        if do_build_long:
            values = ws_wide.get_all_values()
            rows_read = max(0, len(values) - 2)

            long_rows, special_split, last_col = build_long_rows(
                values,
                map_key_to_entity,
                include_empty=include_empty,
                dedup_output=dedup_output,
            )
            rows_mapped = len(long_rows)

            print(f"✅ Mode={'SPECIAL_SPLIT' if special_split else 'ORIGINAL'} | last_col={last_col} | rows_to_write={len(long_rows):,}")

            if clear_output_first:
                safe_clear(
                    ws_long,
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )

            safe_update_range(
                ws_long,
                "A1:D1",
                [LONG_HEADER],
                value_input_option="RAW",
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

            est_rows = 1 + len(long_rows)
            est_cells = est_rows * 4
            if est_cells > 9_500_000:
                print("⚠️ 警告：预计接近/超过单表 1000万 cell 上限。建议：拆分多张 Long_* 表。")

            safe_resize(
                ws_long,
                rows=max(ws_long.row_count, min(est_rows + 50, 2_300_000)),
                cols=4,
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
                    write_chunk_4cols(
                        ws_long,
                        out_row_cursor,
                        buf,
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
                write_chunk_4cols(
                    ws_long,
                    out_row_cursor,
                    buf,
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )
                written += len(buf)

            rows_written = written
            dt = time.time() - t0
            print(f"✅ Done. Output rows written: {written:,} | time: {dt:.1f}s | INCLUDE_EMPTY={include_empty} | DEDUP={dedup_output}")
            preview = [dict(zip(LONG_HEADER, row)) for row in long_rows[:50]]

        if write_runlog_enabled and ws_runlog is not None:
            write_runlog(
                ws_runlog,
                run_id=actual_run_id,
                ts_cn=ts_cn,
                job_id=job_name,
                entity_type="",
                gid="",
                field_key="",
                result="SUCCESS",
                error_message="",
                rows_read=rows_read,
                rows_written=rows_written,
                error_count=0,
                error_summary="",
                target_writes_per_min=target_writes_per_min,
                max_retries=max_retries,
                base_backoff=base_backoff,
                jitter=jitter,
            )

        return {
            "status": "SUCCESS",
            "run_id": actual_run_id,
            "ts_cn": ts_cn,
            "job_id": job_name,
            "summary": {
                "job_name": job_name,
                "site_code": site_code,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "mapped_columns": wide_key_write_meta.get("hit", 0),
                "unmapped_columns": wide_key_write_meta.get("miss", 0),
                "rows_mapped": rows_mapped,
                "include_empty": include_empty,
                "clear_output_first": clear_output_first,
                "do_write_wide_keys": do_write_wide_keys,
                "do_build_long": do_build_long,
                "dedup_output": dedup_output,
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
            },
        }

    except Exception as e:
        error_count = 1
        error_message = trim_text(str(e), max_len=50000)
        error_summary = summarize_error(e, max_len=1000)

        print(f"❌ Run failed: {error_message}")

        if write_runlog_enabled and ws_runlog is not None:
            try:
                write_runlog(
                    ws_runlog,
                    run_id=actual_run_id,
                    ts_cn=ts_cn,
                    job_id=job_name,
                    entity_type="",
                    gid="",
                    field_key="",
                    result="ERROR",
                    error_message=error_message,
                    rows_read=rows_read,
                    rows_written=rows_written,
                    error_count=error_count,
                    error_summary=error_summary,
                    target_writes_per_min=target_writes_per_min,
                    max_retries=max_retries,
                    base_backoff=base_backoff,
                    jitter=jitter,
                )
            except Exception as e2:
                print(f"⚠️ RunLog 写入失败：{e2}")

        raise
