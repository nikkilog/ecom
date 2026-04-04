# shopify_export/build_product_views.py
# -*- coding: utf-8 -*-

import re
import io
import json
import time
import base64
import random
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# 基础工具
# =========================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _safe_str(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s


def _safe_json_loads(x):
    s = _safe_str(x)
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}


def _norm_bool(x) -> bool:
    s = _safe_str(x).upper()
    return s in ("TRUE", "1", "YES", "Y", "ON")


def _make_unique_cols(cols: List[str]) -> List[str]:
    """
    内部 DataFrame 列名唯一化：
    SKU, SKU -> SKU, SKU__2
    """
    seen = {}
    out = []
    for c in cols:
        k = _safe_str(c)
        if not k:
            k = "__blank__"
        seen[k] = seen.get(k, 0) + 1
        if seen[k] == 1:
            out.append(k)
        else:
            out.append(f"{k}__{seen[k]}")
    return out


def _make_display_headers(headers: List[str]) -> List[str]:
    """
    最终显示表头：
    SKU, SKU -> SKU, SKU-2
    """
    seen = {}
    out = []
    for h in headers:
        k = _safe_str(h) or "Unnamed"
        seen[k] = seen.get(k, 0) + 1
        if seen[k] == 1:
            out.append(k)
        else:
            out.append(f"{k}-{seen[k]}")
    return out


def _dedupe_columns_keep_first(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _as_scalar(v):
    """
    若同名列导致 row[col] 返回 Series，则取第一个非空值
    """
    if isinstance(v, pd.Series):
        for x in v.tolist():
            s = _safe_str(x)
            if s:
                return s
        return _safe_str(v.iloc[0]) if len(v) else ""
    return _safe_str(v)


def col_to_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# =========================================================
# Google auth / gspread
# =========================================================

def make_gspread_client_from_b64(service_account_b64: str):
    raw = base64.b64decode(service_account_b64)
    info = json.loads(raw.decode("utf-8"))
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc


def ws_update(ws, a1_range: str, values, value_input_option="RAW", max_retry=6):
    for i in range(max_retry):
        try:
            ws.update(a1_range, values, value_input_option=value_input_option)
            return
        except Exception:
            if i == max_retry - 1:
                raise
            time.sleep((1.2 ** i) + random.random())


def clear_worksheet(ws):
    ws.clear()


def ensure_worksheet(sh, title: str, rows: int = 1000, cols: int = 50):
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def read_ws_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    header = values[0]
    header = [str(x).strip() for x in header]
    header = _make_unique_cols(header)

    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)

    max_len = max(len(header), max((len(r) for r in rows), default=0))
    header = header + [f"__extra_col_{i}" for i in range(len(header) + 1, max_len + 1)]

    norm_rows = []
    for r in rows:
        rr = list(r) + [""] * (max_len - len(r))
        norm_rows.append(rr[:max_len])

    df = pd.DataFrame(norm_rows, columns=header)
    df = df.fillna("")
    return df


# =========================================================
# Console / sheet 定位
# =========================================================

def get_label_sheet_url_from_cfg_sites(gc, console_core_url: str, site_code: str, label: str) -> str:
    sh = gc.open_by_url(console_core_url)
    ws = sh.worksheet("Cfg__Sites")
    df = read_ws_df(ws)

    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()

    hit = df[
        (df["site_code"] == site_code.strip().upper()) &
        (df["label"] == label.strip())
    ]

    if hit.empty:
        raise ValueError(f"Cfg__Sites 未找到 site_code={site_code}, label={label}")

    url = _safe_str(hit.iloc[0].get("sheet_url"))
    if not url:
        raise ValueError(f"Cfg__Sites 命中但 sheet_url 为空: site_code={site_code}, label={label}")
    return url


# =========================================================
# Header / field / filter
# =========================================================

def normalize_idx_columns(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    把 IDX 表中的 core./mf./v_mf./raw./mo. 补成实体前缀：
    PRODUCT|core.title
    VARIANT|core.sku
    """
    if df is None or df.empty:
        return df

    existing = set(df.columns.astype(str))
    rename_map = {}

    for c in df.columns:
        c0 = str(c).strip()
        if c0.startswith(prefix + "|"):
            continue
        if c0.startswith(("core.", "mf.", "v_mf.", "raw.", "mo.")):
            new_name = f"{prefix}|{c0}"
            if new_name in existing:
                continue
            rename_map[c] = new_name

    if rename_map:
        df = df.rename(columns=rename_map)

    if df.columns.duplicated().any():
        df.columns = _make_unique_cols([str(x) for x in df.columns])

    return df


def split_filters_by_entity(filters: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    pf, vf = {}, {}
    for k, v in (filters or {}).items():
        kk = _safe_str(k)
        if kk.startswith("PRODUCT|"):
            pf[kk] = v
        elif kk.startswith("VARIANT|"):
            vf[kk] = v
    return pf, vf


def apply_entity_filters(df: pd.DataFrame, filters: Dict[str, Any], mode: str = "AND") -> pd.DataFrame:
    if df is None or df.empty or not filters:
        return df

    mode = _safe_str(mode).upper() or "AND"
    masks = []

    for col, want in filters.items():
        if col not in df.columns:
            continue

        series = df[col].astype(str).fillna("").str.strip()

        if isinstance(want, list):
            want_set = set([_safe_str(x) for x in want])
            mask = series.isin(want_set)
        else:
            mask = series == _safe_str(want)

        masks.append(mask)

    if not masks:
        return df

    final_mask = masks[0]
    for m in masks[1:]:
        final_mask = (final_mask & m) if mode == "AND" else (final_mask | m)

    return df[final_mask].copy()


# =========================================================
# Formula
# =========================================================

TOKEN_RE = re.compile(r"\{([^}]+)\}")


def compile_formula(expr: str, row_num_1based: int, token_to_col_letter: Dict[str, str]) -> str:
    s = _safe_str(expr)
    if not s.startswith("="):
        return s

    def repl(m):
        token = m.group(1).strip()
        col = token_to_col_letter.get(token)
        if not col:
            return ""
        return f"{col}{row_num_1based}"

    return TOKEN_RE.sub(repl, s)


def _group_contiguous_cols(formula_cols):
    cols = sorted(formula_cols, key=lambda x: x[0])
    blocks = []
    cur = []
    prev = None
    for item in cols:
        ci = item[0]
        if prev is None or ci == prev + 1:
            cur.append(item)
        else:
            blocks.append(cur)
            cur = [item]
        prev = ci
    if cur:
        blocks.append(cur)
    return blocks


def write_formula_columns_chunked(
    ws,
    formula_cols,      # [(ci, expr, output_key), ...]
    start_row_1based: int,
    nrows: int,
    token_to_col_letter: Dict[str, str],
    chunk_rows: int = 1200,
):
    if nrows <= 0 or not formula_cols:
        return

    blocks = _group_contiguous_cols(formula_cols)
    total_chunks = (nrows + chunk_rows - 1) // chunk_rows

    for chunk_i in range(total_chunks):
        r_start = start_row_1based + chunk_i * chunk_rows
        r_end = min(start_row_1based + nrows - 1, r_start + chunk_rows - 1)

        for block in blocks:
            c0 = block[0][0]
            c1 = block[-1][0]
            a1 = f"{col_to_letter(c0)}{r_start}:{col_to_letter(c1)}{r_end}"

            values = []
            for rr in range(r_start, r_end + 1):
                row_vals = []
                for (ci, expr0, output_key) in block:
                    row_vals.append(compile_formula(expr0, rr, token_to_col_letter))
                values.append(row_vals)

            ws_update(ws, a1, values, value_input_option="USER_ENTERED")


# =========================================================
# field / join / value 解析
# =========================================================

def _prepare_field_rows(vf: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    每一列内部唯一 output_key；
    token 支持：
    1) field_id，例如 {PRODUCT|core.legacy_id}
    2) alias，例如 {Product Image}
    3) output_key（内部）
    """
    rows = []
    output_seen = {}

    for _, r in vf.iterrows():
        field_id = _safe_str(r.get("field_id"))
        alias = _safe_str(r.get("alias")) or field_id
        expr = _safe_str(r.get("expr"))
        field_type = _safe_str(r.get("field_type")).upper() or "RAW"
        entity_type = _safe_str(r.get("entity_type")).upper()
        field_key = _safe_str(r.get("field_key"))
        data_type = _safe_str(r.get("data_type"))
        join_key = _safe_str(r.get("join_key"))
        agg = _safe_str(r.get("agg"))

        base_key = alias or field_id or "col"
        output_seen[base_key] = output_seen.get(base_key, 0) + 1
        if output_seen[base_key] == 1:
            output_key = base_key
        else:
            output_key = f"{base_key}__{output_seen[base_key]}"

        rows.append({
            "field_id": field_id,
            "alias": alias,
            "expr": expr,
            "field_type": field_type,
            "entity_type": entity_type,
            "field_key": field_key,
            "data_type": data_type,
            "join_key": join_key,
            "agg": agg,
            "output_key": output_key,
        })

    return rows


def _build_row_maps(df_products: pd.DataFrame, df_variants: pd.DataFrame):
    """
    建立 PRODUCT / VARIANT 的常用索引
    """
    product_by_gid = {}
    variant_by_gid = {}

    product_gid_col = "PRODUCT|core.gid" if "PRODUCT|core.gid" in df_products.columns else None
    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None

    if product_gid_col:
        for _, r in df_products.iterrows():
            gid = _as_scalar(r.get(product_gid_col, ""))
            if gid:
                product_by_gid[gid] = r

    if variant_gid_col:
        for _, r in df_variants.iterrows():
            gid = _as_scalar(r.get(variant_gid_col, ""))
            if gid:
                variant_by_gid[gid] = r

    return {
        "product_by_gid": product_by_gid,
        "variant_by_gid": variant_by_gid,
    }


def _build_variant_product_bridge(df_variants: pd.DataFrame) -> Dict[str, str]:
    """
    让 VARIANT 行能找到所属 PRODUCT gid
    优先尝试这些列：
    - VARIANT|core.product.gid
    - VARIANT|core.parent.gid
    - PRODUCT|core.gid（如果 merge 后已在变体表里）
    """
    bridge = {}
    if df_variants is None or df_variants.empty:
        return bridge

    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None
    candidate_product_gid_cols = [
        "VARIANT|core.product.gid",
        "VARIANT|core.parent.gid",
        "PRODUCT|core.gid",
    ]

    if not variant_gid_col:
        return bridge

    for _, r in df_variants.iterrows():
        vg = _as_scalar(r.get(variant_gid_col, ""))
        if not vg:
            continue
        pg = ""
        for c in candidate_product_gid_cols:
            if c in df_variants.columns:
                pg = _as_scalar(r.get(c, ""))
                if pg:
                    break
        if pg:
            bridge[vg] = pg

    return bridge


def _build_long_value_map(df_dl_values_long: pd.DataFrame) -> Dict[Tuple[str, str, str], str]:
    """
    key = (entity_type, gid_or_handle, field_key)
    """
    dl = df_dl_values_long.copy()
    if dl is None or dl.empty:
        return {}

    dl.columns = [_safe_str(c) for c in dl.columns]
    required_cols = {"entity_type", "gid_or_handle", "field_key", "desired_value"}
    if not required_cols.issubset(set(dl.columns)):
        return {}

    dl["entity_type"] = dl["entity_type"].astype(str).str.upper().str.strip()
    dl["gid_or_handle"] = dl["gid_or_handle"].astype(str).str.strip()
    dl["field_key"] = dl["field_key"].astype(str).str.strip()

    mp = {}
    for _, r in dl.iterrows():
        et = _safe_str(r["entity_type"]).upper()
        gid = _safe_str(r["gid_or_handle"])
        fk = _safe_str(r["field_key"])
        dv = _safe_str(r["desired_value"])
        if et and gid and fk:
            mp[(et, gid, fk)] = dv
    return mp


def _resolve_related_rows(
    base_entity_type: str,
    base_row: pd.Series,
    row_maps: Dict[str, Any],
    variant_product_bridge: Dict[str, str],
) -> Dict[str, Optional[pd.Series]]:
    """
    给当前 base row 同时解析出 product_row / variant_row
    """
    product_row = None
    variant_row = None

    if base_entity_type == "PRODUCT":
        product_row = base_row

    elif base_entity_type == "VARIANT":
        variant_row = base_row
        variant_gid = _as_scalar(base_row.get("VARIANT|core.gid", ""))
        product_gid = variant_product_bridge.get(variant_gid, "")
        if product_gid:
            product_row = row_maps["product_by_gid"].get(product_gid)

    return {
        "PRODUCT": product_row,
        "VARIANT": variant_row,
    }


def _get_from_row_by_candidates(row: Optional[pd.Series], candidates: List[str]) -> str:
    if row is None:
        return ""
    for c in candidates:
        if c in row.index:
            v = _as_scalar(row[c])
            if v != "":
                return v
    return ""


def _resolve_raw_value(
    *,
    field_row: Dict[str, Any],
    related_rows: Dict[str, Optional[pd.Series]],
    long_value_map: Dict[Tuple[str, str, str], str],
) -> str:
    """
    RAW 取值规则：
    1. 先按 field_id / field_key 到对应实体 row 找
    2. 找不到再按 expr（非公式）指向的列名找
    3. 还找不到再去 DL__ValuesLong 按 (entity_type, gid, field_key) 找
    """
    field_id = _safe_str(field_row.get("field_id"))
    field_key = _safe_str(field_row.get("field_key"))
    expr = _safe_str(field_row.get("expr"))
    entity_type = _safe_str(field_row.get("entity_type")).upper()

    row = related_rows.get(entity_type)

    # 1) 直接按 field_id / field_key 查实体 row
    candidates = []
    if field_id:
        candidates.append(field_id)
        if "|" not in field_id and entity_type:
            candidates.append(f"{entity_type}|{field_id}")
    if field_key:
        candidates.append(field_key)
        if "|" not in field_key and entity_type:
            candidates.append(f"{entity_type}|{field_key}")

    v = _get_from_row_by_candidates(row, candidates)
    if v != "":
        return v

    # 2) expr 非公式时，允许 expr 作为“源列名”
    if expr and not expr.startswith("="):
        expr_candidates = [expr]
        if "|" not in expr and entity_type:
            expr_candidates.append(f"{entity_type}|{expr}")
        v = _get_from_row_by_candidates(row, expr_candidates)
        if v != "":
            return v

    # 3) 去 DL__ValuesLong
    if row is not None and field_key:
        gid = _get_from_row_by_candidates(row, [f"{entity_type}|core.gid", f"{entity_type}|gid", "core.gid", "gid"])
        if gid:
            v = long_value_map.get((entity_type, gid, field_key), "")
            if v != "":
                return v

    return ""


# =========================================================
# 主流程
# =========================================================

def build_and_write_view(
    *,
    sh_data,
    cfg_tabs_df: pd.DataFrame,
    cfg_fields_df: pd.DataFrame,
    df_idx_products: pd.DataFrame,
    df_idx_variants: pd.DataFrame,
    df_dl_values_long: pd.DataFrame,
    view_id: str,
    global_filters: Optional[Dict[str, Any]] = None,
    use_default_filters: bool = True,
    view_filter_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    verbose: bool = True,
) -> Dict[str, Any]:

    view_id = _safe_str(view_id)
    global_filters = global_filters or {}
    view_filter_overrides = view_filter_overrides or {}

    tab_row = cfg_tabs_df[cfg_tabs_df["view_id"].astype(str).str.strip() == view_id]
    if tab_row.empty:
        raise ValueError(f"Cfg__ExportTabs 找不到 view_id={view_id}")

    tab = tab_row.iloc[0].to_dict()

    target_sheet = _safe_str(tab.get("target_sheet")) or view_id
    layout = _safe_str(tab.get("layout")) or "WIDE"
    base_entity_type = _safe_str(tab.get("base_entity_type")).upper()
    base_sheet = _safe_str(tab.get("base_sheet")).upper()
    base_key_field_id = _safe_str(tab.get("base_key_field_id"))
    fixed_filter_mode = _safe_str(tab.get("fixed_filter_mode")) or "AND"
    fixed_filters_json = _safe_json_loads(tab.get("fixed_filters_json"))

    if layout != "WIDE":
        raise ValueError(f"当前只支持 WIDE，view_id={view_id}, layout={layout}")

    if base_entity_type not in ("PRODUCT", "VARIANT"):
        raise ValueError(f"view_id={view_id} 的 base_entity_type 非 PRODUCT/VARIANT")

    vf = cfg_fields_df[cfg_fields_df["view_id"].astype(str).str.strip() == view_id].copy()
    if vf.empty:
        raise ValueError(f"Cfg__ExportTabFields 找不到 view_id={view_id} 的字段")

    if "seq" in vf.columns:
        vf["seq"] = pd.to_numeric(vf["seq"], errors="coerce")
        vf = vf.sort_values(["seq", "field_id"], na_position="last")

    # ---- base df
    if base_entity_type == "PRODUCT":
        base_df = df_idx_products.copy()
    else:
        base_df = df_idx_variants.copy()

    base_df = _dedupe_columns_keep_first(base_df)

    # ---- filters
    global_pf, global_vf = split_filters_by_entity(global_filters)
    fixed_pf, fixed_vf = split_filters_by_entity(fixed_filters_json if use_default_filters else {})
    override_pf, override_vf = split_filters_by_entity(view_filter_overrides.get(view_id, {}))

    if base_entity_type == "PRODUCT":
        merged_filters = {}
        merged_filters.update(global_pf)
        merged_filters.update(fixed_pf)
        merged_filters.update(override_pf)
    else:
        merged_filters = {}
        merged_filters.update(global_vf)
        merged_filters.update(fixed_vf)
        merged_filters.update(override_vf)

    base_df = apply_entity_filters(base_df, merged_filters, fixed_filter_mode)

    if base_key_field_id and base_key_field_id not in base_df.columns:
        # 容忍历史 header 未补实体前缀的情况
        short_key = base_key_field_id.split("|", 1)[-1] if "|" in base_key_field_id else base_key_field_id
        if short_key in base_df.columns:
            base_key_field_id = short_key

    if base_key_field_id and base_key_field_id not in base_df.columns:
        raise ValueError(f"view_id={view_id} 的 base_key_field_id 在 base_df 中不存在：{base_key_field_id}")

    # ---- 预备 map
    row_maps = _build_row_maps(df_idx_products, df_idx_variants)
    variant_product_bridge = _build_variant_product_bridge(df_idx_variants)
    long_value_map = _build_long_value_map(df_dl_values_long)

    # ---- fields
    field_rows = _prepare_field_rows(vf)

    output_keys = [fr["output_key"] for fr in field_rows]
    display_headers_raw = [fr["alias"] or fr["field_id"] or fr["output_key"] for fr in field_rows]
    display_headers = _make_display_headers(display_headers_raw)

    # ---- 构造数据
    out_rows = []

    for _, base_row in base_df.iterrows():
        base_row = base_row.copy()
        related_rows = _resolve_related_rows(
            base_entity_type=base_entity_type,
            base_row=base_row,
            row_maps=row_maps,
            variant_product_bridge=variant_product_bridge,
        )

        one = {}

        for fr in field_rows:
            field_type = _safe_str(fr["field_type"]).upper()
            output_key = fr["output_key"]

            if field_type == "CALC" and _safe_str(fr["expr"]).startswith("="):
                one[output_key] = ""
            else:
                one[output_key] = _resolve_raw_value(
                    field_row=fr,
                    related_rows=related_rows,
                    long_value_map=long_value_map,
                )

        out_rows.append(one)

    out_df = pd.DataFrame(out_rows, columns=output_keys).fillna("")

    # ---- 写表：header + raw body
    ws = ensure_worksheet(
        sh_data,
        target_sheet,
        rows=max(len(out_df) + 20, 1000),
        cols=max(len(display_headers) + 10, 50),
    )
    clear_worksheet(ws)

    if len(display_headers) == 0:
        ws_update(ws, "A1", [["No columns"]])
        return {
            "view_id": view_id,
            "target_sheet": target_sheet,
            "rows_written": 0,
            "cols_written": 0,
        }

    ws_update(ws, f"A1:{col_to_letter(len(display_headers))}1", [display_headers])

    if len(out_df) > 0:
        body_values = out_df[output_keys].astype(str).values.tolist()
        ws_update(ws, f"A2:{col_to_letter(len(display_headers))}{len(body_values)+1}", body_values)

    # ---- 公式 token -> col 映射
    # 支持三种 token：
    # 1) {field_id}         例如 {PRODUCT|core.legacy_id}
    # 2) {alias}            例如 {Product Image}
    # 3) {output_key}       内部 key
    token_to_col_letter = {}
    for i, fr in enumerate(field_rows, start=1):
        col_letter = col_to_letter(i)

        output_key = fr["output_key"]
        field_id = _safe_str(fr["field_id"])
        alias = _safe_str(fr["alias"])

        token_to_col_letter[output_key] = col_letter
        if field_id:
            token_to_col_letter[field_id] = col_letter
        if alias:
            token_to_col_letter[alias] = col_letter

    # ---- 公式列
    formula_cols = []
    for i, fr in enumerate(field_rows, start=1):
        expr = _safe_str(fr["expr"])
        field_type = _safe_str(fr["field_type"]).upper()
        if field_type == "CALC" and expr.startswith("="):
            formula_cols.append((i, expr, fr["output_key"]))

    if formula_cols and len(out_df) > 0:
        write_formula_columns_chunked(
            ws=ws,
            formula_cols=formula_cols,
            start_row_1based=2,
            nrows=len(out_df),
            token_to_col_letter=token_to_col_letter,
        )

    if verbose:
        print(f"view={view_id}")
        print(f"target_sheet={target_sheet}")
        print(f"base_entity_type={base_entity_type}")
        print(f"base_key_field_id={base_key_field_id}")
        print(f"rows={len(out_df)} cols={len(display_headers)}")

    return {
        "view_id": view_id,
        "target_sheet": target_sheet,
        "rows_written": int(len(out_df)),
        "cols_written": int(len(display_headers)),
    }


def run(
    *,
    site_code: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    view_toggles: Dict[str, bool],
    use_default_filters: bool = True,
    view_filter_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    verbose: bool = True,
):
    gc = make_gspread_client_from_b64(gsheet_sa_b64)

    config_url = get_label_sheet_url_from_cfg_sites(gc, console_core_url, site_code, "config")
    export_product_url = get_label_sheet_url_from_cfg_sites(gc, console_core_url, site_code, "export_product")

    sh_cfg = gc.open_by_url(config_url)
    sh_data = gc.open_by_url(export_product_url)

    ws_tabs = sh_cfg.worksheet("Cfg__ExportTabs")
    ws_fields = sh_cfg.worksheet("Cfg__ExportTabFields")
    cfg_tabs_df = read_ws_df(ws_tabs)
    cfg_fields_df = read_ws_df(ws_fields)

    ws_idx_products = sh_data.worksheet("IDX__Products")
    ws_idx_variants = sh_data.worksheet("IDX__Variants")
    ws_dl = sh_data.worksheet("DL__ValuesLong")

    df_idx_products = read_ws_df(ws_idx_products)
    df_idx_variants = read_ws_df(ws_idx_variants)
    df_dl_values_long = read_ws_df(ws_dl)

    df_idx_products = normalize_idx_columns(df_idx_products, "PRODUCT")
    df_idx_variants = normalize_idx_columns(df_idx_variants, "VARIANT")

    if verbose:
        print("loaded:")
        print("  cfg tabs rows     :", len(cfg_tabs_df))
        print("  cfg fields rows   :", len(cfg_fields_df))
        print("  idx products rows :", len(df_idx_products))
        print("  idx variants rows :", len(df_idx_variants))
        print("  dl rows           :", len(df_dl_values_long))
        print("  products dup cols :", df_idx_products.columns.duplicated().any())
        print("  variants dup cols :", df_idx_variants.columns.duplicated().any())

    enabled_view_ids = [k for k, v in (view_toggles or {}).items() if bool(v)]
    if not enabled_view_ids:
        raise ValueError("没有任何启用的 view。请在 VIEW_TOGGLES 里至少打开一个 True。")

    results = []
    for vid in enabled_view_ids:
        if verbose:
            print(f"\n=== build view: {vid} ===")
        res = build_and_write_view(
            sh_data=sh_data,
            cfg_tabs_df=cfg_tabs_df,
            cfg_fields_df=cfg_fields_df,
            df_idx_products=df_idx_products,
            df_idx_variants=df_idx_variants,
            df_dl_values_long=df_dl_values_long,
            view_id=vid,
            global_filters={},
            use_default_filters=use_default_filters,
            view_filter_overrides=view_filter_overrides or {},
            verbose=verbose,
        )
        results.append(res)
        if verbose:
            print("done:", res)

    return {
        "site_code": site_code,
        "view_count": len(results),
        "results": results,
        "finished_at": _now_ts(),
    }
