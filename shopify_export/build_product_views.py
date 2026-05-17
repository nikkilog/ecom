# shopify_export/build_product_views.py
# -*- coding: utf-8 -*-

import re
import io
import json
import time
import base64
import random
from collections import defaultdict
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



def _safe_json_list_loads(x) -> List[Any]:
    """
    Parse a cell that may contain:
    - JSON list: ["url1", "url2"]
    - old pipe list: url1 | url2
    - plain single value
    """
    if isinstance(x, list):
        return x

    s = _safe_str(x)
    if not s:
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, list) else []
        except Exception:
            pass

    if " | " in s:
        return [p.strip() for p in s.split(" | ") if p.strip()]

    return [s]


def _is_image_list_field_row(fr: Dict[str, Any]) -> bool:
    field_key = _safe_str(fr.get("field_key")).lower()
    alias = _safe_str(fr.get("alias")).lower()
    field_id = _safe_str(fr.get("field_id")).lower()
    data_type = _safe_str(fr.get("data_type")).lower()

    if field_key.endswith("images_urls") or field_key.endswith("images_json"):
        return True

    # Compatible with aliases from Cfg__ExportTabFields.
    if alias in ("product images urls", "product images json", "variant product images urls", "variant product images json"):
        return True

    if field_id.endswith("product.images_urls") or field_id.endswith("product.images_json"):
        return True
    if field_id.endswith("variant.product_images_urls") or field_id.endswith("variant.product_images_json"):
        return True

    return False


def _expand_image_field_rows_for_output(
    field_rows: List[Dict[str, Any]],
    base_df: pd.DataFrame,
    max_images: int = 15,
) -> List[Dict[str, Any]]:
    """
    Build final output field rows.

    IDX may keep image list as one raw JSON/list column.
    This function expands it only in the final product view output:
    Product Images URLs -> Product Image 01, Product Image 02, ...

    This keeps IDX clean and machine-readable while final tabs stay human-readable.
    """
    out: List[Dict[str, Any]] = []
    output_seen: Dict[str, int] = {}

    def unique_output_key(base_key: str) -> str:
        base_key = _safe_str(base_key) or "Product Image"
        output_seen[base_key] = output_seen.get(base_key, 0) + 1
        if output_seen[base_key] == 1:
            return base_key
        return f"{base_key}__{output_seen[base_key]}"

    for fr in field_rows:
        fid = _safe_str(fr.get("field_id"))

        if not _is_image_list_field_row(fr):
            fr2 = dict(fr)
            fr2["output_key"] = unique_output_key(_safe_str(fr2.get("output_key")) or _safe_str(fr2.get("alias")) or fid)
            out.append(fr2)
            continue

        if not fid or base_df is None or base_df.empty or fid not in base_df.columns:
            # Keep a single blank column if the configured field does not exist yet.
            fr2 = dict(fr)
            fr2["alias"] = "Product Image 01"
            fr2["output_key"] = unique_output_key("Product Image 01")
            fr2["__image_source_field_id"] = fid
            fr2["__image_index"] = 0
            out.append(fr2)
            continue

        lists = base_df[fid].apply(_safe_json_list_loads)
        real_max = min(max_images, max([len(x) for x in lists.tolist()] + [0]))

        if real_max <= 0:
            real_max = 1

        for i in range(real_max):
            fr2 = dict(fr)
            fr2["alias"] = f"Product Image {i + 1:02d}"
            fr2["output_key"] = unique_output_key(fr2["alias"])
            fr2["field_type"] = "RAW"
            fr2["expr"] = ""
            fr2["__image_source_field_id"] = fid
            fr2["__image_index"] = i
            out.append(fr2)

    return out


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




def _extract_gid_like(v):
    """
    支持：
    - 纯 gid: gid://shopify/Product/123
    - JSON 字符串: {"id":"gid://shopify/Product/123"}
    - dict: {"id":"gid://shopify/Product/123"}
    其他情况回退为字符串本身
    """
    if isinstance(v, dict):
        if "id" in v:
            return _safe_str(v.get("id"))
        return _safe_str(v)
    s = _safe_str(v)
    if not s:
        return ""
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and "id" in obj:
                return _safe_str(obj.get("id"))
        except Exception:
            pass
    return s

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


def resize_worksheet(ws, rows: int, cols: int):
    rows = max(int(rows or 1), 1)
    cols = max(int(cols or 1), 1)
    if ws.row_count != rows or ws.col_count != cols:
        ws.resize(rows=rows, cols=cols)


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


def write_formula_columns_filldown(
    ws,
    formula_cols,      # [(ci, expr, output_key), ...]
    start_row_1based: int,
    nrows: int,
    token_to_col_letter: Dict[str, str],
):
    if nrows <= 0 or not formula_cols:
        return

    blocks = _group_contiguous_cols(formula_cols)

    # 先只写第 2 行（或 start_row_1based 指定行）
    for block in blocks:
        c0 = block[0][0]
        c1 = block[-1][0]
        a1 = f"{col_to_letter(c0)}{start_row_1based}:{col_to_letter(c1)}{start_row_1based}"
        values = [[compile_formula(expr0, start_row_1based, token_to_col_letter) for (_, expr0, _) in block]]
        ws_update(ws, a1, values, value_input_option="USER_ENTERED")

    # 只有一行数据时，到这里就够了
    if nrows <= 1:
        return

    requests = []
    for block in blocks:
        c0 = block[0][0]
        c1 = block[-1][0]
        requests.append({
            "copyPaste": {
                "source": {
                    "sheetId": ws.id,
                    "startRowIndex": start_row_1based - 1,
                    "endRowIndex": start_row_1based,
                    "startColumnIndex": c0 - 1,
                    "endColumnIndex": c1,
                },
                "destination": {
                    "sheetId": ws.id,
                    "startRowIndex": start_row_1based,
                    "endRowIndex": start_row_1based - 1 + nrows,
                    "startColumnIndex": c0 - 1,
                    "endColumnIndex": c1,
                },
                "pasteType": "PASTE_FORMULA",
                "pasteOrientation": "NORMAL",
            }
        })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})


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

        # 兼容 "join_key" 和 "join key"
        join_key = _safe_str(r.get("join_key"))
        if not join_key:
            join_key = _safe_str(r.get("join key"))

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
    product_by_legacy_id = {}
    variant_by_gid = {}

    product_gid_col = "PRODUCT|core.gid" if "PRODUCT|core.gid" in df_products.columns else None
    product_legacy_cols = [
        "PRODUCT|core.legacy_id",
        "PRODUCT|core.legacyResourceId",
        "PRODUCT|product.legacyResourceId",
    ]

    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None

    if product_gid_col:
        for _, r in df_products.iterrows():
            gid = _as_scalar(r.get(product_gid_col, ""))
            if gid:
                product_by_gid[gid] = r

    for _, r in df_products.iterrows():
        pid = ""
        for c in product_legacy_cols:
            if c in df_products.columns:
                pid = _as_scalar(r.get(c, ""))
                if pid:
                    break
        if pid:
            product_by_legacy_id[pid] = r

    if variant_gid_col:
        for _, r in df_variants.iterrows():
            gid = _as_scalar(r.get(variant_gid_col, ""))
            if gid:
                variant_by_gid[gid] = r

    return {
        "product_by_gid": product_by_gid,
        "product_by_legacy_id": product_by_legacy_id,
        "variant_by_gid": variant_by_gid,
    }


def _build_variant_product_bridge(
    df_variants: pd.DataFrame,
    row_maps: Dict[str, Any],
) -> Dict[str, str]:
    """
    bridge[variant_gid] = product_gid
    兼容 VARIANT|core.product_gid 为纯 gid / JSON 字符串 / dict
    """
    bridge = {}
    if df_variants is None or df_variants.empty:
        return bridge

    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None
    if not variant_gid_col:
        return bridge

    candidate_product_gid_cols = [
        "VARIANT|core.product_gid",
        "VARIANT|core.product.gid",
        "VARIANT|core.parent.gid",
        "PRODUCT|core.gid",
    ]

    candidate_product_legacy_cols = [
        "VARIANT|core.product.legacy_id",
        "VARIANT|core.product.legacyResourceId",
        "VARIANT|product.legacyResourceId",
        "PRODUCT|core.legacy_id",
        "PRODUCT|core.legacyResourceId",
    ]

    for _, r in df_variants.iterrows():
        vg = _extract_gid_like(r.get(variant_gid_col, ""))
        if not vg:
            continue

        pg = ""
        for c in candidate_product_gid_cols:
            if c in df_variants.columns:
                pg = _extract_gid_like(r.get(c, ""))
                if pg:
                    break

        if not pg:
            product_legacy_id = ""
            for c in candidate_product_legacy_cols:
                if c in df_variants.columns:
                    product_legacy_id = _as_scalar(r.get(c, ""))
                    if product_legacy_id:
                        break

            if product_legacy_id:
                product_row = row_maps.get("product_by_legacy_id", {}).get(product_legacy_id)
                if product_row is not None:
                    pg = _extract_gid_like(product_row.get("PRODUCT|core.gid", ""))

        if pg:
            bridge[vg] = pg

    return bridge

    variant_gid_col = "VARIANT|core.gid" if "VARIANT|core.gid" in df_variants.columns else None
    if not variant_gid_col:
        return bridge

    candidate_product_gid_cols = [
        "VARIANT|core.product_gid",
        "VARIANT|core.product.gid",
        "VARIANT|core.parent.gid",
        "PRODUCT|core.gid",
    ]

    candidate_product_legacy_cols = [
        "VARIANT|core.product.legacy_id",
        "VARIANT|core.product.legacyResourceId",
        "VARIANT|product.legacyResourceId",
        "PRODUCT|core.legacy_id",
        "PRODUCT|core.legacyResourceId",
    ]

    for _, r in df_variants.iterrows():
        vg = _as_scalar(r.get(variant_gid_col, ""))
        if not vg:
            continue

        # 1) 先直接找 product gid
        pg = ""
        for c in candidate_product_gid_cols:
            if c in df_variants.columns:
                pg = _as_scalar(r.get(c, ""))
                if pg:
                    break

        # 2) gid 没找到，再用 legacy_id 反查
        if not pg:
            product_legacy_id = ""
            for c in candidate_product_legacy_cols:
                if c in df_variants.columns:
                    product_legacy_id = _as_scalar(r.get(c, ""))
                    if product_legacy_id:
                        break

            if product_legacy_id:
                product_row = row_maps.get("product_by_legacy_id", {}).get(product_legacy_id)
                if product_row is not None:
                    pg = _as_scalar(product_row.get("PRODUCT|core.gid", ""))

        if pg:
            bridge[vg] = pg

    return bridge


def _normalize_long_field_keys(owner_type: str, fk: str) -> List[str]:
    owner_type = _safe_str(owner_type).upper()
    fk0 = _safe_str(fk)
    if not fk0:
        return []

    keys = [fk0]
    if fk0.startswith("custom."):
        if owner_type == "PRODUCT":
            keys.append("mf." + fk0)
        elif owner_type == "VARIANT":
            keys.append("v_mf." + fk0)
    if fk0.startswith("mf.custom."):
        keys.append(fk0.replace("mf.", "", 1))
    if fk0.startswith("v_mf.custom."):
        keys.append(fk0.replace("v_mf.", "", 1))

    out = []
    seen = set()
    for k in keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _build_long_value_map(df_dl_values_long: pd.DataFrame) -> Dict[Tuple[str, str, str], str]:
    """
    兼容两种 DL 结构：
    A) owner_entity_type / owner_gid / field_key / value
    B) entity_type / gid_or_handle / field_key / desired_value
    """
    dl = df_dl_values_long.copy()
    if dl is None or dl.empty:
        return {}

    dl.columns = [_safe_str(c) for c in dl.columns]

    if {"owner_entity_type", "owner_gid", "field_key", "value"}.issubset(set(dl.columns)):
        et_col = "owner_entity_type"
        gid_col = "owner_gid"
        val_col = "value"
    elif {"owner_type", "owner_gid", "field_key", "value"}.issubset(set(dl.columns)):
        et_col = "owner_type"
        gid_col = "owner_gid"
        val_col = "value"
    elif {"entity_type", "gid_or_handle", "field_key", "desired_value"}.issubset(set(dl.columns)):
        et_col = "entity_type"
        gid_col = "gid_or_handle"
        val_col = "desired_value"
    else:
        return {}

    mp = {}
    for _, r in dl.iterrows():
        et = _safe_str(r.get(et_col)).upper()
        gid = _extract_gid_like(r.get(gid_col))
        fk = _safe_str(r.get("field_key"))
        val = _safe_str(r.get(val_col))
        if not (et and gid and fk):
            continue
        for fk_norm in _normalize_long_field_keys(et, fk):
            mp[(et, gid, fk_norm)] = val
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

        # 先尝试直接从当前 variant 行里拿 product gid
        direct_product_gid_candidates = [
            "VARIANT|core.product_gid",
            "VARIANT|core.product.gid",
            "VARIANT|core.parent.gid",
            "PRODUCT|core.gid",
        ]
        direct_product_gid = _get_from_row_by_candidates(base_row, direct_product_gid_candidates)

        if direct_product_gid:
            product_row = row_maps["product_by_gid"].get(direct_product_gid)

        # 再用 bridge
        if product_row is None:
            variant_gid = _as_scalar(base_row.get("VARIANT|core.gid", ""))
            product_gid = variant_product_bridge.get(variant_gid, "")
            if product_gid:
                product_row = row_maps["product_by_gid"].get(product_gid)

        # 再 fallback：直接用 product legacy id 反查
        if product_row is None:
            direct_product_legacy_candidates = [
                "VARIANT|core.product.legacy_id",
                "VARIANT|core.product.legacyResourceId",
                "VARIANT|product.legacyResourceId",
                "PRODUCT|core.legacy_id",
                "PRODUCT|core.legacyResourceId",
            ]
            product_legacy_id = _get_from_row_by_candidates(base_row, direct_product_legacy_candidates)
            if product_legacy_id:
                product_row = row_maps.get("product_by_legacy_id", {}).get(product_legacy_id)

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
    base_row: Optional[pd.Series] = None,
) -> str:
    """
    RAW 取值规则：
    1. 先按 field_id / field_key 到对应实体 row 找
    2. 若当前是 VARIANT base，且要取 PRODUCT 字段，允许直接从 base_row 上取 PRODUCT|... 列
    3. 找不到再按 expr（非公式）指向的列名找
    4. 还找不到再去 DL__ValuesLong 按 (entity_type, gid, field_key) 找
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

    # 2) fallback：当前 base_row 里如果已经带了 PRODUCT|... / VARIANT|... 列，也直接拿
    if base_row is not None:
        v = _get_from_row_by_candidates(base_row, candidates)
        if v != "":
            return v

    # 3) expr 非公式时，允许 expr 作为“源列名”
    if expr and not expr.startswith("="):
        expr_candidates = [expr]
        if "|" not in expr and entity_type:
            expr_candidates.append(f"{entity_type}|{expr}")

        v = _get_from_row_by_candidates(row, expr_candidates)
        if v != "":
            return v

        if base_row is not None:
            v = _get_from_row_by_candidates(base_row, expr_candidates)
            if v != "":
                return v

    # 4) 去 DL__ValuesLong
    if row is not None and field_key:
        gid = _get_from_row_by_candidates(
            row,
            [f"{entity_type}|core.gid", f"{entity_type}|gid", "core.gid", "gid"]
        )
        if gid:
            v = long_value_map.get((entity_type, gid, field_key), "")
            if v != "":
                return v

    return ""



def _field_id_to_long_field_key(field_id: str) -> str:
    if "|" not in field_id:
        return _safe_str(field_id)
    return _safe_str(field_id.split("|", 1)[1])


def _agg_series_values(s: pd.Series, agg_name: str):
    agg_name = _safe_str(agg_name).upper()
    ss = s.fillna("").astype(str)
    ss = ss[ss.str.strip() != ""]
    if len(ss) == 0:
        return ""
    if agg_name in ("", "FIRST"):
        return ss.iloc[0]
    if agg_name == "FIRST_SORTED":
        return ss.sort_values().iloc[0]
    if agg_name == "LIST":
        return ", ".join(ss.tolist())
    if agg_name == "LIST_DISTINCT":
        seen = []
        for x in ss.tolist():
            if x not in seen:
                seen.append(x)
        return ", ".join(seen)
    return ss.iloc[0]


def _aggregate_variant_fields_to_product(df_variants: pd.DataFrame, variant_rows: pd.DataFrame) -> pd.DataFrame:
    if df_variants is None or df_variants.empty or variant_rows is None or variant_rows.empty:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    if "VARIANT|core.product_gid" not in df_variants.columns:
        raise ValueError("IDX__Variants 缺 VARIANT|core.product_gid，无法把 VARIANT 聚合到 PRODUCT")

    agg_groups: Dict[str, List[str]] = defaultdict(list)
    seen_fids = set()
    for _, r in variant_rows.iterrows():
        fid = _safe_str(r.get("field_id"))
        agg_name = _safe_str(r.get("agg")).upper() or "FIRST"
        if not fid or fid not in df_variants.columns or fid in seen_fids:
            continue
        agg_groups[agg_name].append(fid)
        seen_fids.add(fid)

    if not agg_groups:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    needed_cols = sorted(seen_fids)
    variant_work = df_variants[["VARIANT|core.product_gid"] + needed_cols].copy()
    variant_work["__product_gid_norm"] = variant_work["VARIANT|core.product_gid"].map(_extract_gid_like)
    variant_work = variant_work[variant_work["__product_gid_norm"].astype(str).str.strip() != ""].copy()
    if variant_work.empty:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    pieces = []
    for agg_name, fids in agg_groups.items():
        sub = variant_work[["__product_gid_norm"] + fids].copy()
        grouped = sub.groupby("__product_gid_norm", sort=False, dropna=False)
        agg_df = grouped[fids].agg(lambda s, agg_name=agg_name: _agg_series_values(s, agg_name))
        pieces.append(agg_df)

    if not pieces:
        return pd.DataFrame(columns=["PRODUCT|core.gid"])

    variant_agg_df = pd.concat(pieces, axis=1)
    variant_agg_df = variant_agg_df.loc[:, ~variant_agg_df.columns.duplicated()].reset_index()
    variant_agg_df = variant_agg_df.rename(columns={"__product_gid_norm": "PRODUCT|core.gid"})
    return variant_agg_df


def ensure_columns_from_long(
    df: pd.DataFrame,
    needed_cols: List[str],
    long_value_map: Dict[Tuple[str, str, str], str],
) -> pd.DataFrame:
    if df is None or df.empty or not needed_cols:
        return df

    cols_missing = [c for c in needed_cols if c not in df.columns]
    if not cols_missing:
        return df

    var_gid_col = "VARIANT|core.gid"
    prd_gid_col = "PRODUCT|core.gid"
    var_prd_gid = "VARIANT|core.product_gid"

    has_var_gid = var_gid_col in df.columns
    has_prd_gid = prd_gid_col in df.columns
    has_var_prd = var_prd_gid in df.columns

    var_gids = [_extract_gid_like(x) for x in df[var_gid_col].tolist()] if has_var_gid else None
    prd_gids = (
        [_extract_gid_like(x) for x in df[prd_gid_col].tolist()] if has_prd_gid
        else ([_extract_gid_like(x) for x in df[var_prd_gid].tolist()] if has_var_prd else None)
    )

    add_cols = {}
    for col in cols_missing:
        if not isinstance(col, str) or "|" not in col:
            continue

        ent = _safe_str(col.split("|", 1)[0]).upper()
        fk = _field_id_to_long_field_key(col)
        if not (fk.startswith("mf.") or fk.startswith("v_mf.") or fk.startswith("custom.")):
            continue

        if ent == "VARIANT":
            if not var_gids:
                continue
            add_cols[col] = [long_value_map.get(("VARIANT", gid, fk), "") for gid in var_gids]
        elif ent == "PRODUCT":
            if not prd_gids:
                continue
            add_cols[col] = [long_value_map.get(("PRODUCT", gid, fk), "") for gid in prd_gids]

    if not add_cols:
        return df

    add_df = pd.DataFrame(add_cols, index=df.index)
    return pd.concat([df, add_df], axis=1).copy()


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
    view_id: str,
    long_value_map: Optional[Dict[Tuple[str, str, str], str]] = None,
    df_dl_values_long: Optional[pd.DataFrame] = None,
    global_filters: Optional[Dict[str, Any]] = None,
    filter_mode: str = "AND",
    view_filter_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    verbose: bool = True,
) -> Dict[str, Any]:

    view_id = _safe_str(view_id)
    global_filters = global_filters or {}
    view_filter_overrides = view_filter_overrides or {}
    if long_value_map is None:
        long_value_map = _build_long_value_map(df_dl_values_long if df_dl_values_long is not None else pd.DataFrame())

    tab_row = cfg_tabs_df[cfg_tabs_df["view_id"].astype(str).str.strip() == view_id]
    if tab_row.empty:
        raise ValueError(f"Cfg__ExportTabs 找不到 view_id={view_id}")

    tab = tab_row.iloc[0].to_dict()

    target_sheet = _safe_str(tab.get("target_sheet")) or view_id
    layout = _safe_str(tab.get("layout")) or "WIDE"
    base_entity_type = _safe_str(tab.get("base_entity_type")).upper()
    base_sheet = _safe_str(tab.get("base_sheet")).upper()
    base_key_field_id = _safe_str(tab.get("base_key_field_id"))
    fixed_filter_mode = _safe_str(tab.get("fixed_filter_mode")) or filter_mode or "AND"
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

    if "agg" not in vf.columns:
        vf["agg"] = ""

    df_products = _dedupe_columns_keep_first(df_idx_products.copy())
    df_variants = _dedupe_columns_keep_first(df_idx_variants.copy())

    global_pf, global_vf = split_filters_by_entity(global_filters)
    fixed_pf, fixed_vf = split_filters_by_entity(fixed_filters_json)
    override_pf, override_vf = split_filters_by_entity(view_filter_overrides.get(view_id, {}))

    merged_pf = {}
    merged_pf.update(global_pf)
    merged_pf.update(fixed_pf)
    merged_pf.update(override_pf)

    merged_vf = {}
    merged_vf.update(global_vf)
    merged_vf.update(fixed_vf)
    merged_vf.update(override_vf)

    needed_product_cols = set(vf[vf["entity_type"].astype(str).str.upper().eq("PRODUCT")]["field_id"].astype(str).tolist())
    needed_variant_cols = set(vf[vf["entity_type"].astype(str).str.upper().eq("VARIANT")]["field_id"].astype(str).tolist())
    needed_product_cols |= set(merged_pf.keys())
    needed_variant_cols |= set(merged_vf.keys())

    df_products = ensure_columns_from_long(df_products, list(needed_product_cols), long_value_map)
    df_variants = ensure_columns_from_long(df_variants, list(needed_variant_cols), long_value_map)

    product_filters_exist = len(merged_pf) > 0
    if base_entity_type == "VARIANT" and product_filters_exist:
        df_products_for_filter = apply_entity_filters(df_products.copy(), merged_pf, fixed_filter_mode)

        if "PRODUCT|core.gid" not in df_products_for_filter.columns:
            raise ValueError("IDX__Products 缺 PRODUCT|core.gid，无法按 PRODUCT filters 预筛 VARIANT")

        allowed_product_gids = set(_extract_gid_like(x) for x in df_products_for_filter["PRODUCT|core.gid"].tolist())

        if "VARIANT|core.product_gid" not in df_variants.columns:
            raise ValueError("IDX__Variants 缺 VARIANT|core.product_gid，无法按 PRODUCT filters 预筛 VARIANT")

        before_n = len(df_variants)
        df_variants = df_variants[
            df_variants["VARIANT|core.product_gid"].map(_extract_gid_like).isin(allowed_product_gids)
        ].copy()
        if verbose:
            print(f"prefilter variants by product filters: {before_n} -> {len(df_variants)}")

    if base_entity_type == "PRODUCT":
        base_df = apply_entity_filters(df_products.copy(), merged_pf, fixed_filter_mode)
    else:
        base_df = apply_entity_filters(df_variants.copy(), merged_vf, fixed_filter_mode)

    if base_entity_type == "PRODUCT":
        variant_rows = vf[vf["entity_type"].astype(str).str.upper().eq("VARIANT")].copy()
        if not variant_rows.empty:
            if "PRODUCT|core.gid" not in base_df.columns:
                raise ValueError("IDX__Products 缺 PRODUCT|core.gid，无法 merge 聚合后的 VARIANT 字段")

            variant_agg_df = _aggregate_variant_fields_to_product(df_variants, variant_rows)
            if not variant_agg_df.empty:
                base_df = base_df.merge(variant_agg_df, how="left", on="PRODUCT|core.gid")

    if base_entity_type == "VARIANT":
        if "VARIANT|core.product_gid" not in base_df.columns:
            raise ValueError("IDX__Variants 缺 VARIANT|core.product_gid，无法 join PRODUCT 字段")
        if "PRODUCT|core.gid" not in df_products.columns:
            raise ValueError("IDX__Products 缺 PRODUCT|core.gid，无法 join 到 VARIANT")

        needed_product_cols2 = set(vf[vf["entity_type"].astype(str).str.upper().eq("PRODUCT")]["field_id"].astype(str).tolist())
        needed_product_cols2 |= set(merged_pf.keys())

        take_cols = ["PRODUCT|core.gid"] + [c for c in needed_product_cols2 if c in df_products.columns]
        prod_take = df_products[take_cols].drop_duplicates(subset=["PRODUCT|core.gid"]).copy()
        prod_take["__product_gid_norm"] = prod_take["PRODUCT|core.gid"].map(_extract_gid_like)

        base_df = base_df.copy()
        base_df["__variant_product_gid_norm"] = base_df["VARIANT|core.product_gid"].map(_extract_gid_like)
        base_df = base_df.merge(
            prod_take.drop(columns=["PRODUCT|core.gid"]).rename(columns={"__product_gid_norm": "__variant_product_gid_norm"}),
            how="left",
            on="__variant_product_gid_norm",
        )

        if product_filters_exist:
            base_df = apply_entity_filters(base_df, merged_pf, fixed_filter_mode)

    if base_key_field_id and base_key_field_id not in base_df.columns:
        short_key = base_key_field_id.split("|", 1)[-1] if "|" in base_key_field_id else base_key_field_id
        if short_key in base_df.columns:
            base_key_field_id = short_key

    if base_key_field_id and base_key_field_id not in base_df.columns:
        raise ValueError(f"view_id={view_id} 的 base_key_field_id 在 base_df 中不存在：{base_key_field_id}")

    field_rows_raw = _prepare_field_rows(vf)

    # Keep IDX as raw JSON/list columns. Expand image lists only in this final view builder.
    field_rows = _expand_image_field_rows_for_output(
        field_rows_raw,
        base_df,
        max_images=15,
    )

    output_keys = [fr["output_key"] for fr in field_rows]
    display_headers_raw = [fr["alias"] or fr["field_id"] or fr["output_key"] for fr in field_rows]
    display_headers = _make_display_headers(display_headers_raw)

    out_rows = []
    for _, base_row in base_df.iterrows():
        base_row = base_row.copy()
        one = {}

        for fr in field_rows:
            field_type = _safe_str(fr["field_type"]).upper()
            output_key = fr["output_key"]
            fid = _safe_str(fr.get("field_id"))

            if "__image_source_field_id" in fr:
                src_fid = _safe_str(fr.get("__image_source_field_id"))
                img_idx = int(fr.get("__image_index") or 0)
                arr = _safe_json_list_loads(base_row.get(src_fid, ""))
                one[output_key] = arr[img_idx] if img_idx < len(arr) else ""
            elif field_type == "CALC" and _safe_str(fr["expr"]).startswith("="):
                one[output_key] = ""
            else:
                one[output_key] = _as_scalar(base_row.get(fid, "")) if fid else ""

        out_rows.append(one)

    out_df = pd.DataFrame(out_rows, columns=output_keys).fillna("")

    target_rows = max(len(out_df) + 20, 1000)
    target_cols = max(len(display_headers) + 10, 50)
    ws = ensure_worksheet(sh_data, target_sheet, rows=target_rows, cols=target_cols)
    resize_worksheet(ws, target_rows, target_cols)
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

    formula_cols = []
    for i, fr in enumerate(field_rows, start=1):
        if "__image_source_field_id" in fr:
            continue
        expr = _safe_str(fr["expr"])
        field_type = _safe_str(fr["field_type"]).upper()
        if field_type == "CALC" and expr.startswith("="):
            formula_cols.append((i, expr, fr["output_key"]))

    if formula_cols and len(out_df) > 0:
        write_formula_columns_filldown(
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
        print(f"base_sheet={base_sheet}")
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
    global_filters: Optional[Dict[str, Any]] = None,
    filter_mode: str = "AND",
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
    long_value_map = _build_long_value_map(df_dl_values_long)

    if verbose:
        print("loaded:")
        print("  cfg tabs rows     :", len(cfg_tabs_df))
        print("  cfg fields rows   :", len(cfg_fields_df))
        print("  idx products rows :", len(df_idx_products))
        print("  idx variants rows :", len(df_idx_variants))
        print("  dl rows           :", len(df_dl_values_long))
        print("  dl long keys      :", len(long_value_map))
        print("  products dup cols :", df_idx_products.columns.duplicated().any())
        print("  variants dup cols :", df_idx_variants.columns.duplicated().any())
        print("  has product_gid   :", "VARIANT|core.product_gid" in df_idx_variants.columns)
        print("  has product.gid   :", "VARIANT|core.product.gid" in df_idx_variants.columns)

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
            view_id=vid,
            long_value_map=long_value_map,
            global_filters=global_filters or {},
            filter_mode=filter_mode,
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
