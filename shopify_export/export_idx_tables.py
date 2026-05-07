# -*- coding: utf-8 -*-
"""
shopify_export/export_idx_tables.py

用途：
- 从 Shopify 导出两张 IDX 薄索引表：
  - IDX__Products
  - IDX__Variants
- 配置来源：
  - console_core_url -> Cfg__Sites
  - label=config      -> Cfg__ExportTabFields
  - label=export_product -> 输出表
- 支持：
  - CALC 依赖展开
  - MF_VALUE("ns","key")
  - COALESCE(...)
  - JSON(...)
  - GET({fid}, n).name / .value
  - xxx[0] / nodes[0] 路径
  - core.tags 人类可读输出
  - 旧版 variant.weight / variant.weightUnit 自动 remap
  - 过滤 ARCHIVED
"""

from __future__ import annotations

import base64
import html
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


# =========================================================
# 基础
# =========================================================

TailStep = Union[str, int]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FIELD_DEF: Dict[str, Dict[str, Any]] = {}


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_int(x: Any, default: int = 999999) -> int:
    try:
        return int(str(x).strip())
    except Exception:
        return default


def _clean_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _is_blank(x: Any) -> bool:
    return _clean_str(x) == ""


def _gql_safe_alias(s: str) -> str:
    a = re.sub(r"[^0-9A-Za-z_]", "_", str(s or ""))
    if re.match(r"^\d", a):
        a = "f_" + a
    return a or "f_blank"


def _listify_records(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# =========================================================
# Google Sheets
# =========================================================

def build_gspread_client_from_b64(sa_b64: str) -> gspread.Client:
    info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def open_ws(gc: gspread.Client, sheet_url: str, worksheet_title: str):
    sh = gc.open_by_url(sheet_url)
    return sh.worksheet(worksheet_title)


def ws_to_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    body = values[1:] if len(values) > 1 else []
    return pd.DataFrame(body, columns=header)


def ensure_ws(gc: gspread.Client, sheet_url: str, worksheet_title: str, rows: int = 2000, cols: int = 60):
    sh = gc.open_by_url(sheet_url)
    try:
        return sh.worksheet(worksheet_title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=worksheet_title, rows=rows, cols=cols)


def write_df(ws, df: pd.DataFrame, mode: str = "REPLACE"):
    mode = _clean_str(mode).upper() or "REPLACE"
    if mode == "REPLACE":
        ws.clear()
        if df is None or df.empty:
            ws.update("A1", [[""]])
            return
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        return

    existing = ws.get_all_values()
    if not existing:
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        return

    start_row = len(existing) + 1
    set_with_dataframe(
        ws,
        df,
        row=start_row,
        col=1,
        include_index=False,
        include_column_header=False,
        resize=False,
    )


# =========================================================
# Shopify GraphQL
# =========================================================

@dataclass
class ShopifyClient:
    shop_domain: str
    api_version: str
    access_token: str
    timeout: int = 60
    min_sleep: float = 0.0

    def __post_init__(self):
        self.url = f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"
        self.sess = requests.Session()
        self.sess.headers.update({
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        })

    def gql(self, query: str, variables: Optional[dict] = None, max_retry: int = 5) -> dict:
        payload = {"query": query, "variables": variables or {}}

        for i in range(max_retry):
            resp = self.sess.post(self.url, json=payload, timeout=self.timeout)
            txt = resp.text

            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(2 ** i, 10)
                time.sleep(sleep_s)
                continue

            resp.raise_for_status()
            data = resp.json()

            if data.get("errors"):
                raise RuntimeError(f"Shopify GraphQL errors: {json.dumps(data['errors'], ensure_ascii=False)}")

            user_errors = []
            d = data.get("data") or {}
            if isinstance(d, dict):
                for v in d.values():
                    if isinstance(v, dict) and v.get("userErrors"):
                        user_errors.extend(v["userErrors"])
            if user_errors:
                raise RuntimeError(f"Shopify userErrors: {json.dumps(user_errors, ensure_ascii=False)}")

            ext = data.get("extensions", {}) or {}
            ts = (ext.get("cost", {}) or {}).get("throttleStatus", {}) or {}
            currently = ts.get("currentlyAvailable")
            restore = ts.get("restoreRate")
            if currently is not None and restore:
                if currently < 50:
                    time.sleep(max(0.2, (100 - currently) / max(restore, 1)))

            if self.min_sleep > 0:
                time.sleep(self.min_sleep)

            return d

        raise RuntimeError("GraphQL request failed after retries")


# =========================================================
# 配置读取
# =========================================================

def get_label_sheet_url(gc: gspread.Client, console_core_url: str, site_code: str, label: str) -> str:
    ws = open_ws(gc, console_core_url, "Cfg__Sites")
    df = ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__Sites is empty")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()

    hit = df[
        (df["site_code"] == str(site_code).strip().upper()) &
        (df["label"] == str(label).strip())
    ]

    if hit.empty:
        raise ValueError(f"Cfg__Sites 中未找到 site_code={site_code}, label={label}")

    sheet_url = _clean_str(hit.iloc[0].get("sheet_url"))
    if not sheet_url:
        raise ValueError(f"Cfg__Sites 中 sheet_url 为空: site_code={site_code}, label={label}")

    return sheet_url


def load_cfg_fields(gc: gspread.Client, config_sheet_url: str, worksheet_title: str = "Cfg__Fields") -> pd.DataFrame:
    ws = open_ws(gc, config_sheet_url, worksheet_title)
    df = ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__Fields is empty")

    need_cols = ["field_id", "entity_type", "field_key", "expr", "field_type", "data_type"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = ""

    return df.fillna("")


def load_export_tab_fields(gc: gspread.Client, config_sheet_url: str, worksheet_title: str = "Cfg__ExportTabFields") -> pd.DataFrame:
    ws = open_ws(gc, config_sheet_url, worksheet_title)
    df = ws_to_df(ws)
    if df.empty:
        raise ValueError("Cfg__ExportTabFields is empty")

    need_cols = ["view_id", "field_id", "seq", "field_type", "entity_type", "field_key", "expr"]
    for c in need_cols:
        if c not in df.columns:
            raise ValueError(f"Cfg__ExportTabFields 缺少字段: {c}")

    return df.fillna("")


def build_field_def_map(cfg_fields_df: pd.DataFrame, cfg_export_df: Optional[pd.DataFrame] = None) -> Dict[str, Dict[str, Any]]:
    """
    FIELD_DEF 必须优先来自 Cfg__Fields。
    这样当某个依赖 field_id 不在当前 view 中时，仍然能补回它的 expr / entity_type / data_type。
    若 export 里存在同 field_id 的补充信息，则只做兜底合并，不覆盖 Cfg__Fields 主定义。
    """
    out: Dict[str, Dict[str, Any]] = {}

    for _, r in cfg_fields_df.fillna("").iterrows():
        fid = _clean_str(r.get("field_id"))
        if fid:
            out[fid] = dict(r)

    if cfg_export_df is not None and not cfg_export_df.empty:
        for _, r in cfg_export_df.fillna("").iterrows():
            fid = _clean_str(r.get("field_id"))
            if not fid:
                continue
            if fid not in out:
                out[fid] = dict(r)
            else:
                for k, v in dict(r).items():
                    if _is_blank(out[fid].get(k)) and not _is_blank(v):
                        out[fid][k] = v

    return out


def get_view_cfg(cfg_df: pd.DataFrame, view_id: str) -> pd.DataFrame:
    out = cfg_df[cfg_df["view_id"].astype(str).str.strip() == view_id].copy()
    if out.empty:
        raise ValueError(f"Cfg__ExportTabFields 中未找到 view_id={view_id}")
    out["__seq"] = out["seq"].apply(_to_int)
    out = out.sort_values("__seq").drop(columns=["__seq"])
    return out


# =========================================================
# CALC 依赖 / fetch_df
# =========================================================

_placeholder_re = re.compile(r"\{([^}]+)\}")
_mf_re = re.compile(r'MF_VALUE\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)')


def parse_mf_value_expr(expr: Any) -> Optional[Tuple[str, str]]:
    if not isinstance(expr, str):
        return None
    m = _mf_re.search(expr.strip())
    if not m:
        return None
    return m.group(1), m.group(2)


def expand_calc_dependencies(view_df: pd.DataFrame) -> set[str]:
    """
    找出当前 view 中所有 CALC expr 依赖到的 field_id。
    关键修复：
    - 递归展开时，不能只看当前 view；
    - 若占位符 field_id 只存在于 Cfg__Fields，也必须继续向下追。
    """
    deps = set()
    changed = True

    view_rows = {_clean_str(r.get("field_id")): dict(r) for _, r in view_df.iterrows()}

    def get_row(fid: str) -> Dict[str, Any]:
        return view_rows.get(fid) or FIELD_DEF.get(fid) or {}

    while changed:
        changed = False
        scan_rows: List[Dict[str, Any]] = [dict(r) for _, r in view_df.iterrows()]

        for dep in list(deps):
            dep_row = get_row(dep)
            if dep_row:
                scan_rows.append(dep_row)

        for r in scan_rows:
            ft = _clean_str(r.get("field_type")).upper()
            ex = _clean_str(r.get("expr"))
            if ft != "CALC" or not ex:
                continue

            refs = set(_placeholder_re.findall(ex))
            for fid in refs:
                if fid not in deps:
                    deps.add(fid)
                    changed = True

    return deps


def make_fetch_df(view_df: pd.DataFrame, deps: set[str]) -> pd.DataFrame:
    """
    规则：
    - 当前 view 中非 CALC 行要取
    - CALC 自身不取，但其依赖 field_id 要补进 fetch
    - 依赖若不在当前 view，必须回到 FIELD_DEF（Cfg__Fields）取完整定义
    这就是本次修复点；否则像 core.product_gid 这类“被间接依赖但不在当前 view 的字段”会被漏抓。
    """
    rows: List[Dict[str, Any]] = []
    fid_map_view = {_clean_str(r.get("field_id")): dict(r) for _, r in view_df.iterrows()}
    seen = set()

    for _, r in view_df.iterrows():
        fid = _clean_str(r.get("field_id"))
        ft = _clean_str(r.get("field_type")).upper()
        if not fid:
            continue

        if ft != "CALC" and fid not in seen:
            rows.append(dict(r))
            seen.add(fid)

    for dep in deps:
        if dep in seen:
            continue

        dep_row = fid_map_view.get(dep)
        if dep_row is None:
            dep_row = dict(FIELD_DEF.get(dep) or {})

        if not dep_row:
            continue

        dep_row.setdefault("view_id", "__FETCH_DEPS__")
        dep_row.setdefault("join key", "")
        dep_row.setdefault("seq", "999999")
        dep_row.setdefault("alias", "")
        dep_row.setdefault("required", "")
        dep_row.setdefault("notes", "auto-added dep for CALC / join")
        dep_row["field_id"] = dep
        dep_row["field_type"] = _clean_str(dep_row.get("field_type")).upper() or "RAW"

        rows.append(dep_row)
        seen.add(dep)

    out = pd.DataFrame(rows).fillna("")
    if out.empty:
        raise ValueError("make_fetch_df 结果为空")
    out["__seq"] = out["seq"].apply(_to_int)
    return out.sort_values("__seq").drop(columns=["__seq"])


# =========================================================
# expr 路径处理
# =========================================================

def strip_entity_prefix(expr: str, entity_type: str) -> str:
    """
    统一把 expr 转成 GraphQL 节点下路径。

    特殊规则：
    - PRODUCT product.description 改拉 descriptionHtml。
      后续在 node_to_row 里转成人类可读 text。
      原因：Shopify product.description 会丢掉 HTML 结构，容易把段落粘在一起。
    - VARIANT weight / weightUnit 兼容旧版写法。
    """
    pref = {
        "PRODUCT": "product.",
        "VARIANT": "variant.",
    }.get(entity_type, "")

    s = _clean_str(expr)
    if pref and s.startswith(pref):
        s = s[len(pref):]

    if entity_type == "PRODUCT":
        k = s.strip()
        if k == "description":
            return "descriptionHtml"

    # ---- 兼容旧版 weight 写法（与原 ipynb 一致：先去前缀，再 remap）----
    if entity_type == "VARIANT":
        k = s.strip()
        if k == "weight":
            return "inventoryItem.measurement.weight.value"
        if k == "weightUnit":
            return "inventoryItem.measurement.weight.unit"

    return s

_index_part_re = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\[(\d+)\]$")


def build_nested_fields(parts: List[str]) -> str:
    if not parts:
        return "id"
    inner = parts[-1]
    for p in reversed(parts[:-1]):
        inner = f"{p} {{ {inner} }}"
    return inner


def build_selected_options_selection(alias: str) -> str:
    return f"""
{alias}: selectedOptions {{
  name
  value
}}
""".strip()


def extract_leaf(val: Any, tail: List[TailStep]) -> Any:
    cur = val
    for step in tail:
        if isinstance(step, int):
            if not isinstance(cur, list) or step < 0 or step >= len(cur):
                return ""
            cur = cur[step]
        else:
            if not isinstance(cur, dict):
                return ""
            cur = cur.get(step, "")
    return "" if cur is None else cur


# =========================================================
# CALC evaluator
# =========================================================

_coalesce_re = re.compile(r"^COALESCE\((.*)\)\s*$", re.IGNORECASE)
_json_re = re.compile(r"^JSON\(\s*(\{[^}]+\})\s*\)\s*$", re.IGNORECASE)
_get_re = re.compile(r"^GET\(\s*(\{[^}]+\})\s*,\s*(\d+)\s*\)\.(name|value)\s*$", re.IGNORECASE)


def eval_calc(expr: str, row: Dict[str, Any]) -> Any:
    s = _clean_str(expr)
    if not s:
        return ""

    m = _json_re.match(s)
    if m:
        fid = m.group(1)[1:-1].strip()
        val = row.get(fid, "")
        try:
            return json.dumps(val, ensure_ascii=False)
        except Exception:
            return json.dumps(str(val), ensure_ascii=False)

    m = _get_re.match(s)
    if m:
        fid = m.group(1)[1:-1].strip()
        idx = int(m.group(2))
        attr = m.group(3).lower()
        arr = row.get(fid, [])
        if not isinstance(arr, list) or idx <= 0 or idx > len(arr):
            return ""
        item = arr[idx - 1] or {}
        return item.get(attr, "")

    m = _coalesce_re.match(s)
    if m:
        inside = m.group(1)
        parts = [p.strip() for p in inside.split(",") if p.strip()]
        for p in parts:
            if p.startswith("{") and p.endswith("}"):
                fid = p[1:-1].strip()
                v = row.get(fid, "")
                if v not in ("", None):
                    return v
            else:
                if p not in ("", None):
                    return p
        return ""

    return ""


def _try_parse_json_list(s: str) -> Optional[list]:
    ss = _clean_str(s)
    if not (ss.startswith("[") and ss.endswith("]")):
        return None
    try:
        v = json.loads(ss)
        return v if isinstance(v, list) else None
    except Exception:
        return None


def tags_to_human(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        parts = [str(x).strip() for x in v if str(x).strip() not in ("", "None", "nan")]
        return ", ".join(parts)
    if isinstance(v, str):
        parsed = _try_parse_json_list(v)
        if parsed is not None:
            parts = [str(x).strip() for x in parsed if str(x).strip() not in ("", "None", "nan")]
            return ", ".join(parts)
        return v.strip()
    return str(v).strip()



def html_to_readable_text(html_text: Any) -> str:
    """
    把 Shopify descriptionHtml 转成人类可读文本：
    - <br>, </p>, </div>, </li>, </tr>, 标题标签 -> 换行
    - 去掉剩余 HTML 标签
    - 反解码 &amp; / &quot; / &#39; 等实体
    - 压缩多余空格，但保留段落换行
    """
    s = "" if html_text is None else str(html_text)
    if not s.strip():
        return ""

    # 去掉 script/style
    s = re.sub(r"(?is)<\s*(script|style)[^>]*>.*?</\s*\1\s*>", "", s)

    # 先把明显的结构标签转成换行
    s = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", s)
    s = re.sub(r"(?i)</\s*(p|div|section|article|li|tr|h1|h2|h3|h4|h5|h6)\s*>", "\n", s)
    s = re.sub(r"(?i)<\s*li[^>]*>", "• ", s)

    # 表格单元格之间给一点间隔
    s = re.sub(r"(?i)</\s*(td|th)\s*>", " ", s)

    # 块级起始标签也给轻微断开，避免前文直接粘到新块
    s = re.sub(r"(?i)<\s*(p|div|section|article|tr|h1|h2|h3|h4|h5|h6)[^>]*>", "\n", s)

    # 去掉剩余标签
    s = re.sub(r"(?s)<[^>]+>", "", s)

    # HTML entity 还原
    s = html.unescape(s)

    # 统一换行
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # 每一行内部压缩空格
    lines = []
    for line in s.split("\n"):
        line = re.sub(r"[ \t\u00a0]+", " ", line).strip()
        if line:
            lines.append(line)

    return "\n".join(lines).strip()


def normalize_sheet_cell(v: Any) -> Any:
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return v


# =========================================================
# build_plan
# =========================================================

def build_plan(fetch_df: pd.DataFrame, entity_type: str) -> Dict[str, Any]:
    gql_lines: List[str] = []
    alias_map: Dict[str, str] = {}
    leaf_tail: Dict[str, List[TailStep]] = {}
    raw_rows: List[Tuple[str, str]] = []
    calc_rows: List[Tuple[str, str]] = []
    join_fids = set()

    def find_index_seg(parts: List[str]):
        for i, seg in enumerate(parts):
            m = _index_part_re.match(seg)
            if m:
                return i, m.group(1), int(m.group(2))
        return None

    for _, r in fetch_df.iterrows():
        ft = _clean_str(r.get("field_type")).upper()
        fid = _clean_str(r.get("field_id"))
        ex = _clean_str(r.get("expr"))
        jk = _clean_str(r.get("join key"))

        if not fid:
            continue

        if jk:
            join_fids.add(fid)

        a = _gql_safe_alias(fid)
        alias_map[fid] = a

        if ft == "CALC":
            calc_rows.append((fid, ex))
            continue

        mf = parse_mf_value_expr(ex)
        if mf:
            ns, key = mf
            gql_lines.append(f'{a}: metafield(namespace: "{ns}", key: "{key}") {{ value }}')
            raw_rows.append((fid, ex))
            continue

        if entity_type == "VARIANT" and ex.endswith("selectedOptions"):
            gql_lines.append(build_selected_options_selection(a))
            raw_rows.append((fid, ex))
            continue

        path = strip_entity_prefix(ex, entity_type)
        parts = [p for p in path.split(".") if p]
        if not parts:
            continue

        idx_hit = find_index_seg(parts)
        if idx_hit:
            k, name, idx = idx_hit
            after = parts[k + 1:]
            node_fields = build_nested_fields(after)
            first_n = idx + 1

            # case A: nodes[0]
            if name.lower() == "nodes" and k > 0:
                conn = parts[k - 1]
                head = parts[0]
                mid = parts[1:k - 1]
                conn_sel = f"{conn}(first:{first_n}) {{ nodes {{ {node_fields} }} }}"

                if k - 1 == 0:
                    gql_lines.append(f"{a}: {conn}(first:{first_n}) {{ nodes {{ {node_fields} }} }}")
                    leaf_tail[fid] = ["nodes", idx] + after
                else:
                    inner = conn_sel
                    for p in reversed(mid):
                        inner = f"{p} {{ {inner} }}"
                    gql_lines.append(f"{a}: {head} {{ {inner} }}")
                    leaf_tail[fid] = mid + [conn, "nodes", idx] + after

                raw_rows.append((fid, ex))
                continue

            # case B: xxx[0]
            before = parts[:k]
            if before and before[-1] == name:
                before = before[:-1]

            conn_sel = f"{name}(first:{first_n}) {{ nodes {{ {node_fields} }} }}"

            if not before:
                gql_lines.append(f"{a}: {conn_sel}")
                leaf_tail[fid] = ["nodes", idx] + after
            else:
                head = before[0]
                mid = before[1:]
                inner = conn_sel
                for p in reversed(mid):
                    inner = f"{p} {{ {inner} }}"
                gql_lines.append(f"{a}: {head} {{ {inner} }}")
                leaf_tail[fid] = mid + [name, "nodes", idx] + after

            raw_rows.append((fid, ex))
            continue

        if len(parts) == 1:
            gql_lines.append(f"{a}: {parts[0]}")
        else:
            head = parts[0]
            tail = build_nested_fields(parts[1:])
            gql_lines.append(f"{a}: {head} {{ {tail} }}")
            # 与原 ipynb 一致：普通嵌套对象字段必须记录 leaf_tail，
            # 否则 node_to_row 会把整个对象直接 json 化写进表里。
            leaf_tail[fid] = parts[1:]

        raw_rows.append((fid, ex))

    return {
        "gql_selections": "\n".join(gql_lines),
        "alias_map": alias_map,
        "leaf_tail": leaf_tail,
        "raw_rows": raw_rows,
        "calc_rows": calc_rows,
        "join_fids": join_fids,
    }


# =========================================================
# fetchers
# =========================================================

def fetch_all_products(client: ShopifyClient, page_size: int, plan: Dict[str, Any], debug_every: int = 5) -> List[dict]:
    sel = plan["gql_selections"]
    q = f"""
query Products($first:Int!, $after:String) {{
  products(first:$first, after:$after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      {sel}
    }}
  }}
}}
""".strip()

    out: List[dict] = []
    after = None
    page = 0
    while True:
        page += 1
        data = client.gql(q, {"first": page_size, "after": after})
        box = data["products"]
        out.extend(box.get("nodes") or [])
        if page % max(1, debug_every) == 0:
            print(f"Products pages={page} rows={len(out)}")
        if not box["pageInfo"]["hasNextPage"]:
            break
        after = box["pageInfo"]["endCursor"]
    return out


def fetch_all_variants(client: ShopifyClient, page_size: int, plan: Dict[str, Any], debug_every: int = 5) -> List[dict]:
    sel = plan["gql_selections"]
    q = f"""
query Variants($first:Int!, $after:String) {{
  productVariants(first:$first, after:$after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      {sel}
    }}
  }}
}}
""".strip()

    out: List[dict] = []
    after = None
    page = 0
    while True:
        page += 1
        data = client.gql(q, {"first": page_size, "after": after})
        box = data["productVariants"]
        out.extend(box.get("nodes") or [])
        if page % max(1, debug_every) == 0:
            print(f"Variants pages={page} rows={len(out)}")
        if not box["pageInfo"]["hasNextPage"]:
            break
        after = box["pageInfo"]["endCursor"]
    return out


# =========================================================
# row / export df
# =========================================================

def node_to_row(node: dict, plan: Dict[str, Any]) -> Dict[str, Any]:
    alias_map = plan["alias_map"]
    leaf_tail = plan["leaf_tail"]
    join_fids = plan["join_fids"]

    row: Dict[str, Any] = {}

    for fid, _ex in plan["raw_rows"]:
        a = alias_map[fid]
        val = node.get(a, "")

        if isinstance(val, dict) and "value" in val:
            row[fid] = val.get("value") or ""
        elif fid in leaf_tail:
            row[fid] = extract_leaf(val, leaf_tail[fid])
        else:
            row[fid] = val

        if fid in join_fids and isinstance(row[fid], (dict, list)):
            row[fid] = ""

    for fid, ex in plan["calc_rows"]:
        row[fid] = eval_calc(ex, row)

    for fid in list(row.keys()):
        fk = (FIELD_DEF.get(str(fid), {}) or {}).get("field_key", "")
        fk = _clean_str(fk)

        if fk == "core.tags":
            row[fid] = tags_to_human(row.get(fid, ""))

        # Product Description (text)
        # 配置里仍然可以是 product.description / core.description，
        # 但 strip_entity_prefix 已经改成实际拉 descriptionHtml；
        # 这里把 HTML 转成带换行的可读文本。
        if fk == "core.description":
            row[fid] = html_to_readable_text(row.get(fid, ""))

    for k in list(row.keys()):
        row[k] = normalize_sheet_cell(row[k])

    return row

def find_synced_at_fids(view_df: pd.DataFrame) -> List[str]:
    out = []
    for _, r in view_df.iterrows():
        if _clean_str(r.get("field_key")) == "core.synced_at":
            fid = _clean_str(r.get("field_id"))
            if fid:
                out.append(fid)
    return out


def append_internal_status_fetch(fetch_df: pd.DataFrame, entity_type: str) -> Tuple[pd.DataFrame, str]:
    """
    确保 fetch_df 一定包含内部状态字段，用于过滤 archived
    """
    out = fetch_df.copy()

    if entity_type == "PRODUCT":
        internal_fid = "__FILTER_PRODUCT_STATUS__"
        internal_expr = "product.status"
    else:
        internal_fid = "__FILTER_PARENT_PRODUCT_STATUS__"
        internal_expr = "variant.product.status"

    for _, r in out.iterrows():
        fid = _clean_str(r.get("field_id"))
        ex = _clean_str(r.get("expr"))
        ft = _clean_str(r.get("field_type")).upper()
        if ft == "CALC":
            continue
        if ex == internal_expr:
            return out, fid

    add_row = pd.DataFrame([{
        "view_id": "__INTERNAL_FILTER__",
        "field_id": internal_fid,
        "join key": "",
        "seq": "999998",
        "field_type": "RAW",
        "entity_type": entity_type,
        "field_key": "__internal.status__",
        "expr": internal_expr,
        "alias": "",
        "data_type": "single_line_text_field",
        "required": "",
        "notes": "internal use: filter archived",
    }])

    out = pd.concat([out, add_row], ignore_index=True)
    out["__seq"] = out["seq"].apply(_to_int)
    out = out.sort_values("__seq").drop(columns=["__seq"])
    return out, internal_fid


def build_export_df_filtered(
    entity_type: str,
    view_df: pd.DataFrame,
    fetch_df: pd.DataFrame,
    nodes: List[dict],
    status_fid: str,
    exclude_archived: bool = True,
    only_first_row_synced_at: bool = True,
) -> Tuple[pd.DataFrame, int]:
    plan = build_plan(fetch_df, entity_type)
    rows = [node_to_row(n, plan) for n in nodes]
    df_all = pd.DataFrame(rows)

    removed_cnt = 0
    if exclude_archived:
        if status_fid in df_all.columns:
            status_ser = df_all[status_fid].astype(str).str.strip().str.upper()
            keep_mask = status_ser.ne("ARCHIVED")
            removed_cnt = int((~keep_mask).sum())
            df_all = df_all[keep_mask].copy()
        else:
            print(f"⚠️ status_fid 不在导出结果中，未执行 archived 过滤: {status_fid}")

    synced_fids = find_synced_at_fids(view_df)
    if synced_fids:
        ts = _now_iso_utc()
        for fid in synced_fids:
            if fid in df_all.columns:
                if only_first_row_synced_at:
                    df_all[fid] = ""
                    if len(df_all) > 0:
                        df_all.loc[df_all.index[0], fid] = ts
                else:
                    df_all[fid] = ts

    tmp = view_df.copy()
    tmp["__seq"] = tmp["seq"].apply(_to_int)
    tmp = tmp.sort_values("__seq").drop(columns=["__seq"])

    export_fids = [_clean_str(x) for x in tmp["field_id"].tolist() if _clean_str(x)]
    export_fids = [c for c in export_fids if c in df_all.columns]

    df_out = df_all[export_fids].copy() if export_fids else pd.DataFrame()
    if not df_out.empty:
        df_out.columns = export_fids

    return df_out, removed_cnt


# =========================================================
# 主入口
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    shop_domain: str,
    api_version: str,
    gsheet_sa_b64: str,
    shopify_access_token: str,
    cfg_export_tab_fields_ws: str = "Cfg__ExportTabFields",
    out_tab_idx_products: str = "IDX__Products",
    out_tab_idx_variants: str = "IDX__Variants",
    export_idx_products: bool = True,
    export_idx_variants: bool = True,
    write_mode: str = "REPLACE",
    write_mode_products: str = "",
    write_mode_variants: str = "",
    page_size: int = 100,
    debug_every: int = 5,
    exclude_archived: bool = True,
    only_first_row_synced_at: bool = True,
) -> Dict[str, Any]:
    """
    返回：
    {
      "site_code": ...,
      "config_sheet_url": ...,
      "out_sheet_url": ...,
      "products_rows": ...,
      "variants_rows": ...,
      "products_filtered_archived": ...,
      "variants_filtered_archived": ...,
    }
    """
    global FIELD_DEF

    site_code = _clean_str(site_code).upper()

    gc = build_gspread_client_from_b64(gsheet_sa_b64)
    client = ShopifyClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=shopify_access_token,
        timeout=60,
        min_sleep=0.0,
    )

    config_sheet_url = get_label_sheet_url(gc, console_core_url, site_code, "config")
    out_sheet_url = get_label_sheet_url(gc, console_core_url, site_code, "export_product")

    cfg_fields_df = load_cfg_fields(gc, config_sheet_url, worksheet_title="Cfg__Fields")
    cfg_df = load_export_tab_fields(gc, config_sheet_url, worksheet_title=cfg_export_tab_fields_ws)
    FIELD_DEF = build_field_def_map(cfg_fields_df, cfg_df)

    def get_effective_mode(default_mode: str, override: str) -> str:
        m = _clean_str(override).upper()
        if m in ("REPLACE", "APPEND"):
            return m
        return _clean_str(default_mode).upper() or "REPLACE"

    result = {
        "site_code": site_code,
        "config_sheet_url": config_sheet_url,
        "out_sheet_url": out_sheet_url,
        "products_rows": 0,
        "variants_rows": 0,
        "products_filtered_archived": 0,
        "variants_filtered_archived": 0,
    }

    # ---- Products ----
    if export_idx_products:
        view_p = get_view_cfg(cfg_df, "IDX__PRODUCTS")
        deps_p = expand_calc_dependencies(view_p)
        fetch_p = make_fetch_df(view_p, deps_p)
        fetch_p, status_fid_p = append_internal_status_fetch(fetch_p, "PRODUCT")

        print("CALC deps (products):", sorted(list(deps_p))[:30], "..." if len(deps_p) > 30 else "")
        plan_p = build_plan(fetch_p, "PRODUCT")
        nodes_p = fetch_all_products(client, page_size, plan_p, debug_every=debug_every)

        df_p, removed_p = build_export_df_filtered(
            "PRODUCT",
            view_p,
            fetch_p,
            nodes_p,
            status_fid_p,
            exclude_archived=exclude_archived,
            only_first_row_synced_at=only_first_row_synced_at,
        )

        mode_p = get_effective_mode(write_mode, write_mode_products)
        ws_p = ensure_ws(gc, out_sheet_url, out_tab_idx_products)
        write_df(ws_p, df_p, mode_p)

        result["products_rows"] = len(df_p)
        result["products_filtered_archived"] = removed_p
        print(f"✅ IDX__Products exported: rows={len(df_p)} cols={len(df_p.columns)} mode={mode_p} filtered_archived={removed_p}")

    # ---- Variants ----
    if export_idx_variants:
        view_v = get_view_cfg(cfg_df, "IDX__VARIANTS")
        deps_v = expand_calc_dependencies(view_v)
        fetch_v = make_fetch_df(view_v, deps_v)
        fetch_v, status_fid_v = append_internal_status_fetch(fetch_v, "VARIANT")

        print("CALC deps (variants):", sorted(list(deps_v))[:30], "..." if len(deps_v) > 30 else "")
        plan_v = build_plan(fetch_v, "VARIANT")
        nodes_v = fetch_all_variants(client, page_size, plan_v, debug_every=debug_every)

        df_v, removed_v = build_export_df_filtered(
            "VARIANT",
            view_v,
            fetch_v,
            nodes_v,
            status_fid_v,
            exclude_archived=exclude_archived,
            only_first_row_synced_at=only_first_row_synced_at,
        )

        mode_v = get_effective_mode(write_mode, write_mode_variants)
        ws_v = ensure_ws(gc, out_sheet_url, out_tab_idx_variants)
        write_df(ws_v, df_v, mode_v)

        result["variants_rows"] = len(df_v)
        result["variants_filtered_archived"] = removed_v
        print(f"✅ IDX__Variants exported: rows={len(df_v)} cols={len(df_v.columns)} mode={mode_v} filtered_archived={removed_v}")

    print("✅ export_idx_tables done.")
    return result
