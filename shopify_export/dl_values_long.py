# shopify_export/dl_values_long.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import base64
import io
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


RUNLOG_HEADERS = [
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

DL_HEADERS = [
    "owner_entity_type",
    "owner_gid",
    "owner_legacy_id",
    "field_key",
    "value",
    "value_type",
    "source_view_ids",
    "source_ref_field_key",
    "ref_gid",
    "ref_type",
]

VALID_ENTITY_PREFIXES = {"PRODUCT", "VARIANT", "COLLECTION", "PAGE", "METAOBJECT_ENTRY"}
LONG_PREFIXES = ("mf.", "v_mf.", "mo.")
MO_EXPR_RE = re.compile(r'^MO_REF\(\s*"([^"]+)"\s*\)\s*$', re.I)
PH_RE = re.compile(r"\{([^}]+)\}")


@dataclass
class ShopifyClient:
    shop_domain: str
    api_version: str
    access_token: str
    timeout: int = 60
    max_retries: int = 5
    backoff_factor: float = 1.2

    def __post_init__(self):
        self.url = f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json"
        self.session = requests.Session()
        retry = Retry(
            total=self.max_retries,
            connect=self.max_retries,
            read=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
            }
        )

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        resp = self.session.post(self.url, json=payload, timeout=self.timeout)
        try:
            data = resp.json()
        except Exception:
            raise RuntimeError(f"Shopify GraphQL non-json response: status={resp.status_code}")

        if resp.status_code >= 400:
            raise RuntimeError(f"Shopify GraphQL HTTP {resp.status_code}: {data}")

        if data.get("errors"):
            raise RuntimeError(f"Shopify GraphQL errors: {data['errors']}")

        out = data.get("data") or {}
        ext = data.get("extensions") or {}
        throttle = ((ext.get("cost") or {}).get("throttleStatus") or {})
        avail = throttle.get("currentlyAvailable")
        restore = throttle.get("restoreRate")
        if isinstance(avail, (int, float)) and isinstance(restore, (int, float)):
            if avail < 100:
                sleep_s = max(0.8, min(3.0, (100 - avail) / max(restore, 1)))
                time.sleep(sleep_s)
        return out


def build_gspread_client_from_b64(sa_b64: str) -> gspread.Client:
    raw = base64.b64decode(sa_b64).decode("utf-8")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def now_cn_str() -> str:
    return pd.Timestamp.utcnow().tz_localize("UTC").tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")


def normalize_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def is_true(x: Any) -> bool:
    s = normalize_str(x).upper()
    return s in {"TRUE", "1", "YES", "Y", "T"}


def gql_safe_alias(s: str) -> str:
    a = re.sub(r"[^0-9A-Za-z_]", "_", str(s))
    if re.match(r"^\d", a):
        a = "f_" + a
    return a


def chunk_list(xs: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [xs]
    return [xs[i:i + size] for i in range(0, len(xs), size)]


def ensure_ws(gc: gspread.Client, sheet_url: str, tab_name: str):
    sh = gc.open_by_url(sheet_url)
    try:
        return sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab_name, rows=2000, cols=30)


def write_df_replace(ws, df: pd.DataFrame):
    ws.clear()
    if df is None or df.empty:
        ws.update("A1", [DL_HEADERS])
        return
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)


def append_runlog_rows(gc: gspread.Client, runlog_url: str, runlog_tab: str, rows: List[List[Any]]):
    if not runlog_url or not runlog_tab:
        return
    ws = ensure_ws(gc, runlog_url, runlog_tab)
    existing = ws.get_all_values()
    if not existing:
        ws.update("A1", [RUNLOG_HEADERS])
    elif existing[0] != RUNLOG_HEADERS:
        ws.clear()
        ws.update("A1", [RUNLOG_HEADERS])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


def open_ws(gc: gspread.Client, url: str, tab: str):
    sh = gc.open_by_url(url)
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"Worksheet not found: {tab}")


def read_table(gc: gspread.Client, url: str, tab: str) -> pd.DataFrame:
    ws = open_ws(gc, url, tab)
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    return pd.DataFrame(data, columns=header).replace({"": None})


def _normalize_header(xs):
    return [normalize_str(x) for x in xs]


def _strip_entity_prefix(col: str) -> str:
    col = normalize_str(col)
    if "|" in col:
        pfx, rest = col.split("|", 1)
        if normalize_str(pfx).upper() in VALID_ENTITY_PREFIXES:
            return normalize_str(rest)
    return col


def _find_header_row(rows, required_any=("core.gid", "core.legacy_id"), max_scan=80):
    req = set(required_any)
    for i in range(min(len(rows), max_scan)):
        raw = _normalize_header(rows[i])
        stripped = [_strip_entity_prefix(c) for c in raw]
        if req.intersection(set(raw)) or req.intersection(set(stripped)):
            return i
    return 0


def _build_gid(owner_type: str, legacy_id: str) -> str:
    legacy_id = normalize_str(legacy_id)
    if not legacy_id:
        return ""
    ot = owner_type.upper()
    if ot == "PRODUCT":
        return f"gid://shopify/Product/{legacy_id}"
    if ot == "VARIANT":
        return f"gid://shopify/ProductVariant/{legacy_id}"
    if ot == "COLLECTION":
        return f"gid://shopify/Collection/{legacy_id}"
    if ot == "PAGE":
        return f"gid://shopify/Page/{legacy_id}"
    return legacy_id


def read_idx_df(gc: gspread.Client, sheet_url: str, tab: str, owner_type: str) -> pd.DataFrame:
    ws = open_ws(gc, sheet_url, tab)
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        raise RuntimeError(f"IDX tab empty: {tab}")

    header_i = _find_header_row(rows)
    header_raw = _normalize_header(rows[header_i])
    data = rows[header_i + 1:]
    df = pd.DataFrame(data, columns=header_raw).replace({"": None})

    rename_map = {}
    for c in df.columns:
        c2 = _strip_entity_prefix(c)
        if c2 != c:
            rename_map[c] = c2
    if rename_map:
        df = df.rename(columns=rename_map)

    if "core.legacy_id" not in df.columns:
        raise RuntimeError(f"IDX tab {tab} missing core.legacy_id")
    if "core.gid" not in df.columns:
        df["core.gid"] = df["core.legacy_id"].apply(lambda x: _build_gid(owner_type, x))

    df = df.dropna(subset=["core.legacy_id"]).copy()
    df["core.legacy_id"] = df["core.legacy_id"].astype(str).str.strip()
    df["core.gid"] = df["core.gid"].astype(str).str.strip()
    df = df[df["core.gid"].str.len() > 0].copy()
    return df


def parse_mf_field_key(field_key: str) -> Tuple[str, str, str]:
    s = normalize_str(field_key)
    if s.startswith("mf."):
        parts = s.split(".", 2)
        if len(parts) == 3:
            return "mf", parts[1], parts[2]
    if s.startswith("v_mf."):
        parts = s.split(".", 2)
        if len(parts) == 3:
            return "v_mf", parts[1], parts[2]
    return "", "", ""


def parse_mo_field_key(field_key: str) -> Tuple[str, str]:
    s = normalize_str(field_key)
    if not s.startswith("mo."):
        return "", ""
    rest = s[3:]
    if "." not in rest:
        return "", ""
    metaobject_type, meta_field_key = rest.rsplit(".", 1)
    return metaobject_type.strip(), meta_field_key.strip()


def parse_mo_ref_expr(expr: str) -> str:
    s = normalize_str(expr)
    m = MO_EXPR_RE.match(s)
    if not m:
        return ""
    return normalize_str(m.group(1))


def build_alias_to_fieldkey(cfg_fields: pd.DataFrame) -> Dict[str, str]:
    out = {}
    for _, r in cfg_fields.fillna("").iterrows():
        fk = normalize_str(r.get("field_key"))
        alias = normalize_str(r.get("alias"))
        if fk:
            out[fk] = fk
        if alias:
            out[alias] = fk
    return out


def extract_placeholders(expr: str) -> List[str]:
    return [normalize_str(x) for x in PH_RE.findall(normalize_str(expr)) if normalize_str(x)]


def get_view_cfg(cfg_export: pd.DataFrame, view_id: str) -> pd.DataFrame:
    df = cfg_export[cfg_export["view_id"].astype(str).str.strip() == view_id].copy()
    if df.empty:
        return df
    if "seq" not in df.columns:
        df["seq"] = ""
    df["__seq"] = pd.to_numeric(df["seq"], errors="coerce").fillna(999999).astype(int)
    return df.sort_values("__seq").drop(columns=["__seq"])


def decide_enabled_views(cfg_tabs: pd.DataFrame, view_toggles: List[Tuple[str, bool]]) -> List[str]:
    if isinstance(view_toggles, list) and len(view_toggles) > 0:
        enabled = [normalize_str(v) for (v, on) in view_toggles if on and normalize_str(v)]
        return list(dict.fromkeys(enabled))

    if cfg_tabs is not None and not cfg_tabs.empty:
        cfg_tabs = cfg_tabs.copy()
        cfg_tabs.columns = [normalize_str(c) for c in cfg_tabs.columns]
        if "view_id" in cfg_tabs.columns:
            if "enabled" in cfg_tabs.columns:
                df = cfg_tabs[cfg_tabs["enabled"].apply(is_true)].copy()
                vids = [normalize_str(x) for x in df["view_id"].tolist() if normalize_str(x)]
                if vids:
                    return list(dict.fromkeys(vids))
            vids = [normalize_str(x) for x in cfg_tabs["view_id"].tolist() if normalize_str(x)]
            if vids:
                return list(dict.fromkeys(vids))
    return []


def parse_default_filters_from_tabs(cfg_tabs: pd.DataFrame, enabled_views: List[str]) -> Dict[str, Dict[str, Tuple[bool, str]]]:
    """
    支持 Cfg__ExportTabs 里这些可选列：
    - view_filters_json
    - filters_json
    JSON 结构示例：
    {
      "PRODUCT|mf.custom.new_date": [true, "260326"],
      "VARIANT|core.sku": [false, ""]
    }
    """
    out: Dict[str, Dict[str, Tuple[bool, str]]] = {}
    if cfg_tabs is None or cfg_tabs.empty or "view_id" not in cfg_tabs.columns:
        return out

    work = cfg_tabs.copy().fillna("")
    work.columns = [normalize_str(c) for c in work.columns]
    for v in enabled_views:
        row = work[work["view_id"].astype(str).str.strip() == v]
        if row.empty:
            continue
        rr = row.iloc[0].to_dict()
        raw = normalize_str(rr.get("view_filters_json") or rr.get("filters_json"))
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        view_map: Dict[str, Tuple[bool, str]] = {}
        for k, val in obj.items():
            kk = normalize_str(k)
            if not kk:
                continue
            if isinstance(val, list) and len(val) >= 2:
                view_map[kk] = (bool(val[0]), normalize_str(val[1]))
            elif isinstance(val, dict):
                view_map[kk] = (bool(val.get("enabled")), normalize_str(val.get("value")))
        if view_map:
            out[v] = view_map
    return out


def merge_view_filters(
    default_filters: Dict[str, Dict[str, Tuple[bool, str]]],
    override_filters: Dict[str, Dict[str, Tuple[bool, str]]],
    use_default_filters: bool,
) -> Dict[str, Dict[str, Tuple[bool, str]]]:
    if not use_default_filters:
        return override_filters or {}

    out = {}
    for v, mp in (default_filters or {}).items():
        out[v] = dict(mp)

    for v, mp in (override_filters or {}).items():
        if v not in out:
            out[v] = {}
        for fk, vv in mp.items():
            out[v][fk] = vv
    return out


def _parse_filter_key(k: str) -> Tuple[str, str]:
    s = normalize_str(k)
    if "|" not in s:
        return "", s
    a, b = s.split("|", 1)
    return normalize_str(a).upper(), normalize_str(b)


def _eval_one_filter(series: pd.Series, value_expr: str) -> pd.Series:
    s = series.fillna("").astype(str)
    v = normalize_str(value_expr)
    if v == "":
        return pd.Series([True] * len(s), index=s.index)

    if v.startswith("~"):
        pat = v[1:].strip()
        try:
            rgx = re.compile(pat, flags=re.IGNORECASE)
            return s.apply(lambda x: bool(rgx.search(str(x))))
        except re.error:
            return pd.Series([False] * len(s), index=s.index)

    items = [x.strip() for x in v.split(",") if x.strip()]
    if not items:
        return pd.Series([True] * len(s), index=s.index)
    items_set = set([x.lower() for x in items])
    return s.apply(lambda x: str(x).strip().lower() in items_set)


def apply_filters_for_owner(
    df: pd.DataFrame,
    owner_type: str,
    global_filters: Dict[str, Tuple[bool, str]],
    view_filters: Dict[str, Dict[str, Tuple[bool, str]]],
    enabled_views: List[str],
    warnings: List[str],
) -> pd.DataFrame:
    work = df.copy()
    owner_type = owner_type.upper()

    for k, (enabled, value) in (global_filters or {}).items():
        if not enabled:
            continue
        et, col = _parse_filter_key(k)
        if et and et != owner_type:
            continue
        if col not in work.columns:
            warnings.append(f"skip global filter missing col: {k}")
            continue
        work = work[_eval_one_filter(work[col], value)]

    for view_id in enabled_views:
        vf = (view_filters or {}).get(view_id) or {}
        if not vf:
            continue

        mask_view = pd.Series([True] * len(work), index=work.index)
        used_any = False

        for k, (enabled, value) in vf.items():
            if not enabled:
                continue
            et, col = _parse_filter_key(k)
            if et and et != owner_type:
                continue
            if col not in work.columns:
                warnings.append(f"skip view filter missing col: view={view_id} key={k}")
                continue
            used_any = True
            mask_view = mask_view & _eval_one_filter(work[col], value)

        if used_any:
            work = work[mask_view]

    return work.copy()


def build_long_need(
    cfg_fields: pd.DataFrame,
    cfg_export: pd.DataFrame,
    enabled_views_non_idx: List[str],
    idx_coverage_product: set,
    idx_coverage_variant: set,
) -> Tuple[Dict[str, Any], List[str]]:
    warnings: List[str] = []

    need_cols_export = ["view_id", "seq", "field_type", "entity_type", "field_key", "expr", "alias", "data_type", "required", "notes"]
    need_cols_fields = ["entity_type", "field_key", "expr", "alias", "source_type", "namespace", "key", "data_type", "field_type"]
    for c in need_cols_export:
        if c not in cfg_export.columns:
            cfg_export[c] = ""
    for c in need_cols_fields:
        if c not in cfg_fields.columns:
            cfg_fields[c] = ""

    alias_to_fieldkey = build_alias_to_fieldkey(cfg_fields)

    product_mf: set = set()
    variant_mf: set = set()
    mo_specs: Dict[str, Dict[str, Any]] = {}

    for view_id in enabled_views_non_idx:
        view_df = get_view_cfg(cfg_export, view_id)
        if view_df.empty:
            warnings.append(f"view not found in Cfg__ExportTabFields: {view_id}")
            continue

        for _, r in view_df.fillna("").iterrows():
            field_key = normalize_str(r.get("field_key"))
            expr = normalize_str(r.get("expr"))
            entity_type = normalize_str(r.get("entity_type")).upper()
            field_type = normalize_str(r.get("field_type")).upper()

            candidates: List[str] = []
            if field_key:
                candidates.append(field_key)

            if field_type == "CALC" and expr:
                for ph in extract_placeholders(expr):
                    mapped = alias_to_fieldkey.get(ph) or ph
                    if mapped:
                        candidates.append(mapped)

            for fk in candidates:
                if not fk.startswith(LONG_PREFIXES):
                    continue

                if fk.startswith("mf."):
                    if fk not in idx_coverage_product:
                        product_mf.add(fk)

                elif fk.startswith("v_mf."):
                    if fk not in idx_coverage_variant:
                        variant_mf.add(fk)

                elif fk.startswith("mo."):
                    mo_type, mo_field_key = parse_mo_field_key(fk)
                    if not mo_type or not mo_field_key:
                        warnings.append(f"bad mo field_key: {fk} view={view_id}")
                        continue

                    ref_fk = parse_mo_ref_expr(expr)
                    if not ref_fk:
                        warnings.append(f"mo field missing MO_REF expr: field_key={fk} view={view_id}")
                        continue

                    if not (ref_fk.startswith("mf.") or ref_fk.startswith("v_mf.")):
                        warnings.append(f"mo expr invalid source ref: field_key={fk} expr={expr} view={view_id}")
                        continue

                    src_prefix, ns, key = parse_mf_field_key(ref_fk)
                    if not ns or not key:
                        warnings.append(f"mo expr parse failed: field_key={fk} expr={expr} view={view_id}")
                        continue

                    # source ref 自己也需要抓
                    if ref_fk.startswith("mf.") and ref_fk not in idx_coverage_product:
                        product_mf.add(ref_fk)
                    if ref_fk.startswith("v_mf.") and ref_fk not in idx_coverage_variant:
                        variant_mf.add(ref_fk)

                    base_row = cfg_fields[cfg_fields["field_key"].astype(str).str.strip() == fk]
                    if base_row.empty:
                        warnings.append(f"mo field not found in Cfg__Fields: {fk}")
                        continue

                    rr = base_row.iloc[0].fillna("").to_dict()
                    if normalize_str(rr.get("entity_type")).upper() != "METAOBJECT_ENTRY":
                        warnings.append(f"mo field entity_type not METAOBJECT_ENTRY: {fk}")
                        continue
                    if normalize_str(rr.get("source_type")).upper() != "METAOBJECT_REF":
                        warnings.append(f"mo field source_type not METAOBJECT_REF: {fk}")
                        continue

                    spec = mo_specs.get(fk) or {
                        "field_key": fk,
                        "metaobject_type": mo_type,
                        "meta_field_key": mo_field_key,
                        "source_ref_field_keys": set(),
                        "source_view_ids": set(),
                        "data_type": normalize_str(rr.get("data_type")),
                        "field_type": normalize_str(rr.get("field_type")),
                    }
                    spec["source_ref_field_keys"].add(ref_fk)
                    spec["source_view_ids"].add(view_id)
                    mo_specs[fk] = spec

    return {
        "product_mf": sorted(product_mf),
        "variant_mf": sorted(variant_mf),
        "mo_specs": mo_specs,
    }, warnings


def build_mf_selection_and_map(prefix: str, mf_field_keys: List[str]) -> Tuple[str, Dict[str, str]]:
    lines = []
    alias_to_fk = {}
    for fk in mf_field_keys:
        _, ns, key = parse_mf_field_key(fk)
        if not ns or not key:
            continue
        alias = gql_safe_alias(fk)
        alias_to_fk[alias] = fk
        lines.append(f'{alias}: metafield(namespace: "{ns}", key: "{key}") {{ value type references(first: 50) {{ nodes {{ __typename ... on Metaobject {{ id type handle }} ... on Collection {{ id }} ... on ProductTaxonomyValue {{ id name fullName }} ... on GenericFile {{ id url }} ... on MediaImage {{ id image {{ url altText }} }} }} }} }}')
    return "\n".join(lines), alias_to_fk


def fetch_nodes_products(client: ShopifyClient, ids: List[str], mf_field_keys: List[str], chunk_size_ids: int, chunk_size_mf: int) -> Dict[str, dict]:
    if not ids or not mf_field_keys:
        return {}

    out: Dict[str, dict] = {}
    for id_part in chunk_list(ids, chunk_size_ids):
        for mf_part in chunk_list(mf_field_keys, chunk_size_mf):
            sel, alias_to_fk = build_mf_selection_and_map("mf", mf_part)
            q = f"""
query NodesProducts($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on Product {{
      id
      legacyResourceId
      updatedAt
      {sel}
    }}
  }}
}}
""".strip()
            data = client.gql(q, {"ids": id_part})
            for n in (data.get("nodes") or []):
                if not n:
                    continue
                gid = normalize_str(n.get("id"))
                if not gid:
                    continue
                if gid not in out:
                    out[gid] = {"id": gid, "legacyResourceId": n.get("legacyResourceId"), "updatedAt": n.get("updatedAt"), "__mf": {}}
                mf_map = out[gid]["__mf"]
                for alias, fk in alias_to_fk.items():
                    block = n.get(alias)
                    if block is None:
                        continue
                    refs = (((block.get("references") or {}).get("nodes")) or [])
                    mf_map[fk] = {
                        "value": "" if block.get("value") is None else str(block.get("value")),
                        "type": normalize_str(block.get("type")),
                        "references": refs,
                    }
    return out


def fetch_nodes_variants(client: ShopifyClient, ids: List[str], mf_field_keys: List[str], chunk_size_ids: int, chunk_size_mf: int) -> Dict[str, dict]:
    if not ids or not mf_field_keys:
        return {}

    out: Dict[str, dict] = {}
    for id_part in chunk_list(ids, chunk_size_ids):
        for mf_part in chunk_list(mf_field_keys, chunk_size_mf):
            sel, alias_to_fk = build_mf_selection_and_map("v_mf", mf_part)
            q = f"""
query NodesVariants($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on ProductVariant {{
      id
      legacyResourceId
      updatedAt
      {sel}
    }}
  }}
}}
""".strip()
            data = client.gql(q, {"ids": id_part})
            for n in (data.get("nodes") or []):
                if not n:
                    continue
                gid = normalize_str(n.get("id"))
                if not gid:
                    continue
                if gid not in out:
                    out[gid] = {"id": gid, "legacyResourceId": n.get("legacyResourceId"), "updatedAt": n.get("updatedAt"), "__mf": {}}
                mf_map = out[gid]["__mf"]
                for alias, fk in alias_to_fk.items():
                    block = n.get(alias)
                    if block is None:
                        continue
                    refs = (((block.get("references") or {}).get("nodes")) or [])
                    mf_map[fk] = {
                        "value": "" if block.get("value") is None else str(block.get("value")),
                        "type": normalize_str(block.get("type")),
                        "references": refs,
                    }
    return out


def fetch_metaobjects_for_specs(
    client: ShopifyClient,
    ref_gid_to_expected_type: Dict[str, str],
    specs: List[Dict[str, Any]],
    chunk_size_ids: int,
) -> Dict[str, Dict[str, Any]]:
    """
    返回:
    {
      metaobject_gid: {
        "id": "...",
        "type": "...",
        "__fields": {"title": {"value": "...", "type": "single_line_text_field"}, ...}
      }
    }
    """
    if not ref_gid_to_expected_type:
        return {}

    all_meta_keys = sorted({normalize_str(s["meta_field_key"]) for s in specs if normalize_str(s.get("meta_field_key"))})
    field_lines = []
    for k in all_meta_keys:
        alias = gql_safe_alias(f"mo_field_{k}")
        field_lines.append(f'{alias}: field(key: "{k}") {{ key value type reference {{ __typename ... on Metaobject {{ id type handle }} ... on Collection {{ id }} ... on ProductTaxonomyValue {{ id name fullName }} ... on GenericFile {{ id url }} ... on MediaImage {{ id image {{ url altText }} }} }} references(first: 50) {{ nodes {{ __typename ... on Metaobject {{ id type handle }} ... on Collection {{ id }} ... on ProductTaxonomyValue {{ id name fullName }} ... on GenericFile {{ id url }} ... on MediaImage {{ id image {{ url altText }} }} }} }} }}')
    sel = "\n".join(field_lines)

    out: Dict[str, Dict[str, Any]] = {}
    ids = list(ref_gid_to_expected_type.keys())
    for id_part in chunk_list(ids, chunk_size_ids):
        q = f"""
query MetaobjectsByIds($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on Metaobject {{
      id
      type
      handle
      {sel}
    }}
  }}
}}
""".strip()
        data = client.gql(q, {"ids": id_part})
        for n in (data.get("nodes") or []):
            if not n:
                continue
            gid = normalize_str(n.get("id"))
            if not gid:
                continue
            out[gid] = {
                "id": gid,
                "type": normalize_str(n.get("type")),
                "handle": normalize_str(n.get("handle")),
                "__fields": {},
            }
            for meta_field_key in all_meta_keys:
                alias = gql_safe_alias(f"mo_field_{meta_field_key}")
                blk = n.get(alias)
                if not blk:
                    continue
                refs = (((blk.get("references") or {}).get("nodes")) or [])
                out[gid]["__fields"][meta_field_key] = {
                    "value": "" if blk.get("value") is None else str(blk.get("value")),
                    "type": normalize_str(blk.get("type")),
                    "reference": blk.get("reference"),
                    "references": refs,
                }
    return out


def serialize_reference_node(node: Dict[str, Any]) -> str:
    if not isinstance(node, dict):
        return ""
    t = normalize_str(node.get("__typename"))
    if t == "Metaobject":
        return normalize_str(node.get("id"))
    if t == "Collection":
        return normalize_str(node.get("id"))
    if t == "ProductTaxonomyValue":
        return normalize_str(node.get("fullName") or node.get("name") or node.get("id"))
    if t == "GenericFile":
        return normalize_str(node.get("url") or node.get("id"))
    if t == "MediaImage":
        img = node.get("image") or {}
        return normalize_str(img.get("url") or node.get("id"))
    return normalize_str(node.get("id"))


def extract_mo_value(meta_field_block: Dict[str, Any], join_sep: str) -> Tuple[str, str]:
    if not meta_field_block:
        return "", ""

    refs = meta_field_block.get("references") or []
    if refs:
        vals = [serialize_reference_node(x) for x in refs if serialize_reference_node(x)]
        return join_sep.join(vals), normalize_str(meta_field_block.get("type"))

    ref = meta_field_block.get("reference")
    if ref:
        return serialize_reference_node(ref), normalize_str(meta_field_block.get("type"))

    return normalize_str(meta_field_block.get("value")), normalize_str(meta_field_block.get("type"))


def dedupe_long_df(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return df_long
    key_cols = ["owner_entity_type", "owner_gid", "field_key", "value"]
    return df_long.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)


def run(
    *,
    site_code: str,
    shop_domain: str,
    api_version: str,
    shopify_access_token: str,
    gsheet_sa_b64: str,
    console_core_url: str,
    view_toggles: List[Tuple[str, bool]],
    use_default_filters: bool = True,
    view_filter_overrides: Optional[Dict[str, Dict[str, Tuple[bool, str]]]] = None,
    global_filters: Optional[Dict[str, Tuple[bool, str]]] = None,
    cfg_sites_tab: str = "Cfg__Sites",
    cfg_tabs_tab: str = "Cfg__ExportTabs",
    cfg_fields_tab: str = "Cfg__Fields",
    cfg_export_tab: str = "Cfg__ExportTabFields",
    idx_products_tab: str = "IDX__Products",
    idx_variants_tab: str = "IDX__Variants",
    values_long_tab: str = "DL__ValuesLong",
    idx_view_products: str = "IDX__Products",
    idx_view_variants: str = "IDX__Variants",
    runlog_tab: str = "Ops__RunLog",
    mo_list_join_sep: str = " , ",
    gql_ids_per_query: int = 50,
    gql_mf_per_query: int = 25,
) -> Dict[str, Any]:

    gc = build_gspread_client_from_b64(gsheet_sa_b64)
    client = ShopifyClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=shopify_access_token,
    )

    run_id = f"dl_values_long_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}"
    ts_cn = now_cn_str()
    warnings: List[str] = []
    error_details: List[str] = []

    cfg_sites = read_table(gc, console_core_url, cfg_sites_tab).fillna("")
    cfg_tabs = read_table(gc, console_core_url, cfg_tabs_tab).fillna("")
    cfg_fields = read_table(gc, console_core_url, cfg_fields_tab).fillna("")
    cfg_export = read_table(gc, console_core_url, cfg_export_tab).fillna("")

    if cfg_export.empty:
        raise RuntimeError("Cfg__ExportTabFields is empty")

    site_rows = cfg_sites[cfg_sites["site_code"].astype(str).str.strip().str.upper() == site_code.upper()].copy()
    if site_rows.empty:
        raise RuntimeError(f"C not found in Cfg__Sites: {site_code}")

    def get_sheet_url(label: str) -> str:
        x = site_rows[site_rows["label"].astype(str).str.strip() == label]
        if x.empty:
            return ""
        return normalize_str(x.iloc[0].get("sheet_url"))

    data_sheet_url = get_sheet_url("export_product")
    runlog_sheet_url = get_sheet_url("runlog_sheet")

    if not data_sheet_url:
        raise RuntimeError(f"site {site_code} missing label=export_product in Cfg__Sites")

    enabled_views = decide_enabled_views(cfg_tabs, view_toggles)
    enabled_views_non_idx = [v for v in enabled_views if v not in (idx_view_products, idx_view_variants)]

    idx_p_cfg = get_view_cfg(cfg_export, idx_view_products)
    idx_v_cfg = get_view_cfg(cfg_export, idx_view_variants)
    idx_coverage_product = set([normalize_str(x) for x in idx_p_cfg.get("field_key", pd.Series(dtype=str)).tolist() if normalize_str(x)])
    idx_coverage_variant = set([normalize_str(x) for x in idx_v_cfg.get("field_key", pd.Series(dtype=str)).tolist() if normalize_str(x)])

    need_result, need_warnings = build_long_need(
        cfg_fields=cfg_fields,
        cfg_export=cfg_export,
        enabled_views_non_idx=enabled_views_non_idx,
        idx_coverage_product=idx_coverage_product,
        idx_coverage_variant=idx_coverage_variant,
    )
    warnings.extend(need_warnings)

    product_mf = need_result["product_mf"]
    variant_mf = need_result["variant_mf"]
    mo_specs = need_result["mo_specs"]

    idx_products = read_idx_df(gc, data_sheet_url, idx_products_tab, "PRODUCT")
    idx_variants = read_idx_df(gc, data_sheet_url, idx_variants_tab, "VARIANT")

    default_view_filters = parse_default_filters_from_tabs(cfg_tabs, enabled_views_non_idx)
    merged_view_filters = merge_view_filters(
        default_filters=default_view_filters,
        override_filters=(view_filter_overrides or {}),
        use_default_filters=use_default_filters,
    )

    product_df = apply_filters_for_owner(
        df=idx_products,
        owner_type="PRODUCT",
        global_filters=global_filters or {},
        view_filters=merged_view_filters,
        enabled_views=enabled_views_non_idx,
        warnings=warnings,
    )

    variant_df = apply_filters_for_owner(
        df=idx_variants,
        owner_type="VARIANT",
        global_filters=global_filters or {},
        view_filters=merged_view_filters,
        enabled_views=enabled_views_non_idx,
        warnings=warnings,
    )

    product_ids = sorted(product_df["core.gid"].astype(str).str.strip().unique().tolist()) if not product_df.empty else []
    variant_ids = sorted(variant_df["core.gid"].astype(str).str.strip().unique().tolist()) if not variant_df.empty else []

    product_nodes = fetch_nodes_products(client, product_ids, product_mf, gql_ids_per_query, gql_mf_per_query)
    variant_nodes = fetch_nodes_variants(client, variant_ids, variant_mf, gql_ids_per_query, gql_mf_per_query)

    ref_gid_to_expected_type: Dict[str, str] = {}
    for owner_type, node_map in [("PRODUCT", product_nodes), ("VARIANT", variant_nodes)]:
        for owner_gid, nd in node_map.items():
            mf_map = nd.get("__mf") or {}
            for fk, spec in mo_specs.items():
                for src_ref_fk in sorted(spec["source_ref_field_keys"]):
                    if owner_type == "PRODUCT" and not src_ref_fk.startswith("mf."):
                        continue
                    if owner_type == "VARIANT" and not src_ref_fk.startswith("v_mf."):
                        continue

                    src_blk = mf_map.get(src_ref_fk) or {}
                    refs = src_blk.get("references") or []
                    for rr in refs:
                        if normalize_str(rr.get("__typename")) == "Metaobject":
                            ref_gid = normalize_str(rr.get("id"))
                            if ref_gid:
                                ref_gid_to_expected_type[ref_gid] = spec["metaobject_type"]

    metaobject_cache = fetch_metaobjects_for_specs(
        client=client,
        ref_gid_to_expected_type=ref_gid_to_expected_type,
        specs=list(mo_specs.values()),
        chunk_size_ids=gql_ids_per_query,
    )

    long_rows: List[Dict[str, Any]] = []

    # mf.*
    for owner_type, src_df, node_map, prefix in [
        ("PRODUCT", product_df, product_nodes, "mf."),
        ("VARIANT", variant_df, variant_nodes, "v_mf."),
    ]:
        if src_df.empty:
            continue
        for _, row in src_df.iterrows():
            owner_gid = normalize_str(row.get("core.gid"))
            owner_legacy_id = normalize_str(row.get("core.legacy_id"))
            nd = node_map.get(owner_gid) or {}
            mf_map = nd.get("__mf") or {}
            for fk, blk in mf_map.items():
                if not fk.startswith(prefix):
                    continue
                val = normalize_str(blk.get("value"))
                typ = normalize_str(blk.get("type"))
                if val == "":
                    continue
                long_rows.append(
                    {
                        "owner_entity_type": owner_type,
                        "owner_gid": owner_gid,
                        "owner_legacy_id": owner_legacy_id,
                        "field_key": fk,
                        "value": val,
                        "value_type": typ,
                        "source_view_ids": "",
                        "source_ref_field_key": "",
                        "ref_gid": "",
                        "ref_type": "",
                    }
                )

    # mo.*
    for owner_type, src_df, node_map in [
        ("PRODUCT", product_df, product_nodes),
        ("VARIANT", variant_df, variant_nodes),
    ]:
        if src_df.empty:
            continue
        for _, row in src_df.iterrows():
            owner_gid = normalize_str(row.get("core.gid"))
            owner_legacy_id = normalize_str(row.get("core.legacy_id"))
            nd = node_map.get(owner_gid) or {}
            mf_map = nd.get("__mf") or {}

            for fk, spec in mo_specs.items():
                source_ref_candidates = sorted(spec["source_ref_field_keys"])
                valid_src = [x for x in source_ref_candidates if (owner_type == "PRODUCT" and x.startswith("mf.")) or (owner_type == "VARIANT" and x.startswith("v_mf."))]
                if not valid_src:
                    continue

                for src_ref_fk in valid_src:
                    src_blk = mf_map.get(src_ref_fk) or {}
                    refs = src_blk.get("references") or []
                    if not refs:
                        continue

                    mo_vals = []
                    ref_gids = []
                    ref_types = []
                    for rr in refs:
                        if normalize_str(rr.get("__typename")) != "Metaobject":
                            continue
                        ref_gid = normalize_str(rr.get("id"))
                        if not ref_gid:
                            continue

                        mo_entry = metaobject_cache.get(ref_gid)
                        if not mo_entry:
                            warnings.append(f"mo ref target missing: owner={owner_gid} src_ref={src_ref_fk} ref_gid={ref_gid}")
                            continue

                        actual_type = normalize_str(mo_entry.get("type"))
                        expected_type = normalize_str(spec["metaobject_type"])
                        if actual_type != expected_type:
                            warnings.append(f"mo ref type mismatch: owner={owner_gid} src_ref={src_ref_fk} field_key={fk} expected={expected_type} actual={actual_type}")
                            continue

                        meta_blk = (mo_entry.get("__fields") or {}).get(spec["meta_field_key"]) or {}
                        vv, vt = extract_mo_value(meta_blk, mo_list_join_sep)
                        if vv == "":
                            continue
                        mo_vals.append(vv)
                        ref_gids.append(ref_gid)
                        ref_types.append(actual_type)

                    if mo_vals:
                        long_rows.append(
                            {
                                "owner_entity_type": owner_type,
                                "owner_gid": owner_gid,
                                "owner_legacy_id": owner_legacy_id,
                                "field_key": fk,
                                "value": mo_list_join_sep.join(mo_vals),
                                "value_type": normalize_str(spec.get("data_type")) or "metaobject_ref_expanded",
                                "source_view_ids": " , ".join(sorted(spec["source_view_ids"])),
                                "source_ref_field_key": src_ref_fk,
                                "ref_gid": " , ".join(ref_gids),
                                "ref_type": " , ".join(sorted(list(dict.fromkeys(ref_types)))),
                            }
                        )

    df_long = pd.DataFrame(long_rows, columns=DL_HEADERS).fillna("")
    df_long = dedupe_long_df(df_long)

    ws_long = ensure_ws(gc, data_sheet_url, values_long_tab)
    write_df_replace(ws_long, df_long)

    summary = {
        "run_id": run_id,
        "site_code": site_code,
        "job_name": "dl_values_long",
        "enabled_views": enabled_views_non_idx,
        "rows_loaded": len(idx_products) + len(idx_variants),
        "rows_pending": len(product_df) + len(variant_df),
        "rows_recognized": len(product_mf) + len(variant_mf) + len(mo_specs),
        "rows_planned": len(long_rows),
        "rows_written": len(df_long),
        "rows_skipped": 0,
        "warning_count": len(warnings),
        "warnings": warnings,
        "data_sheet_url": data_sheet_url,
        "values_long_tab": values_long_tab,
    }

    # runlog: summary 1 条；detail 每种 error_reason 最多 2 条
    runlog_rows = [
        [
            run_id, ts_cn, "dl_values_long", "apply", "summary", "OK", site_code,
            "", "", "", len(idx_products) + len(idx_variants), len(product_df) + len(variant_df),
            len(product_mf) + len(variant_mf) + len(mo_specs), len(long_rows), len(df_long), 0,
            f"enabled_views={len(enabled_views_non_idx)} warnings={len(warnings)}", ""
        ]
    ]

    by_reason: Dict[str, List[str]] = {}
    for w in warnings:
        reason = w.split(":", 1)[0].strip() if ":" in w else "warning"
        by_reason.setdefault(reason, [])
        if len(by_reason[reason]) < 2:
            by_reason[reason].append(w)

    for reason, msgs in by_reason.items():
        for msg in msgs:
            runlog_rows.append(
                [
                    run_id, ts_cn, "dl_values_long", "apply", "detail", "WARN", site_code,
                    "", "", "", "", "", "", "", "", "",
                    msg, reason
                ]
            )

    append_runlog_rows(gc, runlog_sheet_url, runlog_tab, runlog_rows)

    return {
        "summary": summary,
        "df_long": df_long,
        "warnings": warnings,
    }
