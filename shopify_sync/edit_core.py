from __future__ import annotations

import base64
import datetime as dt
import json
import random
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import gspread
import pandas as pd
import requests
from google.oauth2 import service_account

try:
    from google.colab import userdata
except Exception:
    userdata = None


# =========================================================
# Constants
# =========================================================

CFG_SITES_TAB_DEFAULT = "Cfg__Sites"
CFG_FIELDS_TAB_DEFAULT = "Cfg__Fields"

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

SUPPORTED_ENTITY_TYPES = {"PRODUCT", "VARIANT", "COLLECTION", "PAGE"}
SUPPORTED_METAFIELD_ACTIONS = {"SET", "CLEAR"}
SUPPORTED_CORE_TAG_ACTIONS = {"SET", "CLEAR", "ADD", "REMOVE"}
SUPPORTED_CORE_SCALAR_ACTIONS = {"SET", "CLEAR"}
SUPPORTED_CORE_PRICE_ACTIONS = {"SET"}
SUPPORTED_CORE_COMPARE_AT_ACTIONS = {"SET", "CLEAR"}

ALLOWED_PREFIXES = ("mf.", "v_mf.", "core.")
FORBIDDEN_SHOPIFY_PREFIXES = ("mf.shopify.", "v_mf.shopify.", "v.mf.shopify.")

PRODUCT_CORE_KEYS = {"core.title", "core.product_type", "core.tags", "core.description_html"}
VARIANT_CORE_KEYS = {"core.weight", "core.weight_unit", "core.price", "core.compare_at_price"}

Q_PRODUCT_BY_HANDLE = """
query($handle: String!) {
  productByHandle(handle: $handle) { id handle title }
}
"""

Q_COLLECTION_BY_HANDLE = """
query($handle: String!) {
  collectionByHandle(handle: $handle) { id handle title }
}
"""

Q_PAGES_BY_QUERY = """
query($q: String!, $first: Int!) {
  pages(first: $first, query: $q) { edges { node { id handle title } } }
}
"""

Q_VARIANTS_BY_QUERY = """
query($q: String!, $first: Int!) {
  productVariants(first: $first, query: $q) { edges { node { id sku } } }
}
"""

Q_NODES_EXIST = """
query($ids: [ID!]!) {
  nodes(ids: $ids) { id }
}
"""

M_METAFIELDS_SET = """
mutation setMf($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key type value }
    userErrors { field message code }
  }
}
"""

M_PRODUCT_UPDATE = """
mutation productUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id title productType tags descriptionHtml }
    userErrors { field message }
  }
}
"""

Q_VARIANT_PRODUCT_MAP = """
query($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on ProductVariant {
      id
      product {
        id
      }
    }
  }
}
"""

M_PRODUCT_VARIANTS_BULK_UPDATE = """
mutation productVariantsBulkUpdate(
  $productId: ID!,
  $variants: [ProductVariantsBulkInput!]!,
  $allowPartialUpdates: Boolean
) {
  productVariantsBulkUpdate(
    productId: $productId,
    variants: $variants,
    allowPartialUpdates: $allowPartialUpdates
  ) {
    product {
      id
    }
    productVariants {
      id
      price
      compareAtPrice
      inventoryItem {
        id
        measurement {
          weight {
            unit
            value
          }
        }
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""


# =========================================================
# Small data objects
# =========================================================

@dataclass
class ShopifyClient:
    graph_url: str
    headers: dict[str, str]
    timeout: int = 60


# =========================================================
# Generic utils
# =========================================================

def _utc_run_id(prefix: str = "edit") -> str:
    return dt.datetime.utcnow().strftime(f"{prefix}_%Y%m%d_%H%M%S")


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


def _chunk_list(items: list[Any], size: int):
    for i in range(0, len(items), size):
        yield i, items[i:i + size]


def _split_items(s: str) -> list[str]:
    s = _norm_str(s)
    if not s:
        return []
    parts = re.split(r"[,\n;|]+", s)
    return [p.strip() for p in parts if p and p.strip()]


def _is_json_array_string(s: str) -> bool:
    s = _norm_str(s)
    if not (s.startswith("[") and s.endswith("]")):
        return False
    try:
        return isinstance(json.loads(s), list)
    except Exception:
        return False


def _upper_strip(s: Any) -> str:
    return _norm_str(s).upper()


def _lower_strip(s: Any) -> str:
    return _norm_str(s).lower()


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


def build_shopify_client(
    shopify_token_secret: str,
    shop_domain: str,
    api_version: str,
    http_timeout: int = 60,
) -> ShopifyClient:
    token = _get_secret(shopify_token_secret)
    return ShopifyClient(
        graph_url=f"https://{shop_domain}/admin/api/{api_version}/graphql.json",
        headers={
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        },
        timeout=http_timeout,
    )


def gql(client: ShopifyClient, query: str, variables: Optional[dict] = None, retries: int = 6) -> dict:
    payload = {"query": query, "variables": variables or {}}
    last_err = None

    for i in range(retries):
        try:
            r = requests.post(
                client.graph_url,
                headers=client.headers,
                json=payload,
                timeout=client.timeout,
            )
            data = r.json()

            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}")

            if data.get("errors"):
                raise RuntimeError(data["errors"])

            if data.get("data") is None:
                raise RuntimeError(f"No data returned: {data}")

            return data["data"]

        except Exception as e:
            last_err = e
            time.sleep(min(2**i, 12) + random.random())

    raise RuntimeError(f"GraphQL failed after retries: {last_err}")


# =========================================================
# Sheet locating
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
                gid=_norm_str(row.get("gid") or row.get("owner_id")),
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
# Load input
# =========================================================

def load_edit_core(ws_edit) -> pd.DataFrame:
    rows = ws_edit.get_all_records()
    df = pd.DataFrame(rows)

    required_cols = ["entity_type", "gid_or_handle", "field_key", "desired_value", "action", "mode", "note", "run_id"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Edit__Core missing columns: {missing}")

    df["_sheet_row"] = range(2, 2 + len(df))

    for c in required_cols:
        df[c] = df[c].astype(str).fillna("").replace("nan", "").str.strip()

    return df


def filter_pending_rows(
    df: pd.DataFrame,
    mode_default: str,
    only_entity_types: Optional[set[str]],
    only_field_prefixes: Optional[set[str]],
) -> pd.DataFrame:
    d = df.copy()

    d = d[d["run_id"].eq("")]
    d["entity_type"] = d["entity_type"].str.upper().str.strip()
    d["action"] = d["action"].str.upper().str.strip()
    d["mode"] = d["mode"].replace("", mode_default).str.upper().str.strip()

    if only_entity_types:
        allow = {x.upper() for x in only_entity_types}
        d = d[d["entity_type"].isin(allow)]

    if only_field_prefixes:
        prefixes = tuple(only_field_prefixes)
        d = d[d["field_key"].str.startswith(prefixes)]

    d = d[~d["action"].isin(["SKIP", ""])]

    return d


# =========================================================
# Recognition
# =========================================================

def parse_metafield_key(field_key: str):
    field_key = _norm_str(field_key)
    if field_key.startswith("mf."):
        prefix = "mf."
    elif field_key.startswith("v_mf."):
        prefix = "v_mf."
    else:
        return None

    rest = field_key[len(prefix):]
    parts = rest.split(".")
    if len(parts) < 2:
        return None

    namespace = parts[0]
    key = ".".join(parts[1:])
    if not namespace or not key:
        return None

    return prefix, namespace, key


def normalize_owner_ref(entity_type: str, gid_or_handle: str) -> str:
    s = _norm_str(gid_or_handle)
    if s.startswith("gid://"):
        return s

    if re.fullmatch(r"\d+", s):
        if entity_type == "PRODUCT":
            return f"gid://shopify/Product/{s}"
        if entity_type == "VARIANT":
            return f"gid://shopify/ProductVariant/{s}"
        if entity_type == "COLLECTION":
            return f"gid://shopify/Collection/{s}"
        if entity_type == "PAGE":
            return f"gid://shopify/Page/{s}"

    return s


def validate_row(entity_type: str, field_key: str, action: str) -> tuple[bool, str]:
    et = _upper_strip(entity_type)
    fk = _norm_str(field_key)
    act = _upper_strip(action)

    if et not in SUPPORTED_ENTITY_TYPES:
        return False, "unsupported_entity_type"

    if not fk.startswith(ALLOWED_PREFIXES):
        return False, "field_key_not_recognized"

    if fk.startswith(FORBIDDEN_SHOPIFY_PREFIXES):
        return False, "forbidden_shopify_prefixed_field_key"

    if fk.startswith("mf."):
        parsed = parse_metafield_key(fk)
        if not parsed:
            return False, "field_key_not_recognized"
        if et == "VARIANT":
            return False, "prefix_entity_mismatch"
        if act not in SUPPORTED_METAFIELD_ACTIONS:
            return False, "action_not_supported"

    elif fk.startswith("v_mf."):
        parsed = parse_metafield_key(fk)
        if not parsed:
            return False, "field_key_not_recognized"
        if et != "VARIANT":
            return False, "prefix_entity_mismatch"
        if act not in SUPPORTED_METAFIELD_ACTIONS:
            return False, "action_not_supported"

    elif fk == "core.tags":
        if et != "PRODUCT":
            return False, "core_entity_mismatch"
        if act not in SUPPORTED_CORE_TAG_ACTIONS:
            return False, "action_not_supported"

    elif fk in {"core.title", "core.product_type", "core.description_html"}:
        if et != "PRODUCT":
            return False, "core_entity_mismatch"
        if act not in SUPPORTED_CORE_SCALAR_ACTIONS:
            return False, "action_not_supported"

    elif fk in {"core.weight", "core.weight_unit"}:
        if et != "VARIANT":
            return False, "core_entity_mismatch"
        if act not in SUPPORTED_CORE_SCALAR_ACTIONS:
            return False, "action_not_supported"

    elif fk == "core.price":
        if et != "VARIANT":
            return False, "core_entity_mismatch"
        if act not in SUPPORTED_CORE_PRICE_ACTIONS:
            return False, "action_not_supported"

    elif fk == "core.compare_at_price":
        if et != "VARIANT":
            return False, "core_entity_mismatch"
        if act not in SUPPORTED_CORE_COMPARE_AT_ACTIONS:
            return False, "action_not_supported"

    else:
        return False, "field_key_not_recognized"

    return True, ""


def recognize_rows(df_work: pd.DataFrame, mode_default: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    good_rows = []
    bad_rows = []

    for idx, r in df_work.iterrows():
        entity_type = _upper_strip(r.get("entity_type"))
        field_key = _norm_str(r.get("field_key"))
        action = _upper_strip(r.get("action"))
        mode = _upper_strip(r.get("mode") or mode_default)
        desired = _norm_str(r.get("desired_value"))
        owner_raw = _norm_str(r.get("gid_or_handle"))
        owner_ref = normalize_owner_ref(entity_type, owner_raw)
        sheet_row = int(r.get("_sheet_row", -1))

        ok, reason = validate_row(entity_type, field_key, action)
        if not ok:
            bad_rows.append({
                "sheet_row": sheet_row,
                "entity_type": entity_type,
                "gid_or_handle": owner_raw,
                "field_key": field_key,
                "action": action,
                "mode": mode,
                "reason": reason,
                "desired_value": desired,
            })
            continue

        row_type = "core" if field_key.startswith("core.") else "metafield"

        rec = {
            "_row_index": idx,
            "sheet_row": sheet_row,
            "row_type": row_type,
            "entity_type": entity_type,
            "owner_ref": owner_ref,
            "owner_raw": owner_raw,
            "field_key": field_key,
            "action": action,
            "mode": mode,
            "desired_value": desired,
            "note": _norm_str(r.get("note")),
        }

        if row_type == "metafield":
            prefix, ns, key = parse_metafield_key(field_key)
            rec["prefix"] = prefix
            rec["namespace"] = ns
            rec["key"] = key

        good_rows.append(rec)

    return pd.DataFrame(good_rows), pd.DataFrame(bad_rows)


# =========================================================
# Owner resolution + existence preflight
# =========================================================

def normalize_gid_or_numeric(entity_type: str, ref: str) -> Optional[str]:
    s = _norm_str(ref)
    if s.startswith("gid://"):
        return s
    if re.fullmatch(r"\d+", s):
        if entity_type == "PRODUCT":
            return f"gid://shopify/Product/{s}"
        if entity_type == "VARIANT":
            return f"gid://shopify/ProductVariant/{s}"
        if entity_type == "COLLECTION":
            return f"gid://shopify/Collection/{s}"
        if entity_type == "PAGE":
            return f"gid://shopify/Page/{s}"
    return None


def resolve_product_by_handle(client: ShopifyClient, handle: str) -> Optional[str]:
    data = gql(client, Q_PRODUCT_BY_HANDLE, {"handle": handle})
    node = data.get("productByHandle")
    return node["id"] if node else None


def resolve_collection_by_handle(client: ShopifyClient, handle: str) -> Optional[str]:
    data = gql(client, Q_COLLECTION_BY_HANDLE, {"handle": handle})
    node = data.get("collectionByHandle")
    return node["id"] if node else None


def resolve_page_by_handle(client: ShopifyClient, handle: str) -> Optional[str]:
    q = f'handle:"{handle}"'
    data = gql(client, Q_PAGES_BY_QUERY, {"q": q, "first": 5})
    edges = ((data.get("pages") or {}).get("edges") or [])
    return edges[0]["node"]["id"] if edges else None


def resolve_variant_by_sku(client: ShopifyClient, sku: str) -> Optional[str]:
    q = f'sku:"{sku}"'
    data = gql(client, Q_VARIANTS_BY_QUERY, {"q": q, "first": 5})
    edges = ((data.get("productVariants") or {}).get("edges") or [])
    return edges[0]["node"]["id"] if edges else None


def nodes_exist_map(client: ShopifyClient, ids: list[str], chunk_size: int = 80) -> dict[str, bool]:
    out = {}
    ids = [x for x in ids if isinstance(x, str) and x.strip()]
    for _, part in _chunk_list(ids, chunk_size):
        data = gql(client, Q_NODES_EXIST, {"ids": part})
        nodes = data.get("nodes") or []
        exist_set = {n["id"] for n in nodes if n and n.get("id")}
        for x in part:
            out[x] = x in exist_set
    return out


def resolve_owner_ids(client: ShopifyClient, df_parsed: pd.DataFrame) -> pd.DataFrame:
    df_ready = df_parsed.copy()
    df_ready["owner_id"] = df_ready.apply(
        lambda r: normalize_gid_or_numeric(r["entity_type"], r["owner_ref"]),
        axis=1,
    )

    mask_need = df_ready["owner_id"].isna() & df_ready["owner_ref"].ne("")
    need = df_ready.loc[mask_need, ["entity_type", "owner_ref"]].drop_duplicates()

    cache_product = {}
    cache_collection = {}
    cache_page = {}
    cache_variant = {}

    def resolve_one(entity_type: str, ref: str):
        if entity_type == "PRODUCT":
            if ref in cache_product:
                return cache_product[ref]
            v = resolve_product_by_handle(client, ref)
            cache_product[ref] = v
            return v

        if entity_type == "COLLECTION":
            if ref in cache_collection:
                return cache_collection[ref]
            v = resolve_collection_by_handle(client, ref)
            cache_collection[ref] = v
            return v

        if entity_type == "PAGE":
            if ref in cache_page:
                return cache_page[ref]
            v = resolve_page_by_handle(client, ref)
            cache_page[ref] = v
            return v

        if entity_type == "VARIANT":
            if ref in cache_variant:
                return cache_variant[ref]
            v = resolve_variant_by_sku(client, ref)
            cache_variant[ref] = v
            return v

        return None

    resolved_map = {}
    for row in need.itertuples(index=False):
        try:
            resolved_map[(row.entity_type, row.owner_ref)] = resolve_one(row.entity_type, row.owner_ref)
        except Exception:
            resolved_map[(row.entity_type, row.owner_ref)] = None

    df_ready["owner_id"] = df_ready.apply(
        lambda r: r["owner_id"] if r["owner_id"] else resolved_map.get((r["entity_type"], r["owner_ref"])),
        axis=1,
    )

    df_ready["_skip_reason"] = ""
    df_ready.loc[df_ready["owner_id"].isna() | (df_ready["owner_id"].astype(str).str.strip() == ""), "_skip_reason"] = "cannot_resolve_owner_id"

    mask_has_owner = df_ready["_skip_reason"].eq("")
    unique_owner_ids = df_ready.loc[mask_has_owner, "owner_id"].astype(str).drop_duplicates().tolist()

    exist_map = nodes_exist_map(client, unique_owner_ids, chunk_size=80)

    df_ready["_owner_exists"] = df_ready["owner_id"].apply(lambda x: bool(exist_map.get(x, False)) if x else False)
    df_ready.loc[mask_has_owner & (~df_ready["_owner_exists"]), "_skip_reason"] = "owner_not_found_in_shop"

    return df_ready




def get_variant_product_map(client: ShopifyClient, variant_ids: list[str], chunk_size: int = 80) -> dict[str, str]:
    out = {}
    ids = [x for x in variant_ids if isinstance(x, str) and x.strip()]
    for _, part in _chunk_list(ids, chunk_size):
        data = gql(client, Q_VARIANT_PRODUCT_MAP, {"ids": part})
        nodes = data.get("nodes") or []
        for node in nodes:
            if not node:
                continue
            vid = node.get("id")
            product = node.get("product") or {}
            pid = product.get("id")
            if vid and pid:
                out[vid] = pid
    return out


# =========================================================
# Cfg__Fields / metafield typing
# =========================================================

def load_cfg_fields_map(ws_cfg_fields) -> dict[tuple[str, str], str]:
    rows = ws_cfg_fields.get_all_records()
    d = pd.DataFrame(rows)

    if d.empty:
        return {}

    for c in ["entity_type", "field_key", "data_type", "source_type"]:
        if c not in d.columns:
            d[c] = ""

    d["entity_type"] = d["entity_type"].astype(str).str.upper().str.strip()
    d["field_key"] = d["field_key"].astype(str).str.strip()
    d["data_type"] = d["data_type"].astype(str).str.strip().str.lower()
    d["source_type"] = d["source_type"].astype(str).str.strip().str.upper()

    d = d[
        (d["source_type"].eq("METAFIELD"))
        | (d["field_key"].str.startswith("mf."))
        | (d["field_key"].str.startswith("v_mf."))
    ].copy()

    mp = {}
    for r in d.to_dict("records"):
        et = _norm_str(r.get("entity_type"))
        fk = _norm_str(r.get("field_key"))
        dt_ = _lower_strip(r.get("data_type"))
        if et and fk and dt_:
            mp[(et, fk)] = dt_

    return mp


def build_cfg_keyonly_map(cfg_type_map: dict[tuple[str, str], str]) -> dict[str, str]:
    out = {}
    for (_, fk), dt_ in cfg_type_map.items():
        if fk and dt_ and fk not in out:
            out[fk] = dt_
    return out


def resolve_cfg_data_type(
    entity_type: str,
    field_key: str,
    cfg_type_map: dict[tuple[str, str], str],
    cfg_by_keyonly: dict[str, str],
) -> str:
    et = _upper_strip(entity_type)
    fk = _norm_str(field_key)

    v = cfg_type_map.get((et, fk))
    if v:
        return v

    if fk.startswith("v_mf."):
        v2 = cfg_type_map.get((et, "mf." + fk[len("v_mf."):]))
        if v2:
            return v2

    return cfg_by_keyonly.get(fk, "")


def _ref_scalar_default(reference_default_kind: str) -> str:
    k = _lower_strip(reference_default_kind) or "mixed"
    return "metaobject_reference" if k == "metaobject" else "mixed_reference"


def _ref_list_default(reference_default_kind: str) -> str:
    k = _lower_strip(reference_default_kind) or "mixed"
    return "list.metaobject_reference" if k == "metaobject" else "list.mixed_reference"


def map_cfg_dtype_to_shopify_type(cfg_dt: str, reference_default_kind: str) -> str:
    dt_ = _lower_strip(cfg_dt)

    explicit_scalars = {
        "boolean", "json",
        "multi_line_text_field", "number_decimal", "number_integer", "rich_text_field", "single_line_text_field",
        "product_reference", "variant_reference", "collection_reference", "metaobject_reference", "mixed_reference",
    }
    if dt_ in explicit_scalars:
        return dt_

    if dt_.startswith("list."):
        inner = dt_[5:].strip()
        explicit_list_inner = {
            "boolean", "json",
            "multi_line_text_field", "number_decimal", "number_integer", "rich_text_field", "single_line_text_field",
            "product_reference", "variant_reference", "collection_reference", "metaobject_reference", "mixed_reference",
        }
        if inner in explicit_list_inner:
            return "list." + inner
        if inner in ("reference", "ref"):
            return _ref_list_default(reference_default_kind)
        if inner == "string":
            return "list.single_line_text_field"
        if inner == "text":
            return "list.multi_line_text_field"
        if inner in ("int", "integer"):
            return "list.number_integer"
        if inner in ("float", "decimal"):
            return "list.number_decimal"
        return "list.single_line_text_field"

    if dt_ in ("reference", "ref"):
        return _ref_scalar_default(reference_default_kind)

    if dt_ == "text":
        return "multi_line_text_field"
    if dt_ in ("number", "int", "integer"):
        return "number_integer"
    if dt_ in ("decimal", "float"):
        return "number_decimal"

    return "single_line_text_field"


def mf_type_for_row(
    entity_type: str,
    field_key: str,
    cfg_type_map: dict[tuple[str, str], str],
    cfg_by_keyonly: dict[str, str],
    reference_default_kind: str,
    type_override_by_field_key: Optional[dict[str, str]] = None,
) -> str:
    fk = _norm_str(field_key)

    if isinstance(type_override_by_field_key, dict):
        ov = type_override_by_field_key.get(fk)
        if ov:
            return _norm_str(ov)

    cfg_dt = resolve_cfg_data_type(entity_type, fk, cfg_type_map, cfg_by_keyonly)
    if cfg_dt:
        return map_cfg_dtype_to_shopify_type(cfg_dt, reference_default_kind)

    return "single_line_text_field"


# =========================================================
# Metafield value normalization
# =========================================================

def to_product_gid(x: str) -> str:
    s = _norm_str(x)
    if s.startswith("gid://shopify/Product/"):
        return s
    if re.fullmatch(r"\d+", s):
        return f"gid://shopify/Product/{s}"
    raise ValueError(f"Invalid Product reference value: {s}")


def to_variant_gid(x: str) -> str:
    s = _norm_str(x)
    if s.startswith("gid://shopify/ProductVariant/"):
        return s
    if re.fullmatch(r"\d+", s):
        return f"gid://shopify/ProductVariant/{s}"
    raise ValueError(f"Invalid Variant reference value: {s}")


def to_collection_gid(x: str) -> str:
    s = _norm_str(x)
    if s.startswith("gid://shopify/Collection/"):
        return s
    if re.fullmatch(r"\d+", s):
        return f"gid://shopify/Collection/{s}"
    raise ValueError(f"Invalid Collection reference value: {s}")


def normalize_reference_items_by_type(mf_type: str, items: list[str]) -> list[str]:
    t = _lower_strip(mf_type)

    if t == "list.product_reference":
        return [to_product_gid(x) for x in items]
    if t == "product_reference":
        return [to_product_gid(items[0])] if items else []

    if t == "list.variant_reference":
        return [to_variant_gid(x) for x in items]
    if t == "variant_reference":
        return [to_variant_gid(items[0])] if items else []

    if t == "list.collection_reference":
        return [to_collection_gid(x) for x in items]
    if t == "collection_reference":
        return [to_collection_gid(items[0])] if items else []

    return items


def value_for_metafield(mf_type: str, desired: str, action: str) -> str:
    mf_type = _norm_str(mf_type)
    action = _upper_strip(action)
    desired = "" if desired is None else str(desired)

    if action == "CLEAR":
        return "[]" if mf_type.startswith("list.") else ""

    s = desired.strip()

    if mf_type.startswith("list."):
        if s == "":
            return "[]"

        if _is_json_array_string(s):
            arr = json.loads(s)
            arr = normalize_reference_items_by_type(mf_type, arr)
            return json.dumps(arr, ensure_ascii=False)

        items = _split_items(s)
        items = normalize_reference_items_by_type(mf_type, items)
        return json.dumps(items, ensure_ascii=False)

    if mf_type in {"product_reference", "variant_reference", "collection_reference"}:
        if s == "":
            return ""
        items = normalize_reference_items_by_type(mf_type, [s])
        return items[0]

    return desired


# =========================================================
# Core normalization
# =========================================================

def parse_decimal_str(value: str, field_key: str) -> str:
    s = _norm_str(value)
    if s == "":
        raise ValueError(f"{field_key} requires a numeric value")
    try:
        n = float(s)
    except Exception:
        raise ValueError(f"{field_key} must be numeric, got: {s}")
    if n < 0:
        raise ValueError(f"{field_key} must be >= 0, got: {s}")
    return str(n)


def normalize_weight_unit(value: str) -> str:
    s = _upper_strip(value)
    allowed = {"GRAMS", "KILOGRAMS", "OUNCES", "POUNDS"}
    if s not in allowed:
        raise ValueError(f"core.weight_unit must be one of {sorted(allowed)}, got: {value}")
    return s


def normalize_tags_for_set_or_clear(action: str, desired_value: str) -> list[str]:
    act = _upper_strip(action)
    if act == "CLEAR":
        return []
    if act == "SET":
        if _is_json_array_string(desired_value):
            arr = json.loads(desired_value)
            return [_norm_str(x) for x in arr if _norm_str(x)]
        return [x for x in _split_items(desired_value) if x]
    raise ValueError(f"normalize_tags_for_set_or_clear unsupported action: {action}")


# =========================================================
# Planning
# =========================================================

def build_metafield_plan(
    df_ready: pd.DataFrame,
    cfg_type_map: dict[tuple[str, str], str],
    reference_default_kind: str,
    type_override_by_field_key: Optional[dict[str, str]],
) -> dict[str, Any]:
    df_meta = df_ready[(df_ready["_skip_reason"].eq("")) & (df_ready["row_type"] == "metafield")].copy()
    cfg_by_keyonly = build_cfg_keyonly_map(cfg_type_map)

    set_inputs = []
    meta_rows = []
    preview_rows = []
    invalid_rows = []
    missing_cfg_type = 0

    for r in df_meta.itertuples(index=False):
        et = getattr(r, "entity_type", "")
        fk = getattr(r, "field_key", "")
        action = getattr(r, "action", "")
        desired = getattr(r, "desired_value", "")

        cfg_dt = resolve_cfg_data_type(et, fk, cfg_type_map, cfg_by_keyonly)
        has_ov = isinstance(type_override_by_field_key, dict) and bool(type_override_by_field_key.get(_norm_str(fk)))
        if (not cfg_dt) and (not has_ov):
            missing_cfg_type += 1

        mf_type = mf_type_for_row(
            entity_type=et,
            field_key=fk,
            cfg_type_map=cfg_type_map,
            cfg_by_keyonly=cfg_by_keyonly,
            reference_default_kind=reference_default_kind,
            type_override_by_field_key=type_override_by_field_key,
        )

        try:
            value_to_write = value_for_metafield(mf_type, desired, action)
        except Exception as e:
            invalid_rows.append({
                "sheet_row": getattr(r, "sheet_row", None),
                "entity_type": et,
                "owner_id": getattr(r, "owner_id", ""),
                "field_key": fk,
                "error_reason": "invalid_value",
                "message": f"sheet_row={getattr(r, 'sheet_row', None)} | invalid_value={desired} | mf_type={mf_type} | {e}",
            })
            continue

        set_inputs.append({
            "ownerId": getattr(r, "owner_id"),
            "namespace": getattr(r, "namespace"),
            "key": getattr(r, "key"),
            "type": mf_type,
            "value": str(value_to_write),
        })

        meta_rows.append({
            "sheet_row": getattr(r, "sheet_row", None),
            "entity_type": et,
            "owner_id": getattr(r, "owner_id", ""),
            "field_key": fk,
        })

        preview_rows.append({
            "sheet_row": getattr(r, "sheet_row", None),
            "entity_type": et,
            "owner_id": getattr(r, "owner_id", ""),
            "field_key": fk,
            "action": action,
            "plan_type": "metafield",
            "write_type": mf_type,
            "value_preview": str(value_to_write)[:200],
        })

    return {
        "set_inputs": set_inputs,
        "meta_rows": meta_rows,
        "preview_rows": preview_rows,
        "invalid_rows": invalid_rows,
        "missing_cfg_type": missing_cfg_type,
    }



def build_core_plan(df_ready: pd.DataFrame, client: ShopifyClient) -> dict[str, Any]:
    df_core = df_ready[(df_ready["_skip_reason"].eq("")) & (df_ready["row_type"] == "core")].copy()

    product_updates = {}
    variant_updates = {}
    preview_rows = []
    invalid_rows = []

    def get_product_bucket(owner_id: str):
        if owner_id not in product_updates:
            product_updates[owner_id] = {
                "id": owner_id,
                "title": None,
                "productType": None,
                "descriptionHtml": None,
                "tags_mode": None,
                "tags_value": None,
                "source_rows": [],
            }
        return product_updates[owner_id]

    def get_variant_bucket(owner_id: str):
        if owner_id not in variant_updates:
            variant_updates[owner_id] = {
                "id": owner_id,
                "price": None,
                "compareAtPrice_present": False,
                "compareAtPrice": None,
                "weight_value_present": False,
                "weight_value": None,
                "weight_unit_present": False,
                "weight_unit": None,
                "source_rows": [],
                "field_keys": [],
            }
        return variant_updates[owner_id]

    for r in df_core.itertuples(index=False):
        fk = getattr(r, "field_key", "")
        action = _upper_strip(getattr(r, "action", ""))
        desired = _norm_str(getattr(r, "desired_value", ""))
        owner_id = getattr(r, "owner_id", "")
        entity_type = getattr(r, "entity_type", "")
        sheet_row = getattr(r, "sheet_row", None)

        try:
            if fk == "core.title":
                bucket = get_product_bucket(owner_id)
                bucket["title"] = "" if action == "CLEAR" else desired
                bucket["source_rows"].append(sheet_row)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "product_core",
                    "value_preview": bucket["title"],
                })

            elif fk == "core.product_type":
                bucket = get_product_bucket(owner_id)
                bucket["productType"] = "" if action == "CLEAR" else desired
                bucket["source_rows"].append(sheet_row)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "product_core",
                    "value_preview": bucket["productType"],
                })

            elif fk == "core.description_html":
                bucket = get_product_bucket(owner_id)
                bucket["descriptionHtml"] = "" if action == "CLEAR" else desired
                bucket["source_rows"].append(sheet_row)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "product_core",
                    "value_preview": bucket["descriptionHtml"][:200],
                })

            elif fk == "core.tags":
                bucket = get_product_bucket(owner_id)
                if action in {"SET", "CLEAR"}:
                    bucket["tags_mode"] = action
                    bucket["tags_value"] = normalize_tags_for_set_or_clear(action, desired)
                else:
                    items = [x for x in _split_items(desired) if x]
                    if bucket["tags_mode"] not in {"ADD", "REMOVE"}:
                        bucket["tags_mode"] = action
                        bucket["tags_value"] = []
                    if bucket["tags_mode"] != action:
                        raise ValueError("Cannot mix core.tags ADD/REMOVE/SET/CLEAR for the same owner in one run")
                    bucket["tags_value"].extend(items)
                bucket["source_rows"].append(sheet_row)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "product_core",
                    "value_preview": json.dumps(bucket["tags_value"], ensure_ascii=False)[:200],
                })

            elif fk == "core.price":
                bucket = get_variant_bucket(owner_id)
                bucket["price"] = parse_decimal_str(desired, fk)
                bucket["source_rows"].append(sheet_row)
                bucket["field_keys"].append(fk)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "variant_core",
                    "value_preview": bucket["price"],
                })

            elif fk == "core.compare_at_price":
                bucket = get_variant_bucket(owner_id)
                bucket["compareAtPrice_present"] = True
                bucket["compareAtPrice"] = None if action == "CLEAR" else parse_decimal_str(desired, fk)
                bucket["source_rows"].append(sheet_row)
                bucket["field_keys"].append(fk)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "variant_core",
                    "value_preview": "" if bucket["compareAtPrice"] is None else bucket["compareAtPrice"],
                })

            elif fk == "core.weight":
                bucket = get_variant_bucket(owner_id)
                bucket["weight_value_present"] = True
                bucket["weight_value"] = None if action == "CLEAR" else parse_decimal_str(desired, fk)
                bucket["source_rows"].append(sheet_row)
                bucket["field_keys"].append(fk)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "variant_core",
                    "value_preview": "" if bucket["weight_value"] is None else bucket["weight_value"],
                })

            elif fk == "core.weight_unit":
                bucket = get_variant_bucket(owner_id)
                bucket["weight_unit_present"] = True
                bucket["weight_unit"] = None if action == "CLEAR" else normalize_weight_unit(desired)
                bucket["source_rows"].append(sheet_row)
                bucket["field_keys"].append(fk)
                preview_rows.append({
                    "sheet_row": sheet_row,
                    "entity_type": entity_type,
                    "owner_id": owner_id,
                    "field_key": fk,
                    "action": action,
                    "plan_type": "variant_core",
                    "value_preview": "" if bucket["weight_unit"] is None else bucket["weight_unit"],
                })

        except Exception as e:
            invalid_rows.append({
                "sheet_row": sheet_row,
                "entity_type": entity_type,
                "owner_id": owner_id,
                "field_key": fk,
                "error_reason": "invalid_core_value",
                "message": f"sheet_row={sheet_row} | {fk} | {e}",
            })

    product_inputs = []
    product_meta_rows = []
    for owner_id, bucket in product_updates.items():
        input_obj = {"id": owner_id}
        if bucket["title"] is not None:
            input_obj["title"] = bucket["title"]
        if bucket["productType"] is not None:
            input_obj["productType"] = bucket["productType"]
        if bucket["descriptionHtml"] is not None:
            input_obj["descriptionHtml"] = bucket["descriptionHtml"]

        if bucket["tags_mode"] in {"SET", "CLEAR"}:
            input_obj["tags"] = bucket["tags_value"]

        product_inputs.append(input_obj)
        product_meta_rows.append({
            "entity_type": "PRODUCT",
            "owner_id": owner_id,
            "field_key": "core.product_bundle",
            "sheet_rows": bucket["source_rows"],
            "tags_mode": bucket["tags_mode"],
            "tags_value": bucket["tags_value"],
        })

    variant_inputs = []
    variant_meta_rows = []

    variant_owner_ids = list(variant_updates.keys())
    variant_product_map = get_variant_product_map(client, variant_owner_ids, chunk_size=80)

    for owner_id, bucket in variant_updates.items():
        product_id = variant_product_map.get(owner_id)
        if not product_id:
            invalid_rows.append({
                "sheet_row": bucket["source_rows"][0] if bucket["source_rows"] else None,
                "entity_type": "VARIANT",
                "owner_id": owner_id,
                "field_key": ",".join(bucket["field_keys"]) if bucket["field_keys"] else "core.variant",
                "error_reason": "cannot_resolve_variant_product_id",
                "message": f"variant_id={owner_id} | cannot resolve parent product id",
            })
            continue

        input_obj = {"id": owner_id}

        if bucket["price"] is not None:
            input_obj["price"] = bucket["price"]

        if bucket["compareAtPrice_present"]:
            input_obj["compareAtPrice"] = bucket["compareAtPrice"]

        has_any_weight = bucket["weight_value_present"] or bucket["weight_unit_present"]
        if has_any_weight:
            weight_part = {}
            if bucket["weight_value_present"] and bucket["weight_value"] is not None:
                weight_part["value"] = float(bucket["weight_value"])
            if bucket["weight_unit_present"] and bucket["weight_unit"] is not None:
                weight_part["unit"] = bucket["weight_unit"]
            if weight_part:
                input_obj["inventoryItem"] = {"measurement": {"weight": weight_part}}

        variant_inputs.append({
            "product_id": product_id,
            "variant_input": input_obj,
        })
        variant_meta_rows.append({
            "entity_type": "VARIANT",
            "owner_id": owner_id,
            "product_id": product_id,
            "field_key": ",".join(sorted(set(bucket["field_keys"]))) if bucket["field_keys"] else "core.variant",
            "sheet_rows": bucket["source_rows"],
        })

    tag_delta_rows = []
    for owner_id, bucket in product_updates.items():
        if bucket["tags_mode"] in {"ADD", "REMOVE"}:
            tag_delta_rows.append({
                "owner_id": owner_id,
                "mode": bucket["tags_mode"],
                "tags": bucket["tags_value"] or [],
                "sheet_rows": bucket["source_rows"],
            })

    return {
        "product_inputs": product_inputs,
        "product_meta_rows": product_meta_rows,
        "variant_inputs": variant_inputs,
        "variant_meta_rows": variant_meta_rows,
        "tag_delta_rows": tag_delta_rows,
        "preview_rows": preview_rows,
        "invalid_rows": invalid_rows,
    }


# =========================================================
# Apply helpers
# =========================================================

def parse_error_index(field_path):
    try:
        if isinstance(field_path, list) and len(field_path) >= 2 and str(field_path[0]) == "metafields":
            return int(field_path[1])
    except Exception:
        return None
    return None


def apply_metafield_plan(
    client: ShopifyClient,
    set_inputs: list[dict[str, Any]],
    meta_rows: list[dict[str, Any]],
    set_batch_size: int,
) -> dict[str, Any]:
    total = len(set_inputs)
    total_batches = (total + set_batch_size - 1) // set_batch_size if total else 0

    print(f"=== Applying metafieldsSet === total={total}, batches={total_batches}, batch_size={set_batch_size}")

    if total == 0:
        print(f"=== metafieldsSet done === total=0, ok=0, fail=0")
        return {"ok_count": 0, "fail_count": 0, "detail_fail_rows": []}

    ok_count = 0
    fail_count = 0
    detail_fail_rows = []

    for batch_no, (start_idx, batch) in enumerate(_chunk_list(set_inputs, set_batch_size), start=1):
        meta_batch = meta_rows[start_idx:start_idx + len(batch)]
        print(f"Batch {batch_no}/{total_batches}: {len(batch)} items ... ", end="", flush=True)

        try:
            data = gql(client, M_METAFIELDS_SET, {"metafields": batch})
            resp = data["metafieldsSet"]
            user_errors = resp.get("userErrors") or []

            if not user_errors:
                ok_count += len(batch)
                print("OK", flush=True)
                continue

            err_by_i = {}
            non_indexed_errors = []

            for e in user_errors:
                idx = parse_error_index(e.get("field"))
                if idx is None:
                    non_indexed_errors.append(e)
                else:
                    err_by_i.setdefault(idx, []).append(e)

            fail_items = 0
            for idx, errs in err_by_i.items():
                if not (0 <= idx < len(meta_batch)):
                    continue
                fail_items += 1
                r = meta_batch[idx]
                inp = batch[idx]
                detail_fail_rows.append({
                    "entity_type": r.get("entity_type", ""),
                    "owner_id": r.get("owner_id", ""),
                    "field_key": r.get("field_key", ""),
                    "error_reason": "shopify_user_error",
                    "message": (
                        f"sheet_row={r.get('sheet_row')} | "
                        f"msg={errs[0].get('message', '')} | "
                        f"field={errs[0].get('field')} | "
                        f"ns={inp.get('namespace')} key={inp.get('key')} type={inp.get('type')}"
                    ),
                })

            if fail_items == 0:
                fail_count += len(batch)
                detail_fail_rows.append({
                    "entity_type": "",
                    "owner_id": "",
                    "field_key": "",
                    "error_reason": "shopify_batch_error",
                    "message": (
                        f"batch_error start={start_idx} size={len(batch)} | "
                        f"user_errors={json.dumps(non_indexed_errors, ensure_ascii=False)[:500]}"
                    ),
                })
                print(f"FAILED (fail={len(batch)})", flush=True)
            else:
                batch_ok = len(batch) - fail_items
                ok_count += batch_ok
                fail_count += fail_items
                print(f"PARTIAL_FAIL (ok={batch_ok}, fail={fail_items})", flush=True)

        except Exception as e:
            fail_count += len(batch)
            for r in meta_batch:
                detail_fail_rows.append({
                    "entity_type": r.get("entity_type", ""),
                    "owner_id": r.get("owner_id", ""),
                    "field_key": r.get("field_key", ""),
                    "error_reason": "batch_exception",
                    "message": f"sheet_row={r.get('sheet_row')} | exception: {e}",
                })
            print(f"FAILED (fail={len(batch)})", flush=True)

    print(f"=== metafieldsSet done === total={total}, ok={ok_count}, fail={fail_count}")
    return {"ok_count": ok_count, "fail_count": fail_count, "detail_fail_rows": detail_fail_rows}
    

def fetch_product_tags(client: ShopifyClient, product_id: str) -> list[str]:
    q = """
    query($id: ID!) {
      node(id: $id) {
        ... on Product {
          id
          tags
        }
      }
    }
    """
    data = gql(client, q, {"id": product_id})
    node = data.get("node")
    if not node:
        raise RuntimeError(f"Product not found for tag delta: {product_id}")
    return node.get("tags") or []


def apply_product_core_plan(
    client: ShopifyClient,
    product_inputs: list[dict[str, Any]],
    product_meta_rows: list[dict[str, Any]],
    tag_delta_rows: list[dict[str, Any]],
    set_batch_size: int,
) -> dict[str, Any]:
    ops = []

    for inp, meta in zip(product_inputs, product_meta_rows):
        ops.append({
            "op_kind": "product_update",
            "input": inp,
            "meta": meta,
        })

    for row in tag_delta_rows:
        ops.append({
            "op_kind": "product_tags_delta",
            "row": row,
            "meta": {
                "entity_type": "PRODUCT",
                "owner_id": row["owner_id"],
                "field_key": "core.tags",
                "sheet_rows": row.get("sheet_rows", []),
            },
        })

    total = len(ops)
    total_batches = (total + set_batch_size - 1) // set_batch_size if total else 0

    print(f"=== Applying productUpdate === total={total}, batches={total_batches}, batch_size={set_batch_size}")

    if total == 0:
        print(f"=== productUpdate done === total=0, ok=0, fail=0")
        return {"ok_count": 0, "fail_count": 0, "detail_fail_rows": []}

    ok_count = 0
    fail_count = 0
    detail_fail_rows = []

    for batch_no, (start_idx, batch_ops) in enumerate(_chunk_list(ops, set_batch_size), start=1):
        batch_ok = 0
        batch_fail = 0

        for op in batch_ops:
            try:
                if op["op_kind"] == "product_update":
                    inp = op["input"]
                    meta = op["meta"]

                    data = gql(client, M_PRODUCT_UPDATE, {"input": inp})
                    errs = (data.get("productUpdate") or {}).get("userErrors") or []
                    if errs:
                        batch_fail += 1
                        detail_fail_rows.append({
                            "entity_type": meta.get("entity_type", ""),
                            "owner_id": meta.get("owner_id", ""),
                            "field_key": meta.get("field_key", ""),
                            "error_reason": "product_update_error",
                            "message": f"sheet_rows={meta.get('sheet_rows')} | msg={errs[0].get('message', '')} | field={errs[0].get('field')}",
                        })
                    else:
                        batch_ok += 1

                elif op["op_kind"] == "product_tags_delta":
                    row = op["row"]

                    current_tags = fetch_product_tags(client, row["owner_id"])
                    cur_set = {t.strip() for t in current_tags if _norm_str(t)}
                    delta = {t.strip() for t in row["tags"] if _norm_str(t)}

                    if row["mode"] == "ADD":
                        final_tags = sorted(cur_set | delta)
                    else:
                        final_tags = sorted(cur_set - delta)

                    data = gql(client, M_PRODUCT_UPDATE, {"input": {"id": row["owner_id"], "tags": final_tags}})
                    errs = (data.get("productUpdate") or {}).get("userErrors") or []
                    if errs:
                        batch_fail += 1
                        detail_fail_rows.append({
                            "entity_type": "PRODUCT",
                            "owner_id": row["owner_id"],
                            "field_key": "core.tags",
                            "error_reason": "product_tags_delta_error",
                            "message": f"sheet_rows={row.get('sheet_rows')} | msg={errs[0].get('message', '')} | field={errs[0].get('field')}",
                        })
                    else:
                        batch_ok += 1

            except Exception as e:
                batch_fail += 1
                meta = op["meta"]
                detail_fail_rows.append({
                    "entity_type": meta.get("entity_type", ""),
                    "owner_id": meta.get("owner_id", ""),
                    "field_key": meta.get("field_key", ""),
                    "error_reason": (
                        "product_tags_delta_exception"
                        if op["op_kind"] == "product_tags_delta"
                        else "product_update_exception"
                    ),
                    "message": f"sheet_rows={meta.get('sheet_rows')} | exception={e}",
                })

        ok_count += batch_ok
        fail_count += batch_fail

        print(f"Batch {batch_no}/{total_batches}: {len(batch_ops)} items ... ", end="", flush=True)
        if batch_fail == 0:
            print("OK", flush=True)
        elif batch_ok == 0:
            print(f"FAILED (fail={batch_fail})", flush=True)
        else:
            print(f"PARTIAL_FAIL (ok={batch_ok}, fail={batch_fail})", flush=True)

    print(f"=== productUpdate done === total={total}, ok={ok_count}, fail={fail_count}")
    return {"ok_count": ok_count, "fail_count": fail_count, "detail_fail_rows": detail_fail_rows}
    


def apply_variant_core_plan(
    client: ShopifyClient,
    variant_inputs: list[dict[str, Any]],
    variant_meta_rows: list[dict[str, Any]],
    set_batch_size: int,
) -> dict[str, Any]:
    grouped_inputs = defaultdict(list)
    grouped_meta = defaultdict(list)

    for inp, meta in zip(variant_inputs, variant_meta_rows):
        product_id = inp["product_id"]
        grouped_inputs[product_id].append(inp["variant_input"])
        grouped_meta[product_id].append(meta)

    total = len(variant_inputs)
    total_batches = sum((len(v) + set_batch_size - 1) // set_batch_size for v in grouped_inputs.values()) if total else 0

    print(f"=== Applying productVariantsBulkUpdate === total={total}, batches={total_batches}, batch_size={set_batch_size}")

    if total == 0:
        print("=== productVariantsBulkUpdate done === total=0, ok=0, fail=0")
        return {"ok_count": 0, "fail_count": 0, "detail_fail_rows": []}

    ok_count = 0
    fail_count = 0
    detail_fail_rows = []
    batch_no = 0

    for product_id in grouped_inputs.keys():
        inputs = grouped_inputs[product_id]
        metas = grouped_meta[product_id]

        for start_idx, batch_inputs in _chunk_list(inputs, set_batch_size):
            batch_no += 1
            batch_meta = metas[start_idx:start_idx + len(batch_inputs)]

            print(f"Batch {batch_no}/{total_batches}: {len(batch_inputs)} items ... ", end="", flush=True)

            try:
                data = gql(
                    client,
                    M_PRODUCT_VARIANTS_BULK_UPDATE,
                    {
                        "productId": product_id,
                        "variants": batch_inputs,
                        "allowPartialUpdates": True,
                    },
                )
                resp = (data.get("productVariantsBulkUpdate") or {})
                errs = resp.get("userErrors") or []

                if not errs:
                    ok_count += len(batch_inputs)
                    print("OK", flush=True)
                    continue

                err_map = defaultdict(list)
                non_indexed = []

                for e in errs:
                    field = e.get("field") or []
                    idx = None
                    if isinstance(field, list):
                        for i, part in enumerate(field):
                            if str(part) == "variants" and i + 1 < len(field):
                                try:
                                    idx = int(field[i + 1])
                                    break
                                except Exception:
                                    pass
                    if idx is None:
                        non_indexed.append(e)
                    else:
                        err_map[idx].append(e)

                if not err_map:
                    fail_count += len(batch_inputs)
                    for meta in batch_meta:
                        detail_fail_rows.append({
                            "entity_type": meta.get("entity_type", ""),
                            "owner_id": meta.get("owner_id", ""),
                            "field_key": meta.get("field_key", ""),
                            "error_reason": "variant_bulk_update_error",
                            "message": f"sheet_rows={meta.get('sheet_rows')} | product_id={product_id} | user_errors={json.dumps(non_indexed, ensure_ascii=False)[:500]}",
                        })
                    print(f"FAILED (fail={len(batch_inputs)})", flush=True)
                    continue

                batch_fail = 0
                for idx, item_errs in err_map.items():
                    if 0 <= idx < len(batch_meta):
                        meta = batch_meta[idx]
                        batch_fail += 1
                        detail_fail_rows.append({
                            "entity_type": meta.get("entity_type", ""),
                            "owner_id": meta.get("owner_id", ""),
                            "field_key": meta.get("field_key", ""),
                            "error_reason": "variant_bulk_update_error",
                            "message": f"sheet_rows={meta.get('sheet_rows')} | product_id={product_id} | msg={item_errs[0].get('message', '')} | field={item_errs[0].get('field')}",
                        })

                batch_ok = len(batch_inputs) - batch_fail
                ok_count += batch_ok
                fail_count += batch_fail

                if batch_fail == 0:
                    print("OK", flush=True)
                elif batch_ok == 0:
                    print(f"FAILED (fail={batch_fail})", flush=True)
                else:
                    print(f"PARTIAL_FAIL (ok={batch_ok}, fail={batch_fail})", flush=True)

            except Exception as e:
                fail_count += len(batch_inputs)
                for meta in batch_meta:
                    detail_fail_rows.append({
                        "entity_type": meta.get("entity_type", ""),
                        "owner_id": meta.get("owner_id", ""),
                        "field_key": meta.get("field_key", ""),
                        "error_reason": "variant_bulk_update_exception",
                        "message": f"sheet_rows={meta.get('sheet_rows')} | product_id={product_id} | exception={e}",
                    })
                print(f"FAILED (fail={len(batch_inputs)})", flush=True)

    print(f"=== productVariantsBulkUpdate done === total={total}, ok={ok_count}, fail={fail_count}")
    return {"ok_count": ok_count, "fail_count": fail_count, "detail_fail_rows": detail_fail_rows}


# =========================================================
# Main entry
# =========================================================

def run(
    *,
    site_code: str,
    job_name: str = "edit_core",
    gsheet_sa_b64_secret: str,
    shopify_token_secret: str,
    shop_domain: str,
    api_version: str = "2026-01",
    console_core_url: str,
    input_sheet_label: str = "edit",
    worksheet_title: str = "Edit__Core",
    cfg_sheet_label: str = "config",
    cfg_tab_fields: str = CFG_FIELDS_TAB_DEFAULT,
    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",
    cfg_sites_tab: str = CFG_SITES_TAB_DEFAULT,
    run_id: Optional[str] = None,
    dry_run: bool = True,
    confirmed: bool = False,
    preview_limit: int = 50,
    mode_default: str = "STRICT",
    write_mode: str = "UPSERT",
    delete_empty: bool = False,
    only_entity_types: Optional[set[str]] = None,
    only_field_prefixes: Optional[set[str]] = None,
    reference_default_kind: str = "mixed",
    type_override_by_field_key: Optional[dict[str, str]] = None,
    set_batch_size: int = 25,
    http_timeout: int = 60,
    abort_if_fieldkey_contains: str = ".shopify.",
    detail_max_per_reason: int = 2,
) -> dict[str, Any]:
    if write_mode.upper() != "UPSERT":
        raise ValueError(f"Currently only WRITE_MODE='UPSERT' is supported, got: {write_mode}")

    run_id = run_id or _utc_run_id("edit")

    gc = build_gsheet_client(gsheet_sa_b64_secret)
    shopify = build_shopify_client(
        shopify_token_secret=shopify_token_secret,
        shop_domain=shop_domain,
        api_version=api_version,
        http_timeout=http_timeout,
    )

    _, ws_edit, edit_sheet_url = open_ws_by_label_and_title(
        gc=gc,
        console_core_url=console_core_url,
        site_code=site_code,
        label=input_sheet_label,
        worksheet_title=worksheet_title,
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

    df = load_edit_core(ws_edit)
    scope_prefixes = only_field_prefixes or {"mf.", "v_mf.", "core."}

    df_work = filter_pending_rows(
        df=df,
        mode_default=mode_default,
        only_entity_types=only_entity_types,
        only_field_prefixes=scope_prefixes,
    )

    rows_loaded = int(len(df))
    rows_pending = int(len(df_work))

    if df_work.empty:
        logger.log_row(
            phase="preview",
            log_type="summary",
            status="SUCCESS",
            rows_loaded=rows_loaded,
            rows_pending=0,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=0,
            message="No pending rows in scope",
            error_reason="",
        )
        logger.flush()
        return {
            "status": "no_pending_rows",
            "summary": {"rows_loaded": rows_loaded, "rows_pending": 0, "rows_recognized": 0, "rows_planned": 0, "rows_skipped": 0},
            "preview": [],
            "warnings": [],
            "meta": {"site_code": site_code, "job_name": job_name, "run_id": run_id, "edit_sheet_url": edit_sheet_url, "cfg_sheet_url": cfg_sheet_url, "runlog_sheet_url": runlog_sheet_url},
        }

    df_parsed, df_bad = recognize_rows(df_work, mode_default=mode_default)
    rows_recognized = int(len(df_parsed))

    if df_parsed.empty:
        rows_skipped = int(len(df_bad))
        logger.log_row(
            phase="preview",
            log_type="summary",
            status="ERROR",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=rows_skipped,
            message=f"No recognized rows. bad_rows={len(df_bad)}",
            error_reason="no_recognized_rows",
        )
        bad_detail_rows = [{
            "entity_type": _norm_str(r.get("entity_type")),
            "owner_id": "",
            "field_key": _norm_str(r.get("field_key")),
            "error_reason": _norm_str(r.get("reason")),
            "message": f"sheet_row={r.get('sheet_row')} | gid_or_handle={r.get('gid_or_handle')} | action={r.get('action')} | reason={r.get('reason')}",
        } for r in df_bad.to_dict("records")]
        log_grouped_details(
            logger,
            phase="preview",
            status="SKIP",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=0,
            rows_planned=0,
            rows_written=0,
            rows_skipped=rows_skipped,
            detail_rows=bad_detail_rows,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()
        return {
            "status": "no_recognized_rows",
            "summary": {"rows_loaded": rows_loaded, "rows_pending": rows_pending, "rows_recognized": 0, "rows_planned": 0, "rows_skipped": rows_skipped},
            "preview": [],
            "warnings": [{"type": "unrecognized_rows", "count": int(len(df_bad)), "examples": df_bad.head(preview_limit).to_dict("records")}],
            "meta": {"site_code": site_code, "job_name": job_name, "run_id": run_id},
        }

    df_ready = resolve_owner_ids(shopify, df_parsed)
    cfg_type_map = load_cfg_fields_map(ws_cfg_fields)

    meta_plan = build_metafield_plan(
        df_ready=df_ready,
        cfg_type_map=cfg_type_map,
        reference_default_kind=reference_default_kind,
        type_override_by_field_key=type_override_by_field_key,
    )

    core_plan = build_core_plan(df_ready, shopify)

    rows_planned = len(meta_plan["set_inputs"]) + len(core_plan["product_inputs"]) + len(core_plan["tag_delta_rows"]) + len(core_plan["variant_inputs"])

    df_unresolvable = df_ready[df_ready["_skip_reason"] != ""].copy()
    bad_detail_rows = [{
        "entity_type": _norm_str(r.get("entity_type")),
        "owner_id": "",
        "field_key": _norm_str(r.get("field_key")),
        "error_reason": _norm_str(r.get("reason")),
        "message": f"sheet_row={r.get('sheet_row')} | gid_or_handle={r.get('gid_or_handle')} | action={r.get('action')} | reason={r.get('reason')}",
    } for r in df_bad.to_dict("records")]

    unresolvable_detail_rows = [{
        "entity_type": _norm_str(r.get("entity_type")),
        "owner_id": _norm_str(r.get("owner_id")),
        "field_key": _norm_str(r.get("field_key")),
        "error_reason": _norm_str(r.get("_skip_reason")),
        "message": f"sheet_row={r.get('sheet_row')} | owner_ref={r.get('owner_ref')} | reason={r.get('_skip_reason')}",
    } for r in df_unresolvable.to_dict("records")]

    invalid_detail_rows = meta_plan["invalid_rows"] + core_plan["invalid_rows"]
    rows_skipped = len(df_bad) + len(df_unresolvable) + len(invalid_detail_rows)

    preview = (meta_plan["preview_rows"] + core_plan["preview_rows"])[:preview_limit]

    warnings = []
    if not df_bad.empty:
        warnings.append({"type": "unrecognized_rows", "count": int(len(df_bad)), "examples": df_bad.head(preview_limit).to_dict("records")})
    if not df_unresolvable.empty:
        warnings.append({
            "type": "unresolvable_rows",
            "count": int(len(df_unresolvable)),
            "examples": df_unresolvable.head(preview_limit)[["sheet_row", "entity_type", "owner_ref", "field_key", "_skip_reason"]].to_dict("records"),
        })
    if invalid_detail_rows:
        warnings.append({"type": "invalid_rows", "count": int(len(invalid_detail_rows)), "examples": invalid_detail_rows[:preview_limit]})

    if not confirmed:
        logger.log_row(
            phase="preview",
            log_type="summary",
            status="NEEDS_CONFIRMATION",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            message=(
                f"Preview generated | rows_loaded={rows_loaded} | rows_pending={rows_pending} | "
                f"rows_recognized={rows_recognized} | rows_planned={rows_planned} | rows_skipped={rows_skipped}"
            ),
            error_reason="",
        )
        log_grouped_details(
            logger,
            phase="preview",
            status="SKIP",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            detail_rows=bad_detail_rows + unresolvable_detail_rows + invalid_detail_rows,
            max_per_reason=detail_max_per_reason,
        )
        logger.flush()
        return {
            "status": "needs_confirmation",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_skipped": rows_skipped,
            },
            "preview": preview,
            "warnings": warnings,
            "meta": {
                "site_code": site_code,
                "job_name": job_name,
                "run_id": run_id,
                "edit_sheet_url": edit_sheet_url,
                "cfg_sheet_url": cfg_sheet_url,
                "runlog_sheet_url": runlog_sheet_url,
            },
        }

    if dry_run:
        logger.log_row(
            phase="apply",
            log_type="summary",
            status="SUCCESS",
            rows_loaded=rows_loaded,
            rows_pending=rows_pending,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=0,
            rows_skipped=rows_skipped,
            message="Confirmed but DRY_RUN=True. No Shopify write executed.",
            error_reason="",
        )
        logger.flush()
        return {
            "status": "dry_run_confirmed_no_apply",
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_written": 0,
                "rows_skipped": rows_skipped,
            },
            "preview": preview,
            "warnings": warnings,
            "meta": {"site_code": site_code, "job_name": job_name, "run_id": run_id},
        }

    print("\n====================================")
    print("APPLY START")
    print("====================================")

    metafield_apply = apply_metafield_plan(
        client=shopify,
        set_inputs=meta_plan["set_inputs"],
        meta_rows=meta_plan["meta_rows"],
        set_batch_size=set_batch_size,
    )

    product_apply = apply_product_core_plan(
        client=shopify,
        product_inputs=core_plan["product_inputs"],
        product_meta_rows=core_plan["product_meta_rows"],
        tag_delta_rows=core_plan["tag_delta_rows"],
        set_batch_size=set_batch_size,
    )

    variant_apply = apply_variant_core_plan(
        client=shopify,
        variant_inputs=core_plan["variant_inputs"],
        variant_meta_rows=core_plan["variant_meta_rows"],
        set_batch_size=set_batch_size,
    )

    print("====================================")
    print("APPLY END")
    print("====================================")
    
    rows_written = metafield_apply["ok_count"] + product_apply["ok_count"] + variant_apply["ok_count"]
    apply_fail_count = metafield_apply["fail_count"] + product_apply["fail_count"] + variant_apply["fail_count"]

    final_status = "SUCCESS"
    if apply_fail_count > 0 and rows_written > 0:
        final_status = "PARTIAL_SUCCESS"
    elif apply_fail_count > 0 and rows_written == 0:
        final_status = "ERROR"

    logger.log_row(
        phase="apply",
        log_type="summary",
        status=final_status,
        rows_loaded=rows_loaded,
        rows_pending=rows_pending,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message=(
            f"Apply completed | rows_planned={rows_planned} | rows_written={rows_written} | "
            f"rows_skipped={rows_skipped} | apply_fail_count={apply_fail_count}"
        ),
        error_reason="",
    )
    log_grouped_details(
        logger,
        phase="apply",
        status="FAIL",
        rows_loaded=rows_loaded,
        rows_pending=rows_pending,
        rows_recognized=rows_recognized,
        rows_planned=rows_planned,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        detail_rows=metafield_apply["detail_fail_rows"] + product_apply["detail_fail_rows"] + variant_apply["detail_fail_rows"],
        max_per_reason=detail_max_per_reason,
    )
    logger.flush()

    return {
        "status": "applied",
        "summary": {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_pending,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_written": rows_written,
            "rows_skipped": rows_skipped,
            "apply_fail_count": apply_fail_count,
        },
        "preview": preview,
        "warnings": warnings,
        "meta": {
            "site_code": site_code,
            "job_name": job_name,
            "run_id": run_id,
            "runlog_sheet_url": runlog_sheet_url,
            "runlog_tab_name": runlog_tab_name,
            "final_status": final_status,
        },
    }
