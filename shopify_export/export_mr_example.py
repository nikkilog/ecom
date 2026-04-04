# shopify_export/export_mr_example.py

import os
import re
import json
import time
import base64
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Iterable, Tuple

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# Basic utils
# =========================================================

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

SUPPORTED_OWNER_ENTITY_TYPES = {"PRODUCT", "COLLECTION", "PAGE"}
JOB_NAME = "export_mr_example"


def _now_cn_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _norm(x) -> str:
    return str(x).strip() if x is not None else ""


def _norm_lower(x) -> str:
    return _norm(x).lower()


def _is_blank(x) -> bool:
    return _norm(x) == ""


def _pick_first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_lower = {_norm_lower(c): c for c in df.columns}
    for c in candidates:
        if _norm_lower(c) in cols_lower:
            return cols_lower[_norm_lower(c)]
    return None


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]
    return df


def _safe_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _json_dumps(v) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def _tail_numeric_from_gid(gid: str) -> str:
    s = _norm(gid)
    if "/" in s:
        return s.rsplit("/", 1)[-1]
    return s


# =========================================================
# Auth / Clients
# =========================================================

def build_gspread_client_from_b64_secret(gsheet_sa_b64: str):
    info = json.loads(base64.b64decode(gsheet_sa_b64).decode("utf-8"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def shopify_graphql(
    shop_domain: str,
    api_version: str,
    token: str,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    timeout: int = 120,
) -> Dict[str, Any]:
    url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }
    resp = requests.post(
        url,
        headers=headers,
        json={"query": query, "variables": variables or {}},
        timeout=timeout,
    )

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Shopify 返回非 JSON：HTTP {resp.status_code} / {resp.text[:500]}")

    if resp.status_code != 200:
        raise RuntimeError(f"Shopify GraphQL HTTP {resp.status_code}: {_json_dumps(data)[:1200]}")

    if data.get("errors"):
        raise RuntimeError(f"Shopify GraphQL errors: {_json_dumps(data['errors'])[:1600]}")

    return data.get("data") or {}


# =========================================================
# Google Sheets helpers
# =========================================================

def open_ws_by_url_and_title(gc, spreadsheet_url: str, worksheet_title: str):
    ss = gc.open_by_url(spreadsheet_url)
    return ss.worksheet(worksheet_title)


def open_ss_by_url(gc, spreadsheet_url: str):
    return gc.open_by_url(spreadsheet_url)


def ws_records(ws) -> pd.DataFrame:
    rows = ws.get_all_records(default_blank="")
    return pd.DataFrame(rows)


def ensure_runlog_header(ws):
    values = ws.get_all_values()
    if not values:
        ws.update(
            values=[RUNLOG_HEADERS],
            range_name="A1",
            value_input_option="RAW",
        )
        return

    header = values[0]
    if header[: len(RUNLOG_HEADERS)] != RUNLOG_HEADERS:
        ws.clear()
        ws.update(
            values=[RUNLOG_HEADERS],
            range_name="A1",
            value_input_option="RAW",
        )


def append_runlog_rows(ws, rows: List[Dict[str, Any]]):
    ensure_runlog_header(ws)
    data = []
    for r in rows:
        data.append([_norm(r.get(h, "")) for h in RUNLOG_HEADERS])

    if data:
        start_row = len(ws.get_all_values()) + 1
        ws.update(
            values=data,
            range_name=f"A{start_row}",
            value_input_option="RAW",
        )


def write_df_to_ws(ws, df: pd.DataFrame, clear_first: bool = True):
    if clear_first:
        ws.clear()

    if df is None or df.empty:
        ws.update(
            values=[list(df.columns) if df is not None else []],
            range_name="A1",
            value_input_option="RAW",
        )
        return

    values = [df.columns.tolist()] + df.astype(str).fillna("").values.tolist()
    ws.update(
        values=values,
        range_name="A1",
        value_input_option="RAW",
    )


# =========================================================
# Config discovery
# =========================================================

def get_site_targets(df_sites: pd.DataFrame, site_code: str) -> Dict[str, str]:
    df = _normalize_headers(df_sites)

    col_site = _pick_first_existing_col(df, ["site_code", "site", "code"])
    col_label = _pick_first_existing_col(df, ["label"])
    col_sheet_url = _pick_first_existing_col(df, ["sheet_url"])
    col_site_url = _pick_first_existing_col(df, ["site_url"])
    col_shop_domain = _pick_first_existing_col(
        df,
        ["shop_domain", "shopify_domain", "myshopify_domain", "shop", "domain"],
    )

    if not col_site or not col_label or not col_sheet_url:
        raise RuntimeError("Cfg__Sites 缺少必要字段：site_code / label / sheet_url")

    dfx = df[df[col_site].astype(str).str.strip().str.upper() == site_code.strip().upper()].copy()
    if dfx.empty:
        raise RuntimeError(f"Cfg__Sites 找不到 site_code={site_code}")

    targets = {}
    site_url = ""
    shop_domain = ""

    for _, r in dfx.iterrows():
        label = _norm(r[col_label])
        sheet_url = _norm(r[col_sheet_url])
        if label:
            targets[label] = sheet_url

        if col_site_url and not site_url:
            site_url = _norm(r[col_site_url])

        if col_shop_domain and not shop_domain:
            shop_domain = _norm(r[col_shop_domain])

    required_labels = ["config", "export_other", "runlog_sheet"]
    miss = [x for x in required_labels if _is_blank(targets.get(x))]
    if miss:
        raise RuntimeError(f"Cfg__Sites 缺少这些 label 对应的 sheet_url：{miss}")

    if _is_blank(shop_domain):
        raise RuntimeError("Cfg__Sites 缺少 shop_domain / myshopify_domain 字段或值，无法访问 Shopify")

    if ".myshopify.com" not in shop_domain:
        shop_domain = f"{shop_domain}.myshopify.com"

    return {
        "site_url": site_url,
        "shop_domain": shop_domain,
        "config_url": targets["config"],
        "export_other_url": targets["export_other"],
        "runlog_url": targets["runlog_sheet"],
    }


def parse_metafield_key(metafield_key: str) -> Tuple[str, str]:
    s = _norm(metafield_key)
    if not s.startswith("mf."):
        raise RuntimeError(f"METAFIELD_KEY 只接受 mf. 前缀，当前为：{metafield_key}")

    s = s[3:]
    parts = s.split(".", 1)
    if len(parts) != 2:
        raise RuntimeError(f"METAFIELD_KEY 格式不对，应类似 mf.custom.breadcrumb_leaf，当前：{metafield_key}")
    return parts[0], parts[1]


def get_cfg_fields_info(
    df_fields: pd.DataFrame,
    owner_entity_type: str,
    metaobject_type: str,
    metafield_key: str,
) -> Dict[str, Any]:
    df = _normalize_headers(df_fields)

    col_entity_type = _pick_first_existing_col(df, ["entity_type"])
    col_field_key = _pick_first_existing_col(df, ["field_key"])
    col_source_type = _pick_first_existing_col(df, ["source_type"])
    col_namespace = _pick_first_existing_col(df, ["namespace"])
    col_key = _pick_first_existing_col(df, ["key"])
    col_seq = _pick_first_existing_col(df, ["seq"])
    col_display_name = _pick_first_existing_col(df, ["display_name", "name"])

    required = {
        "entity_type": col_entity_type,
        "field_key": col_field_key,
        "source_type": col_source_type,
        "namespace": col_namespace,
        "key": col_key,
    }
    miss = [k for k, v in required.items() if not v]
    if miss:
        raise RuntimeError(f"Cfg__Fields 缺少必要字段：{', '.join(miss)}")

    owner = owner_entity_type.strip().upper()

    df_mf = df[
        (df[col_entity_type].astype(str).str.strip().str.upper() == owner)
        & (df[col_field_key].astype(str).str.strip() == metafield_key)
    ].copy()

    if df_mf.empty:
        raise RuntimeError(
            f"Cfg__Fields 找不到精确匹配：entity_type={owner} + field_key={metafield_key}"
        )
    if len(df_mf) > 1:
        raise RuntimeError(
            f"Cfg__Fields 出现重复匹配：entity_type={owner} + field_key={metafield_key}，请先去重"
        )

    df_def = df[
        (df[col_entity_type].astype(str).str.strip().str.upper() == "METAOBJECT_ENTRY")
        & (df[col_source_type].astype(str).str.strip().str.upper() == "METAOBJECT_REF")
        & (df[col_namespace].astype(str).str.strip() == metaobject_type)
    ].copy()

    if df_def.empty:
        raise RuntimeError(
            f"Cfg__Fields 找不到 METAOBJECT_ENTRY + METAOBJECT_REF + namespace={metaobject_type} 的定义字段"
        )

    if col_seq:
        seq_series = pd.to_numeric(df_def[col_seq], errors="coerce").fillna(10**9)
        df_def = df_def.assign(__seq=seq_series).sort_values(["__seq", col_key], kind="stable")
    else:
        df_def = df_def.sort_values([col_key], kind="stable")

    field_order = [_norm(x) for x in df_def[col_key].tolist() if not _is_blank(x)]
    display_name_map = {}
    if col_display_name:
        for _, r in df_def.iterrows():
            display_name_map[_norm(r[col_key])] = _norm(r[col_display_name])

    namespace, key = parse_metafield_key(metafield_key)

    return {
        "namespace": namespace,
        "key": key,
        "field_order": field_order,
        "display_name_map": display_name_map,
    }


# =========================================================
# GraphQL queries
# =========================================================

PRODUCTS_QUERY = """
query ProductsWithTargetMetafield(
  $first: Int!,
  $after: String,
  $namespace: String!,
  $key: String!,
  $fieldRefListFirst: Int!,
  $metaobjectListFirst: Int!
) {
  products(first: $first, after: $after, sortKey: ID) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      legacyResourceId
      handle
      title
      metafield(namespace: $namespace, key: $key) {
        id
        type
        value
        reference {
          ... on Metaobject {
            id
            type
            handle
            displayName
            fields {
              key
              type
              value
              jsonValue
              reference {
                ... on Metaobject {
                  id
                  type
                  handle
                  displayName
                }
              }
              references(first: $fieldRefListFirst) {
                nodes {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
              }
            }
          }
        }
        references(first: $metaobjectListFirst) {
          nodes {
            ... on Metaobject {
              id
              type
              handle
              displayName
              fields {
                key
                type
                value
                jsonValue
                reference {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
                references(first: $fieldRefListFirst) {
                  nodes {
                    ... on Metaobject {
                      id
                      type
                      handle
                      displayName
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

COLLECTIONS_QUERY = """
query CollectionsWithTargetMetafield(
  $first: Int!,
  $after: String,
  $namespace: String!,
  $key: String!,
  $fieldRefListFirst: Int!,
  $metaobjectListFirst: Int!
) {
  collections(first: $first, after: $after, sortKey: ID) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      legacyResourceId
      handle
      title
      metafield(namespace: $namespace, key: $key) {
        id
        type
        value
        reference {
          ... on Metaobject {
            id
            type
            handle
            displayName
            fields {
              key
              type
              value
              jsonValue
              reference {
                ... on Metaobject {
                  id
                  type
                  handle
                  displayName
                }
              }
              references(first: $fieldRefListFirst) {
                nodes {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
              }
            }
          }
        }
        references(first: $metaobjectListFirst) {
          nodes {
            ... on Metaobject {
              id
              type
              handle
              displayName
              fields {
                key
                type
                value
                jsonValue
                reference {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
                references(first: $fieldRefListFirst) {
                  nodes {
                    ... on Metaobject {
                      id
                      type
                      handle
                      displayName
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

PAGES_QUERY = """
query PagesWithTargetMetafield(
  $first: Int!,
  $after: String,
  $namespace: String!,
  $key: String!,
  $fieldRefListFirst: Int!,
  $metaobjectListFirst: Int!
) {
  pages(first: $first, after: $after, sortKey: ID) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      metafield(namespace: $namespace, key: $key) {
        id
        type
        value
        reference {
          ... on Metaobject {
            id
            type
            handle
            displayName
            fields {
              key
              type
              value
              jsonValue
              reference {
                ... on Metaobject {
                  id
                  type
                  handle
                  displayName
                }
              }
              references(first: $fieldRefListFirst) {
                nodes {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
              }
            }
          }
        }
        references(first: $metaobjectListFirst) {
          nodes {
            ... on Metaobject {
              id
              type
              handle
              displayName
              fields {
                key
                type
                value
                jsonValue
                reference {
                  ... on Metaobject {
                    id
                    type
                    handle
                    displayName
                  }
                }
                references(first: $fieldRefListFirst) {
                  nodes {
                    ... on Metaobject {
                      id
                      type
                      handle
                      displayName
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


# =========================================================
# Entity iterators
# =========================================================

def iter_owner_nodes(
    owner_entity_type: str,
    shop_domain: str,
    api_version: str,
    token: str,
    namespace: str,
    key: str,
    page_size: int = 80,
    metaobject_list_first: int = 50,
    field_ref_list_first: int = 50,
    sleep_seconds: float = 0.0,
) -> Iterable[Dict[str, Any]]:
    owner = owner_entity_type.strip().upper()

    if owner == "ORDER":
        raise RuntimeError("OWNER_ENTITY_TYPE=ORDER 暂未实现。原因：需要额外时间范围/查询条件，不能无界全量扫。")

    if owner == "PRODUCT":
        root_key = "products"
        query = PRODUCTS_QUERY
    elif owner == "COLLECTION":
        root_key = "collections"
        query = COLLECTIONS_QUERY
    elif owner == "PAGE":
        root_key = "pages"
        query = PAGES_QUERY
    else:
        raise RuntimeError(f"不支持的 OWNER_ENTITY_TYPE：{owner}")

    after = None
    while True:
        data = shopify_graphql(
            shop_domain=shop_domain,
            api_version=api_version,
            token=token,
            query=query,
            variables={
                "first": page_size,
                "after": after,
                "namespace": namespace,
                "key": key,
                "metaobjectListFirst": metaobject_list_first,
                "fieldRefListFirst": field_ref_list_first,
            },
        )
        root = (data or {}).get(root_key) or {}
        nodes = root.get("nodes") or []
        for node in nodes:
            yield node

        page_info = root.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


# =========================================================
# Metaobject flattening
# =========================================================

def _metaobject_brief_to_text(obj: Optional[Dict[str, Any]]) -> str:
    if not obj:
        return ""
    parts = [
        _norm(obj.get("type")),
        _norm(obj.get("handle")),
        _norm(obj.get("displayName")),
    ]
    parts = [x for x in parts if x]
    return " | ".join(parts)


def _field_value_to_text(field: Dict[str, Any], list_join_sep: str = " | ") -> str:
    if not field:
        return ""

    ref = field.get("reference")
    if ref:
        return _metaobject_brief_to_text(ref)

    refs = ((field.get("references") or {}).get("nodes")) or []
    if refs:
        return list_join_sep.join([_metaobject_brief_to_text(x) for x in refs if x])

    json_value = field.get("jsonValue")
    if isinstance(json_value, (dict, list)):
        return _json_dumps(json_value)

    value = field.get("value")
    if value is not None:
        return _norm(value)

    return ""


def flatten_metaobject_node(
    metaobject_node: Dict[str, Any],
    field_order: List[str],
    list_join_sep: str = " | ",
) -> Dict[str, Any]:
    fmap = {}
    for f in (metaobject_node.get("fields") or []):
        key = _norm(f.get("key"))
        fmap[key] = _field_value_to_text(f, list_join_sep=list_join_sep)

    ordered_map = {fk: _norm(fmap.get(fk, "")) for fk in field_order}

    return {
        "metaobject_gid": _norm(metaobject_node.get("id")),
        "metaobject_type": _norm(metaobject_node.get("type")),
        "metaobject_handle": _norm(metaobject_node.get("handle")),
        "metaobject_display_name": _norm(metaobject_node.get("displayName")),
        "field_map": ordered_map,
    }


def metafield_to_examples(
    metafield_obj: Dict[str, Any],
    field_order: List[str],
    list_join_sep: str = " | ",
) -> List[Dict[str, Any]]:
    if not metafield_obj:
        return []

    out = []

    ref = metafield_obj.get("reference")
    if ref and isinstance(ref, dict):
        ex = flatten_metaobject_node(ref, field_order=field_order, list_join_sep=list_join_sep)
        ex["metafield_value_text"] = _norm(metafield_obj.get("value"))
        out.append(ex)

    refs = ((metafield_obj.get("references") or {}).get("nodes")) or []
    for node in refs:
        if node and isinstance(node, dict):
            ex = flatten_metaobject_node(node, field_order=field_order, list_join_sep=list_join_sep)
            ex["metafield_value_text"] = _norm(metafield_obj.get("value"))
            out.append(ex)

    return out


# =========================================================
# Owner row building
# =========================================================

def owner_node_to_base_row(owner_entity_type: str, node: Dict[str, Any], metafield_key: str) -> Dict[str, Any]:
    owner = owner_entity_type.strip().upper()

    if owner == "PRODUCT":
        gid = _norm(node.get("id"))
        legacy_id = _norm(node.get("legacyResourceId"))
        handle = _norm(node.get("handle"))
        title = _norm(node.get("title"))
        extra = ""
    elif owner == "COLLECTION":
        gid = _norm(node.get("id"))
        legacy_id = _norm(node.get("legacyResourceId"))
        handle = _norm(node.get("handle"))
        title = _norm(node.get("title"))
        extra = ""
    elif owner == "PAGE":
        gid = _norm(node.get("id"))
        legacy_id = _tail_numeric_from_gid(gid)
        handle = _norm(node.get("handle"))
        title = _norm(node.get("title"))
        extra = ""
    else:
        raise RuntimeError(f"不支持的 OWNER_ENTITY_TYPE：{owner}")

    return {
        "Owner Entity Type": owner,
        "Owner GID": gid,
        "Owner ID (numeric)": legacy_id,
        "Owner Handle": handle,
        "Owner Title": title,
        "Owner Extra": extra,
        "Metafield Key": metafield_key,
    }


def build_output_df(
    owner_entity_type: str,
    owner_nodes: List[Dict[str, Any]],
    metafield_key: str,
    field_order: List[str],
    list_join_sep: str = " | ",
) -> Tuple[pd.DataFrame, Dict[str, int], List[Dict[str, Any]]]:
    rows = []
    detail_errors = []

    owners_scanned = 0
    owners_hit = 0
    examples_total = 0

    for node in owner_nodes:
        owners_scanned += 1
        try:
            mf = node.get("metafield")
            if not mf:
                continue

            examples = metafield_to_examples(mf, field_order=field_order, list_join_sep=list_join_sep)
            if not examples:
                continue

            owners_hit += 1

            base = owner_node_to_base_row(
                owner_entity_type=owner_entity_type,
                node=node,
                metafield_key=metafield_key,
            )

            for ex in examples:
                row = dict(base)
                row["Metafield Value"] = _norm(ex.get("metafield_value_text"))
                row["Metaobject GID"] = _norm(ex.get("metaobject_gid"))
                row["Metaobject Type"] = _norm(ex.get("metaobject_type"))
                row["Metaobject Handle"] = _norm(ex.get("metaobject_handle"))
                row["Metaobject Display Name"] = _norm(ex.get("metaobject_display_name"))

                fmap = ex.get("field_map") or {}
                for fk in field_order:
                    row[fk] = _norm(fmap.get(fk))

                rows.append(row)
                examples_total += 1

        except Exception as e:
            detail_errors.append({
                "gid": _norm(node.get("id")),
                "field_key": metafield_key,
                "error_reason": "ROW_BUILD_ERROR",
                "message": f"{type(e).__name__}: {e}",
            })

    cols = [
        "Owner Entity Type",
        "Owner GID",
        "Owner ID (numeric)",
        "Owner Handle",
        "Owner Title",
        "Owner Extra",
        "Metafield Key",
        "Metafield Value",
        "Metaobject GID",
        "Metaobject Type",
        "Metaobject Handle",
        "Metaobject Display Name",
    ] + field_order

    df = pd.DataFrame(rows, columns=cols).fillna("")

    stats = {
        "owners_scanned": owners_scanned,
        "owners_hit": owners_hit,
        "examples_total": examples_total,
        "rows_output": len(df),
    }
    return df, stats, detail_errors


# =========================================================
# Runlog builders
# =========================================================

def build_summary_runlog_row(
    run_id: str,
    site_code: str,
    owner_entity_type: str,
    field_key: str,
    status: str,
    rows_loaded: int,
    rows_recognized: int,
    rows_planned: int,
    rows_written: int,
    rows_skipped: int,
    message: str,
    error_reason: str = "",
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "ts_cn": _now_cn_str(),
        "job_name": JOB_NAME,
        "phase": "apply",
        "log_type": "summary",
        "status": status,
        "site_code": site_code,
        "entity_type": owner_entity_type,
        "gid": "",
        "field_key": field_key,
        "rows_loaded": rows_loaded,
        "rows_pending": 0,
        "rows_recognized": rows_recognized,
        "rows_planned": rows_planned,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "message": message,
        "error_reason": error_reason,
    }


def build_detail_runlog_rows(
    run_id: str,
    site_code: str,
    owner_entity_type: str,
    detail_errors: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out = []
    grouped = {}
    for e in detail_errors:
        reason = _norm(e.get("error_reason")) or "ERROR"
        grouped.setdefault(reason, []).append(e)

    for reason, items in grouped.items():
        for e in items[:2]:
            out.append({
                "run_id": run_id,
                "ts_cn": _now_cn_str(),
                "job_name": JOB_NAME,
                "phase": "apply",
                "log_type": "detail",
                "status": "FAILED",
                "site_code": site_code,
                "entity_type": owner_entity_type,
                "gid": _norm(e.get("gid")),
                "field_key": _norm(e.get("field_key")),
                "rows_loaded": "",
                "rows_pending": "",
                "rows_recognized": "",
                "rows_planned": "",
                "rows_written": "",
                "rows_skipped": "",
                "message": _norm(e.get("message")),
                "error_reason": reason,
            })
    return out


# =========================================================
# Main run
# =========================================================

def run(
    *,
    site_code: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    shopify_token: str,
    shopify_api_version: str,
    owner_entity_type: str,
    metaobject_type: str,
    metafield_key: str,
    cfg_sites_tab: str = "Cfg__Sites",
    cfg_fields_tab: str = "Cfg__Fields",
    export_tab: str = "MR-Example",
    runlog_tab: str = "Ops__RunLog",
    owner_page_size: int = 80,
    metaobject_list_first: int = 50,
    field_ref_list_first: int = 50,
    overwrite_export_sheet: bool = True,
    list_join_sep: str = " | ",
    sleep_seconds: float = 0.0,
) -> Dict[str, Any]:
    owner = _norm(owner_entity_type).upper()
    if owner == "ORDER":
        raise RuntimeError("OWNER_ENTITY_TYPE=ORDER 暂未实现。")
    if owner not in SUPPORTED_OWNER_ENTITY_TYPES:
        raise RuntimeError(
            f"OWNER_ENTITY_TYPE 只支持 {sorted(SUPPORTED_OWNER_ENTITY_TYPES)}，当前为：{owner}"
        )

    if not _norm(metaobject_type):
        raise RuntimeError("METAOBJECT_TYPE 不能为空")
    if not _norm(metafield_key):
        raise RuntimeError("METAFIELD_KEY 不能为空")

    gc = build_gspread_client_from_b64_secret(gsheet_sa_b64)
    run_id = f"{JOB_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # 1) console core -> Cfg__Sites
    ws_sites = open_ws_by_url_and_title(gc, console_core_url, cfg_sites_tab)
    df_sites = ws_records(ws_sites)
    if df_sites.empty:
        raise RuntimeError("Cfg__Sites 为空")

    targets = get_site_targets(df_sites=df_sites, site_code=site_code)

    # 2) config -> Cfg__Fields
    ws_fields = open_ws_by_url_and_title(gc, targets["config_url"], cfg_fields_tab)
    df_fields = ws_records(ws_fields)
    if df_fields.empty:
        raise RuntimeError("Cfg__Fields 为空")

    cfg = get_cfg_fields_info(
        df_fields=df_fields,
        owner_entity_type=owner,
        metaobject_type=metaobject_type,
        metafield_key=metafield_key,
    )

    namespace = cfg["namespace"]
    key = cfg["key"]
    field_order = cfg["field_order"]

    # 3) Shopify pull
    owner_nodes = list(
        iter_owner_nodes(
            owner_entity_type=owner,
            shop_domain=targets["shop_domain"],
            api_version=shopify_api_version,
            token=shopify_token,
            namespace=namespace,
            key=key,
            page_size=owner_page_size,
            metaobject_list_first=metaobject_list_first,
            field_ref_list_first=field_ref_list_first,
            sleep_seconds=sleep_seconds,
        )
    )

    # 4) build output
    df_out, stats, detail_errors = build_output_df(
        owner_entity_type=owner,
        owner_nodes=owner_nodes,
        metafield_key=metafield_key,
        field_order=field_order,
        list_join_sep=list_join_sep,
    )

    # 5) write export sheet
    ws_export = open_ws_by_url_and_title(gc, targets["export_other_url"], export_tab)
    write_df_to_ws(ws_export, df_out, clear_first=overwrite_export_sheet)

    # 6) runlog
    summary_status = "SUCCESS"
    summary_error_reason = ""
    summary_message = (
        f"导出完成；owner_scanned={stats['owners_scanned']}, "
        f"owner_hit={stats['owners_hit']}, rows={stats['rows_output']}"
    )
    if stats["rows_output"] == 0:
        summary_message = (
            f"执行成功但无命中；owner_scanned={stats['owners_scanned']}, "
            f"owner_hit={stats['owners_hit']}, rows=0"
        )

    summary_row = build_summary_runlog_row(
        run_id=run_id,
        site_code=site_code,
        owner_entity_type=owner,
        field_key=metafield_key,
        status=summary_status,
        rows_loaded=stats["owners_scanned"],
        rows_recognized=stats["owners_hit"],
        rows_planned=stats["rows_output"],
        rows_written=stats["rows_output"],
        rows_skipped=max(stats["owners_scanned"] - stats["owners_hit"], 0),
        message=summary_message,
        error_reason=summary_error_reason,
    )
    detail_rows = build_detail_runlog_rows(
        run_id=run_id,
        site_code=site_code,
        owner_entity_type=owner,
        detail_errors=detail_errors,
    )

    ws_runlog = open_ws_by_url_and_title(gc, targets["runlog_url"], runlog_tab)
    append_runlog_rows(ws_runlog, [summary_row] + detail_rows)

    return {
        "ok": True,
        "run_id": run_id,
        "job_name": JOB_NAME,
        "site_code": site_code,
        "owner_entity_type": owner,
        "metaobject_type": metaobject_type,
        "metafield_key": metafield_key,
        "shop_domain": targets["shop_domain"],
        "targets": targets,
        "summary": {
            "owners_scanned": stats["owners_scanned"],
            "owners_hit": stats["owners_hit"],
            "rows_output": stats["rows_output"],
            "detail_error_count": len(detail_errors),
        },
        "preview": df_out.head(20).copy(),
        "warnings": [
            "OWNER_ENTITY_TYPE=ORDER 暂未实现",
        ] if owner == "ORDER" else [],
        "df_out": df_out,
    }
