import re
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests


DEFAULT_WS_CFG_SITES = "Cfg__Sites"
DEFAULT_WS_CFG_FIELDS = "Cfg__Fields"
DEFAULT_WS_EXPORT = "MR-Validate"
DEFAULT_WS_RUNLOG = "Ops__RunLog"


def _now_cn_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _norm(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _norm_lower(x: Any) -> str:
    return _norm(x).lower()


def _is_blank(x: Any) -> bool:
    return _norm(x) == ""


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {_norm_lower(c): c for c in df.columns}
    for c in candidates:
        if _norm_lower(c) in cols:
            return cols[_norm_lower(c)]
    return None


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_norm(c) for c in out.columns]
    return out


def _worksheet_df(ws) -> pd.DataFrame:
    rows = ws.get_all_records(default_blank="")
    return pd.DataFrame(rows)


def _open_ws_by_url_and_title(gc, spreadsheet_url: str, worksheet_title: str):
    return gc.open_by_url(spreadsheet_url).worksheet(worksheet_title)


def _open_ss_by_url(gc, spreadsheet_url: str):
    return gc.open_by_url(spreadsheet_url)


def _write_df_to_ws(ws, df: pd.DataFrame, clear_first: bool = True):
    data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    if clear_first:
        ws.clear()
        time.sleep(1)
        if data:
            ws.update("A1", data, value_input_option="USER_ENTERED")
        return

    start_row = len(ws.get_all_values()) + 1
    payload = data if start_row == 1 else data[1:]
    if payload:
        ws.update(f"A{start_row}", payload, value_input_option="USER_ENTERED")


def _ensure_runlog_headers(ws, headers: List[str]):
    cur = ws.row_values(1)
    cur_norm = [_norm(x) for x in cur]
    if cur_norm == headers:
        return
    if not cur:
        ws.update("A1", [headers], value_input_option="USER_ENTERED")


def _append_runlog_rows(ws, rows: List[Dict[str, Any]], headers: List[str]):
    _ensure_runlog_headers(ws, headers)
    values = [[_norm(r.get(h, "")) for h in headers] for r in rows]
    if not values:
        return
    start = len(ws.get_all_values()) + 1
    ws.update(f"A{start}", values, value_input_option="USER_ENTERED")


def _runlog_headers() -> List[str]:
    return [
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


def _site_targets(df_sites: pd.DataFrame, site_code: str) -> Dict[str, str]:
    df = _normalize_headers(df_sites)

    col_site = _pick_col(df, ["site_code", "site", "code"])
    col_label = _pick_col(df, ["label"])
    col_sheet_url = _pick_col(df, ["sheet_url"])

    if not col_site or not col_label or not col_sheet_url:
        raise RuntimeError("Cfg__Sites 缺少必要字段：site_code / label / sheet_url")

    dfx = df[df[col_site].astype(str).str.strip().str.lower() == site_code.strip().lower()].copy()
    if dfx.empty:
        raise RuntimeError(f"Cfg__Sites 找不到站点：{site_code}")

    targets = {}
    for _, r in dfx.iterrows():
        label = _norm(r[col_label])
        sheet_url = _norm(r[col_sheet_url])
        if label and sheet_url:
            targets[label] = sheet_url

    need = ["config", "export_other", "runlog_sheet"]
    miss = [x for x in need if _is_blank(targets.get(x))]
    if miss:
        raise RuntimeError(f"Cfg__Sites 缺少这些 label：{miss}")

    return {
        "config_url": targets["config"],
        "export_other_url": targets["export_other"],
        "runlog_url": targets["runlog_sheet"],
    }


def _get_shop_domain(df_sites: pd.DataFrame, site_code: str, fallback_shop_domain: str = "") -> str:
    if _norm(fallback_shop_domain):
        return _norm(fallback_shop_domain)

    df = _normalize_headers(df_sites)
    col_site = _pick_col(df, ["site_code", "site", "code"])
    col_domain = _pick_col(df, ["shop_domain", "shopify_domain", "myshopify_domain", "shop", "domain"])

    if not col_site or not col_domain:
        raise RuntimeError("Cfg__Sites 缺少 shop_domain / myshopify_domain 之类字段，且未传入 shop_domain")

    dfx = df[df[col_site].astype(str).str.strip().str.lower() == site_code.strip().lower()].copy()
    if dfx.empty:
        raise RuntimeError(f"Cfg__Sites 找不到站点：{site_code}")

    vals = [_norm(x) for x in dfx[col_domain].tolist() if _norm(x)]
    if not vals:
        raise RuntimeError(f"Cfg__Sites 中站点 {site_code} 没有 shop_domain")

    domain = vals[0]
    if ".myshopify.com" not in domain:
        domain = f"{domain}.myshopify.com"
    return domain


def _parse_metafield_key(metafield_key: str) -> Tuple[str, str]:
    s = _norm(metafield_key)
    if s.startswith("mf."):
        s = s[3:]
    if s.startswith("v_mf."):
        s = s[5:]
    parts = s.split(".", 1)
    if len(parts) != 2:
        raise RuntimeError(f"METAFIELD_KEY 格式不对：{metafield_key}")
    return parts[0], parts[1]


def _get_cfg_field_meta(df_fields: pd.DataFrame, metafield_key: str) -> Dict[str, Any]:
    df = _normalize_headers(df_fields)

    col_entity_type = _pick_col(df, ["entity_type"])
    col_field_key = _pick_col(df, ["field_key"])
    col_field_type = _pick_col(df, ["field_type"])
    col_namespace = _pick_col(df, ["namespace"])
    col_key = _pick_col(df, ["key"])

    if not col_entity_type or not col_field_key:
        raise RuntimeError("Cfg__Fields 缺少必要字段：entity_type / field_key")

    target = _norm_lower(metafield_key)
    dfx = df[df[col_field_key].astype(str).str.strip().str.lower() == target].copy()

    if dfx.empty:
        tail = target.replace("mf.", "").replace("v_mf.", "")
        dfx = df[df[col_field_key].astype(str).str.strip().str.lower().str.endswith(tail)].copy()

    if dfx.empty:
        raise RuntimeError(f"Cfg__Fields 找不到 field_key={metafield_key}")

    entity_types = []
    for x in dfx[col_entity_type].astype(str).tolist():
        s = _norm(x).upper()
        if s and s not in entity_types:
            entity_types.append(s)

    field_type = ""
    if col_field_type:
        vals = [_norm(x) for x in dfx[col_field_type].tolist() if _norm(x)]
        if vals:
            field_type = vals[0]

    namespace = ""
    key = ""
    if col_namespace:
        vals = [_norm(x) for x in dfx[col_namespace].tolist() if _norm(x)]
        if vals:
            namespace = vals[0]
    if col_key:
        vals = [_norm(x) for x in dfx[col_key].tolist() if _norm(x)]
        if vals:
            key = vals[0]

    if not namespace or not key:
        parsed = _parse_metafield_key(metafield_key)
        namespace = namespace or parsed[0]
        key = key or parsed[1]

    return {
        "entity_types": entity_types,
        "field_type": field_type,
        "namespace": namespace,
        "key": key,
    }


def _shopify_graphql(
    shop_domain: str,
    api_version: str,
    token: str,
    query: str,
    variables: Optional[dict] = None,
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
        timeout=120,
    )
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Shopify 返回非 JSON：HTTP {resp.status_code} / {resp.text[:500]}")

    if resp.status_code != 200:
        raise RuntimeError(f"Shopify GraphQL HTTP {resp.status_code}：{json.dumps(data, ensure_ascii=False)[:1000]}")

    if data.get("errors"):
        raise RuntimeError(f"Shopify GraphQL errors：{json.dumps(data['errors'], ensure_ascii=False)[:1200]}")

    return data["data"]


PRODUCTS_QUERY = """
query MRValidateProducts($first: Int!, $after: String, $namespace: String!, $key: String!) {
  products(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        title
        handle
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

VARIANTS_QUERY = """
query MRValidateVariants($first: Int!, $after: String, $namespace: String!, $key: String!) {
  productVariants(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        sku
        title
        product {
          id
          legacyResourceId
          title
          handle
        }
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

COLLECTIONS_QUERY = """
query MRValidateCollections($first: Int!, $after: String, $namespace: String!, $key: String!) {
  collections(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        title
        handle
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

PAGES_QUERY = """
query MRValidatePages($first: Int!, $after: String, $namespace: String!, $key: String!) {
  pages(first: $first, after: $after, sortKey: ID) {
    edges {
      cursor
      node {
        id
        title
        handle
        metafield(namespace: $namespace, key: $key) {
          id
          type
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

NODES_QUERY = """
query MRValidateNodes($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Metaobject {
      id
      type
      handle
      displayName
      updatedAt
      capabilities {
        publishable {
          status
        }
      }
      fields {
        key
        type
        value
      }
    }
  }
}
"""


def _iter_connection(
    shop_domain: str,
    api_version: str,
    token: str,
    query: str,
    root_key: str,
    namespace: str,
    key: str,
    page_size: int,
) -> Iterable[Dict[str, Any]]:
    after = None
    while True:
        data = _shopify_graphql(
            shop_domain=shop_domain,
            api_version=api_version,
            token=token,
            query=query,
            variables={
                "first": page_size,
                "after": after,
                "namespace": namespace,
                "key": key,
            },
        )
        blk = data[root_key]
        edges = blk["edges"] or []
        for edge in edges:
            yield edge["node"]

        if not blk["pageInfo"]["hasNextPage"]:
            break
        after = blk["pageInfo"]["endCursor"]


def _parse_reference_value(raw_value: Any) -> List[str]:
    s = _norm(raw_value)
    if not s:
        return []

    ids: List[str] = []

    try:
        j = json.loads(s)
        if isinstance(j, list):
            for x in j:
                v = _norm(x)
                if v.startswith("gid://shopify/Metaobject/"):
                    ids.append(v)
        elif isinstance(j, str) and j.startswith("gid://shopify/Metaobject/"):
            ids.append(j)
    except Exception:
        pass

    if ids:
        return ids

    found = re.findall(r"gid://shopify/Metaobject/\d+", s)
    if found:
        return found

    if s.startswith("gid://shopify/Metaobject/"):
        return [s]

    return []


def _chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _fetch_metaobject_map(
    shop_domain: str,
    api_version: str,
    token: str,
    entry_ids: List[str],
    batch_size: int,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for batch in _chunked(entry_ids, batch_size):
        data = _shopify_graphql(
            shop_domain=shop_domain,
            api_version=api_version,
            token=token,
            query=NODES_QUERY,
            variables={"ids": batch},
        )
        for node in data["nodes"] or []:
            if not node:
                continue
            out[_norm(node.get("id"))] = node
    return out


def _preview_pairs(metaobject_node: Optional[Dict[str, Any]], limit: int = 2) -> List[str]:
    if not metaobject_node:
        return ["", ""]

    vals = []
    for f in metaobject_node.get("fields") or []:
        key = _norm(f.get("key"))
        value = _norm(f.get("value"))
        if key and value:
            vals.append(f"{key}: {value}")

    vals = vals[:limit]
    while len(vals) < limit:
        vals.append("")
    return vals


def _entry_status(node: Optional[Dict[str, Any]]) -> str:
    if not node:
        return "MISSING"
    status = _norm((((node.get("capabilities") or {}).get("publishable") or {}).get("status")))
    return status or "ACTIVE"


def _validate_message(raw_value: Any, raw_ids: List[str], node: Optional[Dict[str, Any]], field_type: str, ref_mode: str) -> str:
    if _is_blank(raw_value):
        return "raw value is empty"
    if "reference" not in _norm_lower(field_type):
        return f"field_type not reference: {field_type}"
    if not raw_ids:
        return "raw value has no resolvable metaobject gid"
    if not node:
        return "target entry not found"
    if ref_mode == "single" and len(raw_ids) > 1:
        return "single reference but raw value contains multiple gids"
    return "OK"


def _base_row_columns() -> List[str]:
    return [
        "from_entity_type",
        "from_gid",
        "from_handle",
        "from_title",
        "from_variant_sku",
        "field_key",
        "field_type",
        "ref_mode",
        "raw_value_count",
        "entry_order",
        "to_entry_gid",
        "to_entry_handle",
        "to_entry_type",
        "to_entry_display",
        "entry_exists",
        "to_entry_status",
        "to_entry_preview_1",
        "to_entry_preview_2",
        "to_entry_updated_at",
        "to_entry_synced_at",
        "raw_value",
        "validate_message",
    ]


def _rows_from_owner(
    owner_entity_type: str,
    node: Dict[str, Any],
    metafield_key: str,
    field_type: str,
    synced_at: str,
) -> List[Dict[str, Any]]:
    mf = node.get("metafield") or {}
    raw_value = _norm(mf.get("value"))
    raw_ids = _parse_reference_value(raw_value)
    ref_mode = "list" if "list." in _norm_lower(field_type) else "single"

    from_gid = _norm(node.get("id"))
    from_handle = _norm(node.get("handle"))
    from_title = _norm(node.get("title"))
    from_variant_sku = ""

    if owner_entity_type == "VARIANT":
        product = node.get("product") or {}
        from_handle = _norm(product.get("handle"))
        from_title = _norm(product.get("title")) or _norm(node.get("title"))
        from_variant_sku = _norm(node.get("sku"))

    if not raw_ids:
        return [{
            "from_entity_type": owner_entity_type,
            "from_gid": from_gid,
            "from_handle": from_handle,
            "from_title": from_title,
            "from_variant_sku": from_variant_sku,
            "field_key": metafield_key,
            "field_type": field_type,
            "ref_mode": ref_mode,
            "raw_value_count": 0,
            "entry_order": "",
            "to_entry_gid": "",
            "to_entry_handle": "",
            "to_entry_type": "",
            "to_entry_display": "",
            "entry_exists": "FALSE",
            "to_entry_status": "EMPTY",
            "to_entry_preview_1": "",
            "to_entry_preview_2": "",
            "to_entry_updated_at": "",
            "to_entry_synced_at": synced_at,
            "raw_value": raw_value,
            "validate_message": _validate_message(raw_value, raw_ids, None, field_type, ref_mode),
        }]

    rows = []
    for idx, entry_gid in enumerate(raw_ids, start=1):
        rows.append({
            "from_entity_type": owner_entity_type,
            "from_gid": from_gid,
            "from_handle": from_handle,
            "from_title": from_title,
            "from_variant_sku": from_variant_sku,
            "field_key": metafield_key,
            "field_type": field_type,
            "ref_mode": ref_mode,
            "raw_value_count": len(raw_ids),
            "entry_order": idx,
            "to_entry_gid": entry_gid,
            "to_entry_handle": "",
            "to_entry_type": "",
            "to_entry_display": "",
            "entry_exists": "",
            "to_entry_status": "",
            "to_entry_preview_1": "",
            "to_entry_preview_2": "",
            "to_entry_updated_at": "",
            "to_entry_synced_at": synced_at,
            "raw_value": raw_value,
            "validate_message": "",
        })
    return rows


def _fill_target_info(rows: List[Dict[str, Any]], meta_map: Dict[str, Dict[str, Any]], field_type: str):
    for row in rows:
        gid = _norm(row.get("to_entry_gid"))
        node = meta_map.get(gid)

        previews = _preview_pairs(node, limit=2)
        row["to_entry_handle"] = _norm(node.get("handle")) if node else ""
        row["to_entry_type"] = _norm(node.get("type")) if node else ""
        row["to_entry_display"] = _norm(node.get("displayName")) if node else ""
        row["entry_exists"] = "TRUE" if node else "FALSE"
        row["to_entry_status"] = _entry_status(node)
        row["to_entry_preview_1"] = previews[0]
        row["to_entry_preview_2"] = previews[1]
        row["to_entry_updated_at"] = _norm(node.get("updatedAt")) if node else ""

        raw_ids = _parse_reference_value(row.get("raw_value"))
        row["validate_message"] = _validate_message(
            row.get("raw_value"),
            raw_ids,
            node,
            field_type,
            row.get("ref_mode"),
        )


def run(
    *,
    gc,
    shopify_token: str,
    site_code: str,
    console_core_url: str,
    shop_domain: str = "",
    api_version: str = "2026-01",
    metafield_key: str,
    owner_entity_types: Optional[List[str]] = None,
    ws_cfg_sites: str = DEFAULT_WS_CFG_SITES,
    ws_cfg_fields: str = DEFAULT_WS_CFG_FIELDS,
    ws_export: str = DEFAULT_WS_EXPORT,
    ws_runlog: str = DEFAULT_WS_RUNLOG,
    overwrite_export_sheet: bool = True,
    product_page_size: int = 100,
    variant_page_size: int = 100,
    collection_page_size: int = 100,
    page_page_size: int = 100,
    metaobject_batch_size: int = 100,
    preview_rows: int = 30,
    job_name: str = "export_mr_validate",
) -> Dict[str, Any]:
    started_at = _now_cn_str()
    synced_at = started_at
    status = "SUCCESS"
    error_reason = ""
    message = ""
    warnings: List[str] = []

    rows_loaded = 0
    rows_recognized = 0
    rows_planned = 0
    rows_written = 0
    rows_skipped = 0

    all_rows: List[Dict[str, Any]] = []
    df_out = pd.DataFrame(columns=_base_row_columns())
    effective_entity_types: List[str] = []
    targets: Dict[str, str] = {}

    try:
        ws_sites = _open_ws_by_url_and_title(gc, console_core_url, ws_cfg_sites)
        df_sites = _worksheet_df(ws_sites)
        if df_sites.empty:
            raise RuntimeError("Cfg__Sites 为空")

        targets = _site_targets(df_sites, site_code)

        ws_fields = _open_ws_by_url_and_title(gc, targets["config_url"], ws_cfg_fields)
        df_fields = _worksheet_df(ws_fields)
        if df_fields.empty:
            raise RuntimeError("Cfg__Fields 为空")

        cfg = _get_cfg_field_meta(df_fields, metafield_key=metafield_key)
        namespace = cfg["namespace"]
        key = cfg["key"]
        field_type = cfg["field_type"] or "unknown"

        effective_entity_types = [x.upper() for x in (owner_entity_types or cfg["entity_types"])]
        effective_entity_types = [x for x in effective_entity_types if x in {"PRODUCT", "VARIANT", "COLLECTION", "PAGE"}]
        if not effective_entity_types:
            raise RuntimeError(f"Cfg__Fields 无法识别 {metafield_key} 的 owner entity type")

        real_shop_domain = _get_shop_domain(df_sites, site_code=site_code, fallback_shop_domain=shop_domain)

        scans = {
            "PRODUCT": (PRODUCTS_QUERY, "products", product_page_size),
            "VARIANT": (VARIANTS_QUERY, "productVariants", variant_page_size),
            "COLLECTION": (COLLECTIONS_QUERY, "collections", collection_page_size),
            "PAGE": (PAGES_QUERY, "pages", page_page_size),
        }

        for entity_type in effective_entity_types:
            query, root_key, page_size = scans[entity_type]
            for node in _iter_connection(
                shop_domain=real_shop_domain,
                api_version=api_version,
                token=shopify_token,
                query=query,
                root_key=root_key,
                namespace=namespace,
                key=key,
                page_size=page_size,
            ):
                rows_loaded += 1
                mf = node.get("metafield")
                if not mf:
                    continue

                rows = _rows_from_owner(
                    owner_entity_type=entity_type,
                    node=node,
                    metafield_key=metafield_key,
                    field_type=field_type,
                    synced_at=synced_at,
                )
                if rows:
                    rows_recognized += 1
                    all_rows.extend(rows)

        rows_planned = len(all_rows)

        target_ids = []
        seen = set()
        for r in all_rows:
            gid = _norm(r.get("to_entry_gid"))
            if gid and gid not in seen:
                seen.add(gid)
                target_ids.append(gid)

        meta_map = {}
        if target_ids:
            meta_map = _fetch_metaobject_map(
                shop_domain=real_shop_domain,
                api_version=api_version,
                token=shopify_token,
                entry_ids=target_ids,
                batch_size=metaobject_batch_size,
            )

        _fill_target_info(all_rows, meta_map=meta_map, field_type=field_type)

        df_out = pd.DataFrame(all_rows, columns=_base_row_columns()).fillna("")

        ws_export_obj = _open_ws_by_url_and_title(gc, targets["export_other_url"], ws_export)
        _write_df_to_ws(ws_export_obj, df_out, clear_first=overwrite_export_sheet)
        rows_written = len(df_out)

        bad_count = int((df_out["validate_message"] != "OK").sum()) if not df_out.empty else 0
        if bad_count:
            warnings.append(f"发现 {bad_count} 行 validate_message != OK")

        if "reference" not in _norm_lower(field_type):
            warnings.append(f"Cfg__Fields.field_type={field_type}，不像 reference 字段，请检查")

        if not df_out.empty:
            vc = df_out["validate_message"].value_counts(dropna=False).to_dict()
            for k, v in vc.items():
                ks = _norm(k)
                if ks and ks != "OK":
                    warnings.append(f"{ks}: {v}")

        message = f"导出完成：owners_scanned={rows_loaded}, owners_hit={rows_recognized}, rows={rows_written}"

    except Exception as e:
        status = "FAILED"
        error_reason = type(e).__name__
        message = f"{type(e).__name__}: {e}"
        warnings.append(message)
        traceback.print_exc()

    try:
        if targets:
            ws_runlog_obj = _open_ws_by_url_and_title(gc, targets["runlog_url"], ws_runlog)
            run_id = f"{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            log_rows = [{
                "run_id": run_id,
                "ts_cn": _now_cn_str(),
                "job_name": job_name,
                "phase": "preview",
                "log_type": "summary",
                "status": status,
                "site_code": site_code,
                "entity_type": ",".join(effective_entity_types),
                "gid": "",
                "field_key": metafield_key,
                "rows_loaded": rows_loaded,
                "rows_pending": 0,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_written": rows_written,
                "rows_skipped": rows_skipped,
                "message": message,
                "error_reason": error_reason,
            }]

            detail_counts: Dict[str, int] = {}
            if not df_out.empty and "validate_message" in df_out.columns:
                bad = df_out[df_out["validate_message"].astype(str) != "OK"].copy()
                for _, r in bad.iterrows():
                    reason = _norm(r["validate_message"])
                    if not reason:
                        continue

                    cnt = detail_counts.get(reason, 0)
                    if cnt >= 2:
                        continue
                    detail_counts[reason] = cnt + 1

                    log_rows.append({
                        "run_id": run_id,
                        "ts_cn": _now_cn_str(),
                        "job_name": job_name,
                        "phase": "preview",
                        "log_type": "detail",
                        "status": "WARN" if status == "SUCCESS" else status,
                        "site_code": site_code,
                        "entity_type": _norm(r.get("from_entity_type")),
                        "gid": _norm(r.get("from_gid")),
                        "field_key": metafield_key,
                        "rows_loaded": rows_loaded,
                        "rows_pending": 0,
                        "rows_recognized": rows_recognized,
                        "rows_planned": rows_planned,
                        "rows_written": rows_written,
                        "rows_skipped": rows_skipped,
                        "message": f"to_entry_gid={_norm(r.get('to_entry_gid'))}",
                        "error_reason": reason,
                    })

            _append_runlog_rows(ws_runlog_obj, log_rows, _runlog_headers())

    except Exception as e:
        warnings.append(f"RunLog 写入失败：{type(e).__name__}: {e}")

    return {
        "status": status,
        "site_code": site_code,
        "job_name": job_name,
        "metafield_key": metafield_key,
        "rows_exported": rows_written,
        "summary": {
            "status": status,
            "site_code": site_code,
            "job_name": job_name,
            "rows_loaded": rows_loaded,
            "rows_recognized": rows_recognized,
            "rows_planned": rows_planned,
            "rows_written": rows_written,
            "message": message,
        },
        "preview": df_out.head(preview_rows).copy(),
        "warnings": warnings,
        "dataframe": df_out,
    }
