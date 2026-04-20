# shopify_export/export_collections.py

from __future__ import annotations

import base64
import csv
import datetime as dt
import io
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1


JOB_NAME = "export_collections"
RUNLOG_TAB_NAME = "Ops__RunLog"

EXPORT_LABEL_DEFAULT = "export_other"
EXPORT_TAB_DEFAULT = "Collections"
CFG_SITES_TAB = "Cfg__Sites"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MF_LIST: List[Tuple[str, str]] = [
    ("custom", "category_name_1"),
    ("custom", "category_name_2"),
    ("custom", "category_name_3"),
    ("custom", "category_name_4"),
    ("custom", "category_id"),
    ("custom", "collection_level"),
    ("custom", "parent_category"),
    ("custom", "top_category"),
    ("custom", "breadcrumb_leaf"),
]

OUT_HEADER = [
    "Category ID",
    "collection_id",
    "Category Name 1",
    "Category Name 2",
    "Category Name 3",
    "Category Name 4",
    "collection_name",
    "handle",
    "StoreFront URL",
    "Admin URL",
    "Theme template name（templateSuffix）",
    "image url",
    "Collection level",
    "Parent Category",
    "Top Category",
    "Breadcrumb Leaf",
    "SMART/MANUAL",
    "all/any（对应 AND/OR）",
    "规则1（column relation condition）",
    "规则2",
    "规则3",
    "规则4",
    "规则5",
    "是否发布（Online Store）",
    "最后两个更新时间（updatedAt）",
]

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


@dataclass
class ExportConfig:
    site_code: str
    shop_domain: str
    api_version: str
    storefront_base_url: str
    console_core_url: str
    gsheet_sa_b64: str
    shopify_token: str

    export_label: str = EXPORT_LABEL_DEFAULT
    export_tab_name: str = EXPORT_TAB_DEFAULT
    runlog_label: str = "runlog_sheet"

    write_mode: str = "REPLACE"  # REPLACE / APPEND
    write_header_and_desc: bool = False

    status_enabled: bool = True
    online_store_publication_id: str = ""
    auto_find_online_store_publication_id: bool = True

    filter_enabled: bool = False
    filter_namespace: str = "custom"
    filter_key: str = "top_category"
    filter_value: str = "PEX"
    filter_match_mode: str = "EQUALS"  # EQUALS / CONTAINS

    page_size: int = 250
    write_chunk_rows: int = 2000
    retry: int = 6
    sleep_every_n_calls: int = 20
    sleep_seconds: float = 1.0
    request_timeout: int = 60


class ShopifyClient:
    def __init__(self, shop_domain: str, api_version: str, access_token: str, retry: int = 6, timeout: int = 60):
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": access_token.strip(),
            "Content-Type": "application/json",
        }
        self.retry = retry
        self.timeout = timeout
        self.call_count = 0

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        last_err = None

        for i in range(self.retry):
            try:
                r = requests.post(self.url, headers=self.headers, json=payload, timeout=self.timeout)
                if r.status_code == 429:
                    wait_s = round(1.2 * (i + 1), 2)
                    print(f"⚠️ Shopify 429，等待 {wait_s}s 后重试（{i + 1}/{self.retry}）")
                    time.sleep(1.2 * (i + 1))
                    continue
                r.raise_for_status()
                data = r.json()
                if data.get("errors"):
                    raise RuntimeError(str(data["errors"]))
                self.call_count += 1
                return data["data"]
            except Exception as e:
                last_err = e
                wait_s = round(1.0 * (i + 1), 2)
                print(f"⚠️ GraphQL 请求失败，{wait_s}s 后重试（{i + 1}/{self.retry}）：{e}")
                time.sleep(1.0 * (i + 1))

        raise RuntimeError(f"GraphQL failed after retries. Last error: {last_err}")


def now_cn() -> str:
    return (dt.datetime.utcnow() + dt.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")


def now_run_id() -> str:
    return "export_collections_" + (dt.datetime.utcnow() + dt.timedelta(hours=8)).strftime("%Y%m%d_%H%M%S")


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def strip_url(x: str) -> str:
    return (x or "").strip()


def build_admin_url(shop_domain: str, collection_id: str) -> str:
    handle = shop_domain.split(".")[0]
    return f"https://admin.shopify.com/store/{handle}/collections/{collection_id}"


def build_storefront_url(base: str, handle: str) -> str:
    return f"{base.strip().rstrip('/')}/collections/{handle}"


def rule_logic_from_applied_disjunctively(v: Optional[bool]) -> str:
    if v is None:
        return ""
    return "any" if bool(v) else "all"


def friendly_rule(col: Any, rel: Any, cond: Any) -> str:
    if col is None or rel is None:
        return ""
    return f"{str(col).lower()} {str(rel).lower()} {(cond or '').strip()}".strip()


def build_gspread_client(gsheet_sa_b64: str):
    sa_json = json.loads(base64.b64decode(gsheet_sa_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
    return gspread.authorize(creds)


def gs_url_to_csv_export(url: str, sheet_name: str) -> str:
    m = requests.utils.urlparse(url)
    path = m.path
    if "/edit" in path:
        base = f"{m.scheme}://{m.netloc}{path.split('/edit')[0]}"
    else:
        base = f"{m.scheme}://{m.netloc}{path}"
    return f"{base}/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}"


def read_sheet_csv_by_title(sheet_url: str, worksheet_title: str, timeout: int = 60) -> List[Dict[str, str]]:
    export_url = gs_url_to_csv_export(sheet_url, worksheet_title)
    r = requests.get(export_url, timeout=timeout)
    r.raise_for_status()
    text = r.text
    f = io.StringIO(text)
    rows = list(csv.DictReader(f))
    return [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in rows]


def find_site_label_url(console_core_url: str, site_code: str, label: str) -> str:
    rows = read_sheet_csv_by_title(console_core_url, CFG_SITES_TAB)
    site_code = (site_code or "").strip().upper()
    label = (label or "").strip()

    for row in rows:
        if (row.get("site_code", "") or "").strip().upper() == site_code and (row.get("label", "") or "").strip() == label:
            return strip_url(row.get("sheet_url", ""))
    raise RuntimeError(f"Cfg__Sites 未找到 sheet_url: site_code={site_code}, label={label}")


def ensure_tab(ws_book, tab_name: str):
    try:
        return ws_book.worksheet(tab_name)
    except Exception:
        print(f"⚠️ 输出 tab 不存在，自动创建：{tab_name}")
        return ws_book.add_worksheet(title=tab_name, rows=1000, cols=30)


def write_table(ws, rows: List[List[Any]], write_mode: str = "REPLACE"):
    write_mode = (write_mode or "REPLACE").upper().strip()
    if write_mode == "REPLACE":
        print(f"🧹 清空目标表：{ws.title}")
        ws.clear()
        if rows:
            print(f"✍️ 一次性写入 {len(rows)} 行")
            ws.update("A1", rows, value_input_option="RAW")
    elif write_mode == "APPEND":
        if rows:
            print(f"➕ 追加写入 {len(rows)} 行")
            ws.append_rows(rows, value_input_option="RAW")
    else:
        raise RuntimeError(f"不支持 WRITE_MODE: {write_mode}")


def append_rows_chunked(ws, rows: List[List[Any]], chunk_size: int = 2000):
    if not rows:
        print("ℹ️ 没有可追加的数据")
        return
    total = len(rows)
    batch_total = (total + chunk_size - 1) // chunk_size
    print(f"✍️ 开始分块追加：total_rows={total} | batches={batch_total} | chunk_size={chunk_size}")
    for i in range(0, total, chunk_size):
        chunk = rows[i:i + chunk_size]
        batch_no = i // chunk_size + 1
        print(f"   - append batch {batch_no}/{batch_total} | rows={len(chunk)}")
        ws.append_rows(chunk, value_input_option="RAW")


def build_collections_query(include_status: bool, include_filter_mf: bool) -> str:
    mf_blocks = []
    for ns, key in MF_LIST:
        alias = f"mf_{ns}_{key}".replace("-", "_")
        mf_blocks.append(
            f'{alias}: metafield(namespace: "{ns}", key: "{key}") {{ type value }}'
        )

    filter_block = ""
    if include_filter_mf:
        filter_block = """
        __filter_mf: metafield(namespace: $mfNs, key: $mfKey) {
          type
          value
          reference { id }
        }
        """.strip()

    node_fields = [
        "id",
        "legacyResourceId",
        "handle",
        "title",
        "updatedAt",
        "templateSuffix",
        "image { url }",
        """
        ruleSet {
          appliedDisjunctively
          rules { column relation condition }
        }
        """.strip(),
        "\n".join(mf_blocks),
        filter_block,
    ]

    if include_status:
        node_fields.append("publishedOnPublication(publicationId: $pubId)")

    node_block = "\n".join([x for x in node_fields if x])

    var_defs = ["$first: Int!", "$after: String"]
    if include_status:
        var_defs.append("$pubId: ID!")
    if include_filter_mf:
        var_defs.append("$mfNs: String!")
        var_defs.append("$mfKey: String!")

    q = (
        "query(" + ", ".join(var_defs) + "){\n"
        "  collections(first: $first, after: $after){\n"
        "    edges{\n"
        "      node{\n"
        f"{node_block}\n"
        "      }\n"
        "    }\n"
        "    pageInfo { hasNextPage endCursor }\n"
        "  }\n"
        "}\n"
    )
    return q


def match_metafield(mf: Optional[Dict[str, Any]], expected: str, mode: str) -> bool:
    if not mf:
        return False

    expected = (expected or "").strip()
    mode = (mode or "EQUALS").upper().strip()

    value = (mf.get("value") or "").strip()
    ref = mf.get("reference") or {}
    ref_id = (ref.get("id") or "").strip()
    candidates = [x for x in [value, ref_id] if x]

    if mode == "CONTAINS":
        return any(expected.lower() in x.lower() for x in candidates)

    return any(x == expected for x in candidates)


def get_mf_value(node: Dict[str, Any], ns: str, key: str) -> str:
    alias = f"mf_{ns}_{key}".replace("-", "_")
    mf = node.get(alias) or {}
    return safe_str(mf.get("value"))


def resolve_online_store_publication_id(client: ShopifyClient, cfg: ExportConfig) -> Tuple[str, List[Tuple[str, str]]]:
    if not cfg.status_enabled:
        return "", []

    q = """
    query($first:Int!){
      publications(first:$first){
        edges{
          node{
            id
            name
          }
        }
      }
    }
    """
    data = client.gql(q, {"first": 100})
    pubs = [(e["node"].get("id", ""), e["node"].get("name", "")) for e in data["publications"]["edges"]]
    pub_ids = {pid for pid, _ in pubs}

    picked = ""
    for pid, name in pubs:
        if (name or "").strip().lower() == "online store":
            picked = pid
            break

    pub_id = (cfg.online_store_publication_id or "").strip()
    if (not pub_id) and cfg.auto_find_online_store_publication_id:
        pub_id = picked

    if pub_id and pub_id not in pub_ids:
        pub_id = ""

    return pub_id, pubs


def export_rows(client: ShopifyClient, cfg: ExportConfig, pub_id: str) -> Dict[str, Any]:
    include_status = bool(cfg.status_enabled and pub_id)
    include_filter_mf = bool(cfg.filter_enabled)

    q = build_collections_query(include_status=include_status, include_filter_mf=include_filter_mf)

    all_rows: List[List[Any]] = []
    preview_rows: List[List[Any]] = []
    loaded = 0
    kept = 0
    skipped = 0
    page_count = 0
    after = None

    print("🚀 开始拉取 Collections")
    print(
        f"   - page_size={cfg.page_size}"
        f" | status_enabled={include_status}"
        f" | filter_enabled={include_filter_mf}"
    )
    if include_filter_mf:
        print(
            f"   - filter: {cfg.filter_namespace}.{cfg.filter_key} "
            f"{cfg.filter_match_mode} {cfg.filter_value}"
        )

    while True:
        variables: Dict[str, Any] = {
            "first": int(cfg.page_size),
            "after": after,
        }
        if include_status:
            variables["pubId"] = pub_id
        if include_filter_mf:
            variables["mfNs"] = cfg.filter_namespace
            variables["mfKey"] = cfg.filter_key

        data = client.gql(q, variables)
        page = data["collections"]
        edges = page["edges"]
        page_count += 1

        page_loaded = 0
        page_kept = 0
        page_skipped = 0

        for edge in edges:
            node = edge["node"]
            loaded += 1
            page_loaded += 1

            if include_filter_mf:
                filter_mf = node.get("__filter_mf")
                ok = match_metafield(filter_mf, cfg.filter_value, cfg.filter_match_mode)
                if not ok:
                    skipped += 1
                    page_skipped += 1
                    continue

            kept += 1
            page_kept += 1

            legacy_id = safe_str(node.get("legacyResourceId"))
            handle = safe_str(node.get("handle"))
            title = safe_str(node.get("title"))
            updated_at = safe_str(node.get("updatedAt"))
            template_suffix = safe_str(node.get("templateSuffix"))
            image_url = safe_str((node.get("image") or {}).get("url"))
            rule_set = node.get("ruleSet") or {}
            rules = rule_set.get("rules") or []

            row = [
                get_mf_value(node, "custom", "category_id"),
                legacy_id,
                get_mf_value(node, "custom", "category_name_1"),
                get_mf_value(node, "custom", "category_name_2"),
                get_mf_value(node, "custom", "category_name_3"),
                get_mf_value(node, "custom", "category_name_4"),
                title,
                handle,
                build_storefront_url(cfg.storefront_base_url, handle),
                build_admin_url(cfg.shop_domain, legacy_id),
                template_suffix,
                image_url,
                get_mf_value(node, "custom", "collection_level"),
                get_mf_value(node, "custom", "parent_category"),
                get_mf_value(node, "custom", "top_category"),
                get_mf_value(node, "custom", "breadcrumb_leaf"),
                "SMART" if rules else "MANUAL",
                rule_logic_from_applied_disjunctively(rule_set.get("appliedDisjunctively")),
                friendly_rule(*(list(rules[0].values()) if len(rules) > 0 else [None, None, None])),
                friendly_rule(*(list(rules[1].values()) if len(rules) > 1 else [None, None, None])),
                friendly_rule(*(list(rules[2].values()) if len(rules) > 2 else [None, None, None])),
                friendly_rule(*(list(rules[3].values()) if len(rules) > 3 else [None, None, None])),
                friendly_rule(*(list(rules[4].values()) if len(rules) > 4 else [None, None, None])),
                safe_str(node.get("publishedOnPublication")) if include_status else "",
                updated_at,
            ]
            all_rows.append(row)
            if len(preview_rows) < 8:
                preview_rows.append(row)

        print(
            f"📦 page {page_count} done"
            f" | loaded={page_loaded}"
            f" | kept={page_kept}"
            f" | skipped={page_skipped}"
            f" | total_kept={kept}"
        )

        page_info = page["pageInfo"]
        if not page_info["hasNextPage"]:
            print("✅ Collections 拉取完成：已到最后一页")
            break
        after = page_info["endCursor"]

        if cfg.sleep_every_n_calls > 0 and page_count % cfg.sleep_every_n_calls == 0:
            print(f"😴 达到 sleep_every_n_calls={cfg.sleep_every_n_calls}，暂停 {cfg.sleep_seconds}s")
            time.sleep(cfg.sleep_seconds)

    return {
        "rows": all_rows,
        "preview_rows": preview_rows,
        "rows_loaded": loaded,
        "rows_written": kept,
        "rows_skipped": skipped,
        "page_count": page_count,
        "pub_id_used": pub_id,
    }


def write_runlog(gc, runlog_sheet_url: str, records: List[List[Any]]):
    if not runlog_sheet_url:
        return
    try:
        book = gc.open_by_url(runlog_sheet_url)
        ws = ensure_tab(book, RUNLOG_TAB_NAME)
        existing = ws.row_values(1)
        if existing != RUNLOG_HEADER:
            print("🧾 RunLog 表头不一致，重建表头")
            ws.clear()
            ws.update("A1", [RUNLOG_HEADER], value_input_option="RAW")
        if records:
            print(f"🧾 写入 RunLog：{len(records)} 行")
            ws.append_rows(records, value_input_option="RAW")
    except Exception as e:
        print(f"⚠️ RunLog 写入失败：{e}")


def make_runlog_summary(
    run_id: str,
    ts_cn: str,
    site_code: str,
    status: str,
    rows_loaded: int,
    rows_written: int,
    rows_skipped: int,
    message: str,
    error_reason: str = "",
) -> List[Any]:
    return [
        run_id,
        ts_cn,
        JOB_NAME,
        "apply",
        "summary",
        status,
        site_code,
        "COLLECTION",
        "",
        "",
        rows_loaded,
        rows_loaded,
        rows_loaded,
        rows_written,
        rows_written,
        rows_skipped,
        message,
        error_reason,
    ]


def write_export_sheet(ws, header: List[str], data_rows: List[List[Any]], write_mode: str, chunk_size: int):
    ncols = len(header)
    write_mode = (write_mode or "REPLACE").upper().strip()
    total_rows = len(data_rows)

    need_rows = max(ws.row_count, total_rows + 10)
    need_cols = max(ws.col_count, ncols)
    if need_rows != ws.row_count or need_cols != ws.col_count:
        print(f"📐 调整 sheet 尺寸：rows {ws.row_count}->{need_rows} | cols {ws.col_count}->{need_cols}")
        ws.resize(rows=need_rows, cols=need_cols)

    existing = ws.get_all_values()

    if write_mode == "REPLACE":
        print("🧹 REPLACE 模式：清空后重写")
        ws.clear()
        print(f"✍️ 写入表头：1 行 × {ncols} 列")
        ws.update(range_name="A1", values=[header], value_input_option="RAW")

        if not data_rows:
            print("ℹ️ 没有数据行，只写入表头")
            return

        batch_total = (total_rows + chunk_size - 1) // chunk_size
        r0 = 2
        print(f"✍️ 开始分块写入数据：total_rows={total_rows} | batches={batch_total} | chunk_size={chunk_size}")
        for i in range(0, total_rows, chunk_size):
            block = data_rows[i:i + chunk_size]
            batch_no = i // chunk_size + 1
            r1 = r0 + len(block) - 1
            a1 = rowcol_to_a1(r0, 1)
            b1 = rowcol_to_a1(r1, ncols)
            rng = f"{a1}:{b1}"
            print(f"   - write batch {batch_no}/{batch_total} | range={rng} | rows={len(block)}")
            ws.update(range_name=rng, values=block, value_input_option="RAW")
            r0 += len(block)
    elif write_mode == "APPEND":
        print("➕ APPEND 模式")
        if not existing:
            print("✍️ 空表，先写入表头")
            ws.update(range_name="A1", values=[header], value_input_option="RAW")
        append_rows_chunked(ws, data_rows, chunk_size=chunk_size)
    else:
        raise RuntimeError(f"不支持 WRITE_MODE: {write_mode}")


def run(
    *,
    site_code: str,
    shop_domain: str,
    api_version: str,
    storefront_base_url: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    shopify_token: str,
    export_label: str = EXPORT_LABEL_DEFAULT,
    export_tab_name: str = EXPORT_TAB_DEFAULT,
    runlog_label: str = "runlog_sheet",
    write_mode: str = "REPLACE",
    write_header_and_desc: bool = False,
    status_enabled: bool = True,
    online_store_publication_id: str = "",
    auto_find_online_store_publication_id: bool = True,
    filter_enabled: bool = False,
    filter_namespace: str = "custom",
    filter_key: str = "top_category",
    filter_value: str = "PEX",
    filter_match_mode: str = "EQUALS",
    page_size: int = 250,
    write_chunk_rows: int = 2000,
    retry: int = 6,
    sleep_every_n_calls: int = 20,
    sleep_seconds: float = 1.0,
    request_timeout: int = 60,
) -> Dict[str, Any]:

    cfg = ExportConfig(
        site_code=(site_code or "").strip().upper(),
        shop_domain=(shop_domain or "").strip(),
        api_version=(api_version or "").strip(),
        storefront_base_url=(storefront_base_url or "").strip().rstrip("/"),
        console_core_url=(console_core_url or "").strip(),
        gsheet_sa_b64=gsheet_sa_b64,
        shopify_token=shopify_token,
        export_label=(export_label or EXPORT_LABEL_DEFAULT).strip(),
        export_tab_name=(export_tab_name or EXPORT_TAB_DEFAULT).strip(),
        runlog_label=(runlog_label or "runlog_sheet").strip(),
        write_mode=(write_mode or "REPLACE").strip().upper(),
        write_header_and_desc=False,
        status_enabled=bool(status_enabled),
        online_store_publication_id=(online_store_publication_id or "").strip(),
        auto_find_online_store_publication_id=bool(auto_find_online_store_publication_id),
        filter_enabled=bool(filter_enabled),
        filter_namespace=(filter_namespace or "").strip(),
        filter_key=(filter_key or "").strip(),
        filter_value=(filter_value or "").strip(),
        filter_match_mode=(filter_match_mode or "EQUALS").strip().upper(),
        page_size=int(page_size),
        write_chunk_rows=int(write_chunk_rows),
        retry=int(retry),
        sleep_every_n_calls=int(sleep_every_n_calls),
        sleep_seconds=float(sleep_seconds),
        request_timeout=int(request_timeout),
    )

    run_id = now_run_id()
    ts_cn = now_cn()

    print(f"========== {JOB_NAME} | start ==========")
    print(f"site_code={cfg.site_code} | shop_domain={cfg.shop_domain} | tab={cfg.export_tab_name}")

    gc = build_gspread_client(cfg.gsheet_sa_b64)
    print("✅ Google Sheets client ready")

    client = ShopifyClient(
        shop_domain=cfg.shop_domain,
        api_version=cfg.api_version,
        access_token=cfg.shopify_token,
        retry=cfg.retry,
        timeout=cfg.request_timeout,
    )
    print("✅ Shopify client ready")

    export_sheet_url = find_site_label_url(cfg.console_core_url, cfg.site_code, cfg.export_label)
    print(f"✅ export_sheet_url: {export_sheet_url}")

    runlog_sheet_url = find_site_label_url(cfg.console_core_url, cfg.site_code, cfg.runlog_label)
    print(f"✅ runlog_sheet_url: {runlog_sheet_url}")

    pub_id, pubs = resolve_online_store_publication_id(client, cfg)
    print(f"✅ Publications found: {len(pubs)}")
    if cfg.status_enabled:
        print("✅ Using publicationId:", pub_id or "(disabled → status blank)")
    else:
        print("✅ Status disabled")

    result = export_rows(client, cfg, pub_id=pub_id)

    print(f"📄 打开输出表：{cfg.export_tab_name}")
    book = gc.open_by_url(export_sheet_url)
    ws = ensure_tab(book, cfg.export_tab_name)

    write_export_sheet(
        ws=ws,
        header=OUT_HEADER,
        data_rows=result["rows"],
        write_mode=cfg.write_mode,
        chunk_size=cfg.write_chunk_rows,
    )

    msg = (
        f"export ok | pages={result['page_count']} | "
        f"loaded={result['rows_loaded']} | written={result['rows_written']} | skipped={result['rows_skipped']}"
    )

    runlog_records = [
        make_runlog_summary(
            run_id=run_id,
            ts_cn=ts_cn,
            site_code=cfg.site_code,
            status="SUCCESS",
            rows_loaded=result["rows_loaded"],
            rows_written=result["rows_written"],
            rows_skipped=result["rows_skipped"],
            message=msg,
            error_reason="",
        )
    ]
    write_runlog(gc, runlog_sheet_url, runlog_records)

    out = {
        "ok": True,
        "run_id": run_id,
        "job_name": JOB_NAME,
        "site_code": cfg.site_code,
        "output_sheet_url": export_sheet_url,
        "output_tab_name": cfg.export_tab_name,
        "rows_loaded": result["rows_loaded"],
        "rows_written": result["rows_written"],
        "rows_skipped": result["rows_skipped"],
        "page_count": result["page_count"],
        "publication_id_used": result["pub_id_used"],
        "preview_rows": result["preview_rows"],
        "message": msg,
    }

    print("✅", msg)
    print(f"========== {JOB_NAME} | end ==========")
    return out
