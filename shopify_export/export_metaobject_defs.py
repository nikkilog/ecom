# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import datetime as dt
import json
import math
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials


RUNLOG_HEADER_18 = [
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


Q_METAOBJECT_DEFS = """
query ($first: Int!, $after: String) {
  metaobjectDefinitions(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      type
      name
      fieldDefinitions {
        key
        name
        description
        required
        type { name }
      }
    }
  }
}
"""


def _now_str(tz_name: str) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def _gen_run_id(job_name: str, tz_name: str) -> str:
    ts = dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d_%H%M%S")
    return f"{job_name}_{ts}"


def _extract_sheet_id(url_or_id: str) -> str:
    s = str(url_or_id or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
    return m.group(1) if m else s


def _a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v)


def _normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _load_secret(secret_name: str, explicit_value: Optional[str] = None) -> str:
    if explicit_value:
        return explicit_value

    try:
        from google.colab import userdata  # type: ignore
    except Exception as e:
        raise RuntimeError(
            f"Cannot load secret '{secret_name}'. "
            f"Not in Colab or google.colab.userdata unavailable."
        ) from e

    value = userdata.get(secret_name)
    if not value:
        raise ValueError(f"Missing secret: {secret_name}")
    return value


def _build_gspread_client(sa_b64: str) -> gspread.Client:
    sa_info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)


class ShopifyGraphQLClient:
    def __init__(self, shop_domain: str, api_version: str, access_token: str, timeout: int = 90):
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        self.timeout = timeout

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None, retry: int = 6) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        last_err = None
        for i in range(retry):
            try:
                r = requests.post(self.url, headers=self.headers, json=payload, timeout=self.timeout)
                if r.status_code in (429, 502, 503, 504):
                    time.sleep(min(2 ** i, 20) + random.random())
                    continue
                r.raise_for_status()
                data = r.json()
                if data.get("errors"):
                    raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))
                return data["data"]
            except Exception as e:
                last_err = e
                if i < retry - 1:
                    time.sleep(min(2 ** i, 20) + random.random())
                    continue
                raise
        raise RuntimeError(f"GraphQL failed: {last_err}")


@dataclass
class SiteRouting:
    config_sheet_url: str
    runlog_sheet_url: str


def _resolve_site_routing(
    gc: gspread.Client,
    console_core_url: str,
    tab_cfg_sites: str,
    site_code: str,
    config_sheet_label: str,
    runlog_sheet_label: str,
) -> SiteRouting:
    sh = gc.open_by_url(console_core_url)
    ws = sh.worksheet(tab_cfg_sites)
    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        raise ValueError(f"{tab_cfg_sites} is empty: {console_core_url}")

    df = _normalize_df_columns(df)
    need = ["site_code", "sheet_url", "label"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"{tab_cfg_sites} missing columns: {miss}")

    df["site_code"] = df["site_code"].astype(str).str.strip().str.upper()
    df["label"] = df["label"].astype(str).str.strip()
    df["sheet_url"] = df["sheet_url"].astype(str).str.strip()

    sub = df[df["site_code"] == site_code.strip().upper()].copy()
    if sub.empty:
        raise ValueError(f"No rows found in {tab_cfg_sites} for site_code={site_code}")

    def pick(label: str) -> str:
        x = sub[sub["label"] == label]
        if x.empty:
            raise ValueError(f"{tab_cfg_sites} missing label={label} for site_code={site_code}")
        url = str(x.iloc[0]["sheet_url"]).strip()
        if not url:
            raise ValueError(f"{tab_cfg_sites} label={label} has empty sheet_url for site_code={site_code}")
        return url

    return SiteRouting(
        config_sheet_url=pick(config_sheet_label),
        runlog_sheet_url=pick(runlog_sheet_label),
    )


def _fetch_all_metaobject_defs(client: ShopifyGraphQLClient, page_size: int) -> List[Dict[str, Any]]:
    all_nodes: List[Dict[str, Any]] = []
    after = None
    while True:
        data = client.gql(Q_METAOBJECT_DEFS, {"first": page_size, "after": after})
        conn = data["metaobjectDefinitions"]
        nodes = conn.get("nodes") or []
        all_nodes.extend(nodes)

        page_info = conn["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        after = page_info["endCursor"]
    return all_nodes


def _build_defs_df(nodes: List[Dict[str, Any]], tz_name: str) -> pd.DataFrame:
    synced_at = _now_str(tz_name)
    rows: List[Dict[str, Any]] = []

    for d in nodes:
        mo_type = _safe_str(d.get("type"))
        mo_name = _safe_str(d.get("name"))
        gid = _safe_str(d.get("id"))

        for fd in (d.get("fieldDefinitions") or []):
            field_type_obj = fd.get("type") or {}
            field_type_name = ""
            if isinstance(field_type_obj, dict):
                field_type_name = _safe_str(field_type_obj.get("name"))
            else:
                field_type_name = _safe_str(field_type_obj)

            rows.append(
                {
                    "gid": gid,
                    "type": mo_type,
                    "type_name": mo_name,
                    "field_key": _safe_str(fd.get("key")),
                    "field_name": _safe_str(fd.get("name")),
                    "field_type": field_type_name,
                    "required": "TRUE" if bool(fd.get("required")) else "FALSE",
                    "description": _safe_str(fd.get("description")),
                    "updated_at": "",
                    "synced_at": synced_at,
                }
            )

    cols = [
        "gid",
        "type",
        "type_name",
        "field_key",
        "field_name",
        "field_type",
        "required",
        "description",
        "updated_at",
        "synced_at",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if not df.empty:
        df = df.sort_values(["type", "field_key"], kind="stable").reset_index(drop=True)
    return df


def _pick_col(actual_headers: List[str], aliases: List[str]) -> Optional[str]:
    lowers = {h.lower(): h for h in actual_headers}
    for a in aliases:
        if a.lower() in lowers:
            return lowers[a.lower()]
    return None


def _ensure_runlog_header(ws_log: gspread.Worksheet) -> None:
    current = ws_log.row_values(1)
    if current != RUNLOG_HEADER_18:
        ws_log.update("A1:R1", [RUNLOG_HEADER_18])


class RunLogger18:
    def __init__(
        self,
        ws_log: gspread.Worksheet,
        run_id: str,
        job_name: str,
        site_code: str,
        tz_name: str,
        flush_every: int = 100,
    ):
        self.ws_log = ws_log
        self.run_id = run_id
        self.job_name = job_name
        self.site_code = site_code
        self.tz_name = tz_name
        self.flush_every = flush_every
        self.buf: List[List[Any]] = []
        _ensure_runlog_header(ws_log)

    def log(
        self,
        phase: str,
        log_type: str,
        status: str,
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
    ) -> None:
        self.buf.append(
            [
                self.run_id,
                _now_str(self.tz_name),
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
            ]
        )
        if len(self.buf) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        tries = 6
        for i in range(tries):
            try:
                self.ws_log.append_rows(self.buf, value_input_option="RAW", table_range="A:R")
                self.buf = []
                return
            except Exception:
                if i == tries - 1:
                    raise
                time.sleep(min(2 ** i, 20) + random.random())


def _worksheet_values_matrix(ws: gspread.Worksheet) -> List[List[str]]:
    return ws.get_all_values()


def _write_matrix_preserve_shape(
    ws: gspread.Worksheet,
    matrix: List[List[Any]],
) -> None:
    matrix = [[_safe_str(v) for v in row] for row in matrix]
    old = _worksheet_values_matrix(ws)
    old_rows = len(old)
    old_cols = max((len(r) for r in old), default=0)

    new_rows = len(matrix)
    new_cols = max((len(r) for r in matrix), default=0)

    target_rows = max(old_rows, new_rows, 1)
    target_cols = max(old_cols, new_cols, 1)

    ws.resize(rows=target_rows, cols=target_cols)

    if new_rows > 0 and new_cols > 0:
        end_col = _a1_col(new_cols)
        ws.update(f"A1:{end_col}{new_rows}", matrix, value_input_option="RAW")

    if old_rows > new_rows and new_cols > 0:
        end_col = _a1_col(new_cols)
        blank_rows = [[""] * new_cols for _ in range(old_rows - new_rows)]
        ws.update(f"A{new_rows+1}:{end_col}{old_rows}", blank_rows, value_input_option="RAW")

    if old_cols > new_cols and new_rows > 0:
        start_col = _a1_col(new_cols + 1)
        end_col = _a1_col(old_cols)
        blank_extra = [[""] * (old_cols - new_cols) for _ in range(new_rows)]
        ws.update(f"{start_col}1:{end_col}{new_rows}", blank_extra, value_input_option="RAW")


def _upsert_cfg_fields(
    ws_fields: gspread.Worksheet,
    df_defs: pd.DataFrame,
    strict: bool,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    raw = ws_fields.get_all_values()
    warnings: List[str] = []

    if not raw:
        raise ValueError("Cfg__Fields is empty; header row required.")

    header = [str(x).strip() for x in raw[0]]
    rows = raw[1:]
    df_cfg = pd.DataFrame(rows, columns=header)
    df_cfg = _normalize_df_columns(df_cfg)

    col_entity = _pick_col(header, ["entity_type", "owner_type", "entity"])
    col_field_key = _pick_col(header, ["field_key", "fieldkey", "system_key"])
    col_name = _pick_col(header, ["display_name", "display name", "name", "label"])
    col_source_type = _pick_col(header, ["source_type", "field_source", "source"])
    col_namespace = _pick_col(header, ["namespace", "ns"])
    col_key = _pick_col(header, ["key", "mf_key", "metafield_key"])
    col_field_type = _pick_col(header, ["field_type", "type", "value_type", "data_type"])
    col_required = _pick_col(header, ["required", "is_required", "required?"])
    col_desc = _pick_col(header, ["description", "desc", "notes"])
    col_entity_name = _pick_col(header, ["entity_name", "owner_name"])
    col_source_ref_type = _pick_col(header, ["source_ref_type", "ref_type"])

    must_missing = []
    if not col_entity:
        must_missing.append("entity_type")
    if not col_field_key:
        must_missing.append("field_key")

    if must_missing:
        raise ValueError(f"Cfg__Fields missing MUST columns: {must_missing}")

    optional_missing = []
    check_map = {
        "display_name": col_name,
        "source_type": col_source_type,
        "namespace": col_namespace,
        "key": col_key,
        "field_type": col_field_type,
        "required": col_required,
        "description": col_desc,
    }
    for k, v in check_map.items():
        if not v:
            optional_missing.append(k)

    if optional_missing:
        msg = f"Cfg__Fields optional columns not found: {optional_missing}"
        if strict:
            raise ValueError(msg)
        warnings.append(msg)

    if df_cfg.empty:
        df_cfg = pd.DataFrame(columns=header)

    if col_entity not in df_cfg.columns:
        df_cfg[col_entity] = ""
    if col_field_key not in df_cfg.columns:
        df_cfg[col_field_key] = ""

    df_cfg[col_entity] = df_cfg[col_entity].astype(str).str.strip().str.upper()
    df_cfg[col_field_key] = df_cfg[col_field_key].astype(str).str.strip()

    upserts: List[Dict[str, Any]] = []
    for _, r in df_defs.iterrows():
        mo_type = _safe_str(r["type"])
        fd_key = _safe_str(r["field_key"])
        field_name = _safe_str(r["field_name"])
        field_type = _safe_str(r["field_type"])
        required = _safe_str(r["required"])
        description = _safe_str(r["description"])

        x: Dict[str, Any] = {
            col_entity: "METAOBJECT_ENTRY",
            col_field_key: f"mo.{mo_type}.{fd_key}",
        }
        if col_name:
            x[col_name] = field_name or fd_key
        if col_source_type:
            x[col_source_type] = "METAOBJECT_REF"
        if col_namespace:
            x[col_namespace] = mo_type
        if col_key:
            x[col_key] = fd_key
        if col_field_type:
            x[col_field_type] = field_type
        if col_required:
            x[col_required] = required
        if col_desc:
            x[col_desc] = description
        if col_entity_name:
            x[col_entity_name] = mo_type
        if col_source_ref_type:
            x[col_source_ref_type] = "METAOBJECT_DEFINITION"

        upserts.append(x)

    df_up = pd.DataFrame(upserts)
    existing_keys = (df_cfg[col_entity] + "||" + df_cfg[col_field_key]).tolist()
    key_to_idx = {k: i for i, k in enumerate(existing_keys)}

    updated = 0
    inserted = 0

    for _, row in df_up.iterrows():
        k = f"{row[col_entity]}||{row[col_field_key]}"
        if k in key_to_idx:
            idx = key_to_idx[k]
            for col, val in row.items():
                df_cfg.at[idx, col] = _safe_str(val)
            updated += 1
        else:
            new_row = {c: "" for c in df_cfg.columns}
            for col, val in row.items():
                if col in new_row:
                    new_row[col] = _safe_str(val)
            df_cfg = pd.concat([df_cfg, pd.DataFrame([new_row])], ignore_index=True)
            inserted += 1

    stats = {
        "updated": updated,
        "inserted": inserted,
        "rows_after": len(df_cfg),
    }
    return df_cfg, stats, warnings


def run(
    *,
    site_code: str,
    shop_domain: str,
    api_version: str,
    console_core_url: str,
    gsheet_sa_b64_secret: str,
    shopify_token_secret: str,
    sa_b64_value: Optional[str] = None,
    shopify_token_value: Optional[str] = None,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_cfg_metaobject_defs: str = "Cfg__MetaobjectDefs",
    tab_runlog: str = "Ops__RunLog",
    config_sheet_label: str = "config",
    runlog_sheet_label: str = "runlog_sheet",
    page_size: int = 50,
    preview_rows: int = 20,
    sync_cfg_fields: bool = True,
    strict: bool = True,
    dry_run: bool = True,
    confirmed: bool = False,
    write_mode: str = "OVERWRITE",
    tz_name: str = "Asia/Shanghai",
    run_id: Optional[str] = None,
    job_name: str = "export_metaobject_defs",
) -> Dict[str, Any]:
    phase = "preview" if dry_run else "apply"

    if not dry_run and not confirmed:
        raise ValueError("Apply mode requires confirmed=True.")

    if write_mode.upper() != "OVERWRITE":
        raise ValueError("write_mode currently only supports OVERWRITE.")

    run_id = run_id or _gen_run_id(job_name, tz_name)

    sa_b64 = _load_secret(gsheet_sa_b64_secret, explicit_value=sa_b64_value)
    shopify_token = _load_secret(shopify_token_secret, explicit_value=shopify_token_value)

    gc = _build_gspread_client(sa_b64)
    route = _resolve_site_routing(
        gc=gc,
        console_core_url=console_core_url,
        tab_cfg_sites=tab_cfg_sites,
        site_code=site_code,
        config_sheet_label=config_sheet_label,
        runlog_sheet_label=runlog_sheet_label,
    )

    sh_cfg = gc.open_by_url(route.config_sheet_url)
    sh_log = gc.open_by_url(route.runlog_sheet_url)

    ws_defs = sh_cfg.worksheet(tab_cfg_metaobject_defs)
    ws_log = sh_log.worksheet(tab_runlog)
    logger = RunLogger18(
        ws_log=ws_log,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )

    gql_client = ShopifyGraphQLClient(
        shop_domain=shop_domain,
        api_version=api_version,
        access_token=shopify_token,
    )

    warnings: List[str] = []
    detail_counter: Dict[str, int] = {}

    def log_detail_once(error_reason: str, entity_type: str = "", gid: str = "", field_key: str = "", message: str = ""):
        n = detail_counter.get(error_reason, 0)
        if n >= 2:
            return
        logger.log(
            phase=phase,
            log_type="detail",
            status="WARN" if error_reason else "OK",
            entity_type=entity_type,
            gid=gid,
            field_key=field_key,
            message=message,
            error_reason=error_reason,
        )
        detail_counter[error_reason] = n + 1

    try:
        nodes = _fetch_all_metaobject_defs(gql_client, page_size=page_size)
        df_defs = _build_defs_df(nodes, tz_name=tz_name)

        rows_loaded = len(nodes)
        rows_recognized = len(df_defs)
        rows_planned = len(df_defs)
        rows_written = 0
        rows_skipped = 0

        defs_header = [
            "gid",
            "type",
            "type_name",
            "field_key",
            "field_name",
            "field_type",
            "required",
            "description",
            "updated_at",
            "synced_at",
        ]
        defs_matrix = [defs_header] + df_defs[defs_header].fillna("").astype(str).values.tolist()

        cfg_fields_stats = {"updated": 0, "inserted": 0, "rows_after": 0}
        df_cfg_after = None

        if sync_cfg_fields:
            ws_fields = sh_cfg.worksheet(tab_cfg_fields)
            df_cfg_after, cfg_fields_stats, field_warnings = _upsert_cfg_fields(
                ws_fields=ws_fields,
                df_defs=df_defs,
                strict=strict,
            )
            warnings.extend(field_warnings)
            for w in field_warnings:
                log_detail_once("CFG_FIELDS_OPTIONAL_COLUMNS_MISSING", entity_type="CFG_FIELDS", message=w)

        preview_defs = df_defs.head(preview_rows).copy()
        preview_fields = None
        if df_cfg_after is not None:
            col_entity = _pick_col(df_cfg_after.columns.tolist(), ["entity_type", "owner_type", "entity"])
            col_field_key = _pick_col(df_cfg_after.columns.tolist(), ["field_key", "fieldkey", "system_key"])
            if col_entity and col_field_key:
                preview_fields = (
                    df_cfg_after[
                        (df_cfg_after[col_entity].astype(str).str.upper() == "METAOBJECT_ENTRY")
                        & (df_cfg_after[col_field_key].astype(str).str.startswith("mo."))
                    ]
                    .head(preview_rows)
                    .copy()
                )

        if dry_run:
            rows_skipped = rows_planned
            logger.log(
                phase=phase,
                log_type="summary",
                status="OK",
                entity_type="METAOBJECT_DEF",
                rows_loaded=rows_loaded,
                rows_pending=rows_planned,
                rows_recognized=rows_recognized,
                rows_planned=rows_planned,
                rows_written=0,
                rows_skipped=rows_skipped,
                message=(
                    f"dry_run preview only | defs_rows={len(df_defs)} | "
                    f"cfg_fields_updated={cfg_fields_stats['updated']} | "
                    f"cfg_fields_inserted={cfg_fields_stats['inserted']}"
                ),
                error_reason="",
            )
            logger.flush()

            return {
                "ok": True,
                "phase": phase,
                "run_id": run_id,
                "job_name": job_name,
                "site_code": site_code,
                "summary": {
                    "rows_loaded": rows_loaded,
                    "rows_recognized": rows_recognized,
                    "rows_planned": rows_planned,
                    "rows_written": 0,
                    "rows_skipped": rows_skipped,
                    "cfg_fields_updated": cfg_fields_stats["updated"],
                    "cfg_fields_inserted": cfg_fields_stats["inserted"],
                    "warnings_count": len(warnings),
                },
                "preview": {
                    "metaobject_defs": preview_defs,
                    "cfg_fields": preview_fields,
                },
                "warnings": warnings,
                "targets": {
                    "config_sheet_url": route.config_sheet_url,
                    "runlog_sheet_url": route.runlog_sheet_url,
                    "tab_cfg_metaobject_defs": tab_cfg_metaobject_defs,
                    "tab_cfg_fields": tab_cfg_fields,
                    "tab_runlog": tab_runlog,
                },
            }

        _write_matrix_preserve_shape(ws_defs, defs_matrix)
        rows_written += len(df_defs)

        if sync_cfg_fields and df_cfg_after is not None:
            cfg_matrix = [df_cfg_after.columns.tolist()] + df_cfg_after.fillna("").astype(str).values.tolist()
            ws_fields = sh_cfg.worksheet(tab_cfg_fields)
            _write_matrix_preserve_shape(ws_fields, cfg_matrix)

        logger.log(
            phase=phase,
            log_type="summary",
            status="OK",
            entity_type="METAOBJECT_DEF",
            rows_loaded=rows_loaded,
            rows_pending=rows_planned,
            rows_recognized=rows_recognized,
            rows_planned=rows_planned,
            rows_written=rows_written,
            rows_skipped=0,
            message=(
                f"applied | defs_rows={len(df_defs)} | "
                f"cfg_fields_updated={cfg_fields_stats['updated']} | "
                f"cfg_fields_inserted={cfg_fields_stats['inserted']}"
            ),
            error_reason="",
        )
        logger.flush()

        return {
            "ok": True,
            "phase": phase,
            "run_id": run_id,
            "job_name": job_name,
            "site_code": site_code,
            "summary": {
                "rows_loaded": rows_loaded,
                "rows_recognized": rows_recognized,
                "rows_planned": rows_planned,
                "rows_written": rows_written,
                "rows_skipped": 0,
                "cfg_fields_updated": cfg_fields_stats["updated"],
                "cfg_fields_inserted": cfg_fields_stats["inserted"],
                "warnings_count": len(warnings),
            },
            "preview": {
                "metaobject_defs": preview_defs,
                "cfg_fields": preview_fields,
            },
            "warnings": warnings,
            "targets": {
                "config_sheet_url": route.config_sheet_url,
                "runlog_sheet_url": route.runlog_sheet_url,
                "tab_cfg_metaobject_defs": tab_cfg_metaobject_defs,
                "tab_cfg_fields": tab_cfg_fields,
                "tab_runlog": tab_runlog,
            },
        }

    except Exception as e:
        msg = str(e)
        logger.log(
            phase=phase,
            log_type="summary",
            status="FAIL",
            entity_type="METAOBJECT_DEF",
            rows_loaded="",
            rows_pending="",
            rows_recognized="",
            rows_planned="",
            rows_written="",
            rows_skipped="",
            message=msg,
            error_reason="JOB_FAILED",
        )
        logger.flush()
        raise
