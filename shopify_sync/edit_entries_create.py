from __future__ import annotations

import base64
import datetime as dt
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound


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


@dataclass
class EntryJob:
    entry_type: str
    handle: str
    strict: bool
    min_sheet_row: int
    items: List[Dict[str, Any]]


class RunLogger:
    def __init__(
        self,
        ws,
        run_id: str,
        site_code: str,
        job_name: str,
        tz_name: str,
        flush_every: int = 200,
    ):
        self.ws = ws
        self.run_id = run_id
        self.site_code = site_code
        self.job_name = job_name
        self.tz_name = tz_name
        self.flush_every = flush_every
        self.buf: List[List[Any]] = []

    def _now_cn_str(self) -> str:
        tz = ZoneInfo(self.tz_name)
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

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
        self.buf.append([
            self.run_id,
            self._now_cn_str(),
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
            str(message)[:1000],
            str(error_reason)[:200],
        ])
        if len(self.buf) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        self.ws.append_rows(self.buf, value_input_option="RAW", table_range="A:R")
        self.buf = []


class ShopifyGraphQLClient:
    def __init__(self, *, shop_domain: str, access_token: str, api_version: str, timeout: int = 60):
        self.url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        })

    def query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        resp = self.session.post(self.url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))
        return data.get("data") or {}


class EntriesCreatePlanner:
    def __init__(
        self,
        *,
        cfg_fields_df: pd.DataFrame,
        default_mode: str,
        create_empty_policy: str,
        gql_client: ShopifyGraphQLClient,
    ):
        self.cfg_fields_df = _norm_df(cfg_fields_df)
        self.default_mode = default_mode
        self.create_empty_policy = create_empty_policy
        self.gql = gql_client
        self.cfg_by_field_id = self._build_cfg_map(self.cfg_fields_df)
        self.collection_cache: Dict[str, str] = {}
        self.metaobject_cache: Dict[Tuple[str, str], str] = {}

    def build_plan(self, edit_df: pd.DataFrame) -> Dict[str, Any]:
        df = _norm_df(edit_df)
        if "_sheet_row" not in df.columns:
            df["_sheet_row"] = list(range(2, len(df) + 2))

        required = ["op", "entry_type", "handle", "mode", "field_id", "value"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Edit__Entries_Create missing required columns: {missing}")

        for opt in ["slot", "note"]:
            if opt not in df.columns:
                df[opt] = ""

        rows_loaded = len(df)
        df["op"] = df["op"].map(lambda x: str(x).strip().upper())
        df["mode"] = df["mode"].map(self._norm_mode)

        # 跳过全空行
        df = df[~(
            (df["op"] == "") &
            (df["entry_type"] == "") &
            (df["handle"] == "") &
            (df["field_id"] == "") &
            (df["value"] == "")
        )].copy()
        rows_pending = len(df)

        fatal_errors: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []

        bad_op = df[df["op"] != "CREATE"]
        if len(bad_op) > 0:
            for _, r in bad_op.head(50).iterrows():
                fatal_errors.append(_problem("BAD_OP", r, f"only CREATE allowed; got op={r['op']}"))

        bad_mode = df[~df["mode"].isin(["STRICT", "LOOSE"])]
        if len(bad_mode) > 0:
            for _, r in bad_mode.head(50).iterrows():
                fatal_errors.append(_problem("BAD_MODE", r, f"mode must be STRICT/LOOSE; got mode={r['mode']}"))

        bad_required = df[
            (df["entry_type"] == "") |
            (df["handle"] == "") |
            (df["field_id"] == "")
        ]
        if len(bad_required) > 0:
            for _, r in bad_required.head(50).iterrows():
                fatal_errors.append(_problem("MISSING_REQUIRED", r, "entry_type + handle + field_id required"))

        # field config 检查
        for _, r in df.iterrows():
            fid = r["field_id"]
            meta = self.cfg_by_field_id.get(fid)
            if not meta:
                fatal_errors.append(_problem("FIELD_ID_NOT_FOUND", r, f"field_id not found in Cfg__Fields: {fid}"))
                continue
            if str(meta.get("entity_type", "")).strip().upper() != "METAOBJECT_ENTRY":
                fatal_errors.append(_problem(
                    "BAD_ENTITY_TYPE",
                    r,
                    f"field_id entity_type must be METAOBJECT_ENTRY; got {meta.get('entity_type', '')}",
                ))
                continue
            expected_type = meta.get("metaobject_type", "")
            if expected_type and expected_type != r["entry_type"]:
                fatal_errors.append(_problem(
                    "ENTRY_TYPE_MISMATCH",
                    r,
                    f"entry_type={r['entry_type']} but field_id belongs to {expected_type}",
                ))

        if fatal_errors:
            summary = {
                "rows_loaded": rows_loaded,
                "rows_pending": rows_pending,
                "rows_recognized": 0,
                "rows_planned": 0,
                "rows_skipped": 0,
                "jobs_total": 0,
            }
            return {
                "summary": summary,
                "preview_df": pd.DataFrame(),
                "warnings": warnings,
                "fatal_errors": fatal_errors,
                "jobs": [],
            }

        jobs, build_stats = self._build_jobs_and_fields(df, warnings)
        preview_df = self._make_preview_df(jobs)
        summary = {
            "rows_loaded": rows_loaded,
            "rows_pending": rows_pending,
            "rows_recognized": build_stats["rows_recognized"],
            "rows_planned": len(jobs),
            "rows_skipped": build_stats["rows_skipped"],
            "jobs_total": len(jobs),
        }
        return {
            "summary": summary,
            "preview_df": preview_df,
            "warnings": warnings,
            "fatal_errors": [],
            "jobs": jobs,
        }

    def _build_cfg_map(self, df_cfg: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        cols = _lower_col_map(df_cfg.columns)
        c_field_id = _pick_col(cols, ["field_id"])
        c_entity = _pick_col(cols, ["entity_type"])
        c_field_key = _pick_col(cols, ["field_key"])
        c_namespace = _pick_col(cols, ["namespace"])
        c_key = _pick_col(cols, ["key"])
        c_data_type = _pick_col(cols, ["data_type", "field_type"])
        c_metaobject_type = _pick_col(cols, ["metaobject_type", "owner_subtype", "entry_type", "metaobject_entry_type"])

        required = [c_field_id, c_entity, c_field_key, c_namespace, c_key, c_data_type]
        if not all(required):
            raise ValueError(f"Cfg__Fields missing required columns. got={list(df_cfg.columns)}")

        out = {}
        for _, r in df_cfg.iterrows():
            fid = str(r[c_field_id]).strip()
            if not fid:
                continue
            metaobject_type = str(r[c_metaobject_type]).strip() if c_metaobject_type else ""
            if not metaobject_type:
                metaobject_type = _extract_metaobject_type_from_field_id(fid)
            out[fid] = {
                "field_id": fid,
                "entity_type": str(r[c_entity]).strip(),
                "field_key": str(r[c_field_key]).strip(),
                "namespace": str(r[c_namespace]).strip(),
                "key": str(r[c_key]).strip(),
                "data_type": str(r[c_data_type]).strip(),
                "metaobject_type": metaobject_type,
            }
        return out

    def _norm_mode(self, x: Any) -> str:
        s = str(x).strip().upper()
        return s if s else self.default_mode

    def _build_jobs_and_fields(self, df: pd.DataFrame, warnings: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        records = df.to_dict("records")
        groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for r in records:
            groups.setdefault((r["entry_type"], r["handle"]), []).append(r)

        rows_recognized = 0
        rows_skipped = 0
        jobs = []

        for (entry_type, handle), items in sorted(groups.items(), key=lambda x: (x[0][0], min(int(i["_sheet_row"]) for i in x[1]))):
            strict = any(str(x.get("mode", "")).upper() == "STRICT" for x in items)
            min_row = min(int(x["_sheet_row"]) for x in items)
            built = self._build_fields_for_entry(items, entry_type=entry_type, handle=handle, strict=strict, warnings=warnings)

            rows_recognized += built["rows_recognized"]
            rows_skipped += built["rows_skipped"]

            if not built["fields"]:
                warnings.append({
                    "error_reason": "EMPTY_JOB",
                    "sheet_row": min_row,
                    "entry_type": entry_type,
                    "handle": handle,
                    "field_id": "",
                    "field_key": "",
                    "message": "all rows skipped; no fields planned for this entry",
                    "status": "WARN",
                })
                rows_skipped += len(items)
                continue

            jobs.append({
                "entry_type": entry_type,
                "handle": handle,
                "strict": strict,
                "min_sheet_row": min_row,
                "fields": built["fields"],
                "field_count": len(built["fields"]),
                "warning_count": built["warning_count"],
                "sample_field_keys": [x["key"] for x in built["fields"][:5]],
            })

        return jobs, {
            "rows_recognized": rows_recognized,
            "rows_skipped": rows_skipped,
        }

    def _build_fields_for_entry(
        self,
        items: List[Dict[str, Any]],
        *,
        entry_type: str,
        handle: str,
        strict: bool,
        warnings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        by_field: Dict[str, List[Dict[str, Any]]] = {}
        for r in items:
            by_field.setdefault(r["field_id"], []).append(r)

        out_fields = []
        rows_recognized = 0
        rows_skipped = 0
        warning_count = 0

        for field_id, field_rows in by_field.items():
            meta = self.cfg_by_field_id[field_id]
            key = meta["key"]
            data_type = str(meta.get("data_type", "")).strip().lower()

            if not key:
                if strict:
                    raise ValueError(f"sheet_row={field_rows[0]['_sheet_row']} field_id={field_id} missing key in Cfg__Fields")
                warnings.append(_problem("MISSING_CFG_KEY", field_rows[0], f"field_id={field_id} missing key in Cfg__Fields", field_key=""))
                warning_count += 1
                rows_skipped += len(field_rows)
                continue

            if data_type.startswith("list."):
                values, warn_n, skip_n = self._build_list_value(field_rows, meta, entry_type, strict, warnings)
                warning_count += warn_n
                rows_skipped += skip_n
                if values is None:
                    continue
                out_fields.append({"key": key, "value": values})
                rows_recognized += len(field_rows) - skip_n
                continue

            value, warn_n, skip_n = self._build_scalar_value(field_rows, meta, entry_type, strict, warnings)
            warning_count += warn_n
            rows_skipped += skip_n
            if value is None:
                continue
            out_fields.append({"key": key, "value": value})
            rows_recognized += len(field_rows) - skip_n

        return {
            "fields": out_fields,
            "rows_recognized": rows_recognized,
            "rows_skipped": rows_skipped,
            "warning_count": warning_count,
        }

    def _build_scalar_value(
        self,
        rows: List[Dict[str, Any]],
        meta: Dict[str, Any],
        entry_type: str,
        strict: bool,
        warnings: List[Dict[str, Any]],
    ) -> Tuple[Optional[str], int, int]:
        warn_n = 0
        skip_n = 0
        key = meta["key"]
        data_type = str(meta.get("data_type", "")).lower()

        non_empty = [r for r in rows if str(r.get("value", "")).strip() != ""]
        if len(non_empty) == 0:
            if self.create_empty_policy == "CLEAR":
                return ("", 0, 0)
            return (None, 0, len(rows))

        if len(non_empty) > 1:
            msg = f"duplicate scalar rows found for field; last non-empty wins ({len(non_empty)} rows)"
            target = non_empty[-1]
            if strict:
                raise ValueError(f"sheet_row={target['_sheet_row']} {msg} | field_key={key}")
            warnings.append(_problem("DUPLICATE_SCALAR", target, msg, field_key=key))
            warn_n += 1

        target = sorted(non_empty, key=lambda r: int(r["_sheet_row"]))[-1]
        raw = str(target.get("value", "")).strip()

        try:
            if data_type == "collection_reference":
                gid = self._to_gid_collection(raw, strict=strict)
                if not gid:
                    return (None, warn_n, skip_n + 1)
                return (gid, warn_n, skip_n)

            if data_type == "metaobject_reference":
                gid = self._to_gid_metaobject(raw, current_entry_type=entry_type, strict=strict)
                if not gid:
                    return (None, warn_n, skip_n + 1)
                return (gid, warn_n, skip_n)

            return (raw, warn_n, skip_n)
        except Exception as e:
            if strict:
                raise
            warnings.append(_problem("RESOLVE_FAILED", target, str(e), field_key=key))
            return (None, warn_n + 1, skip_n + 1)

    def _build_list_value(
        self,
        rows: List[Dict[str, Any]],
        meta: Dict[str, Any],
        entry_type: str,
        strict: bool,
        warnings: List[Dict[str, Any]],
    ) -> Tuple[Optional[str], int, int]:
        warn_n = 0
        skip_n = 0
        key = meta["key"]
        data_type = str(meta.get("data_type", "")).lower()

        prepared = []
        for r in rows:
            raw = str(r.get("value", "")).strip()
            slot = _slot_to_int(r.get("slot", ""))
            if str(r.get("slot", "")).strip() and slot is None:
                msg = f"invalid slot={r.get('slot')}"
                if strict:
                    raise ValueError(f"sheet_row={r['_sheet_row']} {msg}")
                warnings.append(_problem("BAD_SLOT", r, msg, field_key=key))
                warn_n += 1
                skip_n += 1
                continue
            prepared.append((slot if slot is not None else 999999, int(r["_sheet_row"]), r, raw))

        prepared.sort(key=lambda x: (x[0], x[1]))

        flat_values: List[str] = []
        for _, _, _, raw in prepared:
            if raw == "":
                continue
            flat_values.extend(_split_multi(raw))

        if len(flat_values) == 0:
            if self.create_empty_policy == "CLEAR":
                return ("[]", warn_n, skip_n)
            return (None, warn_n, skip_n + len(rows))

        try:
            if data_type == "list.collection_reference":
                gids = []
                for val in flat_values:
                    gid = self._to_gid_collection(val, strict=strict)
                    if gid:
                        gids.append(gid)
                if len(gids) == 0:
                    return (None, warn_n, skip_n + len(rows))
                return (json.dumps(gids, ensure_ascii=False), warn_n, skip_n)

            if data_type == "list.metaobject_reference":
                gids = []
                for val in flat_values:
                    gid = self._to_gid_metaobject(val, current_entry_type=entry_type, strict=strict)
                    if gid:
                        gids.append(gid)
                if len(gids) == 0:
                    return (None, warn_n, skip_n + len(rows))
                return (json.dumps(gids, ensure_ascii=False), warn_n, skip_n)

            return (json.dumps(flat_values, ensure_ascii=False), warn_n, skip_n)
        except Exception as e:
            if strict:
                raise
            warnings.append(_problem("RESOLVE_FAILED", rows[0], str(e), field_key=key))
            return (None, warn_n + 1, skip_n + len(rows))

    def _make_preview_df(self, jobs: List[Dict[str, Any]]) -> pd.DataFrame:
        rows = []
        for j in jobs:
            rows.append({
                "entry_type": j["entry_type"],
                "handle": j["handle"],
                "strict": "STRICT" if j["strict"] else "LOOSE",
                "field_count": j["field_count"],
                "warning_count": j["warning_count"],
                "sample_field_keys": " | ".join(j["sample_field_keys"]),
                "first_sheet_row": j["min_sheet_row"],
            })
        return pd.DataFrame(rows)

    def _to_gid_collection(self, val: str, strict: bool) -> str:
        v = str(val).strip()
        if not v:
            return ""
        if v.startswith("gid://"):
            return v
        if v in self.collection_cache:
            gid = self.collection_cache[v]
            if strict and not gid:
                raise ValueError(f"collection not found: {v}")
            return gid
        if re.fullmatch(r"\d+", v):
            gid = f"gid://shopify/Collection/{v}"
            self.collection_cache[v] = gid
            return gid

        data = self.gql.query(Q_COLLECTION_BY_HANDLE, {"handle": v})
        node = (data or {}).get("collectionByHandle") or {}
        gid = str(node.get("id", "")).strip()
        self.collection_cache[v] = gid
        if strict and not gid:
            raise ValueError(f"collection not found: {v}")
        return gid

    def _to_gid_metaobject(self, raw: str, current_entry_type: str, strict: bool) -> str:
        v = str(raw).strip()
        if not v:
            return ""
        if v.startswith("gid://"):
            return v
        mo_type, handle = _parse_type_and_handle(v, current_entry_type)
        cache_key = (mo_type, handle)
        if cache_key in self.metaobject_cache:
            gid = self.metaobject_cache[cache_key]
            if strict and not gid:
                raise ValueError(f"metaobject not found: {mo_type}/{handle}")
            return gid

        data = self.gql.query(Q_METAOBJECT_BY_HANDLE, {"handle": {"type": mo_type, "handle": handle}})
        node = (data or {}).get("metaobjectByHandle") or {}
        gid = str(node.get("id", "")).strip()
        self.metaobject_cache[cache_key] = gid
        if strict and not gid:
            raise ValueError(f"metaobject not found: {mo_type}/{handle}")
        return gid


def run(
    *,
    site_code: str,
    job_name: str,
    console_core_url: str,
    gsheet_sa_b64: str,
    shopify_access_token: str,
    shop_domain: str,
    api_version: str = "2026-01",
    dry_run: bool = True,
    confirmed: bool = False,
    tz_name: str = "Asia/Shanghai",
    cfg_sites_tab: str = "Cfg__Sites",
    cfg_fields_tab: str = "Cfg__Fields",
    edit_sheet_label: str = "edit",
    edit_tab_name: str = "Edit__Entries_Create",
    runlog_sheet_label: str = "runlog_sheet",
    runlog_tab_name: str = "Ops__RunLog",
    default_mode: str = "LOOSE",
    create_empty_policy: str = "SKIP",
    execute_chunk_size: int = 25,
    preview_limit: int = 20,
    detail_limit_per_error: int = 2,
    request_timeout: int = 60,
    print_progress_every: int = 25,
) -> Dict[str, Any]:
    default_mode = str(default_mode).strip().upper() or "LOOSE"
    create_empty_policy = str(create_empty_policy).strip().upper() or "SKIP"

    if default_mode not in {"STRICT", "LOOSE"}:
        raise ValueError("default_mode must be STRICT or LOOSE")
    if create_empty_policy not in {"SKIP", "CLEAR"}:
        raise ValueError("create_empty_policy must be SKIP or CLEAR")
    if (not dry_run) and (not confirmed):
        raise ValueError("Apply requires dry_run=False and confirmed=True.")

    run_id = _gen_run_id(job_name=job_name, tz_name=tz_name)

    gc = _build_gspread_client(gsheet_sa_b64)
    gql_client = ShopifyGraphQLClient(
        shop_domain=shop_domain,
        access_token=shopify_access_token,
        api_version=api_version,
        timeout=request_timeout,
    )

    sh_console = _gs_open_by_url(gc, console_core_url)
    cfg_sites_df = _read_worksheet_df(sh_console, cfg_sites_tab)

    edit_sheet_url = _lookup_sheet_url(cfg_sites_df, site_code, edit_sheet_label)
    runlog_sheet_url = _lookup_sheet_url(cfg_sites_df, site_code, runlog_sheet_label)
    config_sheet_url = _lookup_sheet_url(cfg_sites_df, site_code, "config")

    sh_edit = _gs_open_by_url(gc, edit_sheet_url)
    sh_runlog = _gs_open_by_url(gc, runlog_sheet_url)
    sh_config = _gs_open_by_url(gc, config_sheet_url)

    ws_runlog = _gs_get_or_create_ws(sh_runlog, runlog_tab_name, rows=2000, cols=18)
    _ensure_runlog_header(ws_runlog)

    logger = RunLogger(
        ws=ws_runlog,
        run_id=run_id,
        site_code=site_code,
        job_name=job_name,
        tz_name=tz_name,
    )

    cfg_fields_df = _read_worksheet_df(sh_config, cfg_fields_tab)
    edit_df = _read_worksheet_df(sh_edit, edit_tab_name, preserve_blank=True)

    planner = EntriesCreatePlanner(
        cfg_fields_df=cfg_fields_df,
        default_mode=default_mode,
        create_empty_policy=create_empty_policy,
        gql_client=gql_client,
    )

    plan = planner.build_plan(edit_df)
    phase = "preview" if dry_run else "apply"

    print("=" * 72)
    print(f"{job_name} | site={site_code} | phase={phase} | run_id={run_id}")
    print(f"shop_domain={shop_domain}")
    print(f"edit_tab={edit_tab_name} | cfg_fields_tab={cfg_fields_tab} | runlog_tab={runlog_tab_name}")
    print("=" * 72)

    _print_summary(plan["summary"])
    _print_preview_table(plan["preview_df"], limit=preview_limit)
    _print_warnings(plan["warnings"], limit=preview_limit)

    recognized = int(plan["summary"]["rows_recognized"])
    planned = int(plan["summary"]["rows_planned"])
    skipped = int(plan["summary"]["rows_skipped"])

    logger.log(
        phase=phase,
        log_type="summary",
        status="OK" if not plan["fatal_errors"] else "FAIL",
        entity_type="METAOBJECT_ENTRY",
        rows_loaded=plan["summary"]["rows_loaded"],
        rows_pending=plan["summary"]["rows_pending"],
        rows_recognized=recognized,
        rows_planned=planned,
        rows_written=0,
        rows_skipped=skipped,
        message=json.dumps(plan["summary"], ensure_ascii=False)[:1000],
        error_reason="PRECHECK_FAILED" if plan["fatal_errors"] else "",
    )

    _log_detail_examples(
        logger=logger,
        phase=phase,
        detail_limit_per_error=detail_limit_per_error,
        problems=plan["fatal_errors"] + plan["warnings"],
    )
    logger.flush()

    if plan["fatal_errors"]:
        return {
            "ok": False,
            "run_id": run_id,
            "phase": phase,
            "summary": plan["summary"],
            "preview_df": plan["preview_df"],
            "warnings": plan["warnings"],
            "fatal_errors": plan["fatal_errors"],
            "written_count": 0,
        }

    if dry_run:
        print("DRY_RUN=True: preview only, nothing written.")
        return {
            "ok": True,
            "run_id": run_id,
            "phase": phase,
            "summary": plan["summary"],
            "preview_df": plan["preview_df"],
            "warnings": plan["warnings"],
            "fatal_errors": [],
            "written_count": 0,
        }

    results = _execute_apply(
        gql_client=gql_client,
        jobs=plan["jobs"],
        execute_chunk_size=execute_chunk_size,
        print_progress_every=print_progress_every,
    )

    apply_summary = dict(plan["summary"])
    apply_summary["rows_written"] = results["written_count"]
    apply_summary["rows_failed"] = results["failed_count"]
    apply_summary["jobs_total"] = results["jobs_total"]

    print("-" * 72)
    print("Apply summary")
    for k, v in apply_summary.items():
        print(f"- {k}: {v}")
    print("-" * 72)

    logger.log(
        phase="apply",
        log_type="summary",
        status="OK" if results["failed_count"] == 0 else "PARTIAL_FAIL",
        entity_type="METAOBJECT_ENTRY",
        rows_loaded=apply_summary["rows_loaded"],
        rows_pending=apply_summary["rows_pending"],
        rows_recognized=apply_summary["rows_recognized"],
        rows_planned=apply_summary["rows_planned"],
        rows_written=apply_summary["rows_written"],
        rows_skipped=apply_summary["rows_skipped"],
        message=json.dumps(apply_summary, ensure_ascii=False)[:1000],
        error_reason="",
    )
    _log_detail_examples(
        logger=logger,
        phase="apply",
        detail_limit_per_error=detail_limit_per_error,
        problems=results["errors"],
    )
    logger.flush()

    return {
        "ok": results["failed_count"] == 0,
        "run_id": run_id,
        "phase": "apply",
        "summary": apply_summary,
        "preview_df": plan["preview_df"],
        "warnings": plan["warnings"],
        "fatal_errors": [],
        "written_count": results["written_count"],
        "errors": results["errors"],
    }


def _execute_apply(
    *,
    gql_client: ShopifyGraphQLClient,
    jobs: List[Dict[str, Any]],
    execute_chunk_size: int,
    print_progress_every: int,
) -> Dict[str, Any]:
    written_count = 0
    failed_count = 0
    errors: List[Dict[str, Any]] = []
    jobs_total = len(jobs)

    for idx, batch in enumerate(_chunk(jobs, execute_chunk_size), start=1):
        print(f"Batch {idx}: {len(batch)} jobs")
        for job in batch:
            try:
                payload = {
                    "handle": {"type": job["entry_type"], "handle": job["handle"]},
                    "metaobject": {
                        "handle": job["handle"],
                        "fields": job["fields"],
                    },
                }
                data = gql_client.query(M_METAOBJECT_UPSERT, payload)
                res = (data or {}).get("metaobjectUpsert") or {}
                user_errors = res.get("userErrors") or []
                if user_errors:
                    raise RuntimeError(_fmt_user_errors(user_errors))

                written_count += 1
                if written_count % max(print_progress_every, 1) == 0:
                    print(f"  written={written_count}/{jobs_total}")

            except Exception as e:
                failed_count += 1
                errors.append({
                    "error_reason": "UPSERT_FAILED",
                    "sheet_row": job["min_sheet_row"],
                    "entry_type": job["entry_type"],
                    "handle": job["handle"],
                    "field_id": "",
                    "field_key": "",
                    "message": str(e)[:1000],
                    "status": "FAIL",
                })

    return {
        "written_count": written_count,
        "failed_count": failed_count,
        "errors": errors,
        "jobs_total": jobs_total,
    }


def _ensure_runlog_header(ws) -> None:
    existing = ws.row_values(1)
    if existing[:len(RUNLOG_HEADER_18)] != RUNLOG_HEADER_18:
        ws.update("A1:R1", [RUNLOG_HEADER_18], value_input_option="RAW")


def _gs_get_or_create_ws(sh, title: str, rows: int, cols: int):
    try:
        return _gs_retry(lambda: sh.worksheet(title))
    except WorksheetNotFound:
        return _gs_retry(lambda: sh.add_worksheet(title=title, rows=rows, cols=cols))


def _build_gspread_client(sa_b64: str):
    sa_info = json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)


def _read_worksheet_df(sh, ws_title: str, preserve_blank: bool = True) -> pd.DataFrame:
    ws = _gs_retry(lambda: sh.worksheet(ws_title))
    values = _gs_retry(lambda: ws.get_all_values())
    if not values:
        return pd.DataFrame()

    header = [str(x).strip() for x in values[0]]
    data = values[1:]
    rows = []
    width = len(header)

    for idx, row in enumerate(data, start=2):
        row = list(row) + [""] * max(0, width - len(row))
        row = row[:width]
        rec = dict(zip(header, row))
        rec["_sheet_row"] = idx
        rows.append(rec)

    df = pd.DataFrame(rows)
    if preserve_blank and len(df) == 0:
        df = pd.DataFrame(columns=header + ["_sheet_row"])
    return df


def _lookup_sheet_url(df_cfg_sites: pd.DataFrame, site_code: str, label: str) -> str:
    df = _norm_df(df_cfg_sites)
    cols = _lower_col_map(df.columns)
    c_site_code = _pick_col(cols, ["site_code"])
    c_label = _pick_col(cols, ["label"])
    c_sheet_url = _pick_col(cols, ["sheet_url"])

    if not all([c_site_code, c_label, c_sheet_url]):
        raise ValueError(f"Cfg__Sites missing required columns. got={list(df.columns)}")

    m = df[
        (df[c_site_code].str.lower() == str(site_code).strip().lower()) &
        (df[c_label].str.lower() == str(label).strip().lower())
    ].copy()
    m = m[m[c_sheet_url].astype(str).str.strip() != ""]
    if len(m) == 0:
        raise ValueError(f"Cfg__Sites cannot find site_code={site_code}, label={label}")

    return str(m.iloc[0][c_sheet_url]).strip()


def _gs_open_by_url(gc, url: str):
    return _gs_retry(lambda: gc.open_by_url(url))


def _gs_retry(fn, retry: int = 8, base_sleep: float = 1.5, max_sleep: float = 20.0):
    last_e = None
    for i in range(retry):
        try:
            return fn()
        except APIError as e:
            last_e = e
            s = str(e)
            if ("429" not in s) and ("Quota" not in s) and ("Read requests per minute" not in s):
                raise
            sleep_s = min(max_sleep, base_sleep * (2 ** i))
            time.sleep(sleep_s)

    if last_e:
        raise last_e
    raise RuntimeError("_gs_retry failed without exception")


def _norm_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    for c in out.columns:
        out[c] = out[c].map(lambda x: "" if pd.isna(x) else str(x).strip())
    return out


def _lower_col_map(cols) -> Dict[str, str]:
    return {str(c).strip().lower(): str(c).strip() for c in cols}


def _pick_col(col_map: Dict[str, str], candidates: List[str]) -> str:
    for c in candidates:
        if c.lower() in col_map:
            return col_map[c.lower()]
    return ""


def _extract_metaobject_type_from_field_id(field_id: str) -> str:
    s = str(field_id).strip()
    m = re.match(r"^METAOBJECT_ENTRY\|mo\.([^.]+)\..+$", s)
    return m.group(1).strip() if m else ""


def _parse_type_and_handle(raw: str, fallback_type: str) -> Tuple[str, str]:
    s = str(raw).strip()
    if "/" in s:
        left, right = s.split("/", 1)
        if left and right:
            return left.strip(), right.strip()
    if ":" in s:
        left, right = s.split(":", 1)
        if left and right:
            return left.strip(), right.strip()
    return fallback_type, s


def _split_multi(raw: str) -> List[str]:
    return [x.strip() for x in re.split(r"[,\n]+", str(raw)) if x.strip()]


def _slot_to_int(x: Any) -> Optional[int]:
    s = str(x).strip()
    if not s:
        return None
    if re.fullmatch(r"\d+(\.0+)?", s):
        return int(float(s))
    return None


def _problem(error_reason: str, row: Dict[str, Any], message: str, field_key: str = "") -> Dict[str, Any]:
    return {
        "error_reason": error_reason,
        "sheet_row": int(row.get("_sheet_row", 0) or 0),
        "entry_type": str(row.get("entry_type", "")).strip(),
        "handle": str(row.get("handle", "")).strip(),
        "field_id": str(row.get("field_id", "")).strip(),
        "field_key": field_key,
        "message": str(message)[:1000],
        "status": "WARN",
    }


def _log_detail_examples(
    logger: RunLogger,
    phase: str,
    detail_limit_per_error: int,
    problems: List[Dict[str, Any]],
) -> None:
    counter: Dict[str, int] = {}
    for p in problems:
        er = str(p.get("error_reason", "")).strip() or "UNKNOWN"
        counter.setdefault(er, 0)
        if counter[er] >= detail_limit_per_error:
            continue
        counter[er] += 1
        logger.log(
            phase=phase,
            log_type="detail",
            status=p.get("status", "WARN"),
            entity_type=p.get("entry_type", "METAOBJECT_ENTRY") or "METAOBJECT_ENTRY",
            gid=str(p.get("handle", "")),
            field_key=p.get("field_key", ""),
            message=f"sheet_row={p.get('sheet_row', '')} | field_id={p.get('field_id', '')} | {p.get('message', '')}",
            error_reason=er,
        )


def _print_summary(summary: Dict[str, Any]) -> None:
    print("Summary")
    for k, v in summary.items():
        print(f"- {k}: {v}")


def _print_preview_table(df: pd.DataFrame, limit: int = 20) -> None:
    print("-" * 72)
    print("Preview")
    if df is None or len(df) == 0:
        print("(empty)")
        return
    show = df.head(limit).copy()
    try:
        print(show.to_string(index=False))
    except Exception:
        print(show)


def _print_warnings(warnings: List[Dict[str, Any]], limit: int = 20) -> None:
    print("-" * 72)
    print("Warnings")
    if not warnings:
        print("(none)")
        return
    show = pd.DataFrame(warnings).head(limit)
    try:
        print(show.to_string(index=False))
    except Exception:
        print(show)


def _chunk(lst: List[Any], n: int):
    for i in range(0, len(lst), max(n, 1)):
        yield lst[i:i + max(n, 1)]


def _gen_run_id(job_name: str, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return dt.datetime.now(tz).strftime(f"{job_name}_%Y%m%d_%H%M%S")


def _fmt_user_errors(user_errors: List[Dict[str, Any]]) -> str:
    parts = []
    for e in user_errors:
        code = e.get("code", "")
        msg = e.get("message", "")
        field = e.get("field", [])
        parts.append(f"{code} | {msg} | field={field}")
    return " ; ".join(parts)[:1000]


Q_COLLECTION_BY_HANDLE = """
query($handle: String!) {
  collectionByHandle(handle: $handle) {
    id
    handle
  }
}
"""

Q_METAOBJECT_BY_HANDLE = """
query($handle: MetaobjectHandleInput!) {
  metaobjectByHandle(handle: $handle) {
    id
    handle
    type
  }
}
"""

M_METAOBJECT_UPSERT = """
mutation($handle: MetaobjectHandleInput!, $metaobject: MetaobjectUpsertInput!) {
  metaobjectUpsert(handle: $handle, metaobject: $metaobject) {
    metaobject { id handle type }
    userErrors { field message code }
  }
}
"""
