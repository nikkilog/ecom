# business_overview/build_overview_refs.py
# 多站点 Console Core 标准版
# 金标准来源：PBS整站数据_Overview_build_overview_refs.ipynb

from __future__ import annotations

import base64
import json
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

try:
    from google.colab import userdata  # type: ignore
except Exception:  # local fallback only for import safety; actual missing secret still raises
    userdata = None


# ============================================
# Errors
# ============================================

class OverviewConfigError(RuntimeError):
    pass


class OverviewRunError(RuntimeError):
    pass


# ============================================
# Constants
# ============================================

RUNLOG_COLUMNS = [
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

SALES_COLUMNS_M = [
    "Month",
    "orders",
    "gross_sales",
    "net_sales",
    "total_sales",
    "shipping_charges",
    "discounts",
    "returns",
    "taxes",
    "customers",
    "new_customers",
    "returning_customers",
]

SALES_COLUMNS_SE = [
    "Date",
    "orders",
    "gross_sales",
    "net_sales",
    "total_sales",
    "shipping_charges",
    "discounts",
    "returns",
    "taxes",
    "customers",
    "new_customers",
    "returning_customers",
]

SALES_NUMERIC_COLUMNS = [
    "orders",
    "gross_sales",
    "net_sales",
    "total_sales",
    "shipping_charges",
    "discounts",
    "returns",
    "taxes",
    "customers",
    "new_customers",
    "returning_customers",
]

NUMERIC_COLUMNS_BY_SHEET = {
    "Ref-M-S-Sales": {"orders", "gross_sales", "net_sales", "total_sales", "shipping_charge", "shipping_charges", "discounts", "returns", "taxes", "customers", "new_customers", "returning_customers"},
    "Ref-M-S-Sessions": {"sessions", "sessions_with_cart_additions", "sessions_that_reached_checkout", "sessions_that_completed_checkout"},
    "Ref-M-GG-A": {"Impression", "Clicks", "Cost", "Conversions", "Conv.Value"},
    "Ref-M-Meta-A": {"Impr.", "Clicks", "Spend", "Purchase", "Value", "Add to cart", "Checkout"},
    "Ref-M-AWIN": {"Sale Amount", "Commission"},
    "Ref-SE-S-Sales": {"orders", "gross_sales", "net_sales", "total_sales", "shipping_charge", "shipping_charges", "discounts", "returns", "taxes", "customers", "new_customers", "returning_customers"},
    "Ref-SE-S-Sessions": {"sessions", "sessions_with_cart_additions", "sessions_that_reached_checkout", "sessions_that_completed_checkout"},
    "Ref-SE-GG-A": {"Impression", "Clicks", "Cost", "Conversions", "Conv.Value", "CTR", "A.CPC", "ROI", "CVR", "AOV", "CPA"},
    "Ref-SE-Meta-A": {"Impr.", "Clicks", "Spend", "Purchase", "Value", "Add to cart", "Checkout"},
    "Ref-SE-AWIN": {"Sale Amount", "Commission"},
}

META_GRAPH_VERSION = "v22.0"
AWIN_API_BASE = "https://api.awin.com"
AWIN_REQUEST_SLEEP_SECONDS = 3.5
AWIN_429_SLEEP_SECONDS = 70
AWIN_MAX_RETRIES = 6


# ============================================
# Generic helpers
# ============================================

def _require_non_empty(value: Any, name: str) -> str:
    if value is None or str(value).strip() == "":
        raise OverviewConfigError(f"Missing required config value: {name}")
    return str(value).strip()


def _get_secret_required(secret_name: str, purpose: str) -> str:
    secret_name = _require_non_empty(secret_name, f"{purpose} secret name")
    if userdata is None:
        raise OverviewConfigError("google.colab.userdata is not available. This runner expects Colab secrets.")
    value = userdata.get(secret_name)
    if not value:
        raise OverviewConfigError(f"Missing Colab secret for {purpose}: {secret_name}")
    return value


def _get_secret_optional(secret_name: Optional[str]) -> Optional[str]:
    if secret_name is None or str(secret_name).strip() == "":
        return None
    if userdata is None:
        return None
    try:
        return userdata.get(str(secret_name).strip())
    except Exception:
        return None


def _decode_sa_b64(sa_b64: str, secret_name: str) -> Dict[str, Any]:
    try:
        return json.loads(base64.b64decode(sa_b64).decode("utf-8"))
    except Exception as e:
        raise OverviewConfigError(f"Invalid Google SA base64 secret: {secret_name}. {e}")


def _authorize_gspread_from_sa_b64(sa_b64: str, secret_name: str):
    sa_info = _decode_sa_b64(sa_b64, secret_name)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds), sa_info


def _open_ws_by_url_and_title(gc_client, sheet_url: str, worksheet_title: str):
    if not sheet_url or not str(sheet_url).strip():
        raise OverviewConfigError("Missing sheet_url")
    if not worksheet_title or not str(worksheet_title).strip():
        raise OverviewConfigError("Missing worksheet title")
    try:
        spreadsheet = gc_client.open_by_url(str(sheet_url).strip())
    except Exception as e:
        raise OverviewConfigError(f"Cannot open spreadsheet by URL: {sheet_url}. {e}")
    try:
        return spreadsheet.worksheet(str(worksheet_title).strip())
    except Exception as e:
        raise OverviewConfigError(f"Missing tab: {worksheet_title} in spreadsheet: {sheet_url}. {e}")


def _norm_key(x: Any) -> str:
    return str(x or "").strip()


def _norm_key_upper(x: Any) -> str:
    return _norm_key(x).upper()


def _read_account_config_from_values(values: List[List[str]], site_code: str, account_tab: str) -> Dict[str, str]:
    if not values:
        raise OverviewConfigError(f"Tab {account_tab} is empty")
    site_code = str(site_code).strip()
    if not site_code:
        raise OverviewConfigError("SITE_CODE is empty")
    rows = [list(r) for r in values if any(str(c).strip() for c in r)]
    if not rows:
        raise OverviewConfigError(f"Tab {account_tab} has no non-empty rows")

    first_row = [_norm_key_upper(c) for c in rows[0]]
    if "SITE_CODE" in first_row:
        header = [_norm_key(c) for c in rows[0]]
        header_upper = [_norm_key_upper(c) for c in header]
        site_idx = header_upper.index("SITE_CODE")
        matched = None
        for r in rows[1:]:
            if len(r) > site_idx and str(r[site_idx]).strip() == site_code:
                matched = r
                break
        if matched is None:
            raise OverviewConfigError(f"Cannot find SITE_CODE={site_code} in {account_tab}")
        cfg = {}
        for idx, key in enumerate(header):
            if key:
                cfg[key] = matched[idx].strip() if idx < len(matched) else ""
        return cfg

    cfg = {}
    for r in rows:
        if len(r) < 2:
            continue
        key = str(r[0]).strip()
        value = str(r[1]).strip()
        if key:
            cfg[key] = value
    if not cfg:
        raise OverviewConfigError(f"Cannot parse {account_tab}. Expected wide table with SITE_CODE or key-value rows.")
    return cfg


def _require_account_keys(account_cfg: Dict[str, str], required_keys: List[str], account_tab: str):
    missing = [k for k in required_keys if k not in account_cfg]
    if missing:
        raise OverviewConfigError(f"Tab {account_tab} missing required keys/columns: {missing}")


def _read_sites_rows(values: List[List[str]], sites_tab: str) -> List[Dict[str, str]]:
    if not values:
        raise OverviewConfigError(f"Tab {sites_tab} is empty")
    header = [str(c).strip() for c in values[0]]
    header_lower = [c.lower() for c in header]
    required = ["site_code", "label", "sheet_url"]
    missing = [c for c in required if c not in header_lower]
    if missing:
        raise OverviewConfigError(f"Tab {sites_tab} missing required columns: {missing}. Existing columns={header}")
    rows = []
    for raw in values[1:]:
        if not any(str(c).strip() for c in raw):
            continue
        d = {}
        for i, col in enumerate(header):
            d[col] = raw[i].strip() if i < len(raw) else ""
        rows.append(d)
    return rows


def _find_site_sheet_url(sites_rows: List[Dict[str, str]], site_code: str, label: str) -> str:
    site_code = str(site_code).strip()
    label = str(label).strip()
    matches = []
    for r in sites_rows:
        r_lower = {str(k).strip().lower(): v for k, v in r.items()}
        if str(r_lower.get("site_code", "")).strip() == site_code and str(r_lower.get("label", "")).strip() == label:
            matches.append(r_lower)
    if not matches:
        raise OverviewConfigError(f"Cfg__Sites missing label for current site: site_code={site_code}, label={label}")
    if len(matches) > 1:
        raise OverviewConfigError(f"Cfg__Sites has duplicate rows: site_code={site_code}, label={label}")
    sheet_url = str(matches[0].get("sheet_url", "")).strip()
    if not sheet_url:
        raise OverviewConfigError(f"Cfg__Sites row has empty sheet_url: site_code={site_code}, label={label}")
    return sheet_url


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def date_to_yyyy_mm_dd(x: Any, default_day: str = "01") -> str:
    if x is None or x == "":
        return ""
    s = str(x).strip()
    if s.startswith("'"):
        s = s[1:].strip()
    m = re.match(r"^(\d{4})-(\d{2})(?:-(\d{2}))?$", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3) or default_day}"
    m = re.match(r"^(\d{4})/(\d{2})(?:/(\d{2}))?$", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3) or default_day}"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})T", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%b %Y", "%B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y/%m/%d")
        except Exception:
            pass
    return s


def month_to_yyyy_mm_dd(x: Any) -> str:
    if x is None or x == "":
        return ""
    s = str(x).strip()
    if s.startswith("'"):
        s = s[1:].strip()
    for pat in [r"^(\d{4})-(\d{2})$", r"^(\d{4})/(\d{2})$", r"^(\d{4})-(\d{2})-\d{2}$", r"^(\d{4})/(\d{2})/\d{2}$", r"^(\d{4})-(\d{2})-\d{2}T"]:
        m = re.match(pat, s)
        if m:
            return f"{m.group(1)}/{m.group(2)}/01"
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%b %Y", "%B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y/%m/01")
        except Exception:
            pass
    return s


def parse_number(v: Any):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip().replace(",", "").replace("$", "")
    if s == "":
        return None
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return None


def parse_iso_date(s: str):
    return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()


def get_previous_period_range(start_date: str, end_date: str) -> Tuple[str, str, int]:
    cur_start = parse_iso_date(start_date)
    cur_end = parse_iso_date(end_date)
    day_count = (cur_end - cur_start).days + 1
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=day_count - 1)
    return prev_start.strftime("%Y-%m-%d"), prev_end.strftime("%Y-%m-%d"), day_count


def clean_df_month(df: pd.DataFrame) -> pd.DataFrame:
    month_candidates = [c for c in df.columns if str(c).strip().lower() == "month"]
    if not month_candidates:
        month_candidates = [c for c in df.columns if "month" in str(c).strip().lower()]
    if month_candidates:
        src = month_candidates[0]
        df["Month"] = df[src].apply(month_to_yyyy_mm_dd)
        if src != "Month":
            df = df.drop(columns=[src])
        df = df[["Month"] + [c for c in df.columns if c != "Month"]]
    return df


def clean_df_date(df: pd.DataFrame) -> pd.DataFrame:
    date_candidates = [c for c in df.columns if str(c).strip().lower() == "date"]
    if not date_candidates:
        date_candidates = [c for c in df.columns if "date" in str(c).strip().lower()]
    if date_candidates:
        src = date_candidates[0]
        df["Date"] = df[src].apply(lambda x: date_to_yyyy_mm_dd(x, default_day="01"))
        if src != "Date":
            df = df.drop(columns=[src])
        df = df[["Date"] + [c for c in df.columns if c != "Date"]]
    return df


def reorder_columns_case_insensitive(df: pd.DataFrame, desired: List[str]) -> pd.DataFrame:
    actual_map = {str(c).strip().lower(): c for c in df.columns}
    cols = []
    for d in desired:
        key = d.lower()
        if key in actual_map:
            cols.append(actual_map[key])
    rest = [c for c in df.columns if c not in cols]
    return df[cols + rest]


def force_numeric_columns_for_sheet(df: pd.DataFrame, sheet_title: str) -> pd.DataFrame:
    numeric_cols = NUMERIC_COLUMNS_BY_SHEET.get(sheet_title, set())
    if not numeric_cols:
        return df
    actual_map = {str(c).strip().lower(): c for c in df.columns}
    for col in numeric_cols:
        key = str(col).strip().lower()
        if key in actual_map:
            real_col = actual_map[key]
            df[real_col] = df[real_col].apply(parse_number)
    return df


def normalize_df_before_write(df: pd.DataFrame, sheet_title: str) -> pd.DataFrame:
    df = df.copy()
    lower_cols = {str(c).strip().lower() for c in df.columns}
    if "month" in lower_cols:
        df = clean_df_month(df)
    if "date" in lower_cols:
        df = clean_df_date(df)
    return force_numeric_columns_for_sheet(df, sheet_title)


def force_sales_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for c in SALES_NUMERIC_COLUMNS:
        if c in work.columns:
            work[c] = work[c].apply(parse_number)
    return work


# ============================================
# Core runner class
# ============================================

class OverviewRefsRunner:
    def __init__(self, **kwargs):
        self.cfg = kwargs
        self.debug = bool(kwargs.get("DEBUG", True))
        self.print_debug = bool(kwargs.get("PRINT_DEBUG", True))
        self.job_name = str(kwargs.get("JOB_NAME", "build_overview_refs")).strip()
        self.site_code = _require_non_empty(kwargs.get("SITE_CODE"), "SITE_CODE")
        self.run_id = kwargs.get("RUN_ID") or f"{self.job_name}__{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.tz_name = kwargs.get("TZ_NAME", "Asia/Shanghai")
        self.written_info = []
        self.runlog_rows = []
        self._awin_tx_cache = {}
        self.state: Dict[str, Any] = {}

    def dbg(self, *args, **kwargs):
        if self.debug:
            print(*args, **kwargs)

    def ts_cn(self) -> str:
        # 维持旧 RunLog 视觉：YYYY-MM-DD HH:MM:SS。默认按 Asia/Shanghai。
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo(str(self.tz_name))).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def log_row(self, phase: str, log_type: str = "summary", status: str = "success", message: str = "", error_reason: str = "", **counts):
        row = {c: "" for c in RUNLOG_COLUMNS}
        row.update({
            "run_id": self.run_id,
            "ts_cn": self.ts_cn(),
            "job_name": self.job_name,
            "phase": phase,
            "log_type": log_type,
            "status": status,
            "site_code": self.site_code,
            "message": message,
            "error_reason": error_reason,
        })
        for k, v in counts.items():
            if k in row and v is not None:
                row[k] = v
        self.runlog_rows.append(row)

    def flush_runlog(self):
        if not self.runlog_rows:
            return
        runlog_ws = self.state.get("runlog_ws")
        if runlog_ws is None:
            return
        existing = runlog_ws.get_all_values()
        if not existing:
            runlog_ws.update("A1", [RUNLOG_COLUMNS], value_input_option="USER_ENTERED")
            start_row = 2
        else:
            header = existing[0]
            if [str(x).strip() for x in header[:len(RUNLOG_COLUMNS)]] != RUNLOG_COLUMNS:
                raise OverviewConfigError(
                    f"Ops__RunLog header mismatch. Expected={RUNLOG_COLUMNS}, Existing={header}"
                )
            start_row = len(existing) + 1
        values = [[row.get(c, "") for c in RUNLOG_COLUMNS] for row in self.runlog_rows]
        runlog_ws.update(f"A{start_row}", values, value_input_option="USER_ENTERED")
        self.runlog_rows = []

    # ---------------- Config / bootstrap ----------------
    def bootstrap(self):
        c = self.cfg
        account_tab = c.get("ACCOUNT_TAB", "Cfg__account_id")
        sites_tab = c.get("SITES_TAB", "Cfg__Sites")
        console_core_url = _require_non_empty(c.get("CONSOLE_CORE_URL"), "CONSOLE_CORE_URL")
        bootstrap_secret = _require_non_empty(c.get("BOOTSTRAP_GSHEET_SA_B64_SECRET"), "BOOTSTRAP_GSHEET_SA_B64_SECRET")

        bootstrap_sa_b64 = _get_secret_required(bootstrap_secret, "BOOTSTRAP_GSHEET_SA_B64_SECRET")
        gc, sa_info = _authorize_gspread_from_sa_b64(bootstrap_sa_b64, bootstrap_secret)

        account_ws = _open_ws_by_url_and_title(gc, console_core_url, account_tab)
        account_cfg = _read_account_config_from_values(account_ws.get_all_values(), self.site_code, account_tab)

        required = [
            "SHOP_DOMAIN",
            "SHOPIFY_API_VERSION",
            "GSHEET_SA_B64_SECRET",
            "SHOPIFY_TOKEN_SECRET",
            "STOREFRONT_BASE_URL",
            "ADMIN_BASE_URL",
            "META_AD_ACCOUNT_ID",
            "AWIN_ADVERTISER_ID",
            "GOOGLE_ADS_CUSTOMER_ID",
            "GOOGLE_ADS_LOGIN_CUSTOMER_ID",
        ]
        _require_account_keys(account_cfg, required, account_tab)

        gs_secret = _require_non_empty(account_cfg.get("GSHEET_SA_B64_SECRET"), "GSHEET_SA_B64_SECRET")
        if gs_secret != bootstrap_secret:
            raise OverviewConfigError(
                "GSHEET_SA_B64_SECRET mismatch. "
                f"Cell 1 BOOTSTRAP_GSHEET_SA_B64_SECRET={bootstrap_secret}, "
                f"Cfg__account_id GSHEET_SA_B64_SECRET={gs_secret}. "
                "Do not fallback. Fix Console Core or Cell 1."
            )

        shopify_token_secret = _require_non_empty(account_cfg.get("SHOPIFY_TOKEN_SECRET"), "SHOPIFY_TOKEN_SECRET")
        shopify_access_token = _get_secret_required(shopify_token_secret, "SHOPIFY_TOKEN_SECRET")

        sites_ws = _open_ws_by_url_and_title(gc, console_core_url, sites_tab)
        sites_rows = _read_sites_rows(sites_ws.get_all_values(), sites_tab)

        config_label = c.get("CONFIG_SHEET_LABEL", "config")
        runlog_label = c.get("RUNLOG_SHEET_LABEL", "runlog_sheet")
        output_label = c.get("OUTPUT_SHEET_LABEL", "overview_refs")

        config_sheet_url = _find_site_sheet_url(sites_rows, self.site_code, config_label)
        runlog_sheet_url = _find_site_sheet_url(sites_rows, self.site_code, runlog_label)
        target_sheet_url = _find_site_sheet_url(sites_rows, self.site_code, output_label)

        target_sh = gc.open_by_url(target_sheet_url)
        runlog_sh = gc.open_by_url(runlog_sheet_url)
        runlog_ws = _open_ws_by_url_and_title(gc, runlog_sheet_url, c.get("RUNLOG_TAB", "Ops__RunLog"))

        google_ads_developer_token_secret = c.get("GOOGLE_ADS_DEVELOPER_TOKEN_SECRET", "GOOGLE_ADS_DEVELOPER_TOKEN")
        meta_access_token_secret = c.get("META_ACCESS_TOKEN_SECRET") or f"{self.site_code}_META_ACCESS_TOKEN"
        awin_access_token_secret = c.get("AWIN_ACCESS_TOKEN_SECRET") or f"{self.site_code}_AWIN_ACCESS_TOKEN"

        google_ads_developer_token = _get_secret_optional(google_ads_developer_token_secret)
        meta_access_token = _get_secret_optional(meta_access_token_secret)
        awin_access_token = _get_secret_optional(awin_access_token_secret)

        ads_json_path = "/tmp/google_ads_sa.json"
        with open(ads_json_path, "w", encoding="utf-8") as f:
            json.dump(sa_info, f, ensure_ascii=False)

        self.state.update({
            "gc": gc,
            "sa_info": sa_info,
            "account_cfg": account_cfg,
            "SHOP_DOMAIN": _require_non_empty(account_cfg.get("SHOP_DOMAIN"), "SHOP_DOMAIN"),
            "SHOPIFY_API_VERSION": _require_non_empty(account_cfg.get("SHOPIFY_API_VERSION"), "SHOPIFY_API_VERSION"),
            "SHOPIFY_ACCESS_TOKEN": shopify_access_token,
            "STOREFRONT_BASE_URL": str(account_cfg.get("STOREFRONT_BASE_URL") or "").strip(),
            "ADMIN_BASE_URL": str(account_cfg.get("ADMIN_BASE_URL") or "").strip(),
            "META_AD_ACCOUNT_ID": str(account_cfg.get("META_AD_ACCOUNT_ID") or "").strip(),
            "AWIN_ADVERTISER_ID": str(account_cfg.get("AWIN_ADVERTISER_ID") or "").strip(),
            "GOOGLE_ADS_CUSTOMER_ID": str(account_cfg.get("GOOGLE_ADS_CUSTOMER_ID") or "").strip(),
            "GOOGLE_ADS_LOGIN_CUSTOMER_ID": str(account_cfg.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "").strip(),
            "GOOGLE_ADS_DEVELOPER_TOKEN": google_ads_developer_token,
            "META_ACCESS_TOKEN": meta_access_token,
            "AWIN_ACCESS_TOKEN": awin_access_token,
            "GOOGLE_ADS_DEVELOPER_TOKEN_SECRET": google_ads_developer_token_secret,
            "META_ACCESS_TOKEN_SECRET": meta_access_token_secret,
            "AWIN_ACCESS_TOKEN_SECRET": awin_access_token_secret,
            "ADS_JSON_PATH": ads_json_path,
            "CONFIG_SHEET_URL": config_sheet_url,
            "RUNLOG_SHEET_URL": runlog_sheet_url,
            "TARGET_SHEET_URL": target_sheet_url,
            "sh": target_sh,
            "runlog_ws": runlog_ws,
        })

        self.log_row(
            phase="bootstrap",
            log_type="system",
            status="success",
            message=f"bootstrap ok | overview_refs={target_sheet_url} | runlog_sheet={runlog_sheet_url}",
        )
        self.flush_runlog()

        print("✅ Console Core 已连接")
        print(f"✅ SITE_CODE: {self.site_code}")
        print(f"✅ JOB_NAME: {self.job_name}")
        print(f"✅ SHOP_DOMAIN: {self.state['SHOP_DOMAIN']}")
        print(f"✅ SHOPIFY_API_VERSION: {self.state['SHOPIFY_API_VERSION']}")
        print(f"✅ Target sheet label: {output_label}")
        print(f"✅ Target sheet URL: {target_sheet_url}")
        print("✅ Shopify secret 已读取")

        self.compute_channel_availability()

    def compute_channel_availability(self):
        s = self.state
        has_google_ads = bool(str(s.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip() and str(s.get("GOOGLE_ADS_CUSTOMER_ID") or "").strip())
        has_meta_ads = bool(str(s.get("META_ACCESS_TOKEN") or "").strip() and str(s.get("META_AD_ACCOUNT_ID") or "").strip())
        has_awin = bool(str(s.get("AWIN_ACCESS_TOKEN") or "").strip() and str(s.get("AWIN_ADVERTISER_ID") or "").strip())
        s.update({"HAS_GOOGLE_ADS": has_google_ads, "HAS_META_ADS": has_meta_ads, "HAS_AWIN": has_awin})
        print("\n===== External Channel Availability =====")
        print(f"Google Ads available: {has_google_ads}")
        if not has_google_ads:
            print(f"⏭️ Google Ads unavailable: {self.channel_skip_reason('GOOGLE_ADS')}")
        print(f"Meta Ads available  : {has_meta_ads}")
        if not has_meta_ads:
            print(f"⏭️ Meta Ads unavailable: {self.channel_skip_reason('META_ADS')}")
        print(f"AWIN available      : {has_awin}")
        if not has_awin:
            print(f"⏭️ AWIN unavailable: {self.channel_skip_reason('AWIN')}")

    def channel_skip_reason(self, channel: str) -> str:
        s = self.state
        if channel == "GOOGLE_ADS":
            missing = []
            if not str(s.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip():
                missing.append(f"secret:{s.get('GOOGLE_ADS_DEVELOPER_TOKEN_SECRET')}")
            if not str(s.get("GOOGLE_ADS_CUSTOMER_ID") or "").strip():
                missing.append("Cfg__account_id:GOOGLE_ADS_CUSTOMER_ID")
            return ", ".join(missing)
        if channel == "META_ADS":
            missing = []
            if not str(s.get("META_ACCESS_TOKEN") or "").strip():
                missing.append(f"secret:{s.get('META_ACCESS_TOKEN_SECRET')}")
            if not str(s.get("META_AD_ACCOUNT_ID") or "").strip():
                missing.append("Cfg__account_id:META_AD_ACCOUNT_ID")
            return ", ".join(missing)
        if channel == "AWIN":
            missing = []
            if not str(s.get("AWIN_ACCESS_TOKEN") or "").strip():
                missing.append(f"secret:{s.get('AWIN_ACCESS_TOKEN_SECRET')}")
            if not str(s.get("AWIN_ADVERTISER_ID") or "").strip():
                missing.append("Cfg__account_id:AWIN_ADVERTISER_ID")
            return ", ".join(missing)
        return ""

    # ---------------- ShopifyQL ----------------
    def _extract_throttle_wait_seconds(self, resp_json, attempt):
        errs = resp_json.get("errors") or []
        for e in errs:
            ext = e.get("extensions") or {}
            if ext.get("code") != "THROTTLED":
                continue
            cost = ext.get("cost") or {}
            window_reset_at = cost.get("windowResetAt")
            if window_reset_at:
                try:
                    dt = datetime.fromisoformat(window_reset_at.replace("Z", "+00:00"))
                    secs = (dt - datetime.now(timezone.utc)).total_seconds()
                    return max(int(secs) + 2, 2)
                except Exception:
                    pass
        return int(self.cfg.get("SHOPIFYQL_BASE_SLEEP_SECONDS", 8)) * attempt

    def post_shopifyql(self, query_text: str) -> Dict[str, Any]:
        url = f"https://{self.state['SHOP_DOMAIN']}/admin/api/{self.state['SHOPIFY_API_VERSION']}/graphql.json"
        headers = {"X-Shopify-Access-Token": self.state["SHOPIFY_ACCESS_TOKEN"], "Content-Type": "application/json"}
        gql = """
        query RunShopifyQL($query: String!) {
          shopifyqlQuery(query: $query) {
            tableData { columns { name dataType displayName } rows }
            parseErrors
          }
        }
        """
        last_json = None
        max_retries = int(self.cfg.get("SHOPIFYQL_MAX_RETRIES", 6))
        timeout = int(self.cfg.get("SHOPIFYQL_TIMEOUT_SECONDS", 120))
        for attempt in range(1, max_retries + 1):
            resp = requests.post(url, headers=headers, json={"query": gql, "variables": {"query": query_text}}, timeout=timeout)
            resp.raise_for_status()
            j = resp.json()
            last_json = j
            errs = j.get("errors") or []
            throttled = any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs)
            if throttled:
                if attempt >= max_retries:
                    raise RuntimeError(errs)
                wait_seconds = self._extract_throttle_wait_seconds(j, attempt)
                self.dbg(f"⚠️ ShopifyQL 被限流，第 {attempt}/{max_retries} 次重试前等待 {wait_seconds} 秒")
                time.sleep(wait_seconds)
                continue
            if errs:
                raise RuntimeError(errs)
            sq = (j.get("data") or {}).get("shopifyqlQuery") or {}
            parse_errors = sq.get("parseErrors") or []
            if parse_errors:
                raise RuntimeError(f"ShopifyQL parseErrors: {parse_errors}")
            return j
        raise RuntimeError("ShopifyQL 请求失败：超过最大重试次数。最后一次响应：" + json.dumps(last_json, ensure_ascii=False)[:1500])

    def normalize_shopifyql_table(self, resp_json: Dict[str, Any]) -> pd.DataFrame:
        sq = (resp_json.get("data") or {}).get("shopifyqlQuery") or {}
        table_data = sq.get("tableData") or {}
        columns = table_data.get("columns") or []
        rows = table_data.get("rows") or []
        if not columns:
            raise RuntimeError("ShopifyQL 返回里没有 columns：" + json.dumps(resp_json, ensure_ascii=False)[:1500])
        headers = [c.get("name") or c.get("displayName") or "col" for c in columns]
        norm_rows = []
        for row in rows:
            if isinstance(row, dict):
                norm_rows.append([row.get(h, "") for h in headers])
            elif isinstance(row, list):
                norm_rows.append(row)
            else:
                norm_rows.append([""] * len(headers))
        return pd.DataFrame(norm_rows, columns=headers)

    def build_sales_query_monthly(self, start_date, end_date):
        return f"""
FROM sales
SHOW orders, gross_sales, net_sales, total_sales, shipping_charges, discounts, returns, taxes, customers, new_customers, returning_customers
SINCE {start_date} UNTIL {end_date}
GROUP BY month
ORDER BY month ASC
""".strip()

    def build_sessions_query_monthly(self, start_date, end_date):
        return f"""
FROM sessions
SHOW sessions, sessions_with_cart_additions, sessions_that_reached_checkout, sessions_that_completed_checkout
WHERE human_or_bot_session IN ('human')
SINCE {start_date} UNTIL {end_date}
GROUP BY month
ORDER BY month ASC
""".strip()

    def build_sales_query_daily(self, start_date, end_date):
        return f"""
FROM sales
SHOW orders, gross_sales, net_sales, total_sales, shipping_charges, discounts, returns, taxes, customers, new_customers, returning_customers
SINCE {start_date} UNTIL {end_date}
GROUP BY day
ORDER BY day ASC
""".strip()

    def build_sessions_query_daily(self, start_date, end_date):
        return f"""
FROM sessions
SHOW sessions, sessions_with_cart_additions, sessions_that_reached_checkout, sessions_that_completed_checkout
WHERE human_or_bot_session IN ('human')
SINCE {start_date} UNTIL {end_date}
GROUP BY day
ORDER BY day ASC
""".strip()

    def pull_shopify(self):
        c = self.cfg
        out = self.state
        if c.get("RUN_M_OUTPUT", True):
            self.dbg("\n===== M Range =====")
            self.dbg("M range mode :", out["M_RANGE_MODE"])
            self.dbg("M pull range :", out["M_PULL_START_DATE"], "->", out["M_PULL_END_DATE"])
            self.dbg("M write mode :", out["M_EFFECTIVE_WRITE_MODE"])

            if c.get("RUN_M_SHOPIFY_SALES", True):
                phase = "fetch_shopify_sales"
                q = self.build_sales_query_monthly(out["M_PULL_START_DATE"], out["M_PULL_END_DATE"])
                self.dbg("\n===== M / Shopify: Ref-M-S-Sales =====")
                self.dbg(q)
                df = self.normalize_shopifyql_table(self.post_shopifyql(q))
                self.dbg("Ref-M-S-Sales raw columns:", list(df.columns))
                self.dbg("Ref-M-S-Sales raw shape:", df.shape)
                df = clean_df_month(df)
                df = force_sales_numeric_columns(df)
                df = reorder_columns_case_insensitive(df, SALES_COLUMNS_M)
                out["df_m_sales"] = df
                self.log_row(phase, status="success", rows_loaded=len(df), message=f"M Shopify sales pulled rows={len(df)}")
            else:
                self.dbg("\n⏭️ Skip M / Shopify Sales")
                self.log_row("fetch_shopify_sales", status="skipped", rows_skipped=0, message="RUN_M_SHOPIFY_SALES=False")

            if c.get("RUN_M_SHOPIFY_SESSIONS", True):
                q = self.build_sessions_query_monthly(out["M_PULL_START_DATE"], out["M_PULL_END_DATE"])
                self.dbg("\n===== M / Shopify: Ref-M-S-Sessions =====")
                self.dbg(q)
                df = self.normalize_shopifyql_table(self.post_shopifyql(q))
                self.dbg("Ref-M-S-Sessions raw columns:", list(df.columns))
                self.dbg("Ref-M-S-Sessions raw shape:", df.shape)
                df = clean_df_month(df)
                df = force_numeric_columns_for_sheet(df, c.get("TAB_M_SESSIONS", "Ref-M-S-Sessions"))
                df = reorder_columns_case_insensitive(df, ["Month", "sessions", "sessions_with_cart_additions", "sessions_that_reached_checkout", "sessions_that_completed_checkout"])
                out["df_m_sessions"] = df
                self.log_row("fetch_shopify_sessions", status="success", rows_loaded=len(df), message=f"M Shopify sessions pulled rows={len(df)}")
            else:
                self.dbg("\n⏭️ Skip M / Shopify Sessions")
                self.log_row("fetch_shopify_sessions", status="skipped", rows_skipped=0, message="RUN_M_SHOPIFY_SESSIONS=False")

        if c.get("RUN_SE_OUTPUT", False):
            se_prev_start, se_prev_end, se_day_count = get_previous_period_range(c["SE_START_DATE"], c["SE_END_DATE"])
            out.update({"SE_PREV_START_DATE": se_prev_start, "SE_PREV_END_DATE": se_prev_end, "SE_DAY_COUNT": se_day_count})
            self.dbg("\n===== SE Period Range =====")
            self.dbg("Current :", c["SE_START_DATE"], "->", c["SE_END_DATE"])
            self.dbg("Previous:", se_prev_start, "->", se_prev_end)
            self.dbg("Day count:", se_day_count)

            q = self.build_sales_query_daily(se_prev_start, c["SE_END_DATE"])
            self.dbg("\n===== SE / Shopify: Ref-SE-S-Sales =====")
            self.dbg(q)
            df = self.normalize_shopifyql_table(self.post_shopifyql(q))
            self.dbg("Ref-SE-S-Sales raw columns:", list(df.columns))
            self.dbg("Ref-SE-S-Sales raw shape:", df.shape)
            df = df.rename(columns={"day": "Date"})
            df = clean_df_date(df)
            df = force_sales_numeric_columns(df)
            df = reorder_columns_case_insensitive(df, SALES_COLUMNS_SE)
            out["df_se_sales"] = df
            self.log_row("fetch_shopify_sales", status="success", rows_loaded=len(df), message=f"SE Shopify sales pulled rows={len(df)}")

            q = self.build_sessions_query_daily(se_prev_start, c["SE_END_DATE"])
            self.dbg("\n===== SE / Shopify: Ref-SE-S-Sessions =====")
            self.dbg(q)
            df = self.normalize_shopifyql_table(self.post_shopifyql(q))
            self.dbg("Ref-SE-S-Sessions raw columns:", list(df.columns))
            self.dbg("Ref-SE-S-Sessions raw shape:", df.shape)
            df = df.rename(columns={"day": "Date"})
            df = clean_df_date(df)
            df = force_numeric_columns_for_sheet(df, c.get("TAB_SE_SESSIONS", "Ref-SE-S-Sessions"))
            df = reorder_columns_case_insensitive(df, ["Date", "sessions", "sessions_with_cart_additions", "sessions_that_reached_checkout", "sessions_that_completed_checkout"])
            out["df_se_sessions"] = df
            self.log_row("fetch_shopify_sessions", status="success", rows_loaded=len(df), message=f"SE Shopify sessions pulled rows={len(df)}")

    # ---------------- Google Ads ----------------
    def build_google_ads_client(self):
        from google.ads.googleads.client import GoogleAdsClient
        import pkgutil

        config = {
            "developer_token": self.state["GOOGLE_ADS_DEVELOPER_TOKEN"],
            "use_proto_plus": True,
            "json_key_file_path": self.state["ADS_JSON_PATH"],
        }

        if self.state.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID"):
            config["login_customer_id"] = str(self.state["GOOGLE_ADS_LOGIN_CUSTOMER_ID"]).replace("-", "").strip()

        requested_version = str(self.cfg.get("GOOGLE_ADS_API_VERSION", "AUTO") or "AUTO").strip()

        def _installed_google_ads_versions():
            try:
                import google.ads.googleads as googleads_pkg
                versions = []
                for m in pkgutil.iter_modules(googleads_pkg.__path__):
                    name = str(m.name)
                    if re.fullmatch(r"v\d+", name):
                        versions.append(name)
                return sorted(versions, key=lambda x: int(x[1:]), reverse=True)
            except Exception:
                return []

        installed_versions = _installed_google_ads_versions()

        if requested_version and requested_version.upper() != "AUTO":
            requested_version = requested_version.lower()
            if not re.fullmatch(r"v\d+", requested_version):
                raise OverviewConfigError(
                    f"GOOGLE_ADS_API_VERSION 配置错误：{requested_version}。"
                    "应填写 AUTO 或类似 v20 / v21 / v22 / v23 / v24。"
                )

            if requested_version in installed_versions:
                api_version = requested_version
            else:
                api_version = installed_versions[0] if installed_versions else ""
                if not api_version:
                    raise OverviewConfigError(
                        "当前环境没有可用的 google.ads.googleads.vXX 模块。"
                        "请先在 Colab 安装：!pip -q install -U google-ads"
                    )
                self.dbg(
                    f"⚠️ 指定 GOOGLE_ADS_API_VERSION={requested_version}，"
                    f"但当前 google-ads 包未安装该版本；自动改用已安装最高版本：{api_version}"
                )
        else:
            api_version = installed_versions[0] if installed_versions else ""
            if not api_version:
                raise OverviewConfigError(
                    "当前环境没有可用的 google.ads.googleads.vXX 模块。"
                    "请先在 Colab 安装：!pip -q install -U google-ads"
                )

        self.state["GOOGLE_ADS_EFFECTIVE_API_VERSION"] = api_version
        self.dbg(f"✅ Google Ads API client version: {api_version}")

        return GoogleAdsClient.load_from_dict(config, version=api_version)

    def _run_gaql_to_rows(self, ga_service, customer_id: str, query: str, row_mapper):
        rows = []
        stream = ga_service.search_stream(customer_id=customer_id, query=query)
        for batch in stream:
            for r in batch.results:
                rows.append(row_mapper(r))
        return rows

    def fetch_google_ads_monthly(self, start_date, end_date):
        client = self.build_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")
        customer_id = str(self.state["GOOGLE_ADS_CUSTOMER_ID"]).replace("-", "").strip()
        query_traffic = f"""
        SELECT segments.month, metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND campaign.status IN ('ENABLED', 'PAUSED', 'REMOVED')
        ORDER BY segments.month
        """
        self.dbg("\n===== Google Ads / Monthly / traffic =====")
        self.dbg(query_traffic)
        traffic_rows = self._run_gaql_to_rows(ga_service, customer_id, query_traffic, lambda r: {
            "Month": month_to_yyyy_mm_dd(str(r.segments.month)),
            "Impression": int(r.metrics.impressions or 0),
            "Clicks": int(r.metrics.clicks or 0),
            "Cost": float((r.metrics.cost_micros or 0) / 1_000_000),
        })
        df_traffic = pd.DataFrame(traffic_rows).groupby("Month", as_index=False, dropna=False).agg({"Impression": "sum", "Clicks": "sum", "Cost": "sum"}).sort_values("Month").reset_index(drop=True) if traffic_rows else pd.DataFrame(columns=["Month", "Impression", "Clicks", "Cost"])
        query_purchase = f"""
        SELECT segments.month, segments.conversion_action_category, metrics.conversions, metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND campaign.status IN ('ENABLED', 'PAUSED', 'REMOVED')
          AND segments.conversion_action_category = PURCHASE
        ORDER BY segments.month
        """
        self.dbg("\n===== Google Ads / Monthly / purchase =====")
        self.dbg(query_purchase)
        purchase_rows = self._run_gaql_to_rows(ga_service, customer_id, query_purchase, lambda r: {
            "Month": month_to_yyyy_mm_dd(str(r.segments.month)),
            "Conversions": float(r.metrics.conversions or 0),
            "Conv.Value": float(r.metrics.conversions_value or 0),
        })
        df_purchase = pd.DataFrame(purchase_rows).groupby("Month", as_index=False, dropna=False).agg({"Conversions": "sum", "Conv.Value": "sum"}).sort_values("Month").reset_index(drop=True) if purchase_rows else pd.DataFrame(columns=["Month", "Conversions", "Conv.Value"])
        df = pd.merge(df_traffic, df_purchase, on="Month", how="outer").fillna(0)
        for col in ["Impression", "Clicks"]:
            df[col] = df[col].astype(int)
        for col in ["Cost", "Conversions", "Conv.Value"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return df[["Month", "Impression", "Clicks", "Cost", "Conversions", "Conv.Value"]].sort_values("Month").reset_index(drop=True)

    def fetch_google_ads_daily(self, start_date, end_date):
        client = self.build_google_ads_client()
        ga_service = client.get_service("GoogleAdsService")
        customer_id = str(self.state["GOOGLE_ADS_CUSTOMER_ID"]).replace("-", "").strip()
        query_traffic = f"""
        SELECT segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND campaign.status IN ('ENABLED', 'PAUSED', 'REMOVED')
        ORDER BY segments.date
        """
        self.dbg("\n===== Google Ads / Daily / traffic =====")
        self.dbg(query_traffic)
        traffic_rows = self._run_gaql_to_rows(ga_service, customer_id, query_traffic, lambda r: {
            "Date": date_to_yyyy_mm_dd(str(r.segments.date), default_day="01"),
            "Impression": int(r.metrics.impressions or 0),
            "Clicks": int(r.metrics.clicks or 0),
            "Cost": float((r.metrics.cost_micros or 0) / 1_000_000),
        })
        df_traffic = pd.DataFrame(traffic_rows).groupby("Date", as_index=False, dropna=False).agg({"Impression": "sum", "Clicks": "sum", "Cost": "sum"}).sort_values("Date").reset_index(drop=True) if traffic_rows else pd.DataFrame(columns=["Date", "Impression", "Clicks", "Cost"])
        query_purchase = f"""
        SELECT segments.date, segments.conversion_action_category, metrics.conversions, metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND campaign.status IN ('ENABLED', 'PAUSED', 'REMOVED')
          AND segments.conversion_action_category = PURCHASE
        ORDER BY segments.date
        """
        self.dbg("\n===== Google Ads / Daily / purchase =====")
        self.dbg(query_purchase)
        purchase_rows = self._run_gaql_to_rows(ga_service, customer_id, query_purchase, lambda r: {
            "Date": date_to_yyyy_mm_dd(str(r.segments.date), default_day="01"),
            "Conversions": float(r.metrics.conversions or 0),
            "Conv.Value": float(r.metrics.conversions_value or 0),
        })
        df_purchase = pd.DataFrame(purchase_rows).groupby("Date", as_index=False, dropna=False).agg({"Conversions": "sum", "Conv.Value": "sum"}).sort_values("Date").reset_index(drop=True) if purchase_rows else pd.DataFrame(columns=["Date", "Conversions", "Conv.Value"])
        df = pd.merge(df_traffic, df_purchase, on="Date", how="outer").fillna(0)
        for col in ["Impression", "Clicks"]:
            df[col] = df[col].astype(int)
        for col in ["Cost", "Conversions", "Conv.Value"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["CTR"] = df.apply(lambda r: (r["Clicks"] / r["Impression"]) if r["Impression"] else None, axis=1)
        df["A.CPC"] = df.apply(lambda r: (r["Cost"] / r["Clicks"]) if r["Clicks"] else None, axis=1)
        df["ROI"] = df.apply(lambda r: (r["Conv.Value"] / r["Cost"]) if r["Cost"] else None, axis=1)
        df["CVR"] = df.apply(lambda r: (r["Conversions"] / r["Clicks"]) if r["Clicks"] else None, axis=1)
        df["AOV"] = df.apply(lambda r: (r["Conv.Value"] / r["Conversions"]) if r["Conversions"] else None, axis=1)
        df["CPA"] = df.apply(lambda r: (r["Cost"] / r["Conversions"]) if r["Conversions"] else None, axis=1)
        return df[["Date", "Impression", "Clicks", "Cost", "Conversions", "Conv.Value", "CTR", "A.CPC", "ROI", "CVR", "AOV", "CPA"]].sort_values("Date").reset_index(drop=True)

    # ---------------- Meta Ads ----------------
    def _meta_graph_get(self, url, params):
        resp = requests.get(url, params=params, timeout=120)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        if resp.status_code >= 400:
            raise RuntimeError(f"Meta API error {resp.status_code}: {data}")
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"Meta API error: {data['error']}")
        return data

    def _action_value_map(self, items):
        d = {}
        if not items:
            return d
        for item in items:
            k = str(item.get("action_type", "")).strip()
            v = _to_float(item.get("value"), 0.0)
            d[k] = d.get(k, 0.0) + v
        return d

    def _pick_first_metric(self, action_map, key_candidates):
        for k in key_candidates:
            if k in action_map:
                return action_map[k]
        return 0.0

    def _date_chunks_by_month(self, start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        chunks, cur = [], start
        while cur <= end:
            next_month = cur.replace(year=cur.year + 1, month=1, day=1) if cur.month == 12 else cur.replace(month=cur.month + 1, day=1)
            chunk_end = min(next_month - timedelta(days=1), end)
            chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
            cur = chunk_end + timedelta(days=1)
        return chunks

    def _date_chunks_by_days(self, start_date, end_date, days=7):
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        chunks, cur = [], start
        while cur <= end:
            chunk_end = min(cur + timedelta(days=days - 1), end)
            chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
            cur = chunk_end + timedelta(days=1)
        return chunks

    def _fetch_meta_insights_daily_once(self, start_date, end_date):
        account_id = str(self.state["META_AD_ACCOUNT_ID"]).strip().replace("act_", "")
        url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/act_{account_id}/insights"
        params = {
            "access_token": self.state["META_ACCESS_TOKEN"],
            "level": "account",
            "time_increment": 1,
            "fields": "date_start,impressions,inline_link_clicks,spend,actions,action_values",
            "time_range": json.dumps({"since": start_date, "until": end_date}),
            "limit": 5000,
        }
        rows = []
        next_url, next_params = url, params.copy()
        while next_url:
            data = self._meta_graph_get(next_url, next_params)
            for r in data.get("data", []):
                actions_map = self._action_value_map(r.get("actions"))
                action_values_map = self._action_value_map(r.get("action_values"))
                purchase = self._pick_first_metric(actions_map, ["onsite_web_purchase", "offsite_conversion.fb_pixel_purchase", "purchase"])
                value = self._pick_first_metric(action_values_map, ["onsite_web_purchase", "offsite_conversion.fb_pixel_purchase", "purchase"])
                atc = self._pick_first_metric(actions_map, ["onsite_web_add_to_cart", "offsite_conversion.fb_pixel_add_to_cart", "add_to_cart"])
                checkout = self._pick_first_metric(actions_map, ["onsite_web_initiate_checkout", "offsite_conversion.fb_pixel_initiate_checkout", "initiate_checkout"])
                rows.append({"Date": date_to_yyyy_mm_dd(r.get("date_start"), default_day="01"), "Impr.": _to_int(r.get("impressions"), 0), "Clicks": _to_int(r.get("inline_link_clicks"), 0), "Spend": _to_float(r.get("spend"), 0.0), "Purchase": purchase, "Value": value, "Add to cart": atc, "Checkout": checkout})
            paging = data.get("paging", {}) if isinstance(data, dict) else {}
            next_url = paging.get("next")
            next_params = None
        return rows

    def _fetch_meta_insights_daily(self, start_date, end_date):
        chunks = self._date_chunks_by_month(start_date, end_date)
        self.dbg("\n===== Meta Ads / Daily / chunked =====")
        self.dbg(f"account_id: act_{str(self.state['META_AD_ACCOUNT_ID']).strip().replace('act_', '')}")
        self.dbg(f"time_range: {start_date} -> {end_date}")
        self.dbg(f"monthly chunks: {len(chunks)}")
        all_rows = []
        for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            self.dbg(f"Meta monthly chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end}")
            try:
                chunk_rows = self._fetch_meta_insights_daily_once(chunk_start, chunk_end)
                all_rows.extend(chunk_rows)
                self.dbg(f"  ✅ rows: {len(chunk_rows)}")
            except RuntimeError as e:
                msg = str(e)
                if "Please reduce the amount of data" in msg or "code': 1" in msg or '"code": 1' in msg:
                    weekly_chunks = self._date_chunks_by_days(chunk_start, chunk_end, days=7)
                    self.dbg(f"  ⚠️ monthly chunk too large, retry weekly chunks: {len(weekly_chunks)}")
                    for j, (week_start, week_end) in enumerate(weekly_chunks, start=1):
                        self.dbg(f"    Meta weekly chunk {j}/{len(weekly_chunks)}: {week_start} -> {week_end}")
                        week_rows = self._fetch_meta_insights_daily_once(week_start, week_end)
                        all_rows.extend(week_rows)
                        self.dbg(f"    ✅ rows: {len(week_rows)}")
                else:
                    raise
        if not all_rows:
            return pd.DataFrame(columns=["Date", "Impr.", "Clicks", "Spend", "Purchase", "Value", "Add to cart", "Checkout"])
        return pd.DataFrame(all_rows).groupby("Date", as_index=False, dropna=False).agg({"Impr.": "sum", "Clicks": "sum", "Spend": "sum", "Purchase": "sum", "Value": "sum", "Add to cart": "sum", "Checkout": "sum"}).sort_values("Date").reset_index(drop=True)

    def _shape_meta_output(self, df, date_header="Date"):
        header = [date_header, "Impr.", "Clicks", "CPC", "CPM", "CTR", "Spend", "Purchase", "Value", "ROI", "CVR", "AOV", "CPA(Purchase)", "Add to cart", "CPA(ATC)", "Checkout", "CPA(Checkout)"]
        if df is None or df.empty:
            return pd.DataFrame(columns=header)
        work = df.copy()
        if date_header == "Month":
            work["Month"] = work["Date"].apply(month_to_yyyy_mm_dd)
            work = work.groupby("Month", as_index=False, dropna=False).agg({"Impr.": "sum", "Clicks": "sum", "Spend": "sum", "Purchase": "sum", "Value": "sum", "Add to cart": "sum", "Checkout": "sum"}).sort_values("Month").reset_index(drop=True)
        for col in ["CPC", "CPM", "CTR", "ROI", "CVR", "AOV", "CPA(Purchase)", "CPA(ATC)", "CPA(Checkout)"]:
            work[col] = ""
        return work[header]

    def fetch_meta_ads_daily_output(self, start_date, end_date):
        return self._shape_meta_output(self._fetch_meta_insights_daily(start_date, end_date), date_header="Date")

    def fetch_meta_ads_monthly_output(self, start_date, end_date):
        return self._shape_meta_output(self._fetch_meta_insights_daily(start_date, end_date), date_header="Month")

    # ---------------- AWIN ----------------
    def _extract_list_from_awin_response(self, data):
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ["data", "transactions", "results", "items"]:
                if isinstance(data.get(key), list):
                    return data.get(key)
        return []

    def _extract_awin_date(self, row):
        for key in ["date", "transactionDate", "transaction_date", "transactionTime", "transaction_time", "validationDate", "validation_date", "amendDate", "amend_date"]:
            if key in row and row.get(key):
                return date_to_yyyy_mm_dd(row.get(key), default_day="01")
        return ""

    def _extract_awin_money(self, row, keys):
        for key in keys:
            if key not in row:
                continue
            v = row.get(key)
            if isinstance(v, dict):
                for inner_key in ["amount", "value", "total"]:
                    if inner_key in v:
                        return _to_float(v.get(inner_key), 0.0)
            if isinstance(v, list):
                total, found = 0.0, False
                for item in v:
                    if isinstance(item, dict):
                        for inner_key in ["amount", "value", "total"]:
                            if inner_key in item:
                                total += _to_float(item.get(inner_key), 0.0)
                                found = True
                                break
                    else:
                        total += _to_float(item, 0.0)
                        found = True
                if found:
                    return total
            return _to_float(v, 0.0)
        return 0.0

    def _awin_date_chunks_31_days(self, start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        chunks, cur = [], start
        while cur <= end:
            chunk_end = min(cur + timedelta(days=30), end)
            chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
            cur = chunk_end + timedelta(days=1)
        return chunks

    def _awin_get_transactions_once(self, start_date, end_date):
        advertiser_id = str(self.state["AWIN_ADVERTISER_ID"]).strip()
        date_type = self.cfg.get("AWIN_DATE_TYPE", "transaction") or "transaction"
        awin_status = self.cfg.get("AWIN_STATUS", "") or ""
        timezone_name = self.cfg.get("AWIN_TIMEZONE", "UTC") or "UTC"
        cache_key = (advertiser_id, start_date, end_date, str(date_type), str(awin_status), str(timezone_name))
        if cache_key in self._awin_tx_cache:
            self.dbg(f"  ♻️ AWIN cache hit: {start_date} -> {end_date}")
            return self._awin_tx_cache[cache_key]
        url = f"{AWIN_API_BASE}/advertisers/{advertiser_id}/transactions/"
        token = self.state["AWIN_ACCESS_TOKEN"]
        params = {"accessToken": token, "startDate": f"{start_date}T00:00:00", "endDate": f"{end_date}T23:59:59", "dateType": date_type, "timezone": timezone_name}
        if str(awin_status).strip():
            params["status"] = str(awin_status).strip()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        last_error = None
        for attempt in range(1, AWIN_MAX_RETRIES + 1):
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=120)
                try:
                    data = resp.json()
                except Exception:
                    data = {"raw_text": resp.text}
                if resp.status_code == 429:
                    wait_seconds = AWIN_429_SLEEP_SECONDS + random.uniform(0, 5)
                    self.dbg(f"  ⚠️ AWIN 429 rate limited: {start_date} -> {end_date}; attempt {attempt}/{AWIN_MAX_RETRIES}; sleep {wait_seconds:.1f}s")
                    time.sleep(wait_seconds)
                    last_error = RuntimeError(f"AWIN API error 429: {data}")
                    continue
                if 500 <= resp.status_code < 600:
                    wait_seconds = min(90, 8 * attempt) + random.uniform(0, 3)
                    self.dbg(f"  ⚠️ AWIN {resp.status_code} server error: {start_date} -> {end_date}; attempt {attempt}/{AWIN_MAX_RETRIES}; sleep {wait_seconds:.1f}s")
                    time.sleep(wait_seconds)
                    last_error = RuntimeError(f"AWIN API error {resp.status_code}: {data}")
                    continue
                if resp.status_code >= 400:
                    raise RuntimeError(f"AWIN API error {resp.status_code}: {data}")
                rows = self._extract_list_from_awin_response(data)
                self._awin_tx_cache[cache_key] = rows
                time.sleep(AWIN_REQUEST_SLEEP_SECONDS)
                return rows
            except RuntimeError:
                raise
            except Exception as e:
                last_error = e
                wait_seconds = min(90, 8 * attempt) + random.uniform(0, 3)
                self.dbg(f"  ⚠️ AWIN request exception: {start_date} -> {end_date}; attempt {attempt}/{AWIN_MAX_RETRIES}; sleep {wait_seconds:.1f}s; error={e}")
                time.sleep(wait_seconds)
        raise RuntimeError(f"AWIN request failed after {AWIN_MAX_RETRIES} retries: {start_date} -> {end_date}; last_error={last_error}")

    def _awin_get_transactions(self, start_date, end_date):
        chunks = self._awin_date_chunks_31_days(start_date, end_date)
        self.dbg("\n===== AWIN / Transactions / chunked =====")
        self.dbg(f"advertiser_id: {self.state['AWIN_ADVERTISER_ID']}")
        self.dbg(f"time_range: {start_date} -> {end_date}")
        self.dbg(f"dateType: {self.cfg.get('AWIN_DATE_TYPE', 'transaction') or 'transaction'}")
        self.dbg(f"status: {self.cfg.get('AWIN_STATUS') if str(self.cfg.get('AWIN_STATUS') or '').strip() else 'ALL'}")
        self.dbg(f"chunks: {len(chunks)}")
        self.dbg(f"rate_limit_control: sleep {AWIN_REQUEST_SLEEP_SECONDS}s/request")
        all_rows = []
        for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            self.dbg(f"AWIN chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end}")
            chunk_rows = self._awin_get_transactions_once(chunk_start, chunk_end)
            all_rows.extend(chunk_rows)
            self.dbg(f"  ✅ rows: {len(chunk_rows)}")
        self.dbg(f"AWIN transactions pulled total: {len(all_rows)}")
        return all_rows

    def _fetch_awin_daily_raw(self, start_date, end_date):
        tx_rows = self._awin_get_transactions(start_date, end_date)
        rows = []
        for r in tx_rows:
            if not isinstance(r, dict):
                continue
            tx_date = self._extract_awin_date(r)
            if not tx_date:
                continue
            sale_amount = self._extract_awin_money(r, ["sale_amount", "saleAmount", "sale_amount_amount", "amount", "transactionAmount", "transaction_amount"])
            commission = self._extract_awin_money(r, ["commission", "commissionAmount", "commission_amount", "publisherCommission", "publisher_commission"])
            rows.append({"Date": tx_date, "Sale Amount": sale_amount, "Commission": commission})
        if not rows:
            return pd.DataFrame(columns=["Date", "Sale Amount", "Commission"])
        return pd.DataFrame(rows).groupby("Date", as_index=False, dropna=False).agg({"Sale Amount": "sum", "Commission": "sum"}).sort_values("Date").reset_index(drop=True)

    def _shape_awin_output(self, df, monthly=False):
        header = ["Month", "Sale Amount", "Commission", "ROI"] if monthly else ["Date", "Sale Amount", "Commission", "ROI"]
        if df is None or df.empty:
            return pd.DataFrame(columns=header)
        work = df.copy()
        if monthly:
            work["Month"] = work["Date"].apply(month_to_yyyy_mm_dd)
            work = work.groupby("Month", as_index=False, dropna=False).agg({"Sale Amount": "sum", "Commission": "sum"}).sort_values("Month").reset_index(drop=True)
        else:
            work = work[["Date", "Sale Amount", "Commission"]].copy()
        work["ROI"] = ""
        return work[header]

    def fetch_awin_daily_output(self, start_date, end_date):
        return self._shape_awin_output(self._fetch_awin_daily_raw(start_date, end_date), monthly=False)

    def fetch_awin_monthly_output(self, start_date, end_date):
        return self._shape_awin_output(self._fetch_awin_daily_raw(start_date, end_date), monthly=True)

    def pull_external_ads(self):
        c, s = self.cfg, self.state
        if c.get("RUN_M_OUTPUT", True):
            self.dbg("\n===== M / External Ads Range =====")
            self.dbg("M range mode :", s["M_RANGE_MODE"])
            self.dbg("M pull range :", s["M_PULL_START_DATE"], "->", s["M_PULL_END_DATE"])
            if c.get("RUN_M_GOOGLE_ADS", True) and s["HAS_GOOGLE_ADS"]:
                df = self.fetch_google_ads_monthly(s["M_PULL_START_DATE"], s["M_PULL_END_DATE"])
                s["df_m_ads"] = df
                self.log_row("fetch_google_ads", status="success", rows_loaded=len(df), message=f"M Google Ads pulled rows={len(df)}")
            else:
                msg = self.channel_skip_reason("GOOGLE_ADS") if c.get("RUN_M_GOOGLE_ADS", True) else "RUN_M_GOOGLE_ADS=False"
                self.dbg(f"\n⏭️ Skip M / Google Ads: {msg}")
                self.log_row("fetch_google_ads", status="skipped", rows_skipped=0, message=msg)
            if c.get("RUN_M_META_ADS", True) and s["HAS_META_ADS"]:
                df = self.fetch_meta_ads_monthly_output(s["M_PULL_START_DATE"], s["M_PULL_END_DATE"])
                s["df_m_meta_ads"] = df
                self.log_row("fetch_meta_ads", status="success", rows_loaded=len(df), message=f"M Meta Ads pulled rows={len(df)}")
            else:
                msg = self.channel_skip_reason("META_ADS") if c.get("RUN_M_META_ADS", True) else "RUN_M_META_ADS=False"
                self.dbg(f"\n⏭️ Skip M / Meta Ads: {msg}")
                self.log_row("fetch_meta_ads", status="skipped", rows_skipped=0, message=msg)
            if c.get("RUN_M_AWIN", True) and s["HAS_AWIN"]:
                df = self.fetch_awin_monthly_output(s["M_PULL_START_DATE"], s["M_PULL_END_DATE"])
                s["df_m_awin"] = df
                self.log_row("fetch_awin", status="success", rows_loaded=len(df), message=f"M AWIN pulled rows={len(df)}")
            else:
                msg = self.channel_skip_reason("AWIN") if c.get("RUN_M_AWIN", True) else "RUN_M_AWIN=False"
                self.dbg(f"\n⏭️ Skip M / AWIN: {msg}")
                self.log_row("fetch_awin", status="skipped", rows_skipped=0, message=msg)

        if c.get("RUN_SE_OUTPUT", False):
            self.dbg("\n===== SE / External Ads Range =====")
            self.dbg("SE previous + current pull range:", s["SE_PREV_START_DATE"], "->", c["SE_END_DATE"])
            if c.get("RUN_SE_GOOGLE_ADS", True) and s["HAS_GOOGLE_ADS"]:
                df = self.fetch_google_ads_daily(s["SE_PREV_START_DATE"], c["SE_END_DATE"])
                s["df_se_ads"] = df
                self.log_row("fetch_google_ads", status="success", rows_loaded=len(df), message=f"SE Google Ads pulled rows={len(df)}")
            else:
                msg = self.channel_skip_reason("GOOGLE_ADS") if c.get("RUN_SE_GOOGLE_ADS", True) else "RUN_SE_GOOGLE_ADS=False"
                self.dbg(f"\n⏭️ Skip SE / Google Ads: {msg}")
                self.log_row("fetch_google_ads", status="skipped", rows_skipped=0, message=msg)
            if c.get("RUN_SE_META_ADS", True) and s["HAS_META_ADS"]:
                df = self.fetch_meta_ads_daily_output(s["SE_PREV_START_DATE"], c["SE_END_DATE"])
                s["df_se_meta_ads"] = df
                self.log_row("fetch_meta_ads", status="success", rows_loaded=len(df), message=f"SE Meta Ads pulled rows={len(df)}")
            else:
                msg = self.channel_skip_reason("META_ADS") if c.get("RUN_SE_META_ADS", True) else "RUN_SE_META_ADS=False"
                self.dbg(f"\n⏭️ Skip SE / Meta Ads: {msg}")
                self.log_row("fetch_meta_ads", status="skipped", rows_skipped=0, message=msg)
            if c.get("RUN_SE_AWIN", True) and s["HAS_AWIN"]:
                df = self.fetch_awin_daily_output(s["SE_PREV_START_DATE"], c["SE_END_DATE"])
                s["df_se_awin"] = df
                self.log_row("fetch_awin", status="success", rows_loaded=len(df), message=f"SE AWIN pulled rows={len(df)}")
            else:
                msg = self.channel_skip_reason("AWIN") if c.get("RUN_SE_AWIN", True) else "RUN_SE_AWIN=False"
                self.dbg(f"\n⏭️ Skip SE / AWIN: {msg}")
                self.log_row("fetch_awin", status="skipped", rows_skipped=0, message=msg)

    # ---------------- Write helpers ----------------
    def _get_or_create_ws(self, spreadsheet, title, rows=1000, cols=30):
        try:
            return spreadsheet.worksheet(title)
        except Exception:
            return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

    def clear_and_write_df(self, spreadsheet, title: str, df: pd.DataFrame):
        df = normalize_df_before_write(df, title)
        try:
            ws = spreadsheet.worksheet(title)
            ws.clear()
        except Exception:
            ws = spreadsheet.add_worksheet(title=title, rows=max(len(df) + 20, 1000), cols=max(len(df.columns) + 10, 20))
            ws.clear()
        values = [list(df.columns)] + df.where(pd.notnull(df), "").values.tolist()
        ws.update("A1", values, value_input_option="USER_ENTERED")
        return ws

    def write_df(self, spreadsheet, title, df, write_mode=None):
        mode = (write_mode or self.cfg.get("WRITE_MODE") or "REPLACE").strip().upper()
        if mode == "REPLACE":
            return self.clear_and_write_df(spreadsheet, title, df)
        raise ValueError(f"Unsupported WRITE_MODE: {write_mode}")

    def _sheet_values_to_df(self, values):
        if not values:
            return pd.DataFrame()
        header, body = values[0], values[1:]
        if not header:
            return pd.DataFrame()
        width = len(header)
        normalized_rows = []
        for row in body:
            row = list(row)
            if len(row) < width:
                row = row + [""] * (width - len(row))
            elif len(row) > width:
                row = row[:width]
            if any(str(x).strip() != "" for x in row):
                normalized_rows.append(row)
        return pd.DataFrame(normalized_rows, columns=header)

    def _find_month_key_col(self, df, preferred=None):
        cols = list(df.columns) if df is not None else []
        if preferred and preferred in cols:
            return preferred
        for candidate in ["Month", "Date", "month", "date"]:
            if candidate in cols:
                return candidate
        for col in cols:
            if str(col).strip().lower() in ["month", "date"]:
                return col
        raise ValueError(f"Cannot find Month/Date column. columns={cols}")

    def _month_key_value(self, v):
        if v is None or str(v).strip() == "":
            return ""
        s = str(v).strip()
        if s.startswith("'"):
            s = s[1:].strip()
        return month_to_yyyy_mm_dd(s)

    def _month_in_refresh_range(self, v):
        month_value = self._month_key_value(v)
        if not month_value:
            return False
        start_month = month_to_yyyy_mm_dd(self.state["M_PULL_START_DATE"])
        end_month = month_to_yyyy_mm_dd(self.state["M_PULL_END_DATE"])
        return start_month <= month_value <= end_month

    def _sort_df_by_month_or_date(self, df):
        if df is None or df.empty:
            return df
        try:
            key_col = self._find_month_key_col(df)
        except Exception:
            return df
        work = df.copy()
        def sort_key(v):
            s = str(v).strip()
            if s.startswith("'"):
                s = s[1:].strip()
            s = month_to_yyyy_mm_dd(s)
            try:
                return pd.to_datetime(s, format="%Y/%m/%d", errors="coerce")
            except Exception:
                return pd.NaT
        work["_sort_key__"] = work[key_col].apply(sort_key)
        return work.sort_values("_sort_key__", kind="stable").drop(columns=["_sort_key__"]).reset_index(drop=True)

    def write_m_incremental_month_replace(self, spreadsheet, title, df_new, preferred_key_col=None):
        df_new = normalize_df_before_write(df_new, title)
        ws = self._get_or_create_ws(spreadsheet, title, rows=max(len(df_new) + 200, 1000), cols=max(len(df_new.columns) + 10, 30))
        df_old = self._sheet_values_to_df(ws.get_all_values())
        if df_old.empty:
            final_df = df_new.copy()
            self.dbg(f"🆕 {title}: old empty, write new rows={len(final_df)}")
        else:
            canonical_cols = list(df_new.columns) if len(df_new.columns) > 0 else list(df_old.columns)
            for col in canonical_cols:
                if col not in df_old.columns:
                    df_old[col] = ""
                if col not in df_new.columns:
                    df_new[col] = ""
            df_old = df_old[canonical_cols]
            df_new = df_new[canonical_cols]
            key_col = self._find_month_key_col(df_old, preferred=preferred_key_col)
            old_total = len(df_old)
            df_old_keep = df_old[~df_old[key_col].apply(self._month_in_refresh_range)].copy()
            old_removed = old_total - len(df_old_keep)
            if len(df_new) > 0:
                new_key_col = self._find_month_key_col(df_new, preferred=preferred_key_col)
                df_new = df_new[df_new[new_key_col].apply(self._month_in_refresh_range)].copy()
            final_df = pd.concat([df_old_keep, df_new], ignore_index=True)
            final_df = self._sort_df_by_month_or_date(final_df)
            self.dbg(f"🔁 {title}: incremental month replace | old={old_total}, removed_in_range={old_removed}, new={len(df_new)}, final={len(final_df)}")
        ws.clear()
        values = [list(final_df.columns)] + final_df.where(pd.notnull(final_df), "").values.tolist()
        ws.update("A1", values, value_input_option="USER_ENTERED")
        return ws, len(final_df)

    def write_m_df(self, spreadsheet, title, df, preferred_key_col=None):
        mode = str(self.state.get("M_EFFECTIVE_WRITE_MODE") or "INCREMENTAL_MONTH_REPLACE").strip().upper()
        if mode == "INCREMENTAL_MONTH_REPLACE":
            return self.write_m_incremental_month_replace(spreadsheet, title, df, preferred_key_col=preferred_key_col)
        ws = self.write_df(spreadsheet, title, df, mode)
        return ws, len(df)

    def write_se_df(self, spreadsheet, title, df):
        mode = str(self.cfg.get("SE_WRITE_MODE", "REPLACE") or "REPLACE").strip().upper()
        ws = self.write_df(spreadsheet, title, df, mode)
        return ws, len(df)

    def apply_ref_gg_a_formulas(self, ws, row_count, date_header="Month"):
        headers = [date_header, "Impression", "Clicks", "Cost", "Conversions", "Conv.Value", "CTR", "A.CPC", "ROI", "CVR", "AOV", "CPA"]
        ws.update("A1:L1", [headers], value_input_option="USER_ENTERED")
        if row_count <= 0:
            return
        formula_rows = []
        for r in range(2, row_count + 2):
            formula_rows.append([f"=IFERROR(C{r}/B{r})", f"=IFERROR(D{r}/C{r})", f"=IFERROR(F{r}/D{r})", f"=IFERROR(E{r}/C{r})", f"=IFERROR(F{r}/E{r})", f"=IFERROR(D{r}/E{r})"])
        ws.update(f"G2:L{row_count + 1}", formula_rows, value_input_option="USER_ENTERED")

    def apply_ref_meta_a_formulas(self, ws, row_count, date_header="Date"):
        headers = [date_header, "Impr.", "Clicks", "CPC", "CPM", "CTR", "Spend", "Purchase", "Value", "ROI", "CVR", "AOV", "CPA(Purchase)", "Add to cart", "CPA(ATC)", "Checkout", "CPA(Checkout)"]
        ws.update("A1:Q1", [headers], value_input_option="USER_ENTERED")
        if row_count <= 0:
            return
        formula_rows_d_to_f, formula_rows_j_to_m, formula_rows_o, formula_rows_q = [], [], [], []
        for r in range(2, row_count + 2):
            formula_rows_d_to_f.append([f"=IFERROR(G{r}/C{r})", f"=IFERROR(G{r}/B{r}*1000)", f"=IFERROR(C{r}/B{r})"])
            formula_rows_j_to_m.append([f"=IFERROR(I{r}/G{r})", f"=IFERROR(H{r}/C{r})", f"=IFERROR(I{r}/H{r})", f"=IFERROR(G{r}/H{r})"])
            formula_rows_o.append([f"=IFERROR(G{r}/N{r})"])
            formula_rows_q.append([f"=IFERROR(G{r}/P{r})"])
        ws.update(f"D2:F{row_count + 1}", formula_rows_d_to_f, value_input_option="USER_ENTERED")
        ws.update(f"J2:M{row_count + 1}", formula_rows_j_to_m, value_input_option="USER_ENTERED")
        ws.update(f"O2:O{row_count + 1}", formula_rows_o, value_input_option="USER_ENTERED")
        ws.update(f"Q2:Q{row_count + 1}", formula_rows_q, value_input_option="USER_ENTERED")

    def apply_awin_formulas_with_header(self, ws, row_count, date_header="Date"):
        headers = [date_header, "Sale Amount", "Commission", "ROI"]
        ws.update("A1:D1", [headers], value_input_option="USER_ENTERED")
        if row_count <= 0:
            return
        formula_rows = [[f"=IFERROR(B{r}/C{r})"] for r in range(2, row_count + 2)]
        ws.update(f"D2:D{row_count + 1}", formula_rows, value_input_option="USER_ENTERED")

    def write_se_control_dates(self):
        if not self.cfg.get("RUN_SE_OUTPUT", False):
            return
        tab = self.cfg.get("TAB_SE_OVERVIEW", "SE数据")
        ws = _open_ws_by_url_and_title(self.state["gc"], self.state["TARGET_SHEET_URL"], tab)
        ws.update("A4:B4", [[self.cfg["SE_START_DATE"], self.cfg["SE_END_DATE"]]], value_input_option="USER_ENTERED")
        self.log_row("write_se_control_dates", status="success", rows_written=1, message=f"{tab}!A4:B4 written | {self.cfg['SE_START_DATE']} -> {self.cfg['SE_END_DATE']}")

    def write_outputs(self):
        c, s, sh = self.cfg, self.state, self.state["sh"]
        if c.get("RUN_M_OUTPUT", True):
            m_write_label = "M_" + str(s.get("M_EFFECTIVE_WRITE_MODE") or "").upper()
            pairs = [
                ("df_m_sales", c.get("TAB_M_SALES", "Ref-M-S-Sales"), c.get("RUN_M_SHOPIFY_SALES", True), None, "Month"),
                ("df_m_sessions", c.get("TAB_M_SESSIONS", "Ref-M-S-Sessions"), c.get("RUN_M_SHOPIFY_SESSIONS", True), None, "Month"),
                ("df_m_ads", c.get("TAB_M_ADS", "Ref-M-GG-A"), c.get("RUN_M_GOOGLE_ADS", True), self.apply_ref_gg_a_formulas, "Month"),
                ("df_m_meta_ads", c.get("TAB_M_META_ADS", "Ref-M-Meta-A"), c.get("RUN_M_META_ADS", True), self.apply_ref_meta_a_formulas, "Month"),
                ("df_m_awin", c.get("TAB_M_AWIN", "Ref-M-AWIN"), c.get("RUN_M_AWIN", True), self.apply_awin_formulas_with_header, "Month"),
            ]
            for df_key, title, enabled, formula_func, date_header in pairs:
                if enabled and df_key in s:
                    ws, final_rows = self.write_m_df(sh, title, s[df_key], preferred_key_col="Month")
                    if formula_func:
                        formula_func(ws, final_rows, date_header=date_header)
                    self.written_info.append((title, len(s[df_key]), final_rows, m_write_label))
                    self.log_row("write_outputs", status="success", rows_planned=len(s[df_key]), rows_written=final_rows, message=f"{title} written | mode={m_write_label}")

        if c.get("RUN_SE_OUTPUT", False):
            pairs = [
                ("df_se_sales", c.get("TAB_SE_SALES", "Ref-SE-S-Sales"), None, "Date"),
                ("df_se_sessions", c.get("TAB_SE_SESSIONS", "Ref-SE-S-Sessions"), None, "Date"),
                ("df_se_ads", c.get("TAB_SE_ADS", "Ref-SE-GG-A"), self.apply_ref_gg_a_formulas, "Date"),
                ("df_se_meta_ads", c.get("TAB_SE_META_ADS", "Ref-SE-Meta-A"), self.apply_ref_meta_a_formulas, "Date"),
                ("df_se_awin", c.get("TAB_SE_AWIN", "Ref-SE-AWIN"), self.apply_awin_formulas_with_header, "Date"),
            ]
            for df_key, title, formula_func, date_header in pairs:
                if df_key in s:
                    ws, final_rows = self.write_se_df(sh, title, s[df_key])
                    if formula_func:
                        formula_func(ws, final_rows, date_header=date_header)
                    self.written_info.append((title, len(s[df_key]), final_rows, "SE_REPLACE"))
                    self.log_row("write_outputs", status="success", rows_planned=len(s[df_key]), rows_written=final_rows, message=f"{title} written | mode=SE_REPLACE")
            self.write_se_control_dates()

    def derive_ranges(self):
        c = self.cfg
        if c.get("RUN_M_OUTPUT", True):
            if bool(c.get("RUN_M_DATE_RANGE", False)) == bool(c.get("RUN_M_REFRESH_RANGE", True)):
                raise ValueError("M Range Controller 配置错误：RUN_M_DATE_RANGE 和 RUN_M_REFRESH_RANGE 必须二选一。")
            if c.get("RUN_M_DATE_RANGE", False):
                self.state["M_PULL_START_DATE"] = c["M_START_DATE"]
                self.state["M_PULL_END_DATE"] = c["M_END_DATE"]
                self.state["M_EFFECTIVE_WRITE_MODE"] = c.get("M_DATE_RANGE_WRITE_MODE", "REPLACE")
                self.state["M_RANGE_MODE"] = "DATE_RANGE_FULL_REPLACE"
            else:
                self.state["M_PULL_START_DATE"] = c["M_REFRESH_START_DATE"]
                self.state["M_PULL_END_DATE"] = c["M_REFRESH_END_DATE"]
                self.state["M_EFFECTIVE_WRITE_MODE"] = c.get("M_REFRESH_RANGE_WRITE_MODE", "INCREMENTAL_MONTH_REPLACE")
                self.state["M_RANGE_MODE"] = "REFRESH_RANGE_INCREMENTAL_MONTH_REPLACE"
        else:
            self.state.update({"M_PULL_START_DATE": None, "M_PULL_END_DATE": None, "M_EFFECTIVE_WRITE_MODE": None, "M_RANGE_MODE": "M_DISABLED"})

    def print_summary(self):
        c, s = self.cfg, self.state
        print("\n==============================")
        print("✅ 写入完成")
        print(f"RUN_M_OUTPUT: {c.get('RUN_M_OUTPUT', True)}")
        print(f"RUN_SE_OUTPUT: {c.get('RUN_SE_OUTPUT', False)}")
        if c.get("RUN_M_OUTPUT", True):
            print(f"M_RANGE_MODE: {s.get('M_RANGE_MODE')}")
            print(f"M_EFFECTIVE_WRITE_MODE: {s.get('M_EFFECTIVE_WRITE_MODE')}")
            print(f"M pull range: {s.get('M_PULL_START_DATE')} -> {s.get('M_PULL_END_DATE')}")
            print("\nM module toggles:")
            print(f"  Shopify Sales   : {c.get('RUN_M_SHOPIFY_SALES', True)}")
            print(f"  Shopify Sessions: {c.get('RUN_M_SHOPIFY_SESSIONS', True)}")
            print(f"  Google Ads      : {c.get('RUN_M_GOOGLE_ADS', True)}")
            print(f"  Meta Ads        : {c.get('RUN_M_META_ADS', True)}")
            print(f"  AWIN            : {c.get('RUN_M_AWIN', True)}")
        if c.get("RUN_SE_OUTPUT", False):
            print(f"\nSE_WRITE_MODE: {c.get('SE_WRITE_MODE', 'REPLACE')}")
            print(f"SE current : {c.get('SE_START_DATE')} -> {c.get('SE_END_DATE')}")
            print(f"SE previous: {s.get('SE_PREV_START_DATE')} -> {s.get('SE_PREV_END_DATE')}")
        print("\nWritten tabs:")
        for title, new_cnt, final_cnt, mode in self.written_info:
            print(f"{title}: new_rows={new_cnt}, final_rows={final_cnt}, mode={mode}")

    def execute(self):
        try:
            self.derive_ranges()
            self.bootstrap()
            self.log_row("start", log_type="system", status="success", message="run started")
            self.pull_shopify()
            self.pull_external_ads()
            self.write_outputs()
            self.log_row("done", status="success", rows_written=sum(x[2] for x in self.written_info), message="run completed")
            self.flush_runlog()
            self.print_summary()
            return {
                "status": "success",
                "run_id": self.run_id,
                "site_code": self.site_code,
                "job_name": self.job_name,
                "target_sheet_url": self.state.get("TARGET_SHEET_URL"),
                "runlog_sheet_url": self.state.get("RUNLOG_SHEET_URL"),
                "written_info": self.written_info,
                "m_range_mode": self.state.get("M_RANGE_MODE"),
                "m_pull_start_date": self.state.get("M_PULL_START_DATE"),
                "m_pull_end_date": self.state.get("M_PULL_END_DATE"),
            }
        except Exception as e:
            try:
                self.log_row("error", log_type="system", status="failed", message="run failed", error_reason=str(e)[:1000])
                self.flush_runlog()
            except Exception:
                pass
            raise


# ============================================
# Public run entry
# ============================================

def run(**kwargs):
    runner = OverviewRefsRunner(**kwargs)
    return runner.execute()

