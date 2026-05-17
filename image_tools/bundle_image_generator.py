# ecom/image_tools/bundle_image_generator.py

from __future__ import annotations

import base64
import io
import json
import os
import re
import time
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import requests
from PIL import Image


try:
    RESAMPLE_LANCZOS = Image.Resampling.LANCZOS
except AttributeError:
    RESAMPLE_LANCZOS = Image.LANCZOS


# =========================
# Constants
# =========================

REQUIRED_INPUT_COLUMNS = [
    "sku",
    "template_name",
    "image_url",
    "front_url",
    "angle_url",
    "side_url",
    "output_name",
    "note",
]

REQUIRED_TEMPLATE_COLUMNS = [
    "template_name",
    "template_display_name",
    "template_desc",
    "qty_1_desc",
    "qty_2_desc",
    "qty_5_desc",
    "qty_10_desc",
    "qty_20_desc",
    "qty_30_desc",
    "image_1_url",
    "image_2_url",
    "image_5_url",
    "image_10_url",
    "image_20_url",
    "image_30_url",
]

SUPPORTED_QUANTITIES = [1, 2, 5, 10, 20, 30]

VIEW_KEYS = ["image", "front", "angle", "side"]


# =========================
# Layout model
# =========================

@dataclass
class Placement:
    view: str
    x: float
    y: float
    w: float
    h: float
    anchor: str = "center"


@dataclass
class TemplateSpec:
    template_name: str
    quantity_to_placements: Dict[int, List[Placement]]


# =========================
# Utility
# =========================

def normalize_header(value: Any) -> str:
    return str(value).strip()


def clean_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def extract_sheet_id(sheet_url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError(f"Cannot extract Google Sheet ID from URL: {sheet_url}")
    return m.group(1)


def safe_filename(value: str) -> str:
    value = clean_cell(value)
    value = re.sub(r"[^\w\-\.]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "unnamed"


def hash_url(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def parse_service_account_secret(secret_value: str) -> dict:
    """
    Supports:
    1. Raw JSON string
    2. Base64 encoded JSON string
    """
    secret_value = secret_value.strip()

    if secret_value.startswith("{"):
        return json.loads(secret_value)

    decoded = base64.b64decode(secret_value).decode("utf-8")
    return json.loads(decoded)


# =========================
# Google Sheet read
# =========================

def read_google_sheet_tabs(
    sheet_url: str,
    service_account_json_or_b64: str,
    input_tab: str,
    template_tab: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    sa_info = parse_service_account_secret(service_account_json_or_b64)
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    client = gspread.authorize(creds)

    sheet_id = extract_sheet_id(sheet_url)
    sh = client.open_by_key(sheet_id)

    input_ws = sh.worksheet(input_tab)
    template_ws = sh.worksheet(template_tab)

    input_values = input_ws.get_all_values()
    template_values = template_ws.get_all_values()

    input_df = values_to_df(input_values, input_tab)
    template_df = values_to_df(template_values, template_tab)

    return input_df, template_df


def values_to_df(values: List[List[str]], tab_name: str) -> pd.DataFrame:
    if not values:
        raise ValueError(f"Tab is empty: {tab_name}")

    headers = [normalize_header(x) for x in values[0]]
    rows = values[1:]

    width = len(headers)
    normalized_rows = []
    for row in rows:
        row = row[:width] + [""] * max(0, width - len(row))
        normalized_rows.append(row)

    df = pd.DataFrame(normalized_rows, columns=headers)
    df = df.dropna(how="all")
    df = df.loc[~(df.astype(str).apply(lambda r: "".join(r).strip(), axis=1) == "")]
    return df


def validate_columns(df: pd.DataFrame, required_columns: List[str], tab_name: str) -> None:
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {tab_name}: {missing}")


# =========================
# Image download / preprocess
# =========================

def download_image(url: str, cache_dir: Path, timeout: int = 30) -> Path:
    url = clean_cell(url)
    if not url:
        raise ValueError("Empty image URL")

    ensure_dir(cache_dir)

    ext = ".jpg"
    clean_url_lower = url.split("?")[0].lower()
    if clean_url_lower.endswith(".png"):
        ext = ".png"
    elif clean_url_lower.endswith(".webp"):
        ext = ".webp"
    elif clean_url_lower.endswith(".jpeg"):
        ext = ".jpg"

    cache_path = cache_dir / f"{hash_url(url)}{ext}"

    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path

    headers = {
        "User-Agent": "Mozilla/5.0 bundle-image-generator/1.0"
    }

    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    cache_path.write_bytes(resp.content)
    return cache_path


def load_image_as_rgba(path: str | Path) -> Image.Image:
    img = Image.open(path)
    return img.convert("RGBA")


def remove_near_white_background(
    img: Image.Image,
    white_threshold: int = 246,
    alpha_softness: int = 10,
) -> Image.Image:
    """
    Basic white-background removal for product images.

    It keeps non-white pixels opaque and makes near-white background transparent.
    This is intentionally conservative for product photos.
    """
    img = img.convert("RGBA")
    pixels = img.load()
    w, h = img.size

    for y in range(h):
        for x in range(w):
            r, g, b, a = pixels[x, y]
            if a == 0:
                continue

            # distance from white by channel
            min_channel = min(r, g, b)

            if r >= white_threshold and g >= white_threshold and b >= white_threshold:
                pixels[x, y] = (r, g, b, 0)
            elif r >= white_threshold - alpha_softness and g >= white_threshold - alpha_softness and b >= white_threshold - alpha_softness:
                # soft fade for edge anti-aliasing
                diff = 255 - min_channel
                new_alpha = max(0, min(a, diff * 18))
                pixels[x, y] = (r, g, b, new_alpha)

    return img


def crop_to_content(
    img: Image.Image,
    alpha_threshold: int = 10,
    padding: int = 8,
) -> Image.Image:
    img = img.convert("RGBA")
    alpha = img.getchannel("A")
    bbox = alpha.point(lambda p: 255 if p > alpha_threshold else 0).getbbox()

    if not bbox:
        return img

    left, top, right, bottom = bbox
    left = max(0, left - padding)
    top = max(0, top - padding)
    right = min(img.width, right + padding)
    bottom = min(img.height, bottom + padding)

    return img.crop((left, top, right, bottom))


def preprocess_product_image(
    img: Image.Image,
    remove_bg: bool = True,
    white_threshold: int = 246,
) -> Image.Image:
    img = img.convert("RGBA")
    if remove_bg:
        img = remove_near_white_background(img, white_threshold=white_threshold)
    img = crop_to_content(img)
    return img


def fit_image(
    img: Image.Image,
    target_w: int,
    target_h: int,
) -> Image.Image:
    img = img.convert("RGBA")
    if img.width <= 0 or img.height <= 0:
        raise ValueError("Invalid image size")

    scale = min(target_w / img.width, target_h / img.height)
    new_w = max(1, int(img.width * scale))
    new_h = max(1, int(img.height * scale))

    return img.resize((new_w, new_h), RESAMPLE_LANCZOS)


def paste_center(
    canvas: Image.Image,
    img: Image.Image,
    cx: int,
    cy: int,
) -> None:
    x = int(cx - img.width / 2)
    y = int(cy - img.height / 2)
    canvas.alpha_composite(img, (x, y))


# =========================
# View resolution
# =========================

def resolve_url_map(row: pd.Series) -> Dict[str, str]:
    image_url = clean_cell(row.get("image_url", ""))
    front_url = clean_cell(row.get("front_url", ""))
    angle_url = clean_cell(row.get("angle_url", ""))
    side_url = clean_cell(row.get("side_url", ""))

    url_map = {
        "image": image_url,
        "front": front_url,
        "angle": angle_url,
        "side": side_url,
    }

    available = {k: v for k, v in url_map.items() if v}

    if not available:
        raise ValueError(f"No image URL found for sku={clean_cell(row.get('sku', ''))}")

    # Fallback chain.
    # image = generic fallback.
    fallback_order = {
        "angle": ["angle", "front", "side", "image"],
        "front": ["front", "angle", "side", "image"],
        "side": ["side", "angle", "front", "image"],
        "image": ["image", "front", "angle", "side"],
    }

    resolved = {}
    for view, candidates in fallback_order.items():
        for c in candidates:
            if url_map.get(c):
                resolved[view] = url_map[c]
                break

    return resolved


def build_view_images(
    url_map: Dict[str, str],
    cache_dir: Path,
    remove_bg: bool = True,
    white_threshold: int = 246,
) -> Tuple[Dict[str, Image.Image], Dict[str, str]]:
    images: Dict[str, Image.Image] = {}
    source_paths: Dict[str, str] = {}

    for view, url in url_map.items():
        path = download_image(url, cache_dir=cache_dir)
        img = load_image_as_rgba(path)
        img = preprocess_product_image(
            img,
            remove_bg=remove_bg,
            white_threshold=white_threshold,
        )
        images[view] = img
        source_paths[view] = str(path)

    return images, source_paths


# =========================
# Templates
# =========================

def p(view: str, x: float, y: float, w: float, h: float) -> Placement:
    return Placement(view=view, x=x, y=y, w=w, h=h)


def make_big_top_left_grid_template() -> TemplateSpec:
    """
    Template: big_top_left_grid

    Intended visual:
    - 1 pcs: single large image
    - 2 pcs: two angle/image views, diagonal layout
    - 5 pcs: front grid, 1 + 2 + 2 feeling
    - 10 pcs: left grid + right two bigger angle/side
    - 20 pcs: left small grid + right big vertical stack
    - 30 pcs: top-left large hero + remaining grid

    Coordinates are percentages of canvas.
    """
    q: Dict[int, List[Placement]] = {}

    q[1] = [
        p("image", 0.50, 0.50, 0.76, 0.76),
    ]

    q[2] = [
        p("angle", 0.38, 0.34, 0.58, 0.42),
        p("angle", 0.62, 0.66, 0.58, 0.42),
    ]

    q[5] = [
        p("front", 0.50, 0.18, 0.78, 0.26),
        p("front", 0.30, 0.47, 0.46, 0.22),
        p("front", 0.70, 0.47, 0.46, 0.22),
        p("front", 0.30, 0.75, 0.46, 0.22),
        p("front", 0.70, 0.75, 0.46, 0.22),
    ]

    q[10] = []
    # 8 small fronts, left area 2 columns x 4 rows
    xs = [0.24, 0.50]
    ys = [0.17, 0.38, 0.59, 0.80]
    for yy in ys:
        for xx in xs:
            q[10].append(p("front", xx, yy, 0.30, 0.16))
    # 2 larger right-side images
    q[10].extend([
        p("side", 0.80, 0.32, 0.34, 0.42),
        p("side", 0.80, 0.72, 0.34, 0.42),
    ])

    q[20] = []
    # 18 small images on left, 3 columns x 6 rows
    xs = [0.17, 0.37, 0.57]
    ys = [0.12, 0.27, 0.42, 0.57, 0.72, 0.87]
    for yy in ys:
        for xx in xs:
            q[20].append(p("front", xx, yy, 0.22, 0.12))
    # 2 right big side images
    q[20].extend([
        p("side", 0.83, 0.30, 0.34, 0.42),
        p("side", 0.83, 0.72, 0.34, 0.42),
    ])

    q[30] = []
    # 1 large hero top-left
    q[30].append(p("angle", 0.23, 0.18, 0.46, 0.30))

    # 29 grid images
    coords: List[Tuple[float, float]] = []

    # right top area: 2 cols x 2 rows = 4
    for yy in [0.13, 0.29]:
        for xx in [0.62, 0.82]:
            coords.append((xx, yy))

    # main grid lower: 5 cols x 5 rows = 25
    for yy in [0.45, 0.57, 0.69, 0.81, 0.93]:
        for xx in [0.13, 0.32, 0.51, 0.70, 0.89]:
            coords.append((xx, yy))

    for xx, yy in coords[:29]:
        q[30].append(p("front", xx, yy, 0.19, 0.10))

    return TemplateSpec(
        template_name="big_top_left_grid",
        quantity_to_placements=q,
    )


def get_builtin_templates() -> Dict[str, TemplateSpec]:
    templates = [
        make_big_top_left_grid_template(),
    ]
    return {t.template_name: t for t in templates}


# =========================
# Render
# =========================

def render_quantity_image(
    view_images: Dict[str, Image.Image],
    template: TemplateSpec,
    quantity: int,
    canvas_size: int = 1500,
    background_rgb: Tuple[int, int, int] = (255, 255, 255),
) -> Image.Image:
    if quantity not in template.quantity_to_placements:
        raise ValueError(f"Template {template.template_name} does not support quantity={quantity}")

    canvas = Image.new("RGBA", (canvas_size, canvas_size), background_rgb + (255,))

    placements = template.quantity_to_placements[quantity]

    for item in placements:
        view = item.view
        if view not in view_images:
            view = "image"

        src = view_images[view]
        target_w = int(canvas_size * item.w)
        target_h = int(canvas_size * item.h)
        fitted = fit_image(src, target_w, target_h)

        cx = int(canvas_size * item.x)
        cy = int(canvas_size * item.y)
        paste_center(canvas, fitted, cx, cy)

    return canvas.convert("RGB")


# =========================
# Main generation
# =========================

def normalize_input_df(input_df: pd.DataFrame) -> pd.DataFrame:
    df = input_df.copy()
    df.columns = [normalize_header(c) for c in df.columns]
    validate_columns(df, REQUIRED_INPUT_COLUMNS, "Input__BundleImageURLs")

    for c in REQUIRED_INPUT_COLUMNS:
        df[c] = df[c].apply(clean_cell)

    # optional enabled support
    if "enabled" in df.columns:
        df["enabled"] = df["enabled"].apply(lambda x: clean_cell(x).lower())
        df = df[df["enabled"].isin(["", "true", "yes", "y", "1"])].copy()

    df = df[df["sku"].astype(str).str.strip() != ""].copy()
    return df


def normalize_template_df(template_df: pd.DataFrame) -> pd.DataFrame:
    df = template_df.copy()
    df.columns = [normalize_header(c) for c in df.columns]
    validate_columns(df, REQUIRED_TEMPLATE_COLUMNS, "Ref__BundleTemplates")

    for c in REQUIRED_TEMPLATE_COLUMNS:
        df[c] = df[c].apply(clean_cell)

    return df


def generate_from_dataframes(
    input_df: pd.DataFrame,
    template_df: pd.DataFrame,
    output_dir: str | Path,
    cache_dir: str | Path,
    quantities: Optional[List[int]] = None,
    canvas_size: int = 1500,
    output_format: str = "jpg",
    overwrite: bool = True,
    remove_bg: bool = True,
    white_threshold: int = 246,
) -> pd.DataFrame:
    input_df = normalize_input_df(input_df)
    template_df = normalize_template_df(template_df)

    output_dir = ensure_dir(output_dir)
    cache_dir = ensure_dir(cache_dir)

    quantities = quantities or SUPPORTED_QUANTITIES
    quantities = [int(q) for q in quantities]

    builtin_templates = get_builtin_templates()

    runlog: List[Dict[str, Any]] = []

    template_names_in_ref = set(template_df["template_name"].tolist())

    for idx, row in input_df.iterrows():
        sku = clean_cell(row.get("sku", ""))
        output_name = clean_cell(row.get("output_name", "")) or sku
        template_name = clean_cell(row.get("template_name", ""))

        started = time.time()

        try:
            if not template_name:
                raise ValueError(f"template_name is empty for sku={sku}")

            if template_name not in template_names_in_ref:
                raise ValueError(f"template_name not found in Ref__BundleTemplates: {template_name}")

            if template_name not in builtin_templates:
                raise ValueError(
                    f"template_name exists in sheet but not implemented in py: {template_name}"
                )

            template = builtin_templates[template_name]

            url_map = resolve_url_map(row)
            view_images, source_paths = build_view_images(
                url_map=url_map,
                cache_dir=cache_dir,
                remove_bg=remove_bg,
                white_threshold=white_threshold,
            )

            unique_source_urls = set(url_map.values())
            source_count = len(unique_source_urls)

            for qty in quantities:
                out_ext = "jpg" if output_format.lower() in ["jpg", "jpeg"] else "png"
                filename = f"{safe_filename(output_name)}_{qty}pcs.{out_ext}"
                out_path = output_dir / filename

                if out_path.exists() and not overwrite:
                    status = "skipped_exists"
                else:
                    img = render_quantity_image(
                        view_images=view_images,
                        template=template,
                        quantity=qty,
                        canvas_size=canvas_size,
                    )
                    if out_ext == "jpg":
                        img.save(out_path, quality=95, optimize=True)
                    else:
                        img.save(out_path)
                    status = "success"

                runlog.append({
                    "ts": pd.Timestamp.utcnow().isoformat(),
                    "sku": sku,
                    "output_name": output_name,
                    "template_name": template_name,
                    "quantity": qty,
                    "status": status,
                    "output_path": str(out_path),
                    "source_count": source_count,
                    "image_url": url_map.get("image", ""),
                    "front_url": url_map.get("front", ""),
                    "angle_url": url_map.get("angle", ""),
                    "side_url": url_map.get("side", ""),
                    "message": "",
                    "elapsed_sec": round(time.time() - started, 2),
                })

        except Exception as e:
            runlog.append({
                "ts": pd.Timestamp.utcnow().isoformat(),
                "sku": sku,
                "output_name": output_name,
                "template_name": template_name,
                "quantity": "",
                "status": "error",
                "output_path": "",
                "source_count": "",
                "image_url": clean_cell(row.get("image_url", "")),
                "front_url": clean_cell(row.get("front_url", "")),
                "angle_url": clean_cell(row.get("angle_url", "")),
                "side_url": clean_cell(row.get("side_url", "")),
                "message": str(e),
                "elapsed_sec": round(time.time() - started, 2),
            })

    log_df = pd.DataFrame(runlog)
    log_path = output_dir / "bundle_image_runlog.csv"
    log_df.to_csv(log_path, index=False)

    return log_df


def generate_from_google_sheet(
    sheet_url: str,
    service_account_json_or_b64: str,
    input_tab: str,
    template_tab: str,
    output_dir: str | Path,
    cache_dir: str | Path,
    quantities: Optional[List[int]] = None,
    canvas_size: int = 1500,
    output_format: str = "jpg",
    overwrite: bool = True,
    remove_bg: bool = True,
    white_threshold: int = 246,
) -> pd.DataFrame:
    input_df, template_df = read_google_sheet_tabs(
        sheet_url=sheet_url,
        service_account_json_or_b64=service_account_json_or_b64,
        input_tab=input_tab,
        template_tab=template_tab,
    )

    return generate_from_dataframes(
        input_df=input_df,
        template_df=template_df,
        output_dir=output_dir,
        cache_dir=cache_dir,
        quantities=quantities,
        canvas_size=canvas_size,
        output_format=output_format,
        overwrite=overwrite,
        remove_bg=remove_bg,
        white_threshold=white_threshold,
    )
