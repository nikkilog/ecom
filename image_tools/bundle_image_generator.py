from __future__ import annotations

import base64
import json
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
    "Type",
    "sku",
    "template_no",
    "template_name",
    "image_url",
    "front_url",
    "angle_url",
    "side_url",
    "output_name",
    "note",
]

REQUIRED_TEMPLATE_COLUMNS = [
    "template_no",
    "template_name",
    "template_display_name",
]

OPTIONAL_TEMPLATE_COLUMNS = [
    "template_desc",
    "视觉特征",
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

TYPE_TO_QUANTITIES = {
    "10": [1, 2, 5, 10],
    "30": [1, 2, 5, 10, 20, 30],
}

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
    scale: float = 1.0


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


def normalize_template_key(value: Any) -> str:
    """Normalize template_no / Type values from Sheets, including 1.0 -> 1."""
    value = clean_cell(value)
    if not value:
        return ""
    if re.fullmatch(r"\d+\.0+", value):
        return value.split(".")[0]
    return value


def is_numeric_like(value: str) -> bool:
    value = clean_cell(value)
    return bool(re.fullmatch(r"\d+(?:\.0+)?", value))


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
    Conservative: makes near-white backgrounds transparent and keeps product pixels.
    """
    img = img.convert("RGBA")
    pixels = img.load()
    w, h = img.size

    for y in range(h):
        for x in range(w):
            r, g, b, a = pixels[x, y]
            if a == 0:
                continue

            min_channel = min(r, g, b)

            if r >= white_threshold and g >= white_threshold and b >= white_threshold:
                pixels[x, y] = (r, g, b, 0)
            elif (
                r >= white_threshold - alpha_softness
                and g >= white_threshold - alpha_softness
                and b >= white_threshold - alpha_softness
            ):
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
    bbox = alpha.point(lambda px: 255 if px > alpha_threshold else 0).getbbox()

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


def normalize_sprite_canvas(
    img: Image.Image,
    canvas_size: int = 1000,
    max_fill: float = 0.82,
) -> Image.Image:
    """
    Normalize every source product image into a same-size transparent sprite.

    This prevents close-up product photos from becoming huge and far-shot
    photos from becoming tiny when the layout repeats the same item many times.
    """
    img = img.convert("RGBA")
    if img.width <= 0 or img.height <= 0:
        raise ValueError("Invalid image size")

    max_side = int(canvas_size * max_fill)
    scale = min(max_side / img.width, max_side / img.height)
    new_w = max(1, int(img.width * scale))
    new_h = max(1, int(img.height * scale))

    resized = img.resize((new_w, new_h), RESAMPLE_LANCZOS)
    canvas = Image.new("RGBA", (canvas_size, canvas_size), (255, 255, 255, 0))
    x = int((canvas_size - new_w) / 2)
    y = int((canvas_size - new_h) / 2)
    canvas.alpha_composite(resized, (x, y))
    return canvas


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

    fallback_order = {
        "angle": ["angle", "front", "side", "image"],
        "front": ["front", "angle", "side", "image"],
        "side": ["side", "angle", "front", "image"],
        "image": ["image", "front", "angle", "side"],
    }

    resolved = {}
    for view, candidates in fallback_order.items():
        for candidate in candidates:
            if url_map.get(candidate):
                resolved[view] = url_map[candidate]
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

        # Key fix: normalize every source into the same visual sprite canvas
        # before rendering repeated SPU layouts.
        img = normalize_sprite_canvas(
            img,
            canvas_size=1000,
            max_fill=0.82,
        )

        images[view] = img
        source_paths[view] = str(path)

    return images, source_paths


# =========================
# Placement helpers
# =========================

def p(
    view: str,
    x: float,
    y: float,
    w: float,
    h: float,
    scale: float = 1.0,
) -> Placement:
    return Placement(view=view, x=x, y=y, w=w, h=h, scale=scale)


def grid(
    n: int,
    cols: int,
    rows: int,
    x0: float,
    x1: float,
    y0: float,
    y1: float,
    view: str = "front",
    cell_w: Optional[float] = None,
    cell_h: Optional[float] = None,
) -> List[Placement]:
    if n <= 0:
        return []
    if cols <= 0 or rows <= 0:
        raise ValueError("cols and rows must be positive")

    xs = [(x0 + x1) / 2] if cols == 1 else [x0 + (x1 - x0) * i / (cols - 1) for i in range(cols)]
    ys = [(y0 + y1) / 2] if rows == 1 else [y0 + (y1 - y0) * i / (rows - 1) for i in range(rows)]

    if cell_w is None:
        cell_w = min(0.90 / cols, 0.26)
    if cell_h is None:
        cell_h = min(0.90 / rows, 0.24)

    out: List[Placement] = []
    for yy in ys:
        for xx in xs:
            if len(out) >= n:
                return out
            out.append(p(view, xx, yy, cell_w, cell_h))
    return out


def grid_area(
    n: int,
    cols: int,
    rows: int,
    left: float,
    top: float,
    right: float,
    bottom: float,
    view: str = "front",
    fill: float = 0.94,
    scale: float = 1.0,
) -> List[Placement]:
    """
    Create a dense grid inside a rectangular area.

    Unlike grid(), left/top/right/bottom are real area bounds.
    Centers are placed in cells, not on the area edge.
    This gives stable dense bundle layouts.
    """
    if n <= 0:
        return []
    if cols <= 0 or rows <= 0:
        raise ValueError("cols and rows must be positive")
    if right <= left or bottom <= top:
        raise ValueError("Invalid grid area bounds")

    cell_w = (right - left) / cols
    cell_h = (bottom - top) / rows

    out: List[Placement] = []
    for r in range(rows):
        cy = top + cell_h * (r + 0.5)
        for c in range(cols):
            if len(out) >= n:
                return out
            cx = left + cell_w * (c + 0.5)
            out.append(p(view, cx, cy, cell_w * fill, cell_h * fill, scale=scale))
    return out


def spec(name: str, q: Dict[int, List[Placement]]) -> TemplateSpec:
    missing = [x for x in SUPPORTED_QUANTITIES if x not in q]
    if missing:
        raise ValueError(f"Template {name} missing quantities: {missing}")
    return TemplateSpec(template_name=name, quantity_to_placements=q)


# =========================
# Distinct templates
# =========================

def make_big_top_left_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.76, 0.76)]

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

    q[10] = grid(8, 2, 4, 0.24, 0.50, 0.17, 0.80, "front", 0.30, 0.16)
    q[10].extend([
        p("side", 0.80, 0.32, 0.34, 0.42),
        p("side", 0.80, 0.72, 0.34, 0.42),
    ])

    q[20] = grid(18, 3, 6, 0.17, 0.57, 0.12, 0.87, "front", 0.22, 0.12)
    q[20].extend([
        p("side", 0.83, 0.30, 0.34, 0.42),
        p("side", 0.83, 0.72, 0.34, 0.42),
    ])

    q[30] = [p("angle", 0.23, 0.18, 0.46, 0.30)]
    q[30].extend(grid(4, 2, 2, 0.62, 0.82, 0.13, 0.29, "front", 0.19, 0.10))
    q[30].extend(grid(25, 5, 5, 0.13, 0.89, 0.45, 0.93, "front", 0.19, 0.10))

    return spec("big_top_left_grid", q)


def make_mixed_knob_grid_template() -> TemplateSpec:
    """
    Template: mixed_knob_grid

   方案 B：素材标准化保持 0.82，不整体压小；
    通过模板用途控制 scale：
    - 1pcs 主图略大但不顶边
    - hero / 大图约 0.90
    - grid 小图约 0.70
    """
    q: Dict[int, List[Placement]] = {}

    SINGLE_SCALE = 0.96
    HERO_SCALE = 0.90
    SMALL_SCALE = 0.80
    SMALL_SCALE_DENSE = 0.86

    q[1] = [
        p("image", 0.50, 0.50, 0.86, 0.86, scale=SINGLE_SCALE),
    ]

    q[2] = [
        p("front", 0.38, 0.52, 0.54, 0.54, scale=HERO_SCALE),
        p("angle", 0.64, 0.52, 0.54, 0.54, scale=HERO_SCALE),
    ]

    q[5] = [
        *grid_area(4, 2, 2, 0.08, 0.18, 0.48, 0.82, "front", fill=1.00, scale=SMALL_SCALE),
        p("angle", 0.74, 0.52, 0.54, 0.54, scale=HERO_SCALE),
    ]

    q[10] = [
        *grid_area(8, 2, 4, 0.07, 0.12, 0.46, 0.88, "front", fill=1.00, scale=SMALL_SCALE),
        p("angle", 0.74, 0.34, 0.44, 0.44, scale=HERO_SCALE),
        p("angle", 0.76, 0.70, 0.44, 0.44, scale=HERO_SCALE),
    ]

    q[20] = [
        *grid_area(18, 3, 6, 0.06, 0.09, 0.58, 0.91, "front", fill=1.00, scale=SMALL_SCALE_DENSE),
        p("angle", 0.80, 0.34, 0.38, 0.38, scale=0.86),
        p("angle", 0.80, 0.70, 0.38, 0.38, scale=0.86),
    ]

    q[30] = [
        *grid_area(28, 4, 7, 0.05, 0.07, 0.63, 0.93, "front", fill=1.00, scale=0.62),
        p("angle", 0.81, 0.32, 0.36, 0.36, scale=0.84),
        p("angle", 0.82, 0.70, 0.36, 0.36, scale=0.84),
    ]

    return spec("mixed_knob_grid", q)

def make_round_drain_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.74, 0.74)]
    q[2] = [
        p("image", 0.50, 0.35, 0.56, 0.36),
        p("image", 0.50, 0.65, 0.56, 0.36),
    ]
    q[5] = [
        p("image", 0.50, 0.20, 0.42, 0.28),
        *grid(4, 2, 2, 0.32, 0.68, 0.50, 0.78, "image", 0.30, 0.22),
    ]
    q[10] = grid(10, 3, 4, 0.22, 0.78, 0.14, 0.86, "image", 0.25, 0.18)
    q[20] = grid(20, 4, 5, 0.16, 0.84, 0.12, 0.88, "image", 0.19, 0.14)
    q[30] = grid(30, 5, 6, 0.12, 0.88, 0.10, 0.90, "image", 0.16, 0.11)

    return spec("round_drain_grid", q)


def make_square_drain_grid_template() -> TemplateSpec:
    """
    Template: square_drain_grid

   方案 B：方形/平面类产品保留主图清晰度，
    单张和 hero 不顶边，重复 grid 小图适度缩小。
    """
    q: Dict[int, List[Placement]] = {}

    SINGLE_SCALE = 0.96
    HERO_SCALE = 0.90
    SMALL_SCALE = 0.82
    DENSE_SCALE = 0.76

    q[1] = [p("image", 0.50, 0.50, 0.86, 0.86, scale=SINGLE_SCALE)]

    q[2] = [
        p("image", 0.38, 0.50, 0.54, 0.54, scale=HERO_SCALE),
        p("image", 0.64, 0.50, 0.54, 0.54, scale=HERO_SCALE),
    ]

    q[5] = [
        *grid_area(4, 2, 2, 0.08, 0.20, 0.48, 0.82, "image", fill=1.00, scale=SMALL_SCALE),
        p("image", 0.72, 0.52, 0.54, 0.54, scale=HERO_SCALE),
    ]

    q[10] = [
        *grid_area(8, 2, 4, 0.07, 0.12, 0.45, 0.88, "image", fill=1.00, scale=SMALL_SCALE),
        p("image", 0.75, 0.34, 0.42, 0.42, scale=0.86),
        p("image", 0.76, 0.70, 0.42, 0.42, scale=0.86),
    ]

    q[20] = [
        *grid_area(18, 3, 6, 0.07, 0.09, 0.59, 0.91, "image", fill=1.00, scale=DENSE_SCALE),
        p("image", 0.80, 0.34, 0.38, 0.38, scale=0.84),
        p("image", 0.80, 0.70, 0.38, 0.38, scale=0.84),
    ]

    q[30] = [
        *grid_area(28, 4, 7, 0.06, 0.07, 0.64, 0.93, "image", fill=1.00, scale=0.62),
        p("image", 0.82, 0.32, 0.36, 0.36, scale=0.82),
        p("image", 0.82, 0.70, 0.36, 0.36, scale=0.82),
    ]

    return spec("square_drain_grid", q)

def make_elbow_mixed_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("angle", 0.50, 0.50, 0.74, 0.74)]
    q[2] = [
        p("angle", 0.36, 0.40, 0.50, 0.42),
        p("side", 0.66, 0.60, 0.50, 0.42),
    ]
    q[5] = [
        p("angle", 0.70, 0.42, 0.42, 0.40),
        p("side", 0.70, 0.72, 0.34, 0.28),
        *grid(3, 1, 3, 0.28, 0.28, 0.26, 0.74, "front", 0.32, 0.22),
    ]
    q[10] = [
        p("angle", 0.76, 0.35, 0.36, 0.32),
        p("side", 0.76, 0.72, 0.36, 0.32),
        *grid(8, 2, 4, 0.24, 0.52, 0.14, 0.86, "front", 0.25, 0.15),
    ]
    q[20] = [
        p("angle", 0.77, 0.33, 0.32, 0.28),
        p("side", 0.77, 0.72, 0.32, 0.28),
        *grid(18, 3, 6, 0.15, 0.58, 0.11, 0.89, "front", 0.19, 0.11),
    ]
    q[30] = [
        p("angle", 0.80, 0.24, 0.30, 0.24),
        p("side", 0.80, 0.54, 0.30, 0.24),
        p("angle", 0.80, 0.82, 0.28, 0.22),
        *grid(27, 3, 9, 0.15, 0.60, 0.08, 0.92, "front", 0.17, 0.085),
    ]

    return spec("elbow_mixed_grid", q)


def make_round_strainer_mix_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.76, 0.76)]
    q[2] = [
        p("image", 0.40, 0.42, 0.46, 0.42),
        p("angle", 0.62, 0.62, 0.42, 0.38),
    ]
    q[5] = [
        p("image", 0.32, 0.50, 0.42, 0.46),
        *grid(4, 2, 2, 0.62, 0.82, 0.30, 0.72, "angle", 0.22, 0.20),
    ]
    q[10] = [
        p("image", 0.28, 0.50, 0.42, 0.48),
        p("angle", 0.77, 0.28, 0.28, 0.26),
        p("angle", 0.77, 0.74, 0.28, 0.26),
        *grid(7, 2, 4, 0.55, 0.68, 0.15, 0.86, "front", 0.16, 0.13),
    ][:10]
    q[20] = [
        p("image", 0.76, 0.48, 0.34, 0.42),
        *grid(19, 4, 5, 0.14, 0.56, 0.12, 0.88, "front", 0.16, 0.12),
    ][:20]
    q[30] = [
        p("image", 0.80, 0.50, 0.30, 0.38),
        *grid(29, 5, 6, 0.10, 0.63, 0.10, 0.90, "front", 0.13, 0.10),
    ][:30]

    return spec("round_strainer_mix", q)


def make_big_bottom_center_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.76, 0.76)]
    q[2] = [
        p("image", 0.50, 0.34, 0.58, 0.34),
        p("front", 0.50, 0.68, 0.58, 0.34),
    ]
    q[5] = [
        *grid(4, 4, 1, 0.20, 0.80, 0.20, 0.20, "front", 0.22, 0.18),
        p("angle", 0.50, 0.66, 0.58, 0.44),
    ]
    q[10] = [
        *grid(8, 4, 2, 0.17, 0.83, 0.16, 0.42, "front", 0.20, 0.15),
        p("angle", 0.40, 0.74, 0.38, 0.34),
        p("side", 0.68, 0.74, 0.32, 0.30),
    ]
    q[20] = [
        *grid(18, 6, 3, 0.10, 0.90, 0.12, 0.45, "front", 0.14, 0.10),
        p("angle", 0.38, 0.76, 0.36, 0.30),
        p("side", 0.68, 0.76, 0.32, 0.28),
    ]
    q[30] = [
        *grid(28, 7, 4, 0.08, 0.92, 0.09, 0.55, "front", 0.12, 0.085),
        p("angle", 0.38, 0.80, 0.34, 0.28),
        p("side", 0.68, 0.80, 0.30, 0.26),
    ]

    return spec("big_bottom_center_grid", q)


def make_coil_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.78, 0.78)]
    q[2] = [
        p("image", 0.38, 0.50, 0.46, 0.46),
        p("image", 0.62, 0.50, 0.46, 0.46),
    ]
    q[5] = [
        p("image", 0.50, 0.50, 0.42, 0.42),
        p("image", 0.28, 0.28, 0.28, 0.28),
        p("image", 0.72, 0.28, 0.28, 0.28),
        p("image", 0.28, 0.72, 0.28, 0.28),
        p("image", 0.72, 0.72, 0.28, 0.28),
    ]
    q[10] = grid(10, 5, 2, 0.14, 0.86, 0.35, 0.65, "image", 0.16, 0.22)
    q[20] = grid(20, 5, 4, 0.12, 0.88, 0.18, 0.82, "image", 0.16, 0.16)
    q[30] = grid(30, 6, 5, 0.10, 0.90, 0.14, 0.86, "image", 0.13, 0.13)

    return spec("coil_grid", q)


def make_horizontal_plus_small_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.86, 0.58)]
    q[2] = [
        p("image", 0.50, 0.36, 0.78, 0.32),
        p("front", 0.50, 0.66, 0.78, 0.32),
    ]
    q[5] = [
        p("image", 0.50, 0.34, 0.78, 0.30),
        *grid(4, 4, 1, 0.20, 0.80, 0.72, 0.72, "front", 0.18, 0.18),
    ]
    q[10] = [
        p("image", 0.50, 0.24, 0.82, 0.26),
        *grid(9, 3, 3, 0.22, 0.78, 0.50, 0.86, "front", 0.20, 0.12),
    ][:10]
    q[20] = [
        p("image", 0.50, 0.18, 0.82, 0.20),
        *grid(19, 5, 4, 0.12, 0.88, 0.40, 0.88, "front", 0.16, 0.11),
    ][:20]
    q[30] = [
        p("image", 0.50, 0.15, 0.82, 0.18),
        *grid(29, 6, 5, 0.10, 0.90, 0.34, 0.90, "front", 0.13, 0.09),
    ][:30]

    return spec("horizontal_plus_small_grid", q)


def make_horizontal_dense_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.86, 0.58)]
    q[2] = grid(2, 1, 2, 0.50, 0.50, 0.36, 0.64, "image", 0.76, 0.28)
    q[5] = grid(5, 1, 5, 0.50, 0.50, 0.18, 0.82, "image", 0.78, 0.12)
    q[10] = grid(10, 2, 5, 0.30, 0.70, 0.16, 0.84, "image", 0.40, 0.11)
    q[20] = grid(20, 4, 5, 0.14, 0.86, 0.16, 0.84, "image", 0.22, 0.11)
    q[30] = grid(30, 5, 6, 0.10, 0.90, 0.12, 0.88, "image", 0.18, 0.09)

    return spec("horizontal_dense_grid", q)


def make_vertical_big_bottom_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.58, 0.86)]
    q[2] = [
        p("image", 0.38, 0.52, 0.40, 0.72),
        p("front", 0.62, 0.52, 0.40, 0.72),
    ]
    q[5] = [
        *grid(4, 4, 1, 0.18, 0.82, 0.18, 0.18, "front", 0.18, 0.20),
        p("image", 0.50, 0.66, 0.42, 0.54),
    ]
    q[10] = [
        *grid(8, 4, 2, 0.18, 0.82, 0.15, 0.38, "front", 0.18, 0.16),
        p("image", 0.38, 0.72, 0.34, 0.42),
        p("image", 0.64, 0.72, 0.34, 0.42),
    ]
    q[20] = [
        *grid(18, 6, 3, 0.10, 0.90, 0.10, 0.42, "front", 0.13, 0.10),
        p("image", 0.38, 0.74, 0.34, 0.38),
        p("image", 0.66, 0.74, 0.34, 0.38),
    ]
    q[30] = [
        *grid(27, 9, 3, 0.08, 0.92, 0.09, 0.42, "front", 0.09, 0.09),
        p("image", 0.30, 0.76, 0.28, 0.34),
        p("image", 0.52, 0.76, 0.28, 0.34),
        p("image", 0.74, 0.76, 0.28, 0.34),
    ]

    return spec("vertical_big_bottom_grid", q)


def get_builtin_templates() -> Dict[str, TemplateSpec]:
    templates = [
        make_big_top_left_grid_template(),
        make_mixed_knob_grid_template(),
        make_round_drain_grid_template(),
        make_square_drain_grid_template(),
        make_elbow_mixed_grid_template(),
        make_round_strainer_mix_template(),
        make_big_bottom_center_grid_template(),
        make_coil_grid_template(),
        make_horizontal_plus_small_grid_template(),
        make_horizontal_dense_grid_template(),
        make_vertical_big_bottom_grid_template(),
    ]

    out = {t.template_name: t for t in templates}
    if len(out) != len(templates):
        raise ValueError("Duplicate template_name in built-in templates")
    return out


# =========================
# Template / Type resolution
# =========================

def build_template_ref_maps(template_df: pd.DataFrame) -> Tuple[set[str], Dict[str, str]]:
    names = set(template_df["template_name"].apply(clean_cell).tolist())

    no_to_name: Dict[str, str] = {}
    for _, row in template_df.iterrows():
        template_no = normalize_template_key(row.get("template_no", ""))
        template_name = clean_cell(row.get("template_name", ""))
        if not template_no or not template_name:
            continue
        if template_no in no_to_name and no_to_name[template_no] != template_name:
            raise ValueError(
                f"Duplicate template_no with different template_name in Ref__BundleTemplates: "
                f"template_no={template_no}, names={no_to_name[template_no]} / {template_name}"
            )
        no_to_name[template_no] = template_name

    return names, no_to_name


def resolve_template_name(
    row: pd.Series,
    template_names_in_ref: set[str],
    template_no_to_name: Dict[str, str],
) -> Tuple[str, str, str]:
    """
    Rule:
    - template_name and template_no can fill either one.
    - If both are filled and conflict, template_name wins.
    - If template_name is numeric-like and not a real template_name, treat it as template_no.
    Returns: resolved_template_name, template_no, resolution_note
    """
    sku = clean_cell(row.get("sku", ""))
    raw_template_no = normalize_template_key(row.get("template_no", ""))
    raw_template_name = clean_cell(row.get("template_name", ""))

    resolution_note = ""

    # Defensive support: user accidentally entered 1 / 1.0 under template_name.
    if raw_template_name and raw_template_name not in template_names_in_ref and is_numeric_like(raw_template_name):
        if not raw_template_no:
            raw_template_no = normalize_template_key(raw_template_name)
            raw_template_name = ""
            resolution_note = "template_name_numeric_treated_as_template_no"

    # Name wins when it exists.
    if raw_template_name:
        if raw_template_name not in template_names_in_ref:
            raise ValueError(f"template_name not found in Ref__BundleTemplates: {raw_template_name}")

        if raw_template_no:
            mapped_name = template_no_to_name.get(raw_template_no, "")
            if mapped_name and mapped_name != raw_template_name:
                resolution_note = (
                    f"template_no_conflict_ignored: template_no={raw_template_no} "
                    f"maps_to={mapped_name}, used_template_name={raw_template_name}"
                )
            elif mapped_name == raw_template_name:
                resolution_note = "template_no_and_template_name_matched"
            else:
                resolution_note = f"template_no_not_found_ignored: template_no={raw_template_no}"
        else:
            resolution_note = resolution_note or "template_name_only"

        return raw_template_name, raw_template_no, resolution_note

    # No name: use template_no.
    if raw_template_no:
        mapped_name = template_no_to_name.get(raw_template_no, "")
        if not mapped_name:
            raise ValueError(f"template_no not found in Ref__BundleTemplates: {raw_template_no}")
        return mapped_name, raw_template_no, "template_no_only"

    raise ValueError(f"Both template_no and template_name are empty for sku={sku}")


def resolve_quantities_by_type(type_value: Any) -> List[int]:
    type_key = normalize_template_key(type_value)
    if not type_key:
        raise ValueError("Type is empty")
    if type_key not in TYPE_TO_QUANTITIES:
        raise ValueError(
            f"Unsupported Type={type_key}. Supported Type values: {sorted(TYPE_TO_QUANTITIES.keys())}"
        )
    return TYPE_TO_QUANTITIES[type_key]


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
        target_w = int(canvas_size * item.w * item.scale)
        target_h = int(canvas_size * item.h * item.scale)
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

    df["Type"] = df["Type"].apply(normalize_template_key)
    df["template_no"] = df["template_no"].apply(normalize_template_key)

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

    df["template_no"] = df["template_no"].apply(normalize_template_key)

    for c in OPTIONAL_TEMPLATE_COLUMNS:
        if c in df.columns:
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

    # Default: row-level Type controls quantities.
    # quantities remains as a hard global override for temporary testing only.
    quantity_override = None
    if quantities:
        quantity_override = [int(q) for q in quantities]

    builtin_templates = get_builtin_templates()

    runlog: List[Dict[str, Any]] = []

    template_names_in_ref, template_no_to_name = build_template_ref_maps(template_df)

    for _, row in input_df.iterrows():
        sku = clean_cell(row.get("sku", ""))
        output_name = clean_cell(row.get("output_name", "")) or f"{sku}-SPU"
        type_value = clean_cell(row.get("Type", ""))
        raw_template_no = clean_cell(row.get("template_no", ""))
        raw_template_name = clean_cell(row.get("template_name", ""))

        resolved_template_name = raw_template_name
        template_resolution_note = ""
        row_quantities: List[int] = []

        started = time.time()

        try:
            resolved_template_name, resolved_template_no, template_resolution_note = resolve_template_name(
                row=row,
                template_names_in_ref=template_names_in_ref,
                template_no_to_name=template_no_to_name,
            )

            if resolved_template_name not in builtin_templates:
                raise ValueError(
                    f"template_name exists in Ref__BundleTemplates but not implemented in py: {resolved_template_name}"
                )

            template = builtin_templates[resolved_template_name]
            row_quantities = quantity_override or resolve_quantities_by_type(type_value)

            url_map = resolve_url_map(row)
            view_images, source_paths = build_view_images(
                url_map=url_map,
                cache_dir=cache_dir,
                remove_bg=remove_bg,
                white_threshold=white_threshold,
            )

            unique_source_urls = set(url_map.values())
            source_count = len(unique_source_urls)

            for qty in row_quantities:
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
                    "Type": type_value,
                    "output_name": output_name,
                    "template_no_input": raw_template_no,
                    "template_name_input": raw_template_name,
                    "template_name": resolved_template_name,
                    "template_resolution_note": template_resolution_note,
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
                "Type": type_value,
                "output_name": output_name,
                "template_no_input": raw_template_no,
                "template_name_input": raw_template_name,
                "template_name": resolved_template_name,
                "template_resolution_note": template_resolution_note,
                "quantity": ",".join(str(x) for x in row_quantities) if row_quantities else "",
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
