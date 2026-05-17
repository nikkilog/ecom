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

REGISTERED_TEMPLATE_NAMES = [
    "big_top_left_grid",
    "mixed_knob_grid",
    "round_drain_grid",
    "square_drain_grid",
    "elbow_mixed_grid",
    "round_strainer_mix",
    "big_bottom_center_grid",
    "coil_grid",
    "horizontal_plus_small_grid",
    "horizontal_dense_grid",
    "vertical_big_bottom_grid",
]

VIEW_KEYS = ["image", "front", "angle", "side"]

# =========================
# Scale defaults
# =========================
#
# 这块是为 Colab 调试设计的。
# Colab 可以传入 scale_config 覆盖这些值，无需每次改 py。
#
# 层级：
# 1. global.sprite_max_fill：素材进入模板前，在 1000x1000 透明画布里最多占多少
# 2. global.render_scale：所有模板最终整体放大/缩小
# 3. templates.<template_name>.template_scale：某个模板整体放大/缩小
# 4. templates.<template_name>.single/hero/small/dense：某模板里不同用途的图大小
#
DEFAULT_SCALE_CONFIG: Dict[str, Any] = {
    "global": {
        "sprite_canvas_size": 1000,
        "sprite_max_fill": 0.82,
        "render_scale": 1.00,
    },
    "template_defaults": {
        "template_scale": 1.00,
        "single": 0.96,
        "hero": 0.90,
        "small": 0.90,
        "dense": 0.90,
    },
    "templates": {
        "big_top_left_grid": {
            "template_scale": 1.00,
            "single": 0.98,
            "hero": 0.92,
            "small": 0.88,
            "dense": 0.86,
        },
        "mixed_knob_grid": {
            "template_scale": 1.00,
            "single": 0.96,
            "hero": 0.90,
            "small": 0.94,
            "dense": 0.94,
        },
        "round_drain_grid": {
            "template_scale": 1.00,
            "single": 0.96,
            "hero": 0.90,
            "small": 0.90,
            "dense": 0.88,
        },
        "square_drain_grid": {
            "template_scale": 1.00,
            "single": 0.96,
            "hero": 0.90,
            "small": 0.88,
            "dense": 0.86,
        },
        "elbow_mixed_grid": {
            "template_scale": 1.00,
            "single": 0.98,
            "hero": 0.92,
            "small": 0.90,
            "dense": 0.88,
        },
        "round_strainer_mix": {
            "template_scale": 1.00,
            "single": 0.96,
            "hero": 0.90,
            "small": 0.88,
            "dense": 0.86,
        },
        "big_bottom_center_grid": {
            "template_scale": 1.00,
            "single": 0.98,
            "hero": 0.94,
            "small": 0.88,
            "dense": 0.86,
        },
        "coil_grid": {
            "template_scale": 1.00,
            "single": 0.95,
            "hero": 0.88,
            "small": 0.86,
            "dense": 0.84,
        },
        "horizontal_plus_small_grid": {
            "template_scale": 1.00,
            "single": 0.96,
            "hero": 0.90,
            "small": 0.88,
            "dense": 0.86,
        },
        "horizontal_dense_grid": {
            "template_scale": 1.00,
            "single": 0.96,
            "hero": 0.88,
            "small": 0.90,
            "dense": 0.90,
        },
        "vertical_big_bottom_grid": {
            "template_scale": 1.00,
            "single": 0.98,
            "hero": 0.94,
            "small": 0.88,
            "dense": 0.86,
        },
    },
}


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
    role: str = "small"  # single / hero / small / dense
    anchor: str = "center"
    scale: Optional[float] = None  # optional local override


@dataclass
class TemplateSpec:
    template_name: str
    quantity_to_placements: Dict[int, List[Placement]]


# =========================
# Utility
# =========================

def deep_merge(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not override:
        return json.loads(json.dumps(base))
    result = json.loads(json.dumps(base))
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


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
    secret_value = secret_value.strip()
    if secret_value.startswith("{"):
        return json.loads(secret_value)
    decoded = base64.b64decode(secret_value).decode("utf-8")
    return json.loads(decoded)


def normalize_template_key(value: Any) -> str:
    value = clean_cell(value)
    if not value:
        return ""
    if re.fullmatch(r"\d+\.0+", value):
        return value.split(".")[0]
    return value


def is_numeric_like(value: str) -> bool:
    value = clean_cell(value)
    return bool(re.fullmatch(r"\d+(?:\.0+)?", value))


def get_template_scale_cfg(scale_cfg: Dict[str, Any], template_name: str) -> Dict[str, float]:
    defaults = scale_cfg.get("template_defaults", {})
    tmpl = scale_cfg.get("templates", {}).get(template_name, {})
    merged = {**defaults, **tmpl}
    # template_scale multiplies all role scales.
    ts = float(merged.get("template_scale", 1.0))
    return {
        "template_scale": ts,
        "single": float(merged.get("single", 1.0)) * ts,
        "hero": float(merged.get("hero", 1.0)) * ts,
        "small": float(merged.get("small", 1.0)) * ts,
        "dense": float(merged.get("dense", 1.0)) * ts,
    }


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

    input_df = values_to_df(input_ws.get_all_values(), input_tab)
    template_df = values_to_df(template_ws.get_all_values(), template_tab)

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

    headers = {"User-Agent": "Mozilla/5.0 bundle-image-generator/1.0"}
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


def normalize_sprite_canvas(
    img: Image.Image,
    canvas_size: int = 1000,
    max_fill: float = 0.82,
) -> Image.Image:
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
    scale_config: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Image.Image], Dict[str, str]]:
    cfg = deep_merge(DEFAULT_SCALE_CONFIG, scale_config)
    global_cfg = cfg.get("global", {})

    sprite_canvas_size = int(global_cfg.get("sprite_canvas_size", 1000))
    sprite_max_fill = float(global_cfg.get("sprite_max_fill", 0.82))

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

        img = normalize_sprite_canvas(
            img,
            canvas_size=sprite_canvas_size,
            max_fill=sprite_max_fill,
        )

        images[view] = img
        source_paths[view] = str(path)

    return images, source_paths


# =========================
# Template helpers
# =========================

def p(
    view: str,
    x: float,
    y: float,
    w: float,
    h: float,
    role: str = "small",
    scale: Optional[float] = None,
) -> Placement:
    return Placement(view=view, x=x, y=y, w=w, h=h, role=role, scale=scale)


def spec(name: str, q: Dict[int, List[Placement]]) -> TemplateSpec:
    return TemplateSpec(template_name=name, quantity_to_placements=q)


def grid_area(
    count: int,
    cols: int,
    rows: int,
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    view: str,
    role: str = "small",
    fill: float = 0.95,
) -> List[Placement]:
    if cols <= 0 or rows <= 0:
        raise ValueError("cols and rows must be positive")

    placements: List[Placement] = []
    cell_w = (x1 - x0) / cols
    cell_h = (y1 - y0) / rows

    for i in range(count):
        if i >= cols * rows:
            break
        r = i // cols
        c = i % cols
        cx = x0 + cell_w * (c + 0.5)
        cy = y0 + cell_h * (r + 0.5)
        placements.append(
            p(
                view=view,
                x=cx,
                y=cy,
                w=cell_w * fill,
                h=cell_h * fill,
                role=role,
            )
        )

    return placements


def diagonal_pair(view_a: str = "front", view_b: str = "angle") -> List[Placement]:
    return [
        p(view_a, 0.38, 0.50, 0.54, 0.54, role="hero"),
        p(view_b, 0.62, 0.50, 0.54, 0.54, role="hero"),
    ]


def standard_grid_hero_template(
    name: str,
    small_view: str = "front",
    hero_view: str = "angle",
    hero_side: str = "right",
) -> TemplateSpec:
    """
    通用 grid + hero 模板。
    适合大部分 SPU 图：
    - 1pcs：单个大图
    - 2pcs：两个大图
    - 5pcs：4小 + 1大
    - 10pcs：8小 + 2大
    - 20pcs：18小 + 2大
    - 30pcs：28小 + 2大

    具体大小不在模板坐标里调，而在 Colab scale_config 里调。
    """
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.86, 0.86, role="single")]
    q[2] = diagonal_pair(small_view, hero_view)

    if hero_side == "right":
        q[5] = [
            *grid_area(4, 2, 2, 0.08, 0.18, 0.48, 0.82, small_view, role="small", fill=1.00),
            p(hero_view, 0.74, 0.52, 0.54, 0.54, role="hero"),
        ]

        q[10] = [
            *grid_area(8, 2, 4, 0.07, 0.12, 0.46, 0.88, small_view, role="small", fill=1.00),
            p(hero_view, 0.74, 0.34, 0.44, 0.44, role="hero"),
            p(hero_view, 0.76, 0.70, 0.44, 0.44, role="hero"),
        ]

        q[20] = [
            *grid_area(18, 4, 5, 0.05, 0.10, 0.66, 0.90, small_view, role="dense", fill=1.00),
            p(hero_view, 0.82, 0.34, 0.36, 0.36, role="hero"),
            p(hero_view, 0.82, 0.70, 0.36, 0.36, role="hero"),
        ]

        q[30] = [
            *grid_area(28, 5, 6, 0.04, 0.08, 0.70, 0.92, small_view, role="dense", fill=1.00),
            p(hero_view, 0.84, 0.34, 0.32, 0.32, role="hero"),
            p(hero_view, 0.84, 0.70, 0.32, 0.32, role="hero"),
        ]

    elif hero_side == "bottom":
        q[5] = [
            *grid_area(4, 4, 1, 0.14, 0.12, 0.86, 0.38, small_view, role="small", fill=1.00),
            p(hero_view, 0.50, 0.70, 0.58, 0.42, role="hero"),
        ]

        q[10] = [
            *grid_area(8, 4, 2, 0.10, 0.08, 0.90, 0.48, small_view, role="small", fill=1.00),
            p(hero_view, 0.37, 0.75, 0.42, 0.34, role="hero"),
            p(hero_view, 0.64, 0.75, 0.42, 0.34, role="hero"),
        ]

        q[20] = [
            *grid_area(18, 6, 3, 0.08, 0.06, 0.92, 0.62, small_view, role="dense", fill=1.00),
            p(hero_view, 0.36, 0.82, 0.34, 0.26, role="hero"),
            p(hero_view, 0.64, 0.82, 0.34, 0.26, role="hero"),
        ]

        q[30] = [
            *grid_area(28, 7, 4, 0.06, 0.05, 0.94, 0.70, small_view, role="dense", fill=1.00),
            p(hero_view, 0.36, 0.86, 0.30, 0.22, role="hero"),
            p(hero_view, 0.64, 0.86, 0.30, 0.22, role="hero"),
        ]

    else:
        raise ValueError(f"Unsupported hero_side: {hero_side}")

    return spec(name, q)


def horizontal_dense_template(name: str, small_view: str = "front") -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}
    q[1] = [p("image", 0.50, 0.50, 0.86, 0.86, role="single")]
    q[2] = [
        p(small_view, 0.35, 0.50, 0.48, 0.48, role="hero"),
        p(small_view, 0.65, 0.50, 0.48, 0.48, role="hero"),
    ]
    q[5] = grid_area(5, 5, 1, 0.08, 0.30, 0.92, 0.70, small_view, role="small", fill=1.00)
    q[10] = grid_area(10, 5, 2, 0.07, 0.20, 0.93, 0.80, small_view, role="small", fill=1.00)
    q[20] = grid_area(20, 5, 4, 0.06, 0.10, 0.94, 0.90, small_view, role="dense", fill=1.00)
    q[30] = grid_area(30, 6, 5, 0.05, 0.08, 0.95, 0.92, small_view, role="dense", fill=1.00)
    return spec(name, q)


def vertical_big_bottom_template(name: str, small_view: str = "front", hero_view: str = "angle") -> TemplateSpec:
    return standard_grid_hero_template(
        name=name,
        small_view=small_view,
        hero_view=hero_view,
        hero_side="bottom",
    )


# =========================
# Templates
# =========================

def make_big_top_left_grid_template() -> TemplateSpec:
    q: Dict[int, List[Placement]] = {}

    q[1] = [p("image", 0.50, 0.50, 0.86, 0.86, role="single")]

    q[2] = [
        p("angle", 0.38, 0.35, 0.54, 0.42, role="hero"),
        p("angle", 0.62, 0.66, 0.54, 0.42, role="hero"),
    ]

    q[5] = [
        p("front", 0.50, 0.18, 0.72, 0.25, role="hero"),
        *grid_area(4, 2, 2, 0.18, 0.36, 0.82, 0.90, "front", role="small", fill=1.00),
    ]

    q[10] = [
        *grid_area(8, 2, 4, 0.10, 0.10, 0.60, 0.90, "front", role="small", fill=1.00),
        p("side", 0.82, 0.34, 0.34, 0.38, role="hero"),
        p("side", 0.82, 0.70, 0.34, 0.38, role="hero"),
    ]

    q[20] = [
        *grid_area(18, 3, 6, 0.08, 0.07, 0.62, 0.93, "front", role="dense", fill=1.00),
        p("side", 0.82, 0.32, 0.34, 0.36, role="hero"),
        p("side", 0.82, 0.70, 0.34, 0.36, role="hero"),
    ]

    q[30] = [
        p("angle", 0.25, 0.18, 0.38, 0.28, role="hero"),
        *grid_area(29, 5, 6, 0.08, 0.34, 0.92, 0.94, "front", role="dense", fill=1.00),
    ]

    return spec("big_top_left_grid", q)


def make_mixed_knob_grid_template() -> TemplateSpec:
    return standard_grid_hero_template(
        name="mixed_knob_grid",
        small_view="front",
        hero_view="angle",
        hero_side="right",
    )


def make_round_drain_grid_template() -> TemplateSpec:
    return standard_grid_hero_template(
        name="round_drain_grid",
        small_view="front",
        hero_view="image",
        hero_side="right",
    )


def make_square_drain_grid_template() -> TemplateSpec:
    return standard_grid_hero_template(
        name="square_drain_grid",
        small_view="front",
        hero_view="image",
        hero_side="right",
    )


def make_elbow_mixed_grid_template() -> TemplateSpec:
    return standard_grid_hero_template(
        name="elbow_mixed_grid",
        small_view="angle",
        hero_view="side",
        hero_side="right",
    )


def make_round_strainer_mix_template() -> TemplateSpec:
    return standard_grid_hero_template(
        name="round_strainer_mix",
        small_view="front",
        hero_view="angle",
        hero_side="right",
    )


def make_big_bottom_center_grid_template() -> TemplateSpec:
    return standard_grid_hero_template(
        name="big_bottom_center_grid",
        small_view="front",
        hero_view="image",
        hero_side="bottom",
    )


def make_coil_grid_template() -> TemplateSpec:
    return horizontal_dense_template(
        name="coil_grid",
        small_view="image",
    )


def make_horizontal_plus_small_grid_template() -> TemplateSpec:
    return standard_grid_hero_template(
        name="horizontal_plus_small_grid",
        small_view="front",
        hero_view="image",
        hero_side="bottom",
    )


def make_horizontal_dense_grid_template() -> TemplateSpec:
    return horizontal_dense_template(
        name="horizontal_dense_grid",
        small_view="front",
    )


def make_vertical_big_bottom_grid_template() -> TemplateSpec:
    return vertical_big_bottom_template(
        name="vertical_big_bottom_grid",
        small_view="front",
        hero_view="image",
    )


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
    return {t.template_name: t for t in templates}


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
    sku = clean_cell(row.get("sku", ""))
    raw_template_no = normalize_template_key(row.get("template_no", ""))
    raw_template_name = clean_cell(row.get("template_name", ""))

    resolution_note = ""

    if raw_template_name and raw_template_name not in template_names_in_ref and is_numeric_like(raw_template_name):
        if not raw_template_no:
            raw_template_no = normalize_template_key(raw_template_name)
            raw_template_name = ""
            resolution_note = "template_name_numeric_treated_as_template_no"

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
    scale_config: Optional[Dict[str, Any]] = None,
) -> Image.Image:
    cfg = deep_merge(DEFAULT_SCALE_CONFIG, scale_config)
    global_render_scale = float(cfg.get("global", {}).get("render_scale", 1.0))
    role_scales = get_template_scale_cfg(cfg, template.template_name)

    if quantity not in template.quantity_to_placements:
        raise ValueError(f"Template {template.template_name} does not support quantity={quantity}")

    canvas = Image.new("RGBA", (canvas_size, canvas_size), background_rgb + (255,))
    placements = template.quantity_to_placements[quantity]

    for item in placements:
        view = item.view
        if view not in view_images:
            view = "image"

        src = view_images[view]

        role_scale = role_scales.get(item.role, 1.0)
        local_scale = item.scale if item.scale is not None else 1.0
        final_scale = role_scale * local_scale * global_render_scale

        target_w = int(canvas_size * item.w * final_scale)
        target_h = int(canvas_size * item.h * final_scale)

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
    scale_config: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    input_df = normalize_input_df(input_df)
    template_df = normalize_template_df(template_df)

    output_dir = ensure_dir(output_dir)
    cache_dir = ensure_dir(cache_dir)

    quantity_override = None
    if quantities:
        quantity_override = [int(q) for q in quantities]

    builtin_templates = get_builtin_templates()
    runlog: List[Dict[str, Any]] = []
    template_names_in_ref, template_no_to_name = build_template_ref_maps(template_df)

    for idx, row in input_df.iterrows():
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
                scale_config=scale_config,
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
                        scale_config=scale_config,
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
                    "scale_config_json": json.dumps(scale_config or {}, ensure_ascii=False),
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
                "scale_config_json": json.dumps(scale_config or {}, ensure_ascii=False),
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
    scale_config: Optional[Dict[str, Any]] = None,
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
        scale_config=scale_config,
    )
