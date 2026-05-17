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
]

OPTIONAL_TEMPLATE_COLUMNS = [
    "template_display_name",
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

REQUIRED_LAYOUT_COLUMNS = [
    "template_name",
    "quantity",
    "role",
    "pattern",
    "view",
    "count",
    "rows",
    "cols",
    "x1",
    "y1",
    "x2",
    "y2",
    "scale",
    "z_index",
    "note",
]

SUPPORTED_QUANTITIES = [1, 2, 5, 10, 20, 30]

TYPE_TO_QUANTITIES = {
    "10": [1, 2, 5, 10],
    "30": [1, 2, 5, 10, 20, 30],
}

VIEW_KEYS = ["image", "front", "angle", "side"]

DEFAULT_SCALE_CONFIG: Dict[str, Any] = {
    "global": {
        # Source image is normalized into this transparent square first.
        "sprite_canvas_size": 1000,
        "sprite_max_fill": 0.82,

        # Final universal multiplier. Usually keep 1.00.
        "render_scale": 1.00,
    },
    "templates": {
        # Optional per-template multiplier.
        # Example:
        # "mixed_knob_grid": {"template_scale": 1.05}
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
    anchor: str = "center"
    scale: float = 1.0


@dataclass
class LayoutRule:
    template_name: str
    quantity: int
    role: str
    pattern: str
    view: str
    count: int
    rows: int
    cols: int
    x1: float
    y1: float
    x2: float
    y2: float
    scale: float
    z_index: int
    note: str = ""


@dataclass
class TemplateSpec:
    template_name: str
    quantity_to_placements: Dict[int, List[Placement]]


# =========================
# Utility
# =========================

def deep_merge_dict(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    result = json.loads(json.dumps(base))
    if not override:
        return result

    def merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(a.get(k), dict):
                merge(a[k], v)
            else:
                a[k] = v
        return a

    return merge(result, override)


def get_scale_config(scale_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return deep_merge_dict(DEFAULT_SCALE_CONFIG, scale_config)


def get_global_scale_value(scale_config: Optional[Dict[str, Any]], key: str, default: Any) -> Any:
    cfg = get_scale_config(scale_config)
    return cfg.get("global", {}).get(key, default)


def get_template_scale(template_name: str, scale_config: Optional[Dict[str, Any]] = None) -> float:
    cfg = get_scale_config(scale_config)
    return float(cfg.get("templates", {}).get(template_name, {}).get("template_scale", 1.0))


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


def to_int(value: Any, field_name: str, default: Optional[int] = None) -> int:
    value = clean_cell(value)
    if not value:
        if default is not None:
            return default
        raise ValueError(f"{field_name} is empty")
    try:
        return int(float(value))
    except Exception:
        raise ValueError(f"{field_name} must be integer-like, got: {value}")


def to_float(value: Any, field_name: str, default: Optional[float] = None) -> float:
    value = clean_cell(value)
    if not value:
        if default is not None:
            return default
        raise ValueError(f"{field_name} is empty")
    try:
        return float(value)
    except Exception:
        raise ValueError(f"{field_name} must be numeric, got: {value}")


def clamp01(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


# =========================
# Google Sheet read
# =========================

def read_google_sheet_tabs(
    sheet_url: str,
    service_account_json_or_b64: str,
    input_tab: str,
    template_tab: str,
    layout_tab: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
    layout_ws = sh.worksheet(layout_tab)

    input_df = values_to_df(input_ws.get_all_values(), input_tab)
    template_df = values_to_df(template_ws.get_all_values(), template_tab)
    layout_df = values_to_df(layout_ws.get_all_values(), layout_tab)

    return input_df, template_df, layout_df


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
    Normalize every source image into the same transparent square.
    Layout size is mainly controlled by Ref__BundleTemplateLayouts.scale.
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
    images: Dict[str, Image.Image] = {}
    source_paths: Dict[str, str] = {}

    sprite_canvas_size = int(get_global_scale_value(scale_config, "sprite_canvas_size", 1000))
    sprite_max_fill = float(get_global_scale_value(scale_config, "sprite_max_fill", 0.82))

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
# Sheet-driven layout helpers
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


def grid_area(
    count: int,
    cols: int,
    rows: int,
    left: float,
    top: float,
    right: float,
    bottom: float,
    view: str,
    fill: float = 0.92,
    scale: float = 1.0,
) -> List[Placement]:
    if count <= 0:
        return []

    cols = max(1, int(cols))
    rows = max(1, int(rows))

    width = max(0.001, right - left)
    height = max(0.001, bottom - top)
    cell_w = width / cols
    cell_h = height / rows

    placements: List[Placement] = []
    for i in range(count):
        r = i // cols
        c = i % cols
        if r >= rows:
            break

        x = left + cell_w * (c + 0.5)
        y = top + cell_h * (r + 0.5)
        placements.append(
            p(
                view=view,
                x=x,
                y=y,
                w=cell_w * fill,
                h=cell_h * fill,
                scale=scale,
            )
        )

    return placements


def diagonal_area(rule: LayoutRule) -> List[Placement]:
    if rule.count <= 0:
        return []
    if rule.count == 1:
        return [
            p(
                rule.view,
                (rule.x1 + rule.x2) / 2,
                (rule.y1 + rule.y2) / 2,
                rule.x2 - rule.x1,
                rule.y2 - rule.y1,
                scale=rule.scale,
            )
        ]

    width = max(0.001, rule.x2 - rule.x1)
    height = max(0.001, rule.y2 - rule.y1)
    item_w = width * 0.62
    item_h = height * 0.62

    placements: List[Placement] = []
    for i in range(rule.count):
        if rule.count == 1:
            t = 0.5
        else:
            t = i / max(1, rule.count - 1)
        # Slight diagonal from upper-left to lower-right.
        x = rule.x1 + width * (0.34 + 0.32 * t)
        y = rule.y1 + height * (0.34 + 0.32 * t)
        placements.append(p(rule.view, x, y, item_w, item_h, scale=rule.scale))

    return placements


def pyramid_area(rule: LayoutRule) -> List[Placement]:
    if rule.count <= 0:
        return []

    width = max(0.001, rule.x2 - rule.x1)
    height = max(0.001, rule.y2 - rule.y1)

    # Common hand-tuned patterns.
    if rule.count == 3:
        positions = [
            (0.50, 0.25),
            (0.28, 0.72),
            (0.72, 0.72),
        ]
        item_w = width * 0.42
        item_h = height * 0.42
    elif rule.count == 5:
        positions = [
            (0.50, 0.16),
            (0.28, 0.48),
            (0.72, 0.48),
            (0.28, 0.80),
            (0.72, 0.80),
        ]
        item_w = width * 0.38
        item_h = height * 0.32
    else:
        # Fallback: centered triangle-ish rows.
        positions = []
        rows = max(1, rule.rows)
        remaining = rule.count
        y_step = 1.0 / rows
        for r in range(rows):
            n = min(remaining, r + 1)
            remaining -= n
            if n <= 0:
                break
            for c in range(n):
                x = (c + 1) / (n + 1)
                y = y_step * (r + 0.5)
                positions.append((x, y))
            if remaining <= 0:
                break
        item_w = width / max(1, rule.cols) * 0.95
        item_h = height / max(1, rule.rows) * 0.95

    placements: List[Placement] = []
    for rel_x, rel_y in positions[: rule.count]:
        x = rule.x1 + width * rel_x
        y = rule.y1 + height * rel_y
        placements.append(p(rule.view, x, y, item_w, item_h, scale=rule.scale))

    return placements


def placements_from_rule(rule: LayoutRule) -> List[Placement]:
    pattern = rule.pattern.lower().strip()
    role = rule.role.lower().strip()

    # Single-center default.
    if pattern == "center" or role == "single":
        if rule.count <= 1:
            return [
                p(
                    rule.view,
                    (rule.x1 + rule.x2) / 2,
                    (rule.y1 + rule.y2) / 2,
                    rule.x2 - rule.x1,
                    rule.y2 - rule.y1,
                    scale=rule.scale,
                )
            ]
        return grid_area(
            count=rule.count,
            cols=rule.cols,
            rows=rule.rows,
            left=rule.x1,
            top=rule.y1,
            right=rule.x2,
            bottom=rule.y2,
            view=rule.view,
            fill=1.0,
            scale=rule.scale,
        )

    if pattern == "matrix":
        return grid_area(
            count=rule.count,
            cols=rule.cols,
            rows=rule.rows,
            left=rule.x1,
            top=rule.y1,
            right=rule.x2,
            bottom=rule.y2,
            view=rule.view,
            fill=1.0,
            scale=rule.scale,
        )

    if pattern == "vertical":
        rows = rule.rows if rule.rows > 0 else rule.count
        cols = rule.cols if rule.cols > 0 else 1
        return grid_area(
            count=rule.count,
            cols=cols,
            rows=rows,
            left=rule.x1,
            top=rule.y1,
            right=rule.x2,
            bottom=rule.y2,
            view=rule.view,
            fill=1.0,
            scale=rule.scale,
        )

    if pattern == "horizontal":
        rows = rule.rows if rule.rows > 0 else 1
        cols = rule.cols if rule.cols > 0 else rule.count
        return grid_area(
            count=rule.count,
            cols=cols,
            rows=rows,
            left=rule.x1,
            top=rule.y1,
            right=rule.x2,
            bottom=rule.y2,
            view=rule.view,
            fill=1.0,
            scale=rule.scale,
        )

    if pattern == "diagonal":
        return diagonal_area(rule)

    if pattern == "pyramid":
        return pyramid_area(rule)

    if pattern == "manual":
        # Manual is reserved for future x/y-per-piece expansion.
        # Current fallback is matrix so the job still runs.
        return grid_area(
            count=rule.count,
            cols=rule.cols,
            rows=rule.rows,
            left=rule.x1,
            top=rule.y1,
            right=rule.x2,
            bottom=rule.y2,
            view=rule.view,
            fill=1.0,
            scale=rule.scale,
        )

    raise ValueError(
        f"Unsupported pattern={rule.pattern} for template={rule.template_name}, quantity={rule.quantity}"
    )


def build_templates_from_layout_df(
    layout_df: pd.DataFrame,
    scale_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, TemplateSpec]:
    rules: List[LayoutRule] = []

    for _, row in layout_df.iterrows():
        template_name = clean_cell(row.get("template_name", ""))
        if not template_name:
            continue

        quantity = to_int(row.get("quantity", ""), "quantity")
        role = clean_cell(row.get("role", "")).lower()
        pattern = clean_cell(row.get("pattern", "")).lower()
        view = clean_cell(row.get("view", "")).lower() or "image"

        if view not in VIEW_KEYS:
            raise ValueError(
                f"Unsupported view={view} in Ref__BundleTemplateLayouts for template={template_name}, quantity={quantity}"
            )

        scale = to_float(row.get("scale", ""), "scale", default=1.0)
        scale *= get_template_scale(template_name, scale_config)

        rule = LayoutRule(
            template_name=template_name,
            quantity=quantity,
            role=role,
            pattern=pattern,
            view=view,
            count=to_int(row.get("count", ""), "count", default=1),
            rows=to_int(row.get("rows", ""), "rows", default=1),
            cols=to_int(row.get("cols", ""), "cols", default=1),
            x1=clamp01(to_float(row.get("x1", ""), "x1")),
            y1=clamp01(to_float(row.get("y1", ""), "y1")),
            x2=clamp01(to_float(row.get("x2", ""), "x2")),
            y2=clamp01(to_float(row.get("y2", ""), "y2")),
            scale=scale,
            z_index=to_int(row.get("z_index", ""), "z_index", default=1),
            note=clean_cell(row.get("note", "")),
        )

        if rule.x2 <= rule.x1 or rule.y2 <= rule.y1:
            raise ValueError(
                f"Invalid layout bounds for template={template_name}, quantity={quantity}: "
                f"x1/y1/x2/y2 = {rule.x1}/{rule.y1}/{rule.x2}/{rule.y2}"
            )

        rules.append(rule)

    quantity_to_rules: Dict[Tuple[str, int], List[LayoutRule]] = {}
    for rule in rules:
        quantity_to_rules.setdefault((rule.template_name, rule.quantity), []).append(rule)

    templates: Dict[str, TemplateSpec] = {}
    template_names = sorted(set(r.template_name for r in rules))

    for template_name in template_names:
        q: Dict[int, List[Placement]] = {}

        for (name, quantity), group_rules in quantity_to_rules.items():
            if name != template_name:
                continue

            placements: List[Placement] = []
            for rule in sorted(group_rules, key=lambda r: r.z_index):
                placements.extend(placements_from_rule(rule))

            q[quantity] = placements

        templates[template_name] = TemplateSpec(
            template_name=template_name,
            quantity_to_placements=q,
        )

    return templates


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
    if quantity not in template.quantity_to_placements:
        available = sorted(template.quantity_to_placements.keys())
        raise ValueError(
            f"No layout for template={template.template_name}, quantity={quantity}. "
            f"Available quantities for this template: {available}"
        )

    canvas = Image.new("RGBA", (canvas_size, canvas_size), background_rgb + (255,))

    placements = template.quantity_to_placements[quantity]
    render_scale = float(get_global_scale_value(scale_config, "render_scale", 1.0))

    for item in placements:
        view = item.view
        if view not in view_images:
            view = "image"

        src = view_images[view]

        target_w = max(1, int(canvas_size * item.w * item.scale * render_scale))
        target_h = max(1, int(canvas_size * item.h * item.scale * render_scale))

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


def normalize_layout_df(layout_df: pd.DataFrame) -> pd.DataFrame:
    df = layout_df.copy()
    df.columns = [normalize_header(c) for c in df.columns]
    validate_columns(df, REQUIRED_LAYOUT_COLUMNS, "Ref__BundleTemplateLayouts")

    for c in REQUIRED_LAYOUT_COLUMNS:
        df[c] = df[c].apply(clean_cell)

    df = df[df["template_name"].astype(str).str.strip() != ""].copy()
    return df


def generate_from_dataframes(
    input_df: pd.DataFrame,
    template_df: pd.DataFrame,
    layout_df: pd.DataFrame,
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
    layout_df = normalize_layout_df(layout_df)

    output_dir = ensure_dir(output_dir)
    cache_dir = ensure_dir(cache_dir)

    quantity_override = None
    if quantities:
        quantity_override = [int(q) for q in quantities]

    layout_templates = build_templates_from_layout_df(
        layout_df=layout_df,
        scale_config=scale_config,
    )

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

            if resolved_template_name not in layout_templates:
                raise ValueError(
                    f"template_name exists in Ref__BundleTemplates but has no layout rows in "
                    f"Ref__BundleTemplateLayouts: {resolved_template_name}"
                )

            template = layout_templates[resolved_template_name]
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
                    "layout_rows_for_quantity": len(template.quantity_to_placements.get(qty, [])),
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
                "layout_rows_for_quantity": "",
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
    layout_tab: str,
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
    input_df, template_df, layout_df = read_google_sheet_tabs(
        sheet_url=sheet_url,
        service_account_json_or_b64=service_account_json_or_b64,
        input_tab=input_tab,
        template_tab=template_tab,
        layout_tab=layout_tab,
    )

    return generate_from_dataframes(
        input_df=input_df,
        template_df=template_df,
        layout_df=layout_df,
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
