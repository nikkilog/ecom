# -*- coding: utf-8 -*-
"""Build SPU Product Creation Input from SPU_Source.

Formal repository target:
    ecom/shopify_create/7_4_1_spu_product_input.py
Import path:
    shopify_create.7_4_1_spu_product_input

Scope
-----
- Read the first 23 formal columns of ``SPU_Source`` only.
- Treat ``SPU_Source`` as the authoritative business source; existing ``Input``
  values/formulas are never read as source data.
- Validate SPU-V / Status / Variant Base identity contracts.
- CREATE: group by SPU-V + Variant Base and build a new Product model.
- ADD: resolve exactly one existing Target Product ID per SPU-V, exclude Source
  rows whose SPU columns already contain a Product ID, and build only the new
  Variant Base rows for that existing Product.
- Expand Variant Base by Max Quantity-V into Nr01/Nr02/Nr05/Nr10[/Nr20/Nr30].
- Resolve Config-driven metafield field_keys from ``Cfg__Fields`` using
  ``field_id = entity_type|field_key`` identity and owner-aware display names.
- Generate Product Description Rich Text JSON from the already size-cleaned
  Description HTML, so G/H are the same semantic content in two representations.
- Overwrite ``Input`` with a two-row header (display name + field_key) and the
  generated rows.
- Read ``V_Product_Handle`` only when ADD rows actually need to be generated.
- Missing/ambiguous ADD Product lookups are written to ``sys.input_error`` and
  do not stop the whole Input build; structural Source/Schema errors still fail fast.
- Write RunLog evidence.

This module does NOT Prepare/Preview and does NOT call Shopify. Preview and
Result are untouched. It supports Local and Colab through the existing
Console Core runtime/auth infrastructure in 7_1_1_generic_product_prepare.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from bs4 import BeautifulSoup, NavigableString, Tag


MODULE_VERSION = "2026-08-04-dynamic-input-range-v4"
MODULE_PATH = "shopify_create.7_4_1_spu_product_input"
DEFAULT_JOB_NAME = "spu_product_input"

INFRA_MODULE_PATH = "shopify_create.7_1_1_generic_product_prepare"
EXPECTED_INFRA_MODULE_VERSION = "2026-08-02-runtime-boundary-v1"

SOURCE_HEADERS_23 = [
    "SPU-V",
    "Status",
    "Variant Base",
    "Each Box",
    "Each Box",
    "Each Box",
    "Wholesale",
    "Wholesale",
    "SPU",
    "SPU",
    "SPU",
    "Product Type-1",
    "Product Type-2",
    "Product Type-3",
    "Product Type-4",
    "Max Quantity-V",
    "Product Title",
    "Size-V",
    "Product Description (HTML)",
    "Product Description",
    "Price",
    "Primary category",
    "Multiplier-V",
]

# Positional Source columns. Duplicate human headers are intentional.
SRC_SPU_V = 0
SRC_STATUS = 1
SRC_VARIANT_BASE = 2
SRC_SPU_ID_COLS = (8, 9, 10)
SRC_PRODUCT_TYPE_1 = 11
SRC_PRODUCT_TYPE_2 = 12
SRC_PRODUCT_TYPE_3 = 13
SRC_PRODUCT_TYPE_4 = 14
SRC_MAX_QUANTITY = 15
SRC_PRODUCT_TITLE = 16
SRC_SIZE = 17
SRC_DESCRIPTION_HTML = 18
SRC_DESCRIPTION_JSON = 19  # Formal Source column; intentionally not authoritative for H.
SRC_PRICE = 20
SRC_PRIMARY_CATEGORY = 21

QUANTITY_SUFFIXES = {
    10: [(1, "Nr01"), (2, "Nr02"), (5, "Nr05"), (10, "Nr10")],
    30: [
        (1, "Nr01"),
        (2, "Nr02"),
        (5, "Nr05"),
        (10, "Nr10"),
        (20, "Nr20"),
        (30, "Nr30"),
    ],
}

# Human Input schema. Metafield field_keys are resolved at runtime from Config.
INPUT_COLUMNS: List[Tuple[str, str, str]] = [
    ("Action", "SYSTEM", "sys.action"),
    ("Product Key", "SYSTEM", "sys.product_key"),
    ("Variant Key", "SYSTEM", "sys.variant_key"),
    ("Target Product ID", "SYSTEM", "sys.target_product_id"),
    ("Input Error", "SYSTEM", "sys.input_error"),
    ("SKU Title", "SYSTEM", "sys.source_title"),
    ("Title", "PRODUCT", "core.title"),
    ("Handle", "PRODUCT", "core.handle"),
    ("Description HTML", "PRODUCT", "core.description_html"),
    ("Product Description", "PRODUCT", "CFG"),
    ("Vendor", "PRODUCT", "core.vendor"),
    ("Product Type", "PRODUCT", "core.product_type"),
    ("Tags", "PRODUCT", "core.tags"),
    ("Option 1 Name", "PRODUCT", "core.option1_name"),
    ("Option 1 Value", "VARIANT", "core.option1_value"),
    ("Option 2 Name", "PRODUCT", "core.option2_name"),
    ("Option 2 Value", "VARIANT", "core.option2_value"),
    ("Option 3 Name", "PRODUCT", "core.option3_name"),
    ("Option 3 Value", "VARIANT", "core.option3_value"),
    ("SKU", "VARIANT", "core.sku"),
    ("Price", "VARIANT", "core.price"),
    ("Compare-at Price", "VARIANT", "core.compare_at_price"),
    ("Inventory Location", "INVENTORY", "inventory.location_code"),
    ("Inventory Quantity", "INVENTORY", "inventory.quantity"),
    ("Product Type Internal", "PRODUCT", "CFG"),
    ("Product Subtype Internal", "PRODUCT", "CFG"),
    ("Product Type-1", "PRODUCT", "CFG"),
    ("Product Type-2", "PRODUCT", "CFG"),
    ("Product Type-3", "PRODUCT", "CFG"),
    ("Product Type-4", "PRODUCT", "CFG"),
    ("SPU-V", "VARIANT", "CFG"),
    ("Variant Base", "VARIANT", "CFG"),
    ("SKU Suffix-V", "VARIANT", "CFG"),
    ("Size-V", "VARIANT", "CFG"),
    ("Unit Count-V", "VARIANT", "CFG"),
    ("Settlement Quantity-V", "VARIANT", "CFG"),
    ("Multiplier-V", "VARIANT", "CFG"),
    ("SKU Unit Price-V", "VARIANT", "CFG"),
    ("Max Quantity-V", "VARIANT", "CFG"),
    ("SKU Group", "PRODUCT", "CFG"),
    ("Primary category", "PRODUCT", "CFG"),
]

INPUT_HEADERS = [item[0] for item in INPUT_COLUMNS]


@dataclass(frozen=True)
class SourceRow:
    source_row: int
    values: Tuple[str, ...]
    spu_v: str
    status: str
    variant_base: str
    spu_product_ids: Tuple[str, ...]

    def get(self, index: int) -> str:
        return self.values[index]

    @property
    def has_existing_spu_id(self) -> bool:
        return bool(self.spu_product_ids)


class _HTMLVisibleTextTransformer(HTMLParser):
    """Preserve tags/attributes while transforming visible text nodes only."""

    def __init__(self, transform) -> None:
        super().__init__(convert_charrefs=False)
        self.transform = transform
        self.parts: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        self.parts.append(self.get_starttag_text())

    def handle_startendtag(self, tag: str, attrs) -> None:
        self.parts.append(self.get_starttag_text())

    def handle_endtag(self, tag: str) -> None:
        self.parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        self.parts.append(self.transform(data))

    def handle_entityref(self, name: str) -> None:
        self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.parts.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        self.parts.append(f"<!--{data}-->")

    def handle_decl(self, decl: str) -> None:
        self.parts.append(f"<!{decl}>")

    def unknown_decl(self, data: str) -> None:
        self.parts.append(f"<![{data}]>")

    def get_output(self) -> str:
        return "".join(self.parts)


def _infra():
    module = importlib.import_module(INFRA_MODULE_PATH)
    loaded_version = _safe_str(getattr(module, "MODULE_VERSION", ""))
    if loaded_version != EXPECTED_INFRA_MODULE_VERSION:
        raise RuntimeError(
            "SPU Input Builder requires the validated Generic Prepare runtime "
            f"infrastructure version {EXPECTED_INFRA_MODULE_VERSION}; "
            f"loaded={loaded_version or '<blank>'}."
        )
    return module


def read_secret(*args, **kwargs):
    return _infra().read_secret(*args, **kwargs)


def _build_gspread_client(*args, **kwargs):
    return _infra()._build_gspread_client(*args, **kwargs)


def _sheets_retry(*args, **kwargs):
    return _infra()._sheets_retry(*args, **kwargs)


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_header(value: Any) -> str:
    return re.sub(r"\s+", " ", _safe_str(value))


def _is_digits(value: Any) -> bool:
    return bool(re.fullmatch(r"\d+", _safe_str(value)))


def _parse_integer(value: Any, *, label: str) -> int:
    text = _safe_str(value)
    if not text:
        raise ValueError(f"{label} is blank.")
    try:
        dec = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{label} must be an integer; got={text!r}.") from exc
    if dec != dec.to_integral_value():
        raise ValueError(f"{label} must be an integer; got={text!r}.")
    return int(dec)


def _parse_decimal(value: Any, *, label: str) -> Decimal:
    text = _safe_str(value)
    if not text:
        raise ValueError(f"{label} is blank.")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{label} must be numeric; got={text!r}.") from exc


def _money_2(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")


def _slugify_title(title: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "-", _safe_str(title)).strip("-").lower()
    if not value:
        raise ValueError("Generated Product Handle is blank after title normalization.")
    return value


def _normalize_dash_chars(value: str) -> str:
    return value.replace("–", "-").replace("—", "-").replace("−", "-")


def _size_components(size_value: str) -> List[str]:
    size = _normalize_dash_chars(_safe_str(size_value))
    if not size:
        return []
    parts = [p.strip() for p in re.split(r"\s*[xX×]\s*", size) if p.strip()]
    return parts or [size]


def _numeric_phrase_from_size_component(component: str) -> str:
    text = _normalize_dash_chars(component)
    text = re.sub(
        r"\s*(?:\"|″|inches?|inch|in\.?|['’])\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    return text


def _numeric_phrase_regex(phrase: str) -> str:
    """Build tolerant regex for one whole/fraction/mixed-number size token."""
    text = _normalize_dash_chars(_safe_str(phrase))
    text = re.sub(r"\s+", " ", text)

    # Mixed number: 1-1/4, 1 1/4, or 1 - 1/4.
    m = re.fullmatch(r"(\d+)\s*(?:-|\s)\s*(\d+)\s*/\s*(\d+)", text)
    if m:
        whole, num, den = map(re.escape, m.groups())
        return rf"{whole}\s*(?:-\s*|\s+){num}\s*/\s*{den}"

    # Fraction: 1/2.
    m = re.fullmatch(r"(\d+)\s*/\s*(\d+)", text)
    if m:
        num, den = map(re.escape, m.groups())
        return rf"{num}\s*/\s*{den}"

    # Whole number / decimal.
    m = re.fullmatch(r"\d+(?:\.\d+)?", text)
    if m:
        return re.escape(text)

    # Last-resort literal for unusual but explicit Size-V values.
    return re.escape(text).replace(r"\ ", r"\s+")


_INCH_UNIT_RE = r"(?:\s*(?:\"|″|inches?|inch|in\.?|['’]))"


def _build_size_patterns(size_value: str) -> Tuple[Optional[re.Pattern], List[re.Pattern]]:
    components = _size_components(size_value)
    if not components:
        return None, []

    nums = [_numeric_phrase_from_size_component(p) for p in components]
    nums = [n for n in nums if n]
    if not nums:
        return None, []

    num_patterns = [_numeric_phrase_regex(n) for n in nums]

    full_pattern: Optional[re.Pattern] = None
    if len(num_patterns) >= 2:
        # Handles forms such as:
        #   1" x 1/2"
        #   3/4 X 1/2 in.
        # The unit may appear on either/both components, but the final
        # component must carry an inch marker to avoid deleting unrelated math.
        joined = rf"\s*[xX×]\s*".join(
            [rf"(?<![\d/]){p}(?![\d/])(?:{_INCH_UNIT_RE})?" for p in num_patterns[:-1]]
            + [rf"(?<![\d/]){num_patterns[-1]}(?![\d/]){_INCH_UNIT_RE}"]
        )
        full_pattern = re.compile(joined, flags=re.IGNORECASE)

    individual = [
        re.compile(
            rf"(?<![\d/]){p}(?![\d/]){_INCH_UNIT_RE}",
            flags=re.IGNORECASE,
        )
        for p in num_patterns
    ]
    return full_pattern, individual


def _clean_text_after_size_removal(value: str) -> str:
    text = re.sub(r"[ \t]{2,}", " ", value)
    text = re.sub(r"^[ \t]*(?:[xX×]|[-–—,:;|])+[ \t]*", "", text)
    text = re.sub(r"[ \t]*(?:[xX×]|[-–—,:;|])+[ \t]*$", "", text)
    return text.strip()


def _remove_size_matches(text: str, size_value: str) -> Tuple[str, bool]:
    original = "" if text is None else str(text)
    if not _safe_str(size_value):
        return original, False

    full_pattern, individual_patterns = _build_size_patterns(size_value)
    out = original
    matched = False
    if full_pattern is not None:
        out, count = full_pattern.subn(" ", out)
        matched = matched or count > 0
    for pattern in individual_patterns:
        out, count = pattern.subn(" ", out)
        matched = matched or count > 0
    return out, matched


def remove_size_expressions(text: str, size_value: str) -> str:
    """Remove all expressions corresponding to Size-V from plain text.

    Supports both contiguous size strings (``1\" x 1/2\"``) and separated
    dimensions (``1\" CTS ... x 1/2\" ...``). Size-V blank is a no-op.
    Edge separators are cleaned because this function is used for Product Title.
    """
    original = "" if text is None else str(text)
    out, matched = _remove_size_matches(original, size_value)
    if not matched:
        return original
    return _clean_text_after_size_removal(out)


def _remove_size_from_html_text_node(text: str, size_value: str) -> str:
    """Remove sizes inside one HTML text node without deleting sibling separators.

    A text node can begin with punctuation that semantically connects it to a
    previous inline element (for example ``</strong> – Designed...``). Therefore
    HTML cleanup only collapses duplicate horizontal whitespace; it does not strip
    edge punctuation.
    """
    original = "" if text is None else str(text)
    out, matched = _remove_size_matches(original, size_value)
    if not matched:
        return original
    return re.sub(r"[ \t]{2,}", " ", out)

def remove_size_from_html(html_value: str, size_value: str) -> str:
    raw = "" if html_value is None else str(html_value)
    if not raw or not _safe_str(size_value):
        return raw
    parser = _HTMLVisibleTextTransformer(
        lambda data: _remove_size_from_html_text_node(data, size_value)
    )
    parser.feed(raw)
    parser.close()
    return parser.get_output()


def _normalize_inline_text(value: str) -> str:
    return " ".join((value or "").split())


def _rich_text_text_node(
    value: str,
    *,
    bold: bool = False,
    italic: bool = False,
) -> Optional[Dict[str, Any]]:
    cleaned = _normalize_inline_text(value)
    if not cleaned:
        return None
    node: Dict[str, Any] = {"type": "text", "value": cleaned}
    if bold:
        node["bold"] = True
    if italic:
        node["italic"] = True
    return node


def _rich_text_inline_children(
    node: Any,
    *,
    bold: bool = False,
    italic: bool = False,
) -> List[Dict[str, Any]]:
    """Convert inline HTML nodes using the existing PBS RichText capability.

    This is intentionally limited to the previously used semantics: text,
    bold/italic, links, and recursive inline children. Block conversion happens
    in ``html_to_shopify_rich_text_json``.
    """
    if isinstance(node, NavigableString):
        text = _rich_text_text_node(str(node), bold=bold, italic=italic)
        return [text] if text else []

    if not isinstance(node, Tag):
        return []

    tag_name = node.name.lower()
    next_bold = bold or tag_name in {"b", "strong"}
    next_italic = italic or tag_name in {"i", "em"}

    if tag_name == "br":
        # The historical converter treats line breaks as formatting boundaries;
        # empty text nodes are not useful to Shopify Rich Text.
        return []

    children: List[Dict[str, Any]] = []
    for child in node.children:
        children.extend(
            _rich_text_inline_children(
                child,
                bold=next_bold,
                italic=next_italic,
            )
        )

    if tag_name == "a":
        url = _safe_str(node.get("href"))
        if url and children:
            link_node: Dict[str, Any] = {
                "type": "link",
                "url": url,
                "children": children,
            }
            title = _safe_str(node.get("title"))
            if title:
                link_node["title"] = title
            return [link_node]

    return children


def _rich_text_convert_list(tag: Tag) -> Optional[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for li in tag.find_all("li", recursive=False):
        children: List[Dict[str, Any]] = []
        for child in li.children:
            # Existing capability flattens/ignores nested list blocks at this
            # level rather than inventing a different Shopify tree contract.
            if isinstance(child, Tag) and child.name.lower() in {"ul", "ol"}:
                continue
            children.extend(_rich_text_inline_children(child))
        if not children:
            fallback = _rich_text_text_node(li.get_text(" ", strip=True))
            if fallback:
                children = [fallback]
        if children:
            items.append({"type": "list-item", "children": children})

    if not items:
        return None
    return {
        "type": "list",
        "listType": "ordered" if tag.name.lower() == "ol" else "unordered",
        "children": items,
    }


def _rich_text_convert_paragraph(tag: Tag) -> Optional[Dict[str, Any]]:
    children: List[Dict[str, Any]] = []
    for child in tag.children:
        children.extend(_rich_text_inline_children(child))
    if not children:
        return None
    return {"type": "paragraph", "children": children}


def html_to_shopify_rich_text_json(html_value: str) -> str:
    """Convert cleaned Description HTML to Shopify rich_text_field JSON.

    Capability donor: the existing PBS ``Build_Shopify_RichText_From_HTML``
    converter. 7.4.1 deliberately derives H from the already-cleaned G HTML so
    both columns carry the same semantic content and Source JSON cannot become a
    stale/incomplete competing source.
    """
    html_text = _safe_str(html_value)
    if not html_text:
        return ""

    soup = BeautifulSoup(html_text, "html.parser")
    root_children: List[Dict[str, Any]] = []

    for node in soup.contents:
        if isinstance(node, NavigableString):
            text = _rich_text_text_node(str(node))
            if text:
                root_children.append({"type": "paragraph", "children": [text]})
            continue
        if not isinstance(node, Tag):
            continue

        tag_name = node.name.lower()
        converted: Optional[Dict[str, Any]]
        if tag_name in {"ul", "ol"}:
            converted = _rich_text_convert_list(node)
        elif tag_name in {"p", "div"}:
            converted = _rich_text_convert_paragraph(node)
        elif tag_name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            heading_children = _rich_text_inline_children(node)
            converted = (
                {
                    "type": "heading",
                    "level": int(tag_name[1]),
                    "children": heading_children,
                }
                if heading_children
                else None
            )
        else:
            # Existing capability degrades unsupported top-level tags (e.g.
            # tables) to a visible-text paragraph instead of silently dropping
            # content. This preserves G/H semantic coverage.
            text = _rich_text_text_node(node.get_text(" ", strip=True))
            converted = (
                {"type": "paragraph", "children": [text]}
                if text
                else None
            )

        if converted:
            root_children.append(converted)

    if not root_children:
        visible = _rich_text_text_node(soup.get_text(" ", strip=True))
        if visible:
            root_children.append({"type": "paragraph", "children": [visible]})

    if not root_children:
        return ""

    return json.dumps(
        {"type": "root", "children": root_children},
        ensure_ascii=False,
        separators=(",", ":"),
    )

def _read_source(values: Sequence[Sequence[Any]]) -> List[SourceRow]:
    if not values:
        raise ValueError("SPU_Source is empty.")

    actual = [_normalize_header(v) for v in list(values[0])[:23]]
    expected = [_normalize_header(v) for v in SOURCE_HEADERS_23]
    if actual != expected:
        raise ValueError(
            "SPU_Source first 23 columns do not match the formal schema. "
            f"expected={expected}; actual={actual}"
        )

    rows: List[SourceRow] = []
    for source_row, raw in enumerate(values[1:], start=2):
        padded = list(raw) + [""] * max(0, 23 - len(raw))
        formal = tuple(_safe_str(v) for v in padded[:23])
        if not any(formal):
            continue

        spu_v = formal[SRC_SPU_V]
        status = formal[SRC_STATUS].upper()
        variant_base = formal[SRC_VARIANT_BASE]
        if not spu_v:
            raise ValueError(f"SPU_Source row {source_row}: SPU-V is blank.")
        if status not in {"CREATE", "ADD"}:
            raise ValueError(
                f"SPU_Source row {source_row}: Status must be CREATE or ADD; "
                f"got={formal[SRC_STATUS]!r}."
            )
        if not variant_base:
            raise ValueError(f"SPU_Source row {source_row}: Variant Base is blank.")

        ids = tuple(
            formal[index]
            for index in SRC_SPU_ID_COLS
            if _is_digits(formal[index])
        )
        rows.append(
            SourceRow(
                source_row=source_row,
                values=formal,
                spu_v=spu_v,
                status=status,
                variant_base=variant_base,
                spu_product_ids=ids,
            )
        )

    if not rows:
        raise ValueError("SPU_Source contains no effective data rows.")

    seen_variant_base: Dict[str, int] = {}
    for row in rows:
        key = row.variant_base.casefold()
        if key in seen_variant_base:
            raise ValueError(
                "Variant Base must be unique across SPU_Source. "
                f"duplicate={row.variant_base!r}; rows="
                f"{seen_variant_base[key]},{row.source_row}"
            )
        seen_variant_base[key] = row.source_row

    return rows


def _group_source(rows: Sequence[SourceRow]) -> "OrderedDict[str, List[SourceRow]]":
    groups: "OrderedDict[str, List[SourceRow]]" = OrderedDict()
    for row in rows:
        groups.setdefault(row.spu_v, []).append(row)
    return groups


def _validate_and_plan_groups(
    groups: Mapping[str, Sequence[SourceRow]],
) -> Tuple[Dict[str, Dict[str, Any]], List[SourceRow]]:
    group_meta: Dict[str, Dict[str, Any]] = {}
    planned_rows: List[SourceRow] = []

    for spu_v, group in groups.items():
        statuses = {row.status for row in group}
        if len(statuses) != 1:
            detail = [(row.source_row, row.status) for row in group]
            raise ValueError(
                f"SPU-V {spu_v!r} mixes CREATE/ADD statuses: {detail}"
            )
        status = next(iter(statuses))
        unique_target_ids = sorted(
            {
                product_id
                for row in group
                for product_id in row.spu_product_ids
            }
        )

        if status == "CREATE":
            if unique_target_ids:
                raise ValueError(
                    f"CREATE SPU-V {spu_v!r} contains existing SPU Product ID(s): "
                    f"{unique_target_ids}. CREATE requires none."
                )
            target_product_id = ""
            included = list(group)
        else:
            if len(unique_target_ids) != 1:
                raise ValueError(
                    f"ADD SPU-V {spu_v!r} must resolve exactly one unique Target "
                    f"Product ID from the three SPU columns; found={unique_target_ids}."
                )
            target_product_id = unique_target_ids[0]
            included = [row for row in group if not row.has_existing_spu_id]

        first = group[0]
        group_meta[spu_v] = {
            "status": status,
            "target_product_id": target_product_id,
            "first_row": first,
            "source_rows": list(group),
            "included_rows": included,
            "excluded_existing_rows": [row for row in group if row not in included],
        }
        planned_rows.extend(included)

    # Validate only rows that will actually generate Input variants.
    for row in planned_rows:
        max_qty = _parse_integer(
            row.get(SRC_MAX_QUANTITY),
            label=f"SPU_Source row {row.source_row} Max Quantity-V",
        )
        if max_qty not in QUANTITY_SUFFIXES:
            raise ValueError(
                f"SPU_Source row {row.source_row}: Max Quantity-V must be 10 or 30; "
                f"got={row.get(SRC_MAX_QUANTITY)!r}."
            )
        _parse_decimal(
            row.get(SRC_PRICE),
            label=f"SPU_Source row {row.source_row} Price",
        )

    return group_meta, planned_rows


def _read_product_handle_map(
    values: Sequence[Sequence[Any]],
    target_product_ids: Iterable[str],
) -> Dict[str, Dict[str, str]]:
    wanted = {_safe_str(v) for v in target_product_ids if _safe_str(v)}
    if not wanted:
        return {}
    if not values:
        raise ValueError("V_Product_Handle is empty but ADD requires existing Product lookup.")

    headers = [_normalize_header(v) for v in values[0]]
    required = {
        "Product ID (numeric)",
        "Product Title",
        "Product Handle",
    }
    missing = sorted(required - set(headers))
    if missing:
        raise ValueError(f"V_Product_Handle missing required columns: {missing}")
    h = {name: headers.index(name) for name in required}

    matches: Dict[str, List[Tuple[str, str, int]]] = {pid: [] for pid in wanted}
    for source_row, raw in enumerate(values[1:], start=2):
        padded = list(raw) + [""] * max(0, len(headers) - len(raw))
        product_id = _safe_str(padded[h["Product ID (numeric)"]])
        if product_id not in wanted:
            continue
        title = _safe_str(padded[h["Product Title"]])
        handle = _safe_str(padded[h["Product Handle"]])
        matches[product_id].append((title, handle, source_row))

    out: Dict[str, Dict[str, str]] = {}
    for product_id in sorted(wanted):
        rows = matches.get(product_id, [])
        if not rows:
            # Business-data lookup errors are written into Input instead of
            # stopping the entire 7.4.1 generation job. The target ID itself
            # is preserved so the operator can repair V_Product_Handle and rerun.
            out[product_id] = {
                "title": "",
                "handle": "",
                "matching_rows": "0",
                "input_error": f"TARGET_PRODUCT_NOT_FOUND: {product_id}",
            }
            continue

        titles = sorted({title for title, _handle, _row in rows if title})
        handles = sorted({handle for _title, handle, _row in rows if handle})
        if len(titles) != 1 or len(handles) != 1:
            out[product_id] = {
                "title": "",
                "handle": "",
                "matching_rows": str(len(rows)),
                "input_error": (
                    "TARGET_PRODUCT_LOOKUP_AMBIGUOUS: "
                    f"{product_id}; titles={titles}; handles={handles}"
                ),
            }
            continue

        out[product_id] = {
            "title": titles[0],
            "handle": handles[0],
            "matching_rows": str(len(rows)),
            "input_error": "",
        }
    return out


def _resolve_cfg_field_keys(cfg_fields: Mapping[str, Any]) -> List[str]:
    records = list(cfg_fields.get("records", []))
    resolved: List[str] = []

    for display_name, owner, field_key_spec in INPUT_COLUMNS:
        if field_key_spec != "CFG":
            resolved.append(field_key_spec)
            continue

        matches = [
            record
            for record in records
            if _normalize_header(record.get("display_name"))
            == _normalize_header(display_name)
            and _safe_str(record.get("entity_type")).upper() == owner
        ]
        # Deduplicate by formal field_id, not by field_key/display name.
        by_field_id = {
            _safe_str(record.get("field_id")): record
            for record in matches
            if _safe_str(record.get("field_id"))
        }
        matches = list(by_field_id.values())
        if len(matches) != 1:
            raise ValueError(
                f"Cfg__Fields must resolve exactly one {owner} field_id for "
                f"display_name={display_name!r}; found="
                f"{sorted(by_field_id)}"
            )
        resolved.append(_safe_str(matches[0].get("field_key")))

    if len(resolved) != len(INPUT_HEADERS):
        raise AssertionError("Input field-key resolution length mismatch.")
    return resolved


def _group_content(
    meta: Mapping[str, Any],
    product_handle_map: Mapping[str, Mapping[str, str]],
) -> Dict[str, str]:
    first: SourceRow = meta["first_row"]
    status = _safe_str(meta["status"])
    size_value = first.get(SRC_SIZE)

    input_error = ""
    if status == "CREATE":
        title = remove_size_expressions(first.get(SRC_PRODUCT_TITLE), size_value)
        if not title:
            raise ValueError(
                f"SPU-V {first.spu_v!r}: generated CREATE Title is blank."
            )
        handle = _slugify_title(title)
    else:
        target_product_id = _safe_str(meta["target_product_id"])
        existing = product_handle_map.get(target_product_id)
        if not existing:
            # Defensive fallback: normal callers populate one map entry for every
            # requested target ID, including unresolved IDs. Do not make a single
            # lookup miss abort the entire Input build.
            title = ""
            handle = ""
            input_error = f"TARGET_PRODUCT_NOT_FOUND: {target_product_id}"
        else:
            title = _safe_str(existing.get("title"))
            handle = _safe_str(existing.get("handle"))
            input_error = _safe_str(existing.get("input_error"))

    description_html = remove_size_from_html(
        first.get(SRC_DESCRIPTION_HTML),
        size_value,
    )
    description_json = html_to_shopify_rich_text_json(description_html)

    return {
        "title": title,
        "handle": handle,
        "input_error": input_error,
        "description_html": description_html,
        "description_json": description_json,
    }


def build_input_rows(
    source_rows: Sequence[SourceRow],
    group_meta: Mapping[str, Mapping[str, Any]],
    product_handle_map: Mapping[str, Mapping[str, str]],
) -> List[List[str]]:
    content_by_spu = {
        spu_v: _group_content(meta, product_handle_map)
        for spu_v, meta in group_meta.items()
        if meta.get("included_rows")
    }

    rows: List[List[str]] = []
    for source in source_rows:
        meta = group_meta[source.spu_v]
        content = content_by_spu[source.spu_v]
        max_qty = _parse_integer(
            source.get(SRC_MAX_QUANTITY),
            label=f"SPU_Source row {source.source_row} Max Quantity-V",
        )
        base_price = _parse_decimal(
            source.get(SRC_PRICE),
            label=f"SPU_Source row {source.source_row} Price",
        )

        for quantity, suffix in QUANTITY_SUFFIXES[max_qty]:
            multiplier = Decimal("0.95") if quantity == max_qty else Decimal("1")
            variant_key = f"{source.variant_base}-{suffix}"
            price = _money_2(base_price * Decimal(quantity) * multiplier)

            row = [
                source.status,                                      # Action
                source.variant_base,                                # Product Key
                variant_key,                                        # Variant Key
                _safe_str(meta.get("target_product_id")),           # Target Product ID
                content["input_error"],                              # Input Error
                source.get(SRC_PRODUCT_TITLE),                       # SKU Title
                content["title"],                                   # Title
                content["handle"],                                  # Handle
                content["description_html"],                        # Description HTML
                content["description_json"],                        # Product Description
                "ERA",                                             # Vendor
                "ERA Product",                                     # Product Type
                source.spu_v,                                       # Tags
                "Size",                                            # Option 1 Name
                source.get(SRC_SIZE),                               # Option 1 Value
                "Quantity",                                        # Option 2 Name
                str(quantity),                                      # Option 2 Value
                "",                                                # Option 3 Name
                "",                                                # Option 3 Value
                variant_key,                                        # SKU
                price,                                              # Price
                "",                                                # Compare-at Price
                "CA",                                              # Inventory Location
                "100000",                                          # Inventory Quantity
                "SPU",                                             # Product Type Internal
                "SPU1",                                            # Product Subtype Internal
                source.get(SRC_PRODUCT_TYPE_1),                     # Product Type-1
                source.get(SRC_PRODUCT_TYPE_2),                     # Product Type-2
                source.get(SRC_PRODUCT_TYPE_3),                     # Product Type-3
                source.get(SRC_PRODUCT_TYPE_4),                     # Product Type-4
                source.spu_v,                                       # SPU-V
                source.variant_base,                                # Variant Base
                suffix,                                             # SKU Suffix-V
                source.get(SRC_SIZE),                               # Size-V
                str(quantity),                                      # Unit Count-V
                str(quantity),                                      # Settlement Quantity-V
                "0.95" if multiplier == Decimal("0.95") else "1",  # Multiplier-V
                _money_2(base_price),                               # SKU Unit Price-V
                str(max_qty),                                       # Max Quantity-V
                "1",                                               # SKU Group
                source.get(SRC_PRIMARY_CATEGORY),                   # Primary category
            ]
            if len(row) != len(INPUT_HEADERS):
                raise AssertionError("Generated Input row length mismatch.")
            rows.append(row)
    return rows


def _now_run_id(job_name: str) -> str:
    return dt.datetime.now(dt.timezone.utc).strftime(f"{job_name}_%Y%m%d_%H%M%S")


def _require_worksheet(book, title: str):
    gp = _infra()
    try:
        return gp._sheets_retry(
            f"open worksheet {title}",
            lambda: book.worksheet(title),
        )
    except Exception as exc:
        raise ValueError(f"Required worksheet is missing: {title}") from exc


def _a1_column_name(column_number: int) -> str:
    """Convert a 1-based column number to an A1 column name (1=A, 27=AA)."""
    if int(column_number) < 1:
        raise ValueError(f"Column number must be >= 1; got {column_number}")
    value = int(column_number)
    parts: List[str] = []
    while value:
        value, remainder = divmod(value - 1, 26)
        parts.append(chr(ord("A") + remainder))
    return "".join(reversed(parts))


def _write_input_sheet(input_ws, matrix: Sequence[Sequence[Any]]) -> None:
    gp = _infra()
    rows_needed = max(len(matrix), 1)
    cols_needed = len(INPUT_HEADERS)

    row_widths = {len(row) for row in matrix}
    if row_widths and row_widths != {cols_needed}:
        raise ValueError(
            "Generated Input matrix width does not match Input schema: "
            f"expected_columns={cols_needed}; observed_row_widths={sorted(row_widths)}"
        )

    if input_ws.row_count < rows_needed or input_ws.col_count < cols_needed:
        gp._sheets_retry(
            "resize Input",
            lambda: input_ws.resize(
                rows=max(input_ws.row_count, rows_needed),
                cols=max(input_ws.col_count, cols_needed),
            ),
        )

    # Clear generated values/formulas but preserve sheet formatting.
    gp._sheets_retry(
        "clear Input values",
        lambda: input_ws.batch_clear(["A:ZZ"]),
    )

    end_col = _a1_column_name(cols_needed)
    gp._sheets_retry(
        f"write Input rows={len(matrix)} cols={cols_needed} range=A1:{end_col}{len(matrix)}",
        lambda: input_ws.update(
            range_name=f"A1:{end_col}{len(matrix)}",
            values=[list(row) for row in matrix],
            value_input_option="RAW",
        ),
    )


def run(
    *,
    site_code: str,
    console_core_url: str,
    bootstrap_gsheet_sa_b64_secret: str,
    tab_cfg_sites: str = "Cfg__Sites",
    tab_cfg_account_id: str = "Cfg__account_id",
    config_sheet_label: str = "config",
    create_sheet_label: str = "create_spu",
    runlog_sheet_label: str = "runlog_sheet",
    tab_cfg_fields: str = "Cfg__Fields",
    tab_source: str = "SPU_Source",
    tab_input: str = "Input",
    tab_product_handle: str = "V_Product_Handle",
    tab_preview: str = "Preview",
    tab_result: str = "Result",
    tab_runlog: str = "Ops__RunLog",
    write_input: bool = True,
    preview_rows: int = 50,
    tz_name: str = "America/New_York",
    run_id: Optional[str] = None,
    job_name: str = DEFAULT_JOB_NAME,
    print_progress: bool = True,
    secret_home: Optional[str] = None,
    local_secret_aliases: Optional[Mapping[str, Mapping[str, str]]] = None,
    sa_b64_value: Optional[str] = None,
) -> Dict[str, Any]:
    gp = _infra()
    site_code = gp._normalize_site_code(site_code)
    if not site_code:
        raise ValueError("site_code is required.")
    if not _safe_str(console_core_url):
        raise ValueError("console_core_url is required.")

    run_id = run_id or _now_run_id(job_name)
    started = time.monotonic()
    logger = None

    def progress(step: int, total: int, message: str) -> None:
        if print_progress:
            print(f"[{step}/{total}] {message}")

    progress(1, 9, f"Resolve Google access | site={site_code}")
    secret = gp.read_secret(
        bootstrap_gsheet_sa_b64_secret,
        explicit_value=sa_b64_value,
        project_code=site_code,
        secret_home=secret_home,
        local_secret_aliases=local_secret_aliases,
    )
    gc, auth_meta = gp._build_gspread_client(secret)
    console = gp._sheets_retry(
        "open Console Core",
        lambda: gc.open_by_url(console_core_url),
    )

    account = gp._load_account_values(console, tab_cfg_account_id)
    configured_secret = _safe_str(account.get("GSHEET_SA_B64_SECRET"))
    if configured_secret and configured_secret != bootstrap_gsheet_sa_b64_secret:
        raise ValueError(
            "Bootstrap Google Secret does not match Cfg__account_id. "
            f"bootstrap={bootstrap_gsheet_sa_b64_secret}; cfg={configured_secret}"
        )

    progress(
        2,
        9,
        "Resolve routed workbooks | "
        f"create={create_sheet_label} | config={config_sheet_label}",
    )
    create_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, create_sheet_label
    )
    config_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, config_sheet_label
    )
    runlog_url = gp._resolve_sheet_url_by_label(
        console, tab_cfg_sites, site_code, runlog_sheet_label
    )

    create_book = gp._sheets_retry(
        "open SPU Create workbook", lambda: gc.open_by_url(create_url)
    )
    config_book = gp._sheets_retry(
        "open Config workbook", lambda: gc.open_by_url(config_url)
    )
    runlog_book = gp._sheets_retry(
        "open RunLog workbook", lambda: gc.open_by_url(runlog_url)
    )
    runlog_ws = _require_worksheet(runlog_book, tab_runlog)
    logger = gp.RunLogger18(
        worksheet=runlog_ws,
        run_id=run_id,
        job_name=job_name,
        site_code=site_code,
        tz_name=tz_name,
    )

    try:
        progress(3, 9, f"Read Source | tab={tab_source} | formal_columns=23")
        source_ws = _require_worksheet(create_book, tab_source)
        source_values = gp._sheets_retry(
            "read SPU_Source", source_ws.get_all_values
        )
        source_rows = _read_source(source_values)
        groups = _group_source(source_rows)
        group_meta, planned_source_rows = _validate_and_plan_groups(groups)

        create_groups = sum(1 for meta in group_meta.values() if meta["status"] == "CREATE")
        add_groups = sum(1 for meta in group_meta.values() if meta["status"] == "ADD")
        existing_skipped = sum(
            len(meta["excluded_existing_rows"]) for meta in group_meta.values()
        )
        add_no_new = sum(
            1
            for meta in group_meta.values()
            if meta["status"] == "ADD" and not meta["included_rows"]
        )
        progress(
            4,
            9,
            "Validate Source groups | "
            f"groups={len(groups)} | CREATE={create_groups} | ADD={add_groups} | "
            f"planned_variant_bases={len(planned_source_rows)} | "
            f"existing_variant_bases_skipped={existing_skipped}",
        )

        target_ids = sorted(
            {
                _safe_str(meta["target_product_id"])
                for meta in group_meta.values()
                if meta["status"] == "ADD" and meta["included_rows"]
            }
            - {""}
        )
        product_handle_map: Dict[str, Dict[str, str]] = {}
        if target_ids:
            progress(
                5,
                9,
                f"Resolve existing ADD Products | tab={tab_product_handle} | "
                f"target_products={len(target_ids)}",
            )
            handle_ws = _require_worksheet(create_book, tab_product_handle)
            handle_values = gp._sheets_retry(
                "read V_Product_Handle", handle_ws.get_all_values
            )
            product_handle_map = _read_product_handle_map(handle_values, target_ids)
        else:
            progress(5, 9, "Resolve existing ADD Products | not required")

        progress(6, 9, f"Resolve Config field identity | tab={tab_cfg_fields}")
        cfg_ws = _require_worksheet(config_book, tab_cfg_fields)
        cfg_values = gp._sheets_retry("read Cfg__Fields", cfg_ws.get_all_values)
        cfg_fields = gp._read_cfg_fields(cfg_values)
        field_keys = _resolve_cfg_field_keys(cfg_fields)
        if print_progress:
            print(
                "[Cfg__Fields] "
                f"records={cfg_fields['stats']['records']} | "
                f"unique_field_ids={cfg_fields['stats']['unique_field_ids']} | "
                f"input_columns={len(field_keys)}"
            )

        progress(7, 9, "Build Input rows | expand Variant Base x Quantity")
        input_rows = build_input_rows(
            planned_source_rows,
            group_meta,
            product_handle_map,
        )
        matrix: List[List[Any]] = [INPUT_HEADERS, field_keys, *input_rows]

        if write_input:
            progress(
                8,
                9,
                f"Write generated Input | tab={tab_input} | rows={len(input_rows)}",
            )
            input_ws = _require_worksheet(create_book, tab_input)
            _write_input_sheet(input_ws, matrix)
            rows_written = len(input_rows)
        else:
            progress(8, 9, "Write generated Input | disabled")
            rows_written = 0

        error_col = INPUT_HEADERS.index("Input Error")
        input_error_rows = [row for row in input_rows if _safe_str(row[error_col])]
        distinct_input_errors = sorted({_safe_str(row[error_col]) for row in input_error_rows})
        target_products_resolved = sum(
            1
            for record in product_handle_map.values()
            if not _safe_str(record.get("input_error"))
        )
        target_products_unresolved = sum(
            1
            for record in product_handle_map.values()
            if _safe_str(record.get("input_error"))
        )
        final_status = "SUCCESS_WITH_ERRORS" if input_error_rows else "SUCCESS"

        elapsed = round(time.monotonic() - started, 3)
        summary = {
            "source_rows_loaded": len(source_rows),
            "spu_groups": len(groups),
            "create_groups": create_groups,
            "add_groups": add_groups,
            "add_groups_no_new_variants": add_no_new,
            "existing_variant_bases_skipped": existing_skipped,
            "variant_bases_planned": len(planned_source_rows),
            "input_rows_planned": len(input_rows),
            "input_rows_written": rows_written,
            "input_columns": len(INPUT_HEADERS),
            "input_error_rows": len(input_error_rows),
            "input_error_types": len(distinct_input_errors),
            "target_products_requested": len(target_ids),
            "target_products_resolved": target_products_resolved,
            "target_products_unresolved": target_products_unresolved,
            "elapsed_seconds": elapsed,
        }

        progress(
            9,
            9,
            "Complete | "
            f"status={final_status} | input_rows={len(input_rows)} | "
            f"input_errors={len(input_error_rows)} | written={rows_written} | "
            f"elapsed={elapsed}s",
        )

        for error_text in distinct_input_errors:
            logger.log(
                phase="build_input",
                log_type="warning",
                status="WARNING",
                entity_type="SPU_PRODUCT_INPUT",
                rows_loaded=len(source_rows),
                rows_pending=len(planned_source_rows),
                rows_planned=len(input_rows),
                rows_written=rows_written,
                rows_skipped=existing_skipped,
                message=error_text,
            )

        logger.log(
            phase="build_input",
            log_type="summary",
            status=final_status,
            entity_type="SPU_PRODUCT_INPUT",
            rows_loaded=len(source_rows),
            rows_pending=len(planned_source_rows),
            rows_recognized=len(groups),
            rows_planned=len(input_rows),
            rows_written=rows_written,
            rows_skipped=existing_skipped,
            message=json.dumps(summary, ensure_ascii=False, sort_keys=True),
        )
        logger.flush()

        preview = [
            dict(zip(INPUT_HEADERS, row))
            for row in input_rows[: max(int(preview_rows), 0)]
        ]
        return {
            "status": final_status,
            "ok": True,
            "job_name": job_name,
            "run_id": run_id,
            "summary": summary,
            "field_keys": dict(zip(INPUT_HEADERS, field_keys)),
            "input_errors": distinct_input_errors,
            "input_preview": preview,
            "runtime": {
                "auth_type": "GOOGLE_SERVICE_ACCOUNT",
                "google_secret_source": auth_meta["source_type"],
                "interactive_auth_used": False,
            },
            "targets": {
                "create_sheet_url": create_url,
                "source_tab": tab_source,
                "input_tab": tab_input,
                "product_handle_tab": tab_product_handle,
                "config_sheet_url": config_url,
                "cfg_fields_tab": tab_cfg_fields,
                "runlog_sheet_url": runlog_url,
                "runlog_tab": tab_runlog,
                "preview_tab": f"{tab_preview} (untouched)",
                "result_tab": f"{tab_result} (untouched)",
                "module_path": MODULE_PATH,
                "module_version": MODULE_VERSION,
                "infra_module_path": INFRA_MODULE_PATH,
                "infra_module_version": EXPECTED_INFRA_MODULE_VERSION,
            },
        }

    except BaseException as exc:
        if logger is not None:
            try:
                logger.log(
                    phase="build_input",
                    log_type="summary",
                    status="FAILED",
                    entity_type="SPU_PRODUCT_INPUT",
                    message=str(exc),
                    error_reason=type(exc).__name__,
                )
                logger.flush()
            except Exception as log_exc:
                if print_progress:
                    print(f"[RunLog warning] failed to write failure log: {log_exc}")
        if print_progress:
            print(f"[FAILED] {type(exc).__name__}: {exc}")
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build SPU Product Creation Input from SPU_Source."
    )
    parser.add_argument("--site-code", required=True)
    parser.add_argument("--console-core-url", required=True)
    parser.add_argument("--bootstrap-gsheet-secret", required=True)
    parser.add_argument("--create-sheet-label", default="create_spu")
    parser.add_argument("--config-sheet-label", default="config")
    parser.add_argument("--no-write-input", action="store_true")
    parser.add_argument("--secret-home", default="")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    result = run(
        site_code=args.site_code,
        console_core_url=args.console_core_url,
        bootstrap_gsheet_sa_b64_secret=args.bootstrap_gsheet_secret,
        create_sheet_label=args.create_sheet_label,
        config_sheet_label=args.config_sheet_label,
        write_input=not args.no_write_input,
        secret_home=args.secret_home or None,
    )
    print(json.dumps({"status": result["status"], "summary": result["summary"]}, indent=2))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
