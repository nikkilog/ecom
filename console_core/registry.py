"""Pure, target-only Registry resource resolution.

Callers supply rows that they obtained through their own supported access and
authentication boundary.  This module performs no credential lookup, cloud
read, or network access.
"""

from collections.abc import Iterable, Mapping
import re
from typing import Any


PROJECT_REGISTRY_TAB = "Cfg__Projects"
CFG_SITES_TAB = "Cfg__Sites"
_ACTIVE_VALUES = {"true", "1", "yes", "y", "是", "active"}
_INACTIVE_VALUES = {"false", "0", "no", "n", "否", "inactive"}


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _normalize_code(value: object) -> str:
    return _text(value).upper()


def _normalize_label(value: object) -> str:
    return _text(value).lower()


def _normalize_header(value: object) -> str:
    return re.sub(r"[\s_]+", " ", _text(value).lower()).strip()


def _canonicalize_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    table_name: str,
) -> list[tuple[int, dict[str, object]]]:
    canonical_rows: list[tuple[int, dict[str, object]]] = []
    for source_row, raw_row in enumerate(rows, start=2):
        normalized: dict[str, object] = {}
        duplicate_headers: list[str] = []
        for raw_header, value in raw_row.items():
            header = _normalize_header(raw_header)
            if not header:
                continue
            if header in normalized:
                duplicate_headers.append(header)
            normalized[header] = value
        if duplicate_headers:
            raise ValueError(
                f"{table_name} row {source_row} has duplicate normalized "
                f"headers: {sorted(set(duplicate_headers))}."
            )
        canonical_rows.append((source_row, normalized))
    return canonical_rows


def _require_headers(
    rows: list[tuple[int, dict[str, object]]],
    *,
    table_name: str,
    required: tuple[str, ...],
) -> None:
    headers = {header for _, row in rows for header in row}
    missing = [header for header in required if _normalize_header(header) not in headers]
    if missing:
        raise ValueError(f"{table_name} is missing required columns: {missing}.")


def _active_state(
    row: Mapping[str, object],
    *,
    identity: str,
    source_row: int,
    required: bool,
) -> str:
    header = "active" if "active" in row else "status" if "status" in row else ""
    if not header:
        if required:
            raise ValueError(f"{identity} is missing required active column.")
        return ""

    raw = _text(row.get(header))
    normalized = raw.lower()
    if normalized in _ACTIVE_VALUES:
        return "TRUE"
    if normalized in _INACTIVE_VALUES or not normalized:
        raise ValueError(f"{identity} is inactive at row {source_row}.")
    raise ValueError(
        f"{identity} has invalid {header} value at row {source_row}: {raw!r}."
    )


def _extract_sheet_id(sheet_url: str) -> str:
    match = re.fullmatch(
        r"https://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)(?:/.*)?",
        sheet_url,
    )
    if not match:
        raise ValueError(f"Malformed Google Sheet URL: {sheet_url!r}.")
    return match.group(1)


def _resolve_cfg_site_resource(
    *,
    site_code: str,
    sheet_label: str,
    cfg_sites_rows: Iterable[Mapping[str, object]],
) -> dict[str, str]:
    """Resolve one Cfg__Sites row; internal compatibility seam."""
    normalized_site = _normalize_code(site_code)
    normalized_label = _text(sheet_label)
    if not normalized_site:
        raise ValueError("site_code is required.")
    if not normalized_label:
        raise ValueError("sheet_label is required.")

    rows = _canonicalize_rows(cfg_sites_rows, table_name=CFG_SITES_TAB)
    _require_headers(
        rows,
        table_name=CFG_SITES_TAB,
        required=("site code", "label"),
    )
    matches = [
        (source_row, row)
        for source_row, row in rows
        if _normalize_code(row.get("site code")) == normalized_site
        and _normalize_label(row.get("label")) == _normalize_label(normalized_label)
    ]
    identity = (
        f"{CFG_SITES_TAB} route for site_code={normalized_site}, "
        f"sheet_label={normalized_label}"
    )
    if not matches:
        raise ValueError(f"No {identity}.")
    if len(matches) > 1:
        raise ValueError(
            f"Duplicate {identity}; rows={[source_row for source_row, _ in matches]}."
        )

    source_row, row = matches[0]
    active = _active_state(
        row,
        identity=identity,
        source_row=source_row,
        required=False,
    )
    sheet_url = _text(row.get("sheet url"))
    sheet_id = _text(row.get("sheet id"))
    if not sheet_url and not sheet_id:
        raise ValueError(f"{identity} has empty sheet_url and sheet_id at row {source_row}.")
    if sheet_url:
        url_sheet_id = _extract_sheet_id(sheet_url)
        if sheet_id and sheet_id != url_sheet_id:
            raise ValueError(
                f"{identity} has conflicting sheet_url and sheet_id at row {source_row}."
            )
        sheet_id = url_sheet_id

    return {
        "site_code": normalized_site,
        "sheet_label": normalized_label,
        "sheet_id": sheet_id,
        "sheet_url": sheet_url,
        "active": active,
        "cfg_sites_source_row": str(source_row),
        "cfg_sites_tab": CFG_SITES_TAB,
    }


def _resolve_project_route(
    *,
    project_code: str,
    project_registry_rows: Iterable[Mapping[str, object]],
) -> dict[str, str]:
    """Resolve one active project route for package-internal reuse."""
    normalized_project = _normalize_code(project_code)
    if not normalized_project:
        raise ValueError("project_code is required.")

    project_rows = _canonicalize_rows(
        project_registry_rows,
        table_name=PROJECT_REGISTRY_TAB,
    )
    _require_headers(
        project_rows,
        table_name=PROJECT_REGISTRY_TAB,
        required=("project code", "active", "console core url"),
    )
    matches = [
        (source_row, row)
        for source_row, row in project_rows
        if _normalize_code(row.get("project code")) == normalized_project
    ]
    identity = f"{PROJECT_REGISTRY_TAB} project_code={normalized_project}"
    if not matches:
        raise ValueError(f"No row in {identity}.")
    if len(matches) > 1:
        raise ValueError(
            f"Duplicate rows in {identity}; "
            f"rows={[source_row for source_row, _ in matches]}."
        )

    project_source_row, project_row = matches[0]
    _active_state(
        project_row,
        identity=identity,
        source_row=project_source_row,
        required=True,
    )
    console_core_url = _text(project_row.get("console core url"))
    if not console_core_url:
        raise ValueError(
            f"{identity} has empty console_core_url at row {project_source_row}."
        )

    return {
        "project_code": normalized_project,
        "console_core_url": console_core_url,
        "project_registry_source_row": str(project_source_row),
        "project_registry_tab": PROJECT_REGISTRY_TAB,
    }


def resolve_sheet_resource(
    *,
    project_code: str,
    site_code: str,
    sheet_label: str,
    project_registry_rows: Iterable[Mapping[str, object]],
    cfg_sites_rows: Iterable[Mapping[str, object]],
) -> dict[str, str]:
    """Resolve exactly one active project and one target Sheet resource."""
    project_route = _resolve_project_route(
        project_code=project_code,
        project_registry_rows=project_registry_rows,
    )

    resource = _resolve_cfg_site_resource(
        site_code=site_code,
        sheet_label=sheet_label,
        cfg_sites_rows=cfg_sites_rows,
    )
    return {
        "project_code": project_route["project_code"],
        **resource,
        "project_registry_source_row": project_route[
            "project_registry_source_row"
        ],
        "project_registry_tab": project_route["project_registry_tab"],
    }
