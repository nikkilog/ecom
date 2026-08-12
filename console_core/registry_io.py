"""Caller-authenticated Google Sheets IO for Console Core Registry rows.

This module owns workbook and worksheet reads, bounded transient retry, row
extraction, and safe IO provenance. Authentication and credentials remain the
caller's responsibility. Registry business selection remains in
``console_core.registry``.
"""

from collections.abc import Callable, Mapping, Sequence
import random
import re
import time
from typing import Any

from .registry import (
    CFG_SITES_TAB,
    PROJECT_REGISTRY_TAB,
    _normalize_header,
    _resolve_project_route,
)


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_RETRYABLE_TEXT = (
    "resource_exhausted",
    "ratelimitexceeded",
    "userratelimitexceeded",
    "rate limit exceeded",
    "quota exceeded",
    "too many requests",
    "backend error",
    "internal error",
    "service unavailable",
    "connection reset",
    "connection aborted",
    "remote end closed connection",
)
_RETRYABLE_EXCEPTION_NAMES = {
    "ConnectTimeout",
    "ConnectionError",
    "ConnectionResetError",
    "ProtocolError",
    "ReadTimeout",
    "RemoteDisconnected",
    "Timeout",
    "TimeoutError",
}
_MAX_ATTEMPTS = 8
_BASE_DELAY_SECONDS = 1.5
_MAX_DELAY_SECONDS = 20.0


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _extract_spreadsheet_id(value: object) -> str:
    text = _text(value)
    if not text:
        raise ValueError("Workspace Project Registry ID/URL is empty.")
    if re.fullmatch(r"[A-Za-z0-9_-]+", text):
        return text
    match = re.fullmatch(
        r"https://docs\.google\.com/spreadsheets/d/"
        r"([A-Za-z0-9_-]+)(?:/.*)?",
        text,
    )
    if match:
        return match.group(1)
    raise ValueError(
        "Workspace Project Registry must be a Google Sheets ID or normal URL."
    )


def _error_status(error: BaseException) -> int | None:
    response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    if status is None:
        status = getattr(response, "status", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _retry_after_seconds(error: BaseException) -> float | None:
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    if not isinstance(headers, Mapping):
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def _is_retryable(error: BaseException) -> bool:
    if _error_status(error) in _RETRYABLE_STATUS:
        return True
    if isinstance(
        error,
        (
            TimeoutError,
            ConnectionError,
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
        ),
    ):
        return True
    if type(error).__name__ in _RETRYABLE_EXCEPTION_NAMES:
        return True
    text = str(error).lower()
    return any(token in text for token in _RETRYABLE_TEXT)


def _with_sheets_retry(
    operation: Callable[[], Any],
    *,
    action: str,
    print_progress: bool,
    sleep: Callable[[float], None] = time.sleep,
    random_value: Callable[[], float] = random.random,
):
    """Run one Sheets operation with finite, classified transient retry."""
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return operation()
        except Exception as error:
            if not _is_retryable(error) or attempt >= _MAX_ATTEMPTS:
                raise

            delay = min(
                _MAX_DELAY_SECONDS,
                (_BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
                + random_value(),
            )
            retry_after = _retry_after_seconds(error)
            if retry_after is not None:
                delay = min(
                    _MAX_DELAY_SECONDS,
                    max(delay, retry_after),
                )
            if print_progress:
                status = _error_status(error)
                reason = (
                    f"HTTP {status}"
                    if status is not None
                    else type(error).__name__
                )
                print(
                    "[Registry IO retry] "
                    f"action={action} | attempt={attempt}/{_MAX_ATTEMPTS} | "
                    f"reason={reason} | sleep={delay:.1f}s",
                    flush=True,
                )
            sleep(delay)
    raise RuntimeError(f"Registry IO operation exhausted retries: {action}")


def _values_to_rows(
    values: Sequence[Sequence[object]],
    *,
    worksheet_title: str,
) -> list[dict[str, object]]:
    if not values:
        raise ValueError(f"Registry worksheet {worksheet_title!r} is empty.")

    raw_headers = list(values[0])
    usable_headers: list[tuple[int, str]] = []
    normalized_headers: set[str] = set()
    duplicates: list[str] = []
    for index, raw_header in enumerate(raw_headers):
        header = _text(raw_header)
        normalized = _normalize_header(header)
        if not normalized:
            continue
        if normalized in normalized_headers:
            duplicates.append(normalized)
        normalized_headers.add(normalized)
        usable_headers.append((index, header))
    if duplicates:
        raise ValueError(
            f"Registry worksheet {worksheet_title!r} has duplicate normalized "
            f"headers: {sorted(set(duplicates))}."
        )
    if not usable_headers:
        raise ValueError(
            f"Registry worksheet {worksheet_title!r} has no usable headers."
        )

    data_values = list(values[1:])
    if not data_values or not any(
        any(_text(cell) for cell in row) for row in data_values
    ):
        raise ValueError(
            f"Registry worksheet {worksheet_title!r} has no data rows."
        )

    rows: list[dict[str, object]] = []
    for raw_row in data_values:
        row = {
            header: raw_row[index] if index < len(raw_row) else ""
            for index, header in usable_headers
        }
        rows.append(row)
    return rows


def _read_worksheet_rows(
    workbook: object,
    *,
    worksheet_title: str,
    action_prefix: str,
    print_progress: bool,
) -> list[dict[str, object]]:
    worksheet = _with_sheets_retry(
        lambda: workbook.worksheet(worksheet_title),
        action=f"{action_prefix}.open_worksheet:{worksheet_title}",
        print_progress=print_progress,
    )
    values = _with_sheets_retry(
        worksheet.get_all_values,
        action=f"{action_prefix}.read_values:{worksheet_title}",
        print_progress=print_progress,
    )
    return _values_to_rows(values, worksheet_title=worksheet_title)


def load_registry_rows(
    *,
    google_client: object,
    workspace_registry_id: str,
    project_code: str,
    workspace_registry_tab: str = PROJECT_REGISTRY_TAB,
    cfg_sites_tab: str = CFG_SITES_TAB,
    print_progress: bool = True,
) -> dict[str, object]:
    """Load project and Cfg__Sites rows with a caller-authenticated client."""
    registry_id = _extract_spreadsheet_id(workspace_registry_id)
    registry_tab = _text(workspace_registry_tab)
    sites_tab = _text(cfg_sites_tab)
    if not registry_tab:
        raise ValueError("workspace_registry_tab is required.")
    if not sites_tab:
        raise ValueError("cfg_sites_tab is required.")

    registry_book = _with_sheets_retry(
        lambda: google_client.open_by_key(registry_id),
        action="workspace_registry.open_by_key",
        print_progress=print_progress,
    )
    project_rows = _read_worksheet_rows(
        registry_book,
        worksheet_title=registry_tab,
        action_prefix="workspace_registry",
        print_progress=print_progress,
    )
    project_route = _resolve_project_route(
        project_code=project_code,
        project_registry_rows=project_rows,
    )
    console_core_url = project_route["console_core_url"]

    console_book = _with_sheets_retry(
        lambda: google_client.open_by_url(console_core_url),
        action="target_console.open_by_url",
        print_progress=print_progress,
    )
    cfg_sites_rows = _read_worksheet_rows(
        console_book,
        worksheet_title=sites_tab,
        action_prefix="target_console",
        print_progress=print_progress,
    )

    if print_progress:
        print(
            "[Registry IO] loaded | "
            f"project={project_route['project_code']} | "
            f"registry_tab={registry_tab} | cfg_sites_tab={sites_tab}",
            flush=True,
        )
    return {
        "project_registry_rows": project_rows,
        "cfg_sites_rows": cfg_sites_rows,
        "project_code": project_route["project_code"],
        "console_core_url": console_core_url,
        "workspace_registry_id": registry_id,
        "workspace_registry_tab": registry_tab,
        "cfg_sites_tab": sites_tab,
    }
