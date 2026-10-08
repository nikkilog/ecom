"""Stable public interfaces for Console Core consumers."""

from .registry import resolve_sheet_resource
from .registry_io import load_registry_rows

__all__ = ["load_registry_rows", "resolve_sheet_resource"]
