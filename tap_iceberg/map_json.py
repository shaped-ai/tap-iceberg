"""Normalize PyArrow / Iceberg map values to JSON-compatible objects."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def _jsonify_plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def normalize_pyarrow_map_for_json(value: Any) -> Any:
    """Convert PyArrow map representations to JSON objects (string keys).

    ``Table.to_pylist()`` often emits map columns as a list of ``(key, value)``
    pairs, while our Singer schema declares maps as JSON objects.
    """
    if value is None:
        return None
    if isinstance(value, Mapping):
        return {str(k): _jsonify_mapped_value(v) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        entries = list(value)
        if not entries:
            return {}
        probe = entries[0]
        if isinstance(probe, Mapping) and "key" in probe and "value" in probe:
            return {
                str(row["key"]): _jsonify_mapped_value(row["value"])
                for row in entries
            }
        if (
            isinstance(probe, Sequence)
            and not isinstance(probe, (str, bytes, bytearray))
            and len(probe) == 2
        ):
            return {
                str(pair[0]): _jsonify_mapped_value(pair[1]) for pair in entries
            }
    return value


def _jsonify_mapped_value(value: Any) -> Any:
    if value is None:
        return None
    normalized = normalize_pyarrow_map_for_json(value)
    if normalized is value and isinstance(value, Mapping):
        return {str(k): _jsonify_mapped_value(v) for k, v in value.items()}
    if normalized is value and isinstance(
        value, Sequence
    ) and not isinstance(value, (str, bytes, bytearray)):
        probe = value[0] if value else None
        if (
            isinstance(probe, Sequence)
            and not isinstance(probe, (str, bytes, bytearray))
            and len(probe) == 2
        ):
            return normalize_pyarrow_map_for_json(list(value))
        return [_jsonify_mapped_value(item) for item in value]
    if normalized is not value:
        return normalized
    return _jsonify_plain(value)
