"""Tests for PyArrow map -> JSON object normalization."""

from __future__ import annotations

from decimal import Decimal

from tap_iceberg.map_json import normalize_pyarrow_map_for_json


def test_map_as_list_of_pairs_with_decimal_values() -> None:
    raw: list[list[object]] = [
        [10, Decimal("0.9075639941274131")],
        [60, Decimal("0.47395735400544153")],
        [120, Decimal("0.2382985750117097")],
        [300, Decimal("0.06791050268829706")],
    ]
    out = normalize_pyarrow_map_for_json(raw)
    assert out == {
        "10": 0.9075639941274131,
        "60": 0.47395735400544153,
        "120": 0.2382985750117097,
        "300": 0.06791050268829706,
    }


def test_map_as_arrow_struct_rows() -> None:
    rows = [{"key": 10, "value": 1.5}, {"key": 20, "value": 2.5}]
    out = normalize_pyarrow_map_for_json(rows)
    assert out == {"10": 1.5, "20": 2.5}


def test_map_empty_sequence() -> None:
    assert normalize_pyarrow_map_for_json([]) == {}
