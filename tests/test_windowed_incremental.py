"""Tests for time-windowed incremental sync (Magnus TAP_ICEBERG__METADATA)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyiceberg")

import pyarrow as pa

from tap_iceberg.streams import IcebergTableStream


def _minimal_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("updated_at", pa.timestamp("ms", tz="UTC")),
        ]
    )


@pytest.fixture
def mock_tap():
    tap = MagicMock()
    tap.name = "tap-iceberg"
    tap._tap_state = {"bookmarks": {}}
    tap.config = {}
    return tap


@pytest.fixture
def mock_batch_reader():
    """Empty Arrow batch iterable."""
    reader = MagicMock()
    reader.__iter__ = lambda _: iter([])
    return reader


@pytest.fixture
def iceberg_table_mock(mock_batch_reader):
    tbl = MagicMock()
    tbl.schema.return_value.as_arrow.return_value = _minimal_arrow_schema()
    scan_mock = MagicMock()
    scan_mock.to_arrow_batch_reader.return_value = mock_batch_reader
    tbl.scan.return_value = scan_mock
    return tbl


def test_incremental_raises_when_window_metadata_missing(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    stream = IcebergTableStream(mock_tap, "ns-tbl", iceberg_table_mock)
    stream.forced_replication_method = "INCREMENTAL"
    stream.replication_key = "updated_at"

    with pytest.raises(ValueError, match="window-size-hours"):
        list(stream.get_records())


def test_incremental_reads_window_fields_from_env_wildcard(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 6,
                    "start-replication-key-value": "2024-01-01T00:00:00+00:00",
                },
            },
        ),
    )

    stream = IcebergTableStream(mock_tap, "ns-tbl", iceberg_table_mock)
    stream.forced_replication_method = "INCREMENTAL"
    stream.replication_key = "updated_at"
    stream.get_starting_replication_key_value = MagicMock(  # type: ignore[method-assign]
        return_value=None,
    )

    list(stream.get_records())

    assert stream._planned_window_end == "2024-01-01T06:00:00+00:00"


def test_stream_specific_metadata_overrides_wildcard(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 6,
                    "start-replication-key-value": "2024-01-01T00:00:00+00:00",
                },
                "ns-tbl": {"window-size-hours": 168},
            },
        ),
    )

    stream = IcebergTableStream(mock_tap, "ns-tbl", iceberg_table_mock)
    stream.forced_replication_method = "INCREMENTAL"
    stream.replication_key = "updated_at"
    stream.get_starting_replication_key_value = MagicMock(  # type: ignore[method-assign]
        return_value=None,
    )

    list(stream.get_records())

    assert stream._planned_window_end == "2024-01-08T00:00:00+00:00"


def test_incremental_planned_bookmark_is_window_right_edge(mock_tap, iceberg_table_mock):
    stream = IcebergTableStream(mock_tap, "ns-tbl", iceberg_table_mock)
    stream.forced_replication_method = "INCREMENTAL"
    stream.replication_key = "updated_at"

    meta = {
        "window-size-hours": 6,
        "start-replication-key-value": "2024-01-01T00:00:00+00:00",
    }
    stream._get_stream_metadata_value = (  # type: ignore[method-assign]
        lambda key, default=None: meta.get(key, default)
    )
    stream.get_starting_replication_key_value = MagicMock(  # type: ignore[method-assign]
        return_value=None,
    )

    list(stream.get_records())

    iceberg_table_mock.scan.assert_called_once()
    row_filter = iceberg_table_mock.scan.call_args.kwargs["row_filter"]
    assert type(row_filter).__name__ == "And"
    assert stream._planned_window_end == "2024-01-01T06:00:00+00:00"


def test_full_table_uses_always_true_scan_and_clears_planned(mock_tap, iceberg_table_mock):
    stream = IcebergTableStream(mock_tap, "ns-tbl", iceberg_table_mock)
    stream.forced_replication_method = None
    stream.replication_key = None

    stream._planned_window_end = "should-not-stick"
    list(stream.get_records())

    iceberg_table_mock.scan.assert_called_once()
    row_filter = iceberg_table_mock.scan.call_args.kwargs["row_filter"]
    assert type(row_filter).__name__ == "AlwaysTrue"
    assert stream._planned_window_end is None
