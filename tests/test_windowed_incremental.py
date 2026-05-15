"""Tests for time-windowed incremental sync (Magnus TAP_ICEBERG__METADATA)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyiceberg")

import pyarrow as pa
from pyiceberg.types import (
    NestedField,
    TimestampType,
    TimestamptzType,
)

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


def _build_iceberg_table_mock(
    batch_reader: MagicMock,
    *,
    replication_column_type,
) -> MagicMock:
    tbl = MagicMock()
    schema_obj = MagicMock()
    schema_obj.as_arrow.return_value = _minimal_arrow_schema()
    rk_field = NestedField(
        field_id=2,
        name="updated_at",
        field_type=replication_column_type,
        required=False,
    )
    schema_obj.find_field.return_value = rk_field
    tbl.schema.return_value = schema_obj
    scan_mock = MagicMock()
    scan_mock.to_arrow_batch_reader.return_value = batch_reader
    tbl.scan.return_value = scan_mock
    return tbl


@pytest.fixture
def iceberg_table_mock(mock_batch_reader):
    """Default fixture: replication column is `timestamptz` (aware)."""
    return _build_iceberg_table_mock(
        mock_batch_reader,
        replication_column_type=TimestamptzType(),
    )


@pytest.fixture
def iceberg_table_mock_naive(mock_batch_reader):
    """Replication column is `timestamp` (no zone)."""
    return _build_iceberg_table_mock(
        mock_batch_reader,
        replication_column_type=TimestampType(),
    )


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

    assert stream._planned_bookmark == "2024-01-01T06:00:00+00:00"
    assert stream._max_observed is None


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

    assert stream._planned_bookmark == "2024-01-08T00:00:00+00:00"


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
    assert stream._planned_bookmark == "2024-01-01T06:00:00+00:00"


def test_naive_column_strips_tz_offset_from_window_literals(
    monkeypatch,
    mock_tap,
    iceberg_table_mock_naive,
):
    """Operator passes ``+00:00`` but the Iceberg column is plain ``timestamp``."""
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 6,
                    "start-replication-key-value": "2026-01-01T00:00:00+00:00",
                },
            },
        ),
    )
    stream = IcebergTableStream(mock_tap, "ns-tbl", iceberg_table_mock_naive)
    stream.forced_replication_method = "INCREMENTAL"
    stream.replication_key = "updated_at"
    stream.get_starting_replication_key_value = MagicMock(  # type: ignore[method-assign]
        return_value=None,
    )

    list(stream.get_records())

    assert stream._planned_bookmark == "2026-01-01T06:00:00"
    row_filter = iceberg_table_mock_naive.scan.call_args.kwargs["row_filter"]
    assert type(row_filter).__name__ == "And"


def test_tz_aware_column_adds_utc_offset_when_input_is_naive(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    """Operator passes naive ISO but the column is ``timestamptz`` — assume UTC."""
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 6,
                    "start-replication-key-value": "2026-01-01T00:00:00",
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

    assert stream._planned_bookmark == "2026-01-01T06:00:00+00:00"


def test_full_table_uses_always_true_scan_and_clears_planned(mock_tap, iceberg_table_mock):
    stream = IcebergTableStream(mock_tap, "ns-tbl", iceberg_table_mock)
    stream.forced_replication_method = None
    stream.replication_key = None

    stream._planned_bookmark = "should-not-stick"
    stream._max_observed = "should-not-stick"
    list(stream.get_records())

    iceberg_table_mock.scan.assert_called_once()
    row_filter = iceberg_table_mock.scan.call_args.kwargs["row_filter"]
    assert type(row_filter).__name__ == "AlwaysTrue"
    assert stream._planned_bookmark is None
    assert stream._max_observed is None


# -- Tail-phase --------------------------------------------------------------


def test_tail_phase_uses_open_ended_filter_and_running_max_bookmark(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    """When ``window_start + window_hours > now`` we switch to an open-ended
    scan and the bookmark is the max replication_key value yielded."""
    # window_start = now - 1h, window-size = 24h -> candidate_end is in the
    # future, forcing tail phase.
    start_dt = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        microsecond=0,
    )
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 24,
                    "start-replication-key-value": start_dt.isoformat(),
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

    # Three records, intentionally out of order, so we exercise the running-max
    # comparison rather than relying on iteration order.
    rk_mid = start_dt + timedelta(minutes=15)
    rk_max = start_dt + timedelta(minutes=45)
    rk_min = start_dt + timedelta(minutes=5)
    stream._scan_and_yield_rows = lambda _filter: iter(  # type: ignore[method-assign]
        [
            {"id": 1, "updated_at": rk_mid},
            {"id": 2, "updated_at": rk_max},
            {"id": 3, "updated_at": rk_min},
        ],
    )

    list(stream.get_records())

    assert stream._planned_bookmark is None
    assert stream._max_observed == rk_max


def test_tail_phase_filter_is_open_ended_greater_than(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    start_dt = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        microsecond=0,
    )
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 24,
                    "start-replication-key-value": start_dt.isoformat(),
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

    iceberg_table_mock.scan.assert_called_once()
    row_filter = iceberg_table_mock.scan.call_args.kwargs["row_filter"]
    assert type(row_filter).__name__ == "GreaterThan"
    assert stream._planned_bookmark is None


def test_finalize_state_writes_planned_bookmark_for_backfill(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    """After a backfill run, ``_finalize_state`` should write the predetermined
    window end to ``replication_key_value`` and leave no progress markers."""
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
    final_state: dict = {}
    stream._finalize_state(final_state)

    assert final_state == {
        "replication_key": "updated_at",
        "replication_key_value": "2024-01-01T06:00:00+00:00",
    }


def test_finalize_state_promotes_max_observed_for_tail(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    start_dt = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        microsecond=0,
    )
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 24,
                    "start-replication-key-value": start_dt.isoformat(),
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

    rk_max = start_dt + timedelta(minutes=42)
    stream._scan_and_yield_rows = lambda _filter: iter(  # type: ignore[method-assign]
        [{"id": 1, "updated_at": rk_max}],
    )

    list(stream.get_records())
    final_state: dict = {}
    stream._finalize_state(final_state)

    assert final_state["replication_key"] == "updated_at"
    assert final_state["replication_key_value"] == rk_max.isoformat()


def test_finalize_state_noop_when_no_records_in_tail_phase(
    monkeypatch,
    mock_tap,
    iceberg_table_mock,
):
    """Tail run with zero records: ``_max_observed`` stays None, no bookmark
    is written (the previous bookmark in Meltano Postgres is preserved)."""
    start_dt = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        microsecond=0,
    )
    monkeypatch.delenv("TAP_ICEBERG__METADATA", raising=False)
    monkeypatch.setenv(
        "TAP_ICEBERG__METADATA",
        json.dumps(
            {
                "*": {
                    "window-size-hours": 24,
                    "start-replication-key-value": start_dt.isoformat(),
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
    final_state: dict = {}
    stream._finalize_state(final_state)

    assert "replication_key_value" not in final_state


def test_check_sorted_disabled():
    """Singer SDK must not raise InvalidStreamSortException on tail scans."""
    assert IcebergTableStream.check_sorted.fget(  # type: ignore[union-attr]
        MagicMock(spec=IcebergTableStream),
    ) is False
