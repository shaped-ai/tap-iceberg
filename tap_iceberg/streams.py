"""Stream type classes for tap-iceberg."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable

import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue, And, GreaterThan, LessThanOrEqual
from pyiceberg.types import TimestamptzType
from singer_sdk import Stream  # JSON Schema typing helpers

from tap_iceberg.map_json import normalize_pyarrow_map_for_json
from tap_iceberg.utils import generate_schema_from_pyarrow

if TYPE_CHECKING:
    from pyiceberg.table import Table

    from tap_iceberg.tap import TapIceberg


if sys.version_info >= (3, 9):
    pass
else:
    pass


logger = logging.getLogger(__name__)

_TAP_METADATA_ENV = "TAP_ICEBERG__METADATA"


class IcebergTableStream(Stream):
    """Stream class for an Iceberg table."""

    def __init__(
        self,
        tap: TapIceberg,
        name: str,
        iceberg_table: Table,
    ) -> None:
        """Initialize the stream."""
        schema = generate_schema_from_pyarrow(iceberg_table.schema().as_arrow())
        super().__init__(tap, schema, name)
        self._iceberg_table = iceberg_table
        arrow_schema = iceberg_table.schema().as_arrow()
        self._map_column_names = frozenset(
            field.name for field in arrow_schema if pa.types.is_map(field.type)
        )

    def _tap_metadata_bundle_from_env(self) -> dict[str, Any] | None:
        """Parse TAP_ICEBERG__METADATA JSON (Magnus / Meltano env).

        Parsed once per tap instance and cached on ``tap`` so repeated lookups stay
        cheap.
        """
        tap = self._tap
        cache_attr = "_tap_iceberg_metadata_bundle_from_env_v1"
        tap_dict = getattr(tap, "__dict__", None)
        # unittest.mock.MagicMock never raises AttributeError and ignores getattr
        # defaults, so probe __dict__ explicitly (real taps also store attrs there).
        if tap_dict is not None and cache_attr in tap_dict:
            return tap_dict[cache_attr]

        raw = os.environ.get(_TAP_METADATA_ENV)
        parsed: dict[str, Any] | None = None
        if raw:
            try:
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    parsed = loaded
                else:
                    logger.warning(
                        "%s must decode to an object at the top level, got %s",
                        _TAP_METADATA_ENV,
                        type(loaded).__name__,
                    )
            except json.JSONDecodeError as exc:
                logger.warning(
                    "Invalid JSON in %s: %s",
                    _TAP_METADATA_ENV,
                    exc,
                )
        if tap_dict is not None:
            tap_dict[cache_attr] = parsed
        else:
            setattr(tap, cache_attr, parsed)
        return parsed

    def _merged_tap_iceberg_metadata_overlay(self) -> dict[str, Any]:
        """Return merged metadata for this stream id from TAP_ICEBERG__METADATA.

        Meltano typically passes a wildcard block under ``'*'``. Optional per-stream
        keys matching :attr:`~singer_sdk.streams.core.Stream.name` override the same
        keys from ``'*'``.
        """
        bundle = self._tap_metadata_bundle_from_env()
        overlay: dict[str, Any] = {}
        if not isinstance(bundle, dict):
            return overlay
        wild = bundle.get("*")
        if isinstance(wild, dict):
            overlay.update(wild)
        specific = bundle.get(self.name)
        if isinstance(specific, dict):
            overlay.update(specific)
        return overlay

    def _get_stream_metadata_value(self, key: str, default=None):
        """Read a Magnus extension key supplied via TAP_ICEBERG__METADATA.

        Singer's ``StreamMetadata.from_dict()`` only keeps Singer-spec catalogue
        fields, so entries like ``window-size-hours`` are **dropped** when the
        catalog is parsed—they must be read from the raw env blob instead.

        Fallback: if catalogue root metadata is stored as a ``dict``, read from
        there (useful for hand-written catalogs in tests).
        """
        overlay = self._merged_tap_iceberg_metadata_overlay()
        if key in overlay:
            return overlay[key]

        root_md = self.metadata.get((), None)
        if isinstance(root_md, dict):
            return root_md.get(key, default)

        return default

    @property
    def is_sorted(self) -> bool:
        """We bypass Singer SDK's per-record bookmark logic entirely (see
        _increment_stream_state and _finalize_state). Returning True keeps the
        SDK from creating progress_markers / signposts that we'd then ignore.
        """
        return True

    def _increment_stream_state(self, latest_record, *, context=None) -> None:  # noqa: ARG002
        """No-op: per-record bookmarks are unsafe on randomly-ordered file scans.
        Bookmark advances only when the whole window completes (_finalize_state).
        """
        return

    def _finalize_state(self, state=None) -> None:
        """Promote the planned window end to the committed bookmark.

        Singer SDK calls this only when get_records returns cleanly (i.e. the
        whole window was processed). If SIGTERM kills the generator mid-window,
        this never runs and the bookmark stays where it was.
        """
        planned_end = getattr(self, "_planned_window_end", None)
        if planned_end is not None and self.replication_key:
            stream_state = state if state is not None else self.get_context_state(
                None,
            )
            stream_state["replication_key"] = self.replication_key
            stream_state["replication_key_value"] = planned_end
            self.logger.info(
                "Window complete; advancing bookmark to %s",
                planned_end,
            )
        super()._finalize_state(state)

    def get_replication_key_signpost(self, context: dict | None = None):  # noqa: ARG002
        """Bounded by Iceberg row_filter for incremental windows, not utc_now."""
        return None

    def _replication_column_is_tz_aware(self) -> bool:
        """Return True iff the Iceberg replication column is ``timestamptz``.

        PyIceberg's ``TimestampType`` (no zone) rejects ISO literals that carry an
        offset (``ValueError: Zone offset provided, but not expected``), and
        ``TimestamptzType`` requires an offset. We inspect the table schema so the
        same tap works regardless of which shape the operator passes in
        ``start-replication-key-value`` / state.
        """
        rk = self.replication_key
        if not rk:
            return False
        try:
            field = self._iceberg_table.schema().find_field(rk)
        except Exception:
            return False
        if field is None:
            return False
        return isinstance(field.field_type, TimestamptzType)

    def get_records(self, context: dict | None = None) -> Iterable[dict]:
        """Yield records from a single closed time window of the Iceberg table.

        Window = (last_bookmark, last_bookmark + window-size-hours]. PyIceberg's
        file/manifest order may scatter replication-key values randomly within
        the unread tail, so we only advance the bookmark when the whole window
        finishes (handled in _finalize_state). SIGTERM mid-window leaves the
        bookmark untouched; the next cron retries the same window.
        """
        if self.replication_method != "INCREMENTAL":
            self._planned_window_end = None
            yield from self._get_records_full_table(context)
            return

        window_hours = self._get_stream_metadata_value("window-size-hours")
        bootstrap_start = self._get_stream_metadata_value(
            "start-replication-key-value",
        )

        if window_hours is None or bootstrap_start is None:
            raise ValueError(
                f"Stream '{self.name}' requires both 'window-size-hours' and "
                "'start-replication-key-value' in TAP_ICEBERG__METADATA for "
                "incremental sync.",
            )

        state_bookmark = self.get_starting_replication_key_value(context)
        window_start_str = state_bookmark or bootstrap_start

        window_start = datetime.fromisoformat(window_start_str)
        column_is_tz_aware = self._replication_column_is_tz_aware()
        if column_is_tz_aware:
            if window_start.tzinfo is None:
                window_start = window_start.replace(tzinfo=timezone.utc)
        else:
            # PyIceberg's `TimestampType` rejects literals with a zone offset.
            # Normalize aware → UTC then strip tzinfo so the column literal matches.
            if window_start.tzinfo is not None:
                window_start = window_start.astimezone(timezone.utc).replace(
                    tzinfo=None,
                )
        window_end = window_start + timedelta(hours=int(window_hours))

        self.logger.info(
            "Iceberg windowed scan: %s < %s <= %s (window=%sh, source=%s)",
            window_start.isoformat(),
            self.replication_key,
            window_end.isoformat(),
            window_hours,
            "state" if state_bookmark else "bootstrap",
        )

        self._planned_window_end = window_end.isoformat()

        filter_expression = And(
            GreaterThan(self.replication_key, window_start.isoformat()),
            LessThanOrEqual(self.replication_key, window_end.isoformat()),
        )
        yield from self._scan_and_yield_rows(filter_expression)

    def _get_records_full_table(self, context: dict | None) -> Iterable[dict]:
        """Non-incremental: scan the whole table."""
        _ = context
        self.logger.info("Starting Iceberg table scan.")
        yield from self._scan_and_yield_rows(AlwaysTrue())

    def _scan_and_yield_rows(self, row_filter: Any) -> Iterable[dict]:
        batch_reader = (
            self._iceberg_table.scan(row_filter=row_filter).to_arrow_batch_reader()
        )

        formatters = self._create_formatters()
        for batch in batch_reader:
            records = batch.to_pylist()
            for record in records:
                if self._map_column_names:
                    record = dict(record)
                    for col in self._map_column_names:
                        if col in record:
                            record[col] = normalize_pyarrow_map_for_json(record[col])
                yield self._format_record(record, formatters)

    def _create_formatters(self) -> dict[str, Callable[[Any], Any]]:
        formatters = {}
        for field, schema in self.schema["properties"].items():
            if schema.get("format") == "date" and schema["type"] == ["string", "null"]:
                formatters[field] = lambda x: self._format_date(x)
            elif "null" in schema["type"]:
                formatters[field] = lambda x: x if x is not None else None
            else:
                formatters[field] = lambda x: x
        return formatters

    def _format_date(self, value: str | date | datetime | None) -> str | None:
        if isinstance(value, (date, datetime)):
            return value.isoformat()[:10]
        elif isinstance(value, str):
            return value[:10]
        else:
            return None

    def _format_record(
        self, record: dict[str, Any], formatters: dict[str, Callable[[Any], Any]]
    ) -> dict[str, Any]:
        return {field: formatters[field](value) for field, value in record.items()}
