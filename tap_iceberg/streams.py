"""Stream type classes for tap-iceberg."""

from __future__ import annotations

import sys
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Callable, Iterable

from pyiceberg.expressions import AlwaysTrue, GreaterThan
from singer_sdk import Stream  # JSON Schema typing helpers

from tap_iceberg.utils import generate_schema_from_pyarrow

if TYPE_CHECKING:
    from pyiceberg.table import Table

    from tap_iceberg.tap import TapIceberg


if sys.version_info >= (3, 9):
    pass
else:
    pass


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

        sort_fields = self._iceberg_table.sort_order().fields
        if len(sort_fields) == 1:
            sort_field_source_id = sort_fields[0].source_id
            sort_field_name = (
                self._iceberg_table.schema().find_field(sort_field_source_id).name
            )
            self._replication_key = sort_field_name

    @property
    def is_sorted(self) -> bool:
        return not self._iceberg_table.sort_order().is_unsorted

    def get_records(self, context: dict | None = None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        filter_expression = AlwaysTrue()
        self.logger.info("Starting Iceberg table scan.")
        start_value = self.get_starting_replication_key_value(context)
        if start_value:
            self.logger.info(
                "Filtering records for replication key %s greater than %s.",
                self.replication_key,
                start_value,
            )

            filter_expression = GreaterThan(self.replication_key, start_value)
        batch_reader = self._iceberg_table.scan(
            row_filter=filter_expression,
        ).to_arrow_batch_reader()

        formatters = self._create_formatters()
        for batch in batch_reader:
            records = batch.to_pylist()
            for record in records:
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
