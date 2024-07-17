"""Stream type classes for tap-iceberg."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Iterable

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
        self.iceberg_table = iceberg_table

    def get_records(self, context: dict | None = None) -> Iterable[dict]:
        context = context or {}
        """Return a generator of record-type dictionary objects."""
        scan = self.iceberg_table.scan()

        # Create an Arrow RecordBatchReader.
        batch_reader = scan.to_arrow_batch_reader()

        # Iterate over batches and yield records.
        for batch in batch_reader:
            yield from batch.to_pylist()
