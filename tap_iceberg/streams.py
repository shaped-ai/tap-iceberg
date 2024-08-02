# !/usr/bin/env python
"""Stream type classes for tap-iceberg."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from pyspark.sql.functions import col, date_format
from singer_sdk import Stream, Tap

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class IcebergTableStream(Stream):
    """Stream class for an Iceberg table using PySpark."""

    def __init__(
        self,
        tap: Tap,
        name: str,
        spark: SparkSession,
        full_table_name: str,
    ) -> None:
        """Initialize the stream."""
        schema = self._infer_schema(spark, full_table_name)
        super().__init__(tap, schema, name)
        self._spark = spark
        self._full_table_name = full_table_name
        self._is_sorted: bool | None = None

    def _infer_schema(
        self, spark: SparkSession, full_table_name: str
    ) -> dict[str, Any]:
        """Infer the JSON schema from the Spark DataFrame schema."""
        df = spark.table(full_table_name)
        schema = {}
        for field in df.schema.fields:
            schema[field.name] = self._spark_type_to_json_schema(
                field.dataType.simpleString()
            )
        return {"type": "object", "properties": schema}

    def _spark_type_to_json_schema(self, spark_type: str) -> dict[str, Any]:
        """Convert Spark data type to JSON schema type."""
        type_mapping = {
            "string": {"type": ["string", "null"]},
            "int": {"type": ["integer", "null"]},
            "long": {"type": ["integer", "null"]},
            "bigint": {"type": ["integer", "null"]},
            "double": {"type": ["number", "null"]},
            "float": {"type": ["number", "null"]},
            "boolean": {"type": ["boolean", "null"]},
            "date": {"type": ["string", "null"], "format": "date"},
            "timestamp": {"type": ["string", "null"], "format": "date-time"},
            "array<string>": {"type": ["array", "null"], "items": {"type": "string"}},
        }
        return type_mapping.get(spark_type, {"type": ["string", "null"]})  # type: ignore[return-value]

    def get_records(
        self, context: dict[str, Any] | None = None
    ) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects."""
        df = self._spark.table(self._full_table_name)

        if self.replication_key:
            start_value = self.get_starting_replication_key_value(context)
            if start_value:
                df = df.filter(col(self.replication_key) > start_value)

        schema = self.schema["properties"]
        select_exprs = [
            date_format(col(key), "yyyy-MM-dd").alias(key)
            if schema[key].get("format") == "date"
            else col(key)
            for key in schema
        ]
        df = df.select(*select_exprs)

        # Use Spark's built-in methods to iterate efficiently over large datasets
        for row in df.toLocalIterator(prefetchPartitions=True):
            yield row.asDict()

    @property
    def is_sorted(self) -> bool:
        """Check if the table has a sort order or partitioning."""
        if self._is_sorted is None:
            self._is_sorted = self._check_is_sorted()
        return self._is_sorted

    def _check_is_sorted(self) -> bool:
        """Query Iceberg metadata to check for sort order or partitioning."""
        try:
            detail = self._spark.sql(f"DESCRIBE DETAIL {self._full_table_name}")
        except Exception as e:
            self.logger.warning("Error checking sort order or partitioning: %s", e)
            return False

        sort_order = detail.select("sort_order").collect()[0][0]
        partition_fields = detail.select("partition_field_names").collect()[0][0]
        return (sort_order and sort_order != "[]") or (
            partition_fields and partition_fields != "[]"
        )
