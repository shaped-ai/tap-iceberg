"""Iceberg tap class."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import load_catalog
from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

    from tap_iceberg.streams import IcebergTableStream


class TapIceberg(Tap):
    """Iceberg tap class."""

    name = "tap-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_name",
            th.StringType,
            required=False,
            description="The name of the Iceberg catalog",
        ),
        th.Property(
            "catalog_uri",
            th.StringType,
            required=True,
            description="The URI of the Iceberg catalog",
        ),
        th.Property(
            "catalog_type",
            th.StringType,
            required=True,
            description="The type of Iceberg catalog (e.g., 'hive', 'rest', 'glue')",
        ),
        th.Property(
            "warehouse",
            th.StringType,
            required=False,
            description="The warehouse location for the Iceberg catalog",
        ),
        th.Property(
            "catalog_properties",
            th.ObjectType(additional_properties=th.StringType()),
            required=False,
            description="Additional properties for the Iceberg catalog",
        ),
    ).to_dict()

    def discover_streams(self) -> list[IcebergTableStream]:
        """Return a list of discovered streams."""
        from tap_iceberg.streams import (
            IcebergTableStream,  # Local import to avoid circular dependency
        )

        catalog = self._get_catalog()
        discovered_streams = []
        for namespace in catalog.list_namespaces():
            for table in catalog.list_tables(namespace):
                table_id = f"{table[0]}.{table[1]}"
                iceberg_table = catalog.load_table(table_id)
                discovered_streams.append(
                    IcebergTableStream(
                        self,
                        name=table_id,
                        iceberg_table=iceberg_table,
                    )
                )

        return discovered_streams

    def _get_catalog(self) -> Catalog:
        """Load and return the Iceberg catalog based on the configuration."""
        catalog_properties = self.config.get("catalog_properties", {})
        catalog_properties.update(
            {
                "uri": self.config["catalog_uri"],
                "warehouse": self.config.get("warehouse"),
                "type": self.config["catalog_type"],
            }
        )

        return load_catalog(
            self.config.get("catalog_name"),
            **{
                key: value
                for key, value in catalog_properties.items()
                if value is not None
            },
        )


if __name__ == "__main__":
    TapIceberg.cli()
