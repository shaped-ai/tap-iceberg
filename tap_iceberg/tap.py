"""Iceberg tap class."""

from __future__ import annotations

import os
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
            required=True,
            description="The name of the Iceberg catalog",
        ),
        th.Property(
            "catalog_type",
            th.StringType,
            required=True,
            description="The type of Iceberg catalog (e.g., 'hive', 'rest', 'glue')",
        ),
        th.Property(
            "client_access_key_id",
            th.StringType,
            required=False,
            secret=True,
            description="The AWS access key ID for accessing S3/Glue catalogs",
        ),
        th.Property(
            "client_secret_access_key",
            th.StringType,
            required=False,
            secret=True,
            description="The AWS secret access key for accessing S3/Glue catalogs",
        ),
        th.Property(
            "client_session_token",
            th.StringType,
            required=False,
            secret=True,
            description="The AWS session token for accessing S3/Glue catalogs",
        ),
        th.Property(
            "client_region",
            th.StringType,
            required=False,
            description="The AWS region for accessing S3/Glue catalogs",
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
                tap_stream_id = f"{table[0]}-{table[1]}"
                iceberg_table = catalog.load_table(table_id)
                discovered_streams.append(
                    IcebergTableStream(
                        self,
                        name=tap_stream_id,
                        iceberg_table=iceberg_table,
                    )
                )

        return discovered_streams

    def _get_catalog(self) -> Catalog:
        """Load and return the Iceberg catalog based on the configuration."""
        catalog_properties = self.config.get("catalog_properties", {})
        catalog_properties.update(
            {
                "type": self.config["catalog_type"],
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            }
        )

        # Export AWS credentials to the catalog properties, and standard AWS
        # environment variables to override any system credentials.
        if self.config.get("client_access_key_id"):
            catalog_properties["client.access-key-id"] = self.config[
                "client_access_key_id"
            ]
            os.environ["AWS_ACCESS_KEY_ID"] = self.config["client_access_key_id"]
        if self.config.get("client_secret_access_key"):
            catalog_properties["client.secret-access-key"] = self.config[
                "client_secret_access_key"
            ]
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.config[
                "client_secret_access_key"
            ]
        if self.config.get("client_session_token"):
            catalog_properties["client.session-token"] = self.config[
                "client_session_token"
            ]
            os.environ["AWS_SESSION_TOKEN"] = self.config["client_session_token"]
        if self.config.get("client_region"):
            catalog_properties["client.region"] = self.config["client_region"]
            os.environ["AWS_DEFAULT_REGION"] = self.config["client_region"]

        self.logger.debug(
            "Loading Iceberg catalog with properties: %s",
            catalog_properties,
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
