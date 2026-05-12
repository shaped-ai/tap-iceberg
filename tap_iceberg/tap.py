"""Iceberg tap class."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import load_catalog
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_iceberg.aws_session import attach_catalog_aws_credentials

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
            "client_iam_role_arn",
            th.StringType,
            required=False,
            description="Optional second-hop IAM role ARN (customer data role)",
        ),
        th.Property(
            "customer_data_access_role_arn",
            th.StringType,
            required=False,
            description=(
                "Shaped intermediary IAM role ARN (first AssumeRole hop from "
                "IRSA/pod identity); also readable from CUSTOMER_DATA_ACCESS_ROLE_ARN"
            ),
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
            IcebergTableStream,
        )

        catalog = self._get_catalog()
        discovered_streams = []
        for namespace in catalog.list_namespaces():
            for table in catalog.list_tables(namespace):
                table_id = f"{table[0]}.{table[1]}"
                tap_stream_id = f"{table[0]}-{table[1]}"
                try:
                    iceberg_table = catalog.load_table(table_id)
                except (KeyError, Exception) as e:
                    if "Parameters" in str(e) or "table_type" in str(e):
                        self.logger.debug(
                            "Skipping %s: not a valid Iceberg table (%s).",
                            table_id,
                            e,
                        )
                        continue
                    raise
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
        catalog_properties = dict(self.config.get("catalog_properties", {}))
        catalog_properties.update(
            {
                "type": self.config["catalog_type"],
            },
        )

        attach_catalog_aws_credentials(
            catalog_properties,
            config=self.config,
            logger=self.logger,
        )

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
