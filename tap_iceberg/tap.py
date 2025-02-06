"""Iceberg tap class."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from botocore.credentials import Credentials
from pyiceberg.catalog import load_catalog
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_iceberg.utils import (
    get_refreshable_botocore_session,
)

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
            description="The ARN of the IAM role to use for accessing S3/Glue catalog",
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
            }
        )

        # Export AWS credentials to the catalog properties, and standard AWS
        # environment variables to override any system credentials.
        client_access_key_id = self.config.get("client_access_key_id")
        client_secret_access_key = self.config.get("client_secret_access_key")
        client_session_token = self.config.get("client_session_token")
        client_region = self.config.get("client_region")

        if client_region:
            catalog_properties["client.region"] = client_region
            os.environ["AWS_DEFAULT_REGION"] = client_region

        # If client IAM role ARN is provided, use provided credentials to assume
        # the role and create a botocore session with the assumed role, using those
        # refreshable credentials.
        if self.config.get("client_iam_role_arn"):
            role_session_name = "TapIceberg"
            self.logger.info(
                "Assuming role %s with session name %s.",
                self.config["client_iam_role_arn"],
                role_session_name,
            )
            os.environ["AWS_ACCESS_KEY_ID"] = client_access_key_id
            os.environ["AWS_SECRET_ACCESS_KEY"] = client_secret_access_key
            os.environ["AWS_SESSION_TOKEN"] = client_session_token
            credentials = Credentials(
                access_key=client_access_key_id,
                secret_key=client_secret_access_key,
                token=client_session_token,
            )
            botocore_session = get_refreshable_botocore_session(
                source_credentials=credentials,
                assume_role_arn=self.config["client_iam_role_arn"],
                role_session_name=role_session_name,
            )
            catalog_properties["botocore_session"] = botocore_session
            catalog_properties["client.role-arn"] = self.config["client_iam_role_arn"]
            catalog_properties["client.session-name"] = role_session_name
        else:
            if client_access_key_id:
                catalog_properties["client.access-key-id"] = client_access_key_id
                os.environ["AWS_ACCESS_KEY_ID"] = client_access_key_id
            if client_secret_access_key:
                catalog_properties["client.secret-access-key"] = (
                    client_secret_access_key
                )
                os.environ["AWS_SECRET_ACCESS_KEY"] = client_secret_access_key
            if client_session_token:
                catalog_properties["client.session-token"] = client_session_token
                os.environ["AWS_SESSION_TOKEN"] = client_session_token

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
