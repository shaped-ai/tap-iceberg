"""Iceberg tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_iceberg import streams


class TapIceberg(Tap):
    """Iceberg tap class."""

    name = "tap-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_uri",
            th.StringType,
            required=True,
            description="The URI of the Iceberg catalog",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.IcebergStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapIceberg.cli()
