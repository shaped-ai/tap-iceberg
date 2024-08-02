from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession
from singer_sdk import Tap
from singer_sdk import typing as th

if TYPE_CHECKING:
    from tap_iceberg.streams import IcebergTableStream


class TapIceberg(Tap):
    """Iceberg tap class using PySpark."""

    name = "tap-iceberg"
    iceberg_version = "1.6.0"
    # Class-level variable to store the singleton SparkSession
    _spark = None

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
            description="The type of the Iceberg catalog",
        ),
        th.Property(
            "aws_access_key_id",
            th.StringType,
            required=False,
            secret=True,
            description="AWS access key ID for Glue catalog access",
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            required=False,
            secret=True,
            description="AWS secret access key for Glue catalog access",
        ),
        th.Property(
            "aws_session_token",
            th.StringType,
            required=False,
            secret=True,
            description="AWS session token for Glue catalog access",
        ),
        th.Property(
            "aws_region",
            th.StringType,
            required=False,
            description="AWS region for Glue catalog access",
        ),
        th.Property(
            "catalog_warehouse_path",
            th.StringType,
            required=False,
            description="The path to the Iceberg warehouse",
        ),
        th.Property(
            "catalog_uri",
            th.StringType,
            required=False,
            description="The URI of the Iceberg catalog",
        ),
    ).to_dict()

    def __init__(self, config: dict[str, str], *args: Any, **kwargs: Any) -> None:
        if not self._spark:
            self._spark = self._create_spark_session(config=config)
        kwargs["config"] = config
        super().__init__(*args, **kwargs)

    def _create_spark_session(self, config: dict[str, str]) -> SparkSession:
        """Create and return a SparkSession configured for Iceberg."""
        catalog_name = config["catalog_name"]
        assert catalog_name, "Missing required config: catalog_name"
        catalog_type = config["catalog_type"]
        assert catalog_type, "Missing required config: catalog_type"

        # Load JARs for Iceberg support.
        iceberg_jars = self.find_iceberg_jars()
        builder = (
            SparkSession.builder.appName("IcebergTap")
            .master(config.get("spark_master", "local[*]"))
            .config("spark.jars", ",".join(iceberg_jars))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkSessionCatalog",
            )
            .config(f"spark.sql.catalog.{catalog_name}.type", config["catalog_type"])
            .config(
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.defaultCatalog", catalog_name)
        )

        if config.get("catalog_warehouse_path"):
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.warehouse",
                config["catalog_warehouse_path"],
            )
        if config.get("catalog_uri"):
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.uri", config["catalog_uri"]
            )
        if config.get("aws_access_key_id"):
            builder = builder.config(
                "spark.hadoop.fs.s3a.access.key", config["aws_access_key_id"]
            )
        if config.get("aws_secret_access_key"):
            builder = builder.config(
                "spark.hadoop.fs.s3a.secret.key", config["aws_secret_access_key"]
            )
        if config.get("aws_session_token"):
            builder = builder.config(
                "spark.hadoop.fs.s3a.session.token", config["aws_session_token"]
            )
        if config.get("aws_region"):
            builder = builder.config("spark.hadoop.fs.s3a.region", config["aws_region"])

        return builder.getOrCreate()

    @classmethod
    def find_iceberg_jars(cls) -> list[str]:
        """Find the Iceberg JARs in the Python environment."""
        iceberg_path = Path(__file__).parent
        jar_dir = iceberg_path / "jars"
        jar_files = list(jar_dir.glob("*.jar"))
        if not jar_files:
            error_msg = "No Iceberg JARs found in the Python environment"
            raise RuntimeError(error_msg)
        return [str(jar) for jar in jar_files]

    def discover_streams(self) -> list[IcebergTableStream]:
        """Return a list of discovered streams."""
        # Local import to avoid circular dependency.
        from tap_iceberg.streams import IcebergTableStream

        assert self._spark, "Spark session not initialized"
        catalog_name = self.config["catalog_name"]
        discovered_streams = []

        catalogs = self._spark.sql("SHOW CATALOGS").collect()
        if catalog_name not in [cat["catalog"] for cat in catalogs]:
            error_msg = (
                f"Catalog '{catalog_name}' does not exist. "
                f"Available catalogs: {[cat['catalog'] for cat in catalogs]}"
            )
            raise ValueError(error_msg)

        namespaces = self._spark.sql(f"SHOW NAMESPACES IN {catalog_name}").collect()
        tables = []
        try:
            for namespace in namespaces:
                tables.extend(
                    self._spark.sql(
                        f"SHOW TABLES IN {namespace['namespace']}"
                    ).collect()
                )
        except Exception:
            self.logger.exception("Error listing tables in catalog '%s'", catalog_name)

        for table in tables:
            namespace = table["namespace"]
            table_name = table["tableName"]
            full_table_name = f"{catalog_name}.{namespace}.{table_name}"

            discovered_streams.append(
                IcebergTableStream(
                    self,
                    name=f"{namespace}-{table_name}",
                    spark=self._spark,
                    full_table_name=full_table_name,
                )
            )

        return discovered_streams

    def stop(self) -> None:
        """Stop the Spark session."""
        if self._spark:
            self._spark.stop()


if __name__ == "__main__":
    TapIceberg.cli()
