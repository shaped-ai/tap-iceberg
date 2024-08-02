import os

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tap_iceberg.tap import TapIceberg

# Initialize Faker
fake = Faker()

# Define constants
ICEBERG_DIR = "test_iceberg_data"
NAMESPACE = "test_namespace"
CATALOG_NAME = "test_catalog"
SORTED_TABLE_NAME = "test_sorted_table"
PARTITIONED_TABLE_NAME = "test_partitioned_table"
SORTED_TABLE_IDENTIFIER = f"{NAMESPACE}.{SORTED_TABLE_NAME}"
PARTITIONED_TABLE_IDENTIFIER = f"{NAMESPACE}.{PARTITIONED_TABLE_NAME}"
ICEBERG_DB_URI = f"jdbc:sqlite:{ICEBERG_DIR}/{CATALOG_NAME}.db"
ICEBERG_WAREHOUSE = f"{ICEBERG_DIR}/{CATALOG_NAME}.warehouse"

schema = StructType(
    [
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("join_date", DateType(), True),
        StructField("last_login", TimestampType(), True),
        StructField("address", StringType(), True),
        StructField("phone_numbers", ArrayType(StringType()), True),
        StructField("updated_at", TimestampType(), False),
    ]
)


def generate_data():
    for _ in range(10_000):
        yield (
            fake.random_int(min=1, max=10000),
            fake.name(),
            fake.random_int(min=18, max=80),
            round(fake.pyfloat(min_value=20000, max_value=100000, right_digits=2), 2),
            fake.boolean(),
            fake.date_between(
                start_date="-5y", end_date="today"
            ),  # Now returns a date object
            fake.date_time_between(
                start_date="-1y", end_date="now"
            ),  # Now returns a datetime object
            f"{fake.street_address()}, {fake.city()}, {fake.zipcode()}",
            [fake.phone_number() for _ in range(fake.random_int(min=1, max=3))],
            fake.date_time_between(
                start_date="-1w", end_date="now"
            ),  # Now returns a datetime object
        )

if __name__ == "__main__":
    # Create directory if it doesn't exist
    if not os.path.exists(ICEBERG_DIR):
        os.makedirs(ICEBERG_DIR)

    # Initialize Spark session
    iceberg_jars = TapIceberg.find_iceberg_jars()
    spark = (
        SparkSession.builder.appName("IcebergTest")
        .master("local[*]")
        .config("spark.jars", ",".join(iceberg_jars))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "jdbc")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", ICEBERG_DB_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", ICEBERG_WAREHOUSE)
        .getOrCreate()
    )
    # Create DataFrame
    df = spark.createDataFrame(generate_data(), schema=schema)

    # Create namespace if it doesn't exist
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")

    # Drop tables if they exist
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SORTED_TABLE_IDENTIFIER}")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{PARTITIONED_TABLE_IDENTIFIER}")

    # Create sorted table
    spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.{SORTED_TABLE_IDENTIFIER} (
        id BIGINT,
        name STRING,
        age INT,
        salary DOUBLE,
        is_active BOOLEAN,
        join_date DATE,
        last_login TIMESTAMP,
        address STRING,
        phone_numbers ARRAY<STRING>,
        updated_at TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'sort-order' = 'updated_at'
    )
    """)

    # Create partitioned table
    spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.{PARTITIONED_TABLE_IDENTIFIER} (
        id BIGINT,
        name STRING,
        age INT,
        salary DOUBLE,
        is_active BOOLEAN,
        join_date DATE,
        last_login TIMESTAMP,
        address STRING,
        phone_numbers ARRAY<STRING>,
        updated_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(updated_at))
    """)

    # Write data to tables
    df.sort("updated_at").writeTo(f"{CATALOG_NAME}.{SORTED_TABLE_IDENTIFIER}").append()
    df.writeTo(f"{CATALOG_NAME}.{PARTITIONED_TABLE_IDENTIFIER}").append()

    # Verify row counts
    sorted_count = spark.table(f"{CATALOG_NAME}.{SORTED_TABLE_IDENTIFIER}").count()
    partitioned_count = spark.table(
        f"{CATALOG_NAME}.{PARTITIONED_TABLE_IDENTIFIER}"
    ).count()

    print(f"Sorted table row count: {sorted_count}")
    print(f"Partitioned table row count: {partitioned_count}")

    assert sorted_count == 10_000
    assert partitioned_count == 10_000

    # Stop Spark session
    spark.stop()
