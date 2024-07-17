import pyarrow as pa
import os
from datetime import datetime, date
from faker import Faker
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType, ListType

fake = Faker()

# Define a more complex schema for the Iceberg table
schema = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="age", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="salary", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="is_active", field_type=BooleanType(), required=False),
    NestedField(field_id=6, name="join_date", field_type=DateType(), required=False),
    NestedField(field_id=7, name="last_login", field_type=TimestampType(), required=False),
    NestedField(field_id=8, name="address", field_type=StringType(), required=False),
    NestedField(field_id=9, name="phone_numbers", field_type=ListType(element_id=1, element=StringType(), element_required=False))
)

# Define the directory where the Iceberg table will be stored
iceberg_dir = "tests/test_iceberg_data"
namespace = "test_namespace"
catalog_name = "test_catalog"
table_name = "test_table"
if not os.path.exists(iceberg_dir):
    os.makedirs(iceberg_dir)

# Create an Iceberg catalog and table
catalog = SqlCatalog(
    catalog_name,
    **{
        "uri": f"sqlite:///{iceberg_dir}/{catalog_name}.db",
        "warehouse": f"file://{iceberg_dir}",
    },
)

if not catalog.list_namespaces():
    catalog.create_namespace(namespace)

# Delete if the table already exists, if not create it
try:
    table = catalog.load_table(identifier=f"{namespace}.{table_name}")
    catalog.drop_table(identifier=f"{namespace}.{table_name}")
except Exception:
    table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema
    )

# Generate test data using Faker
rows = []
for _ in range(10_000):
    row = {
        "id": fake.random_int(min=1, max=10000),
        "age": fake.random_int(min=18, max=80),
        "salary": float(fake.random_number(digits=5)) + fake.random.random(),
        "is_active": fake.boolean(),
        "join_date": fake.date_between(start_date='-5y', end_date='today'),
        "last_login": fake.date_time_between(start_date='-1y', end_date='now'),
        "address": f"{fake.street_address()}, {fake.city()}, {fake.zipcode()}",
        "phone_numbers": [fake.phone_number() for _ in range(fake.random_int(min=1, max=3))]
    }
    rows.append(row)

# Define the Arrow schema to ensure consistency with the Iceberg table schema
arrow_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("age", pa.int32(), nullable=True),
    pa.field("salary", pa.float64(), nullable=True),
    pa.field("is_active", pa.bool_(), nullable=True),
    pa.field("join_date", pa.date32(), nullable=True),
    pa.field("last_login", pa.timestamp('s'), nullable=True),
    pa.field("address", pa.string(), nullable=True),
    pa.field("phone_numbers", pa.list_(pa.string()), nullable=True)
])
pyarrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)

# Append rows to the table using PyIceberg write support
table.append(pyarrow_table)

print(f"Test data has been written to the Iceberg table at {iceberg_dir}.")
