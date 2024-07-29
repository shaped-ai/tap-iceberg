import os

from faker import Faker
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

fake = Faker()

# Define a more complex schema for the Iceberg table
iceberg_schema = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="age", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="salary", field_type=DoubleType(), required=False),
    NestedField(field_id=5, name="is_active", field_type=BooleanType(), required=False),
    NestedField(field_id=6, name="join_date", field_type=DateType(), required=False),
    NestedField(
        field_id=7, name="last_login", field_type=TimestampType(), required=False
    ),
    NestedField(field_id=8, name="address", field_type=StringType(), required=False),
    NestedField(
        field_id=9,
        name="phone_numbers",
        field_type=ListType(
            element_id=11, element_type=StringType(), element_required=False
        ),
        required=False,
    ),
    NestedField(
        field_id=10, name="updated_at", field_type=TimestampType(), required=True
    ),
)

# Define the directory where the Iceberg table will be stored
ICEBERG_DIR = "tests/test_iceberg_data"
NAMESPACE = "test_namespace"
CATALOG_NAME = "test_catalog"
TABLE_NAME = "test_table"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"
ICEBERG_DB_URI = f"sqlite:///{ICEBERG_DIR}/{CATALOG_NAME}.db"
ICEBERG_WAREHOUSE = f"file://{ICEBERG_DIR}/{CATALOG_NAME}.warehouse"
if not os.path.exists(ICEBERG_DIR):
    os.makedirs(ICEBERG_DIR)

if __name__ == "__main__":
    import pyarrow as pa

    # Create an Iceberg catalog and table
    catalog = load_catalog(
        CATALOG_NAME,
        type="sql",
        uri=ICEBERG_DB_URI,
        warehouse=ICEBERG_WAREHOUSE,
    )

    if not catalog.list_namespaces():
        catalog.create_namespace(NAMESPACE)

    # Delete if the table already exists, if not create it
    try:
        table = catalog.load_table(identifier=TABLE_IDENTIFIER)
        catalog.drop_table(identifier=TABLE_IDENTIFIER)
    except Exception:
        pass

    sort_order = SortOrder(
        SortField(
            source_id=10,
            transform=IdentityTransform(),
            direction="asc",
            null_order="nulls-first",
        )
    )
    table = catalog.create_table(
        identifier=TABLE_IDENTIFIER, schema=iceberg_schema, sort_order=sort_order
    )

    # Generate test data using Faker
    rows = []
    for _ in range(10_000):
        row = {
            "id": fake.random_int(min=1, max=10000),
            "name": fake.name(),
            "age": fake.random_int(min=18, max=80),
            "salary": float(fake.random_number(digits=5)) + fake.random.random(),
            "is_active": fake.boolean(),
            "join_date": fake.date_between(start_date="-5y", end_date="today"),
            "last_login": fake.date_time_between(start_date="-1y", end_date="now"),
            "address": f"{fake.street_address()}, {fake.city()}, {fake.zipcode()}",
            "phone_numbers": [
                fake.phone_number() for _ in range(fake.random_int(min=1, max=3))
            ],
            "updated_at": fake.date_time_between(start_date="-1y", end_date="now"),
        }
        rows.append(row)

    # Define the Arrow schema to ensure consistency with the Iceberg table schema.
    pyarrow_table = pa.Table.from_pylist(rows, schema=iceberg_schema.as_arrow())
    # Sort the rows based on the sort order defined for the Iceberg table.
    pyarrow_table = pyarrow_table.sort_by("updated_at")
    # Append rows to the table using PyIceberg write support
    table.overwrite(pyarrow_table)
    # Assert row count
    assert len(table.scan().to_arrow()) == 10_000
