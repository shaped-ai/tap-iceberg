"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_tap_test_class

from tap_iceberg.tap import TapIceberg

from .generate_test_data import CATALOG_NAME, ICEBERG_DB_URI, ICEBERG_WAREHOUSE

SAMPLE_CONFIG = {
    "catalog_type": "jdbc",
    "catalog_name": CATALOG_NAME,
    "catalog_warehouse_path": ICEBERG_WAREHOUSE,
    "catalog_uri": ICEBERG_DB_URI,
}


# Run standard built-in tap tests from the SDK:
TestTapIceberg = get_tap_test_class(
    tap_class=TapIceberg,
    config=SAMPLE_CONFIG,
)

# Run tap tests with state
TestTapIcebergState = get_tap_test_class(
    tap_class=TapIceberg,
    config=SAMPLE_CONFIG,
    state={
        "bookmarks": {
            "test_namespace-test_sorted_table": {
                "replication_key": "updated_at",
                "replication_key_value": "2024-07-23T00:00:00.000000+00:00",
            }
        }
    },
)
