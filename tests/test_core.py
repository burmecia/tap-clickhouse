"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_tap_test_class

from tap_clickhouse.tap import TapClickHouse

SAMPLE_CONFIG = {
    "driver": "http",
    "host": "localhost",
    "port": 8123,
    "username": "test_user",
    "password": "default",
    "database": "default",
    "secure": False,
    "verify": False
}


# Run standard built-in tap tests from the SDK:
TestTapClickHouse = get_tap_test_class(
    tap_class=TapClickHouse,
    config=SAMPLE_CONFIG,
)

