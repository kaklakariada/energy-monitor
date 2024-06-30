import pytest
from testcontainers.influxdb2 import InfluxDb2Container

from importer.db.influx import DbClient
from importer.db.influx_converter_test import _create_event

USERNAME = "admin"
PASSWORD = "admin-password"
ADMIN_TOKEN = "admin-token"
ORG = "test-org"
BUCKET = "test-bucket"


@pytest.fixture(scope="module")
def influx_container():
    with InfluxDb2Container(
        init_mode="setup", username=USERNAME, password=PASSWORD, admin_token=ADMIN_TOKEN, org_name=ORG, bucket=BUCKET
    ) as influx:
        yield influx


def skip_test_insert(influx_container: InfluxDb2Container):
    db = DbClient(url=influx_container.get_url(), token=ADMIN_TOKEN, bucket=BUCKET, org=ORG)
    writer = db.batch_writer()
    writer.insert_status_event("test-device", _create_event())
    writer.close()
    client, _org = influx_container.get_client()
    result = client.query_api().query(f'from(bucket: "{BUCKET}")')
    assert result.to_values() == [[]]
