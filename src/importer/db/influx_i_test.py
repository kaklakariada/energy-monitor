import pytest
from influxdb_client import InfluxDBClient
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

from importer.db.influx import DbClient
from importer.db.influx_converter_test import _create_event

USERNAME = "admin"
PASSWORD = "admin-password"
ADMIN_TOKEN = "admin-token"
ORG = "test-org"
BUCKET = "test-bucket"


@pytest.fixture(scope="module", name="influx_container")
def influx_container_fixture():
    container = (
        DockerContainer("influxdb:2.7")
        .with_exposed_ports(8086)
        .with_env("DOCKER_INFLUXDB_INIT_MODE", "setup")
        .with_env("DOCKER_INFLUXDB_INIT_USERNAME", USERNAME)
        .with_env("DOCKER_INFLUXDB_INIT_PASSWORD", PASSWORD)
        .with_env("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", ADMIN_TOKEN)
        .with_env("DOCKER_INFLUXDB_INIT_ORG", ORG)
        .with_env("DOCKER_INFLUXDB_INIT_BUCKET", BUCKET)
        .waiting_for(HttpWaitStrategy(8086, "/health").for_status_code(200))
    )
    with container as influx:
        yield influx


def skip_test_insert(influx_container: DockerContainer):
    url = f"http://localhost:{influx_container.get_exposed_port(8086)}"
    db = DbClient(url=url, token=ADMIN_TOKEN, bucket=BUCKET, org=ORG)
    writer = db.batch_writer()
    writer.insert_status_event("test-device", _create_event())
    writer.close()
    with InfluxDBClient(url=url, token=ADMIN_TOKEN) as client:
        result = client.query_api().query(f'from(bucket: "{BUCKET}")')
        assert result.to_values() == [[]]
