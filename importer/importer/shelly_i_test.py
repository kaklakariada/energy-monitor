import time
import pytest
from importer.shelly import Shelly
from importer.config import shelly_devices


@pytest.fixture
def shelly():
    shelly = shelly_devices[0]
    return Shelly(shelly["ip"])


def test_system_status(shelly: Shelly):
    data = shelly.get_system_status()
    assert data.uptime > 0


def test_time_sync(shelly: Shelly):
    data = shelly.get_system_status()
    assert abs(data.unixtime - int(time.time())) < 5


def test_get_status(shelly: Shelly):
    data = shelly.get_status()
    assert data.emdata.id == 0
    assert data.em.id == 0
    assert data.temperature > 0
    assert data.sys.unixtime > 0


def test_get_em_status(shelly: Shelly):
    data = shelly.get_em_status()
    assert data.id == 0
    assert data.total_act_power > 0
    assert len(data.user_calibrated_phase) == 0
    assert len(data.phases) == 3
    assert len(data.errors) == 0
    for phase in data.phases:
        assert phase.voltage > 0
        assert phase.current > 0
        assert len(phase.errors) == 0


def test_get_emdata_status(shelly: Shelly):
    data = shelly.get_emdata_status()
    assert data.id == 0
    assert data.total_act > 0
    assert len(data.errors) == 0


def test_get_emdata_records(shelly: Shelly):
    data = shelly.get_emdata_records()
    assert len(data.data_blocks) > 0
    assert data.data_blocks[0].timestamp.timestamp() > 0
    assert data.data_blocks[0].period == 60
    assert data.data_blocks[0].records > 1
