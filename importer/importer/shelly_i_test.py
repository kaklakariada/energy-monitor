import datetime
import logging
import time
from typing import Optional

import pytest

from config import config
from importer.model import NotifyStatusEvent
from importer.shelly import Shelly

pytestmark = pytest.mark.shelly

UTC = datetime.timezone.utc


@pytest.fixture
def shelly():
    return Shelly(config.devices[1])


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


def test_get_csv_data(shelly: Shelly):
    now = datetime.datetime.now(tz=UTC)
    one_hour_ago = now - datetime.timedelta(hours=1)
    data = list(shelly.get_data(timestamp=one_hour_ago))
    assert len(data) == 60
    first_row = data[0]
    last_row = data[-1]
    one_minute = datetime.timedelta(seconds=60)
    assert abs(first_row.timestamp - one_hour_ago) < one_minute
    assert abs(last_row.timestamp - now) < (2 * one_minute)
    assert len(first_row.phases) == 3


def test_subscription(shelly: Shelly):
    event: Optional[NotifyStatusEvent] = None

    def callback(data: NotifyStatusEvent):
        nonlocal event
        if not event:
            event = data

    start = datetime.datetime.now(tz=UTC)
    subscription = shelly.subscribe(callback)
    while not event:
        wait_time = datetime.datetime.now(tz=UTC) - start
        assert wait_time.total_seconds() < 10, f"No event received after {wait_time}"
        time.sleep(0.5)
    subscription.stop()
    delta = datetime.timedelta(seconds=5)
    assert event.timestamp > (start - delta)
    assert event.timestamp < (datetime.datetime.now(tz=UTC) + delta)
