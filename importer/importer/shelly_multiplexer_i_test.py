import datetime
import time
from typing import Optional

import pytest

from config import config
from importer.model import NotifyStatusEvent
from importer.shelly import Shelly
from importer.shelly_i_test import EVENT_TIMEOUT
from importer.shelly_multiplexer import ShellyMultiplexer

pytestmark = pytest.mark.shelly

UTC = datetime.timezone.utc
DEVICES = config.devices


@pytest.fixture
def shellies():
    return ShellyMultiplexer(DEVICES)


def test_get_status(shellies: ShellyMultiplexer):
    data = shellies.get_status()
    assert len(data) == len(DEVICES)
    for device in DEVICES:
        assert data[device.name].emdata.id == 0


def test_subscription(shellies: ShellyMultiplexer):
    event: Optional[NotifyStatusEvent] = None
    device: Optional[Shelly] = None

    def callback(_device: Shelly, _event: NotifyStatusEvent):
        nonlocal event
        nonlocal device
        if not event:
            event = _event
        if not device:
            device = _device

    start = datetime.datetime.now(tz=UTC)
    with shellies.subscribe(callback):
        while not event:
            wait_time = datetime.datetime.now(tz=UTC) - start
            assert wait_time < EVENT_TIMEOUT, f"No event received after {wait_time}"
            time.sleep(0.5)
    delta = datetime.timedelta(seconds=30)
    assert event.timestamp > (start - delta)
    assert event.timestamp < (datetime.datetime.now(tz=UTC) + delta)
    assert device is not None
