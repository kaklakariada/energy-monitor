from datetime import tzinfo
from pathlib import Path
from typing import NamedTuple


class InfluxDBConfig(NamedTuple):
    url: str
    token: str
    org: str
    bucket: str


class DeviceConfig(NamedTuple):
    name: str
    ip: str


class Config(NamedTuple):
    devices: list[DeviceConfig]
    data_dir: Path
    influxdb: InfluxDBConfig
    timezone: tzinfo
