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


class AnalyzedFile(NamedTuple):
    device: str
    file: Path


class Config(NamedTuple):
    devices: list[DeviceConfig]
    data_dir: Path
    influxdb: InfluxDBConfig
    timezone: tzinfo
    files: list[AnalyzedFile]
