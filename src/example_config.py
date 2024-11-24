from pathlib import Path

import pytz

from importer.config_model import AnalyzedFiles, Config, DeviceConfig, InfluxDBConfig

config = Config(
    devices=[DeviceConfig(name="device 1", ip="192.168.178.10"), DeviceConfig(name="device 2", ip="192.168.178.11")],
    data_dir=Path("/home/user/energy-monitor/data"),
    timezone=pytz.timezone("Europe/Berlin"),
    influxdb=InfluxDBConfig(
        url="http://localhost:8086",
        bucket="<bucket_name>",
        org="<org>",
        token="<token>",
    ),
    files=[AnalyzedFiles(device="device 1", dir=Path("/data/"))],
)
