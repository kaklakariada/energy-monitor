from datetime import timezone
from pathlib import Path

import pytz

from importer.config_model import Config, DeviceConfig, InfluxDBConfig

config = Config(
    devices=[DeviceConfig(name="unten", ip="192.168.179.102"), DeviceConfig(name="oben", ip="192.168.179.103")],
    data_dir=Path("/home/chris/git/energy-monitor/data"),
    timezone=pytz.timezone("Europe/Berlin"),
    influxdb=InfluxDBConfig(
        url="http://localhost:8086",
        bucket="shelly11",
        org="k40",
        token="DT1Xc46miIsfFdAR6gLC7BWDCVXhvfjHJH6_ckYimTFir46pU_yE7SCH0kUFlatNo4oWaGJx6Ij_wpVtrihUEg==",
    ),
)
