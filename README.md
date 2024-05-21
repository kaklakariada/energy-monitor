# energy-monitor
Energy monitor

## Initial Setup

First clone this repository:

```sh
git clone https://github.com/kaklakariada/energy-monitor.git
```

Then create file [`importer/importer/config.py`](./importer/importer/config.py) with the following content and adapt it to your environment:

```py
from datetime import timezone
from pathlib import Path

import pytz

from importer.config_model import Config, DeviceConfig, InfluxDBConfig

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
)
```

Install dependencies with

```sh
cd importer
poetry install
```

## Usage

### Download CSV Data

Download data from all devices in CSV format to `data_dir` configured in `config.py`:

```sh
cd importer
poetry run main download $AGE
```

Specify the data age as follows:

* `1h`: one hour
* `2d`: two days
* `3w`: three week

### Import CSV Data to InfluxDB

```sh
cd importer
poetry run main import-csv
```

This will import all CSV files from the data directory. The program will ignore entries with duplicate timestamps.

### Import Live Data to InfluxDB

```sh
cd importer
poetry run main live
```

This will subscribe to data via WebSocket and insert new data as it arrives.

## Development

### Run Type & Style Checker

```sh
poetry run nox -s check
```

### Run Formatter

```sh
poetry run nox -s fix
```
