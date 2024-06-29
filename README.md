# energy-monitor
This provides tools for downloading data from Shelly 3EM devices and analysing it.

## Initial Setup

1. Clone this repository:
    ```sh
    git clone https://github.com/kaklakariada/energy-monitor.git
    ```

2. Copy file [`importer/example-config.py`](./importer/example-config.py) to `importer/config.py` and adapt it to your environment.
3. Install dependencies with
    ```sh
    poetry install
    ```

## Usage

### Download CSV Data

Download data from all devices in CSV format to `data_dir` configured in `config.py`:

```sh
poetry run main download $AGE
```

Specify the data age as follows:

* `1h`: one hour
* `2d`: two days
* `3w`: three week

### Import CSV Data to InfluxDB

```sh
poetry run main import-csv
```

This will import all CSV files from the data directory. The program will ignore entries with duplicate timestamps.

### Import Live Data to InfluxDB

```sh
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
