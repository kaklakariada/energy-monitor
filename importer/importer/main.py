import argparse
import csv
import datetime
import logging
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import typer
from typing_extensions import Annotated

from importer.config import config
from importer.config_model import DeviceConfig
from importer.db import BatchWriter, DbClient
from importer.model import ALL_FIELD_NAMES, CsvRow, NotifyStatusEvent, RawCsvRow
from importer.shelly import Shelly

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("main")


def _configure_logging(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Verbose log output")] = False
) -> None:
    if verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Enable verbose mode")


app = typer.Typer(no_args_is_help=True, callback=_configure_logging)


@app.command()
def download(
    age: Annotated[str, typer.Argument(help="Maximum age of the data to download: ALL|1w|1d|1h|missing")]
) -> None:
    """
    Download CSV data to local files.
    """
    target_dir = config.data_dir
    now = datetime.datetime.now()
    start_timestamp = _get_start_timestamp(age, now)
    for device in config.devices:
        _download_device(device, target_dir, now, start_timestamp)


def _download_device(
    device: DeviceConfig, target_dir: Path, now: datetime.datetime, start_timestamp: Optional[datetime.datetime]
) -> None:
    shelly = Shelly(device)
    target_file = target_dir / device.name / f"{now.isoformat()}.csv"
    logger.info(f"Downloading from {shelly.device_name} to {target_file}...")
    shelly.download_csv_data(timestamp=start_timestamp, end_timestamp=None, target_file=target_file)


def _get_start_timestamp(age: str, now: datetime.datetime) -> Optional[datetime.datetime]:
    if age.lower() == "all":
        logger.debug("Downloading all data. This will take a while...")
        return None
    delta = _get_age(age)
    start_timestamp = now - delta
    logger.debug(f"Downloading data with age {delta}, starting at {start_timestamp}")
    return start_timestamp


def _get_age(delta: str) -> datetime.timedelta:
    match = re.match(r"(\d+)([wdh])", delta.lower())
    if match is None:
        raise ValueError(f"Invalid time delta format: '{delta}'")
    amount, unit = match.groups()
    if unit == "w":
        return datetime.timedelta(weeks=int(amount))
    elif unit == "d":
        return datetime.timedelta(days=int(amount))
    elif unit == "h":
        return datetime.timedelta(hours=int(amount))
    raise ValueError(f"Unsupported time unit '{unit}'")


@app.command()
def live():
    """
    Subscribe to live data and insert it into the database.
    """
    db = DbClient(
        url=config.influxdb.url,
        token=config.influxdb.token,
        org=config.influxdb.org,
        bucket=config.influxdb.bucket,
    )
    db.ensure_bucket_exists()
    writer = db.batch_writer()
    for device in config.devices:
        _subscribe_device(device, writer)
    print("Sleeping....")
    time.sleep(10000)


def _subscribe_device(device: DeviceConfig, writer: BatchWriter) -> None:

    def callback(data: NotifyStatusEvent):
        logger.debug(
            f"Received from {device.name}, act. power: {data.status.total_act_power}W, current: {data.status.total_current}A"
        )
        writer.insert_status_event(device.name, data)

    shelly = Shelly(device)
    shelly.subscribe(callback)
    logger.info(f"Subscribed to {shelly.device_name}")


@app.command()
def import_csv():
    """
    Insert local CSV data into database.
    """
    db = DbClient(
        url=config.influxdb.url,
        token=config.influxdb.token,
        org=config.influxdb.org,
        bucket=config.influxdb.bucket,
    )
    db.ensure_bucket_exists()
    for device in config.devices:
        device_dir = config.data_dir / device.name
        rows = read_csv_files(device_dir)
        db.insert_rows(device=device.name, rows=rows)


def read_csv_files(device_dir) -> Iterable[CsvRow]:
    files = sorted(device_dir.glob("*.csv"))
    unique_rows = dict()
    total_rows = 0
    for file in files:
        for row in read_csv(file):
            total_rows += 1
            unique_rows[row.timestamp] = row
    logger.info(f"Read {len(unique_rows)} unique rows (total: {total_rows}) from {len(files)} files in {device_dir}")
    return unique_rows.values()


def read_csv(file: Path) -> list[CsvRow]:
    with open(file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        assert reader.fieldnames is not None
        assert set(reader.fieldnames) == ALL_FIELD_NAMES
        rows = (CsvRow.from_raw(RawCsvRow.from_dict(row)) for row in reader)
        return list(rows)


def main():
    app()


if __name__ == "__main__":
    main()
