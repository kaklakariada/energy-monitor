import csv
import datetime
import logging
import re
import threading
from pathlib import Path
from typing import Iterable, Optional

import typer
from typing_extensions import Annotated

from config import config
from importer.db.influx import DbClient
from importer.logger import MAIN_LOGGER
from importer.model import ALL_FIELD_NAMES, CsvRow, NotifyStatusEvent
from importer.shelly import Shelly
from importer.shelly_multiplexer import ShellyMultiplexer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")
logger = MAIN_LOGGER.getChild("main")


def _configure_logging(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Verbose log output")] = False
) -> None:
    if verbose:
        MAIN_LOGGER.setLevel(logging.DEBUG)
        MAIN_LOGGER.debug(f"Enable verbose mode for root logger '{logger.name}'")


app = typer.Typer(no_args_is_help=True, callback=_configure_logging)


@app.command()
def download(
    age: Annotated[str, typer.Argument(help="Maximum age of the data to download: ALL|1w|1d|1h|missing")]
) -> None:
    """
    Download CSV data to local files.
    """
    target_dir = config.data_dir
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    start_timestamp = _get_start_timestamp(age, now)
    results = ShellyMultiplexer(config.devices).download_csv_data(target_dir=target_dir, timestamp=start_timestamp)
    for result in results:
        logger.info(f"Downloaded {result.size} bytes from {result.device_name} to {result.target_file}")


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
    if unit == "d":
        return datetime.timedelta(days=int(amount))
    if unit == "h":
        return datetime.timedelta(hours=int(amount))
    raise ValueError(f"Unsupported time unit '{unit}'")


@app.command()
def live():
    """
    Subscribe to live data and insert it into the database.
    """
    with DbClient(
        url=config.influxdb.url,
        token=config.influxdb.token,
        org=config.influxdb.org,
        bucket=config.influxdb.bucket,
    ) as db:
        db.ensure_bucket_exists()
        with db.batch_writer() as writer:

            def callback(_device: Shelly, data: NotifyStatusEvent):
                logger.debug(
                    f"Received from {_device.name}, act. power: {data.status.total_act_power}W, "
                    + "current: {data.status.total_current}A"
                )
                writer.insert_status_event(_device.name, data)

            stop_event = threading.Event()
            with ShellyMultiplexer(config.devices).subscribe(callback):
                try:
                    stop_event.wait()
                except KeyboardInterrupt:
                    logger.debug("Interrupted by user")
    logger.info("Live data capturing stopped.")


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
    unique_rows = {}
    total_rows = 0
    for file in files:
        for row in read_csv(file):
            total_rows += 1
            unique_rows[row.timestamp] = row
    logger.info(f"Read {len(unique_rows)} unique rows (total: {total_rows}) from {len(files)} files in {device_dir}")
    return unique_rows.values()


def read_csv(file: Path) -> list[CsvRow]:
    with open(file, newline="", encoding="UTF-8") as csvfile:
        reader = csv.DictReader(csvfile)
        assert reader.fieldnames is not None
        assert set(reader.fieldnames) == ALL_FIELD_NAMES
        rows = (CsvRow.from_dict(row) for row in reader)
        return list(rows)


def main():
    app()


if __name__ == "__main__":
    main()
