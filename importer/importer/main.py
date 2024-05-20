import csv
import datetime
import logging
from pathlib import Path
from typing import Any, Generator, Iterable, NamedTuple

from importer.config import config
from importer.db import DbClient
from importer.model import ALL_FIELD_NAMES, CsvRow, RawCsvRow
from importer.shelly import Shelly

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("main")


def main():
    import_csv()


def import_csv():
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


if __name__ == "__main__":
    main()
