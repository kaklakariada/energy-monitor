import csv
import datetime
import logging
from typing import Any, Generator, NamedTuple
from importer.shelly import Shelly
from importer.model import ALL_FIELD_NAMES, CsvRow
from importer.config import shelly_devices

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    test_shelly()
    # read_csv()


def test_shelly():
    shelly = shelly_devices[1]
    shelly = Shelly(shelly["ip"])
    print(shelly.get_system_status())


def read_csv() -> Generator[CsvRow, None, None]:
    with open("file.csv", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        print(f"Columns: {reader.fieldnames}")
        assert set(reader.fieldnames) == ALL_FIELD_NAMES
        rows = (CsvRow.from_dict(row) for row in reader)
        return rows


if __name__ == "__main__":
    main()
