import datetime
import glob
from functools import reduce
from pathlib import Path
from typing import Any, Generator, NamedTuple, Optional

import polars as pl

from analyzepolar.logger import POLAR_ANALYZER_LOGGER

_logger = POLAR_ANALYZER_LOGGER.getChild("loader")


class DeviceDataSource(NamedTuple):
    data_dir: Path
    device: str


class DataGap(NamedTuple):
    device: str
    start: datetime.datetime
    end: datetime.datetime

    @property
    def duration(self) -> datetime.timedelta:
        return self.end - self.start


class SingleDeviceData(NamedTuple):
    device: str
    files: list[Path]
    df: pl.DataFrame

    def find_gaps(self) -> Generator[DataGap, Any, Any]:
        prev: Optional[datetime.datetime] = None
        for row in self.df.iter_rows(named=True):
            if prev is not None:
                diff = row["timestamp"] - prev
                if diff.total_seconds() > 60:
                    gap = DataGap(self.device, prev, row["timestamp"])
                    _logger.debug(
                        f"Found gap for device '{gap.device}' of {gap.duration} between {gap.start} and {gap.end}."
                    )
                    yield gap
            prev = row["timestamp"]


class MultiDeviceData(NamedTuple):
    devices: list[SingleDeviceData]
    df: pl.LazyFrame


def read_data(devices: list[DeviceDataSource]) -> MultiDeviceData:
    if len(devices) == 0:
        raise ValueError("No devices given")

    def merge(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        return a.vstack(other=b, in_place=False)

    _logger.debug(f"Merging data for {len(devices)} devices...")
    all_data = [read_csv_dir(data) for data in devices]
    df = reduce(merge, (data.df for data in all_data))
    _logger.info(f"Found {len(df)} rows for {len(devices)} devices...")
    return MultiDeviceData(devices=all_data, df=df.lazy())


def read_csv_dir(data: DeviceDataSource) -> SingleDeviceData:
    csv_files = [Path(file) for file in glob.glob(glob.escape(str(data.data_dir)) + "/*.csv")]
    if not csv_files:
        raise ValueError(f"Data dir {data.data_dir.absolute()} does not contain CSV files")
    return read_csv_files(csv_files, data.device)


def read_csv_files(files: list[Path], device: str) -> SingleDeviceData:
    if not files:
        raise ValueError("No input files")
    _logger.info(f"Reading {len(files)} files for device {device}...")

    def merge(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        df = a.vstack(other=b, in_place=False)
        df = df.unique(subset="timestamp", keep="first", maintain_order=False)
        return df

    _logger.debug(f"Merging data frames for {len(files)} files...")
    df = reduce(merge, (load_csv(file, device).collect() for file in sorted(files)))
    _logger.debug(f"Found {len(df)} unique rows in {len(files)} files")
    df = df.sort(by="timestamp", descending=False)
    return SingleDeviceData(device=device, files=files, df=df)


def load_csv(file: Path, device: str) -> pl.LazyFrame:
    _logger.debug(f"Reading CSV {file} for device {device}")
    df = pl.scan_csv(source=file, has_header=True, infer_schema=True, raise_if_empty=True, include_file_paths=None)
    df = df.with_columns(
        pl.lit(device).alias("device"),
        pl.lit(str(file)).alias("file"),
        pl.from_epoch(column=pl.col("timestamp"), time_unit="s").alias("timestamp"),
    )
    df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
    return df
