import datetime
import glob
from functools import reduce
from pathlib import Path
from typing import Any, Generator, NamedTuple, Optional

import polars as pl

from analyze.logger import POLAR_ANALYZER_LOGGER
from util import format_local_timestamp

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

    @property
    def statistics(self) -> "SingleDeviceStatistics":
        return SingleDeviceStatistics.create(self)


class SingleDeviceStatistics(NamedTuple):
    device: str
    total_rows: int
    first_timestamp: datetime.datetime
    last_timestamp: datetime.datetime
    duration: datetime.timedelta
    gaps: list[DataGap]

    @classmethod
    def create(cls, device: SingleDeviceData) -> "SingleDeviceStatistics":
        first_timestamp = device.df["timestamp"].min()
        last_timestamp = device.df["timestamp"].max()
        if not isinstance(first_timestamp, datetime.datetime) or not isinstance(last_timestamp, datetime.datetime):
            raise ValueError(f"Timestamps are not datetime objects {first_timestamp!r}, {last_timestamp!r}")
        return cls(
            device=device.device,
            total_rows=len(device.df),
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            duration=last_timestamp - first_timestamp,
            gaps=list(device.find_gaps()),
        )

    def to_string(self) -> str:
        return (
            f"Device '{self.device}' with {self.total_rows} rows "
            f"from {format_local_timestamp(self.first_timestamp)} "
            f"to {format_local_timestamp(self.last_timestamp)} "
            f"({self.duration}) with {len(self.gaps)} gaps"
        )


class MultiDeviceStatistics(NamedTuple):
    devices: list[SingleDeviceStatistics]

    @classmethod
    def create(cls, devices: list[SingleDeviceData]) -> "MultiDeviceStatistics":
        return cls([device.statistics for device in devices])

    def to_string(self) -> str:
        return "\n".join([device.to_string() for device in self.devices])


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
    df = reduce(merge, (load_csv(file, device) for file in sorted(files)))
    _logger.debug(f"Found {len(df)} unique rows in {len(files)} files")
    df = df.sort(by="timestamp", descending=False)
    return SingleDeviceData(device=device, files=files, df=df)


def load_csv(file: Path, device: str) -> pl.DataFrame:
    df = pl.scan_csv(source=file, has_header=True, infer_schema=True, raise_if_empty=True, include_file_paths=None)
    df = df.with_columns(
        pl.lit(device).alias("device"),
        pl.lit(str(file)).alias("file"),
        pl.from_epoch(column=pl.col("timestamp"), time_unit="s").alias("timestamp"),
    )
    df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
    try:
        data = df.collect()
    except pl.exceptions.PolarsError as e:
        raise ValueError(f"Error loading data for device {device} from {file}") from e
    first_timestamp = data["timestamp"].min()
    last_timestamp = data["timestamp"].max()
    if not isinstance(first_timestamp, datetime.datetime) or not isinstance(last_timestamp, datetime.datetime):
        raise ValueError(f"Timestamps are not datetime objects {first_timestamp!r}, {last_timestamp!r}")

    _logger.debug(
        f"Read {file.name} with {len(data)} rows from {format_local_timestamp(first_timestamp)} "
        f"to {format_local_timestamp(last_timestamp)} ({last_timestamp - first_timestamp})"
    )
    return data
