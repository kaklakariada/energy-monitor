import datetime
import glob
from functools import reduce
from pathlib import Path

import polars as pl

from analyze.data import (
    DeviceDataSource,
    MultiDeviceData,
    SingleDeviceData,
    SingleFileData,
)
from analyze.logger import POLAR_ANALYZER_LOGGER
from util import format_local_timestamp

_logger = POLAR_ANALYZER_LOGGER.getChild("loader")


def read_data(devices: list[DeviceDataSource]) -> MultiDeviceData:
    if len(devices) == 0:
        raise ValueError("No devices given")

    def merge(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        return a.vstack(other=b, in_place=False)

    _logger.debug(f"Merging data for {len(devices)} devices...")
    all_data = [read_csv_dir(data) for data in devices]
    df = reduce(merge, (data.df for data in all_data))
    _logger.info(f"Found {len(df)} rows for {len(devices)} devices.")
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
    file_data = [load_csv(file, device) for file in sorted(files)]
    df = reduce(merge, (data.df for data in file_data))
    _logger.debug(f"Found {len(df)} unique rows in {len(files)} files")
    df = df.sort(by="timestamp", descending=False)
    return SingleDeviceData(device=device, df=df, file_data=file_data)


def load_csv(file: Path, device: str) -> SingleFileData:
    lazy_df = pl.scan_csv(source=file, has_header=True, infer_schema=True, raise_if_empty=True, include_file_paths=None)
    lazy_df = lazy_df.with_columns(
        pl.lit(device).alias("device"),
        pl.lit(str(file)).alias("file"),
        pl.from_epoch(column=pl.col("timestamp"), time_unit="s").alias("timestamp"),
    )
    lazy_df = lazy_df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
    try:
        df = lazy_df.collect()
    except pl.exceptions.PolarsError as e:
        raise ValueError(f"Error loading data for device {device} from {file}") from e
    first_timestamp = df["timestamp"].min()
    last_timestamp = df["timestamp"].max()
    if not isinstance(first_timestamp, datetime.datetime) or not isinstance(last_timestamp, datetime.datetime):
        raise ValueError(f"Timestamps are not datetime objects {first_timestamp!r}, {last_timestamp!r}")

    _logger.debug(
        f"Read {file.name} with {len(df)} rows from {format_local_timestamp(first_timestamp)} "
        f"to {format_local_timestamp(last_timestamp)} ({last_timestamp - first_timestamp})"
    )
    return SingleFileData(
        device=device, file=file, df=df, first_timestamp=first_timestamp, last_timestamp=last_timestamp
    )
