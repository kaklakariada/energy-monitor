from functools import reduce
import glob
import logging
from pathlib import Path
from typing import NamedTuple
import polars as pl

from analyzepolar.logger import POLAR_ANALYZER_LOGGER
from importer.logger import MAIN_LOGGER

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")
_logger = POLAR_ANALYZER_LOGGER.getChild("main")


class DeviceData(NamedTuple):
    data_dir: Path
    device: str


def main():
    df_csv = read_data([DeviceData(Path("data/unten"), "unten"), DeviceData(Path("data/oben"), "oben")])
    print(df_csv.collect())


def read_data(devices: list[DeviceData]) -> pl.LazyFrame:
    def merge(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        return a.vstack(other=b, in_place=False)

    _logger.debug(f"Merging data for {len(devices)} devices...")
    df = reduce(merge, (read_csv_dir(data).collect() for data in devices))
    _logger.debug(f"Found {len(df)} rows for {len(devices)} devices...")
    return df.lazy()


def read_csv_dir(data: DeviceData) -> pl.LazyFrame:
    csv_files = [Path(file) for file in glob.glob(glob.escape(str(data.data_dir)) + "/*.csv")]
    return read_csvs(csv_files, data.device)


def read_csvs(files: list[Path], device: str) -> pl.LazyFrame:
    _logger.info(f"Reading {len(files)} files for device {device}...")

    def merge(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        df = a.vstack(other=b, in_place=False)
        df = df.unique(subset="timestamp", keep="first", maintain_order=False)
        return df

    _logger.debug(f"Merging data frames for {len(files)} files...")
    df = reduce(merge, (load_csv(file, device).collect() for file in sorted(files)))
    _logger.debug(f"Found {len(df)} unique rows in {len(files)} files")
    return df.lazy().sort(by="timestamp", descending=False)


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


if __name__ == "__main__":
    main()
