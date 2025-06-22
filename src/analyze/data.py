import datetime
from pathlib import Path
from typing import Any, Generator, NamedTuple, Optional

import polars as pl

from analyze.logger import POLAR_ANALYZER_LOGGER
from util import format_local_timestamp

_logger = POLAR_ANALYZER_LOGGER.getChild("data")


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


class SingleFileData(NamedTuple):
    device: str
    file: Path
    df: pl.DataFrame
    first_timestamp: datetime.datetime
    last_timestamp: datetime.datetime

    def __str__(self):
        return f"Device '{self.device}' / {self.file}: {self.first_timestamp} .. {self.last_timestamp} ({len(self.df)} rows)"

    def __repr__(self):
        return str(self)


class SingleDeviceData(NamedTuple):
    device: str
    df: pl.DataFrame
    file_data: list[SingleFileData]

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

    def find_duplicate_files(self) -> list[SingleFileData]:
        if len(self.file_data) == 0:
            raise ValueError("No files")

        def by_timestamps(f: SingleFileData):
            return (
                f.first_timestamp,
                -len(f.df),
                f.file,
            )

        sorted_files = sorted(
            self.file_data,
            key=by_timestamps,
        )
        _logger.debug(f"Find duplicates in {len(sorted_files)} files")
        for idx, file in enumerate(sorted_files):
            _logger.debug(f"- {idx} {file}")

        duplicates: list[SingleFileData] = []
        required: list[SingleFileData] = [sorted_files.pop(0)]
        for file in sorted_files:
            if (
                file.first_timestamp <= required[-1].last_timestamp
                and file.last_timestamp <= required[-1].last_timestamp
            ):
                duplicates.append(file)
        _logger.debug(f"Found {len(duplicates)} duplicate files")
        return duplicates


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
