import datetime
import logging
from typing import NamedTuple

import polars as pl

from analyze.data import SingleDeviceData, SingleFileData
from analyze.loader import DeviceDataSource
from analyze.logger import POLAR_ANALYZER_LOGGER
from analyze.model import PolarDeviceData
from config import config

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")

_logger = POLAR_ANALYZER_LOGGER.getChild("main")


class RedundantFile(NamedTuple):
    file: SingleFileData
    sources: list[SingleFileData]


def _find_covering_files(
    file: SingleFileData,
    rows: pl.DataFrame,
    other_rows: list[tuple[SingleFileData, pl.DataFrame]],
    row_columns: list[str],
) -> list[SingleFileData] | None:
    uncovered = rows
    sources: list[SingleFileData] = []
    for other_file, rows_from_other in other_rows:
        if (
            other_file is file
            or other_file.last_timestamp < file.first_timestamp
            or other_file.first_timestamp > file.last_timestamp
        ):
            continue
        remaining = uncovered.join(rows_from_other, on=row_columns, how="anti")
        if remaining.height < uncovered.height:
            sources.append(other_file)
            uncovered = remaining
        if uncovered.is_empty():
            return sources
    return None


def find_redundant_files(device: SingleDeviceData) -> list[RedundantFile]:
    """Return removable files with the retained files that cover their rows."""
    if not device.file_data:
        return []

    ordered_files = sorted(device.file_data, key=lambda file: str(file.file))
    row_columns = [column for column in ordered_files[0].df.columns if column not in ("device", "file")]
    rows_by_file = [(file, file.df.select(row_columns).unique()) for file in ordered_files]

    candidates: list[tuple[SingleFileData, pl.DataFrame]] = []
    for file, rows in rows_by_file:
        if _find_covering_files(file, rows, rows_by_file, row_columns) is not None:
            candidates.append((file, rows))

    redundant_files: list[RedundantFile] = []
    candidates.sort(key=lambda candidate: str(candidate[0].file), reverse=True)
    candidates.sort(key=lambda candidate: len(candidate[0].df), reverse=True)
    retained_files = list(rows_by_file)
    for file, rows in candidates:
        sources = _find_covering_files(
            file,
            rows,
            [
                (retained_file, retained_rows)
                for retained_file, retained_rows in retained_files
                if retained_file is not file
            ],
            row_columns,
        )
        if sources is not None:
            redundant_files.append(RedundantFile(file, sources))
            retained_files = [
                (retained_file, retained_rows)
                for retained_file, retained_rows in retained_files
                if retained_file is not file
            ]

    return redundant_files


def _format_timestamp(timestamp: datetime.datetime) -> str:
    return timestamp.strftime("%Y-%m-%d %H:%M")


def _format_time_range(file: SingleFileData) -> str:
    duration = file.last_timestamp - file.first_timestamp
    days = duration.days
    hours, seconds = divmod(duration.seconds, 3600)
    minutes = seconds // 60
    if days:
        duration_text = f"{days} days, {hours:02}:{minutes:02}"
    else:
        total_hours = duration.seconds // 3600
        duration_text = f"{total_hours:02}:{minutes:02}"
    return (
        f"{_format_timestamp(file.first_timestamp)} .. {_format_timestamp(file.last_timestamp)} " f"({duration_text})"
    )


def report_device_data(device: SingleDeviceData) -> None:
    redundant_files = find_redundant_files(device)
    if redundant_files:
        for redundant in redundant_files:
            file = redundant.file
            print(f"Redundant CSV candidate for device '{device.device}':")
            print(f"  File: {file.file}")
            print(f"  Data range: {_format_time_range(file)}")
            print("  Covered by:")
            for source in redundant.sources:
                print(f"    - {source.file}")
                print(f"      Data range: {_format_time_range(source)}")
    else:
        print(f"No redundant CSV files found for device '{device.device}'.")

    for gap in device.find_gaps():
        print(
            f"WARNING: Device '{gap.device}' has a data gap between "
            f"{_format_timestamp(gap.start)} and {_format_timestamp(gap.end)} "
            f"({gap.duration})."
        )


def main():
    data = PolarDeviceData.load([DeviceDataSource(f.dir, f.device) for f in config.files])
    for device in data.device_data:
        report_device_data(device)
    # df = data.total_energy(every="1mo", group_by=None).collect()
    # print(df)
    # print(data.statistics.to_string())


if __name__ == "__main__":
    main()
