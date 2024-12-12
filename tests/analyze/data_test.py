import datetime
import logging
from pathlib import Path
from tokenize import Single

import pytest
from analyze.loader import SingleDeviceData, SingleFileData
import polars as pl
from analyze.data import _logger

_logger.setLevel(logging.DEBUG)

TS1 = datetime.datetime(year=2024, month=10, day=1)
TS2 = datetime.datetime(year=2024, month=10, day=2)
TS3 = datetime.datetime(year=2024, month=10, day=3)
TS4 = datetime.datetime(year=2024, month=10, day=4)
TS5 = datetime.datetime(year=2024, month=10, day=5)
TS6 = datetime.datetime(year=2024, month=10, day=6)


def single_device_data(files: list[SingleFileData]) -> SingleDeviceData:
    return SingleDeviceData(device="dev", df=pl.DataFrame(), file_data=files)


def data(
    file_name: str, first_timestamp: datetime.datetime, last_timestamp: datetime.datetime, row_count: int = 0
) -> SingleFileData:
    df = pl.DataFrame(data={"a": range(row_count)})
    return SingleFileData(
        device="dev",
        file=Path("data") / file_name,
        df=df,
        first_timestamp=first_timestamp,
        last_timestamp=last_timestamp,
    )


def test_find_duplicate_files_empty_input():
    with pytest.raises(ValueError, match="No files"):
        single_device_data([]).find_duplicate_files()


def test_find_duplicate_files_single_input():
    assert_no_duplicates([data("file1", TS1, TS2)])


def test_find_duplicate_files_two_overlapping():
    assert_no_duplicates([data("file1", TS1, TS3), data("file2", TS2, TS4)])


def test_find_duplicate_files_two_overlapping_reverse():
    assert_no_duplicates([data("file2", TS2, TS4), data("file1", TS1, TS3)])


def test_find_duplicate_files_two_non_overlapping():
    assert_no_duplicates([data("file1", TS1, TS2), data("file2", TS3, TS4)])


def test_find_duplicate_files_two_non_overlapping_reverse():
    assert_no_duplicates([data("file2", TS3, TS4), data("file1", TS1, TS2)])


def test_find_duplicate_files_two_included():
    d1 = data("file1", TS1, TS4)
    d2 = data("file2", TS2, TS3)
    assert_duplicates([d1, d2], [d2])


def test_find_duplicate_files_two_included_same_first():
    d1 = data("file1", TS1, TS4)
    d2 = data("file2", TS1, TS3)
    assert_duplicates([d1, d2], [d2])


def test_find_duplicate_files_two_included_same_last():
    d1 = data("file1", TS1, TS4)
    d2 = data("file2", TS2, TS4)
    assert_duplicates([d1, d2], [d2])


def test_find_duplicate_files_two_included_reverse():
    d1 = data("file1", TS1, TS4)
    d2 = data("file2", TS2, TS3)
    assert_duplicates([d2, d1], [d2])


def test_find_duplicate_files_same_timestamps_prefers_file_order():
    d1 = data("file1", TS1, TS2)
    d2 = data("file2", TS1, TS2)
    assert_duplicates([d1, d2], [d2])


def test_find_duplicate_files_same_timestamps_prefers_file_order_reverse():
    d1 = data("file1", TS1, TS2)
    d2 = data("file2", TS1, TS2)
    assert_duplicates([d2, d1], [d2])


def test_find_duplicate_files_same_timestamps_prefers_longer_file():
    d1 = data("file2", TS1, TS2, row_count=10)
    d2 = data("file1", TS1, TS2, row_count=5)
    assert_duplicates([d1, d2], [d2])


def test_find_duplicate_files_same_timestamps_prefers_longer_file_reverse():
    d1 = data("file2", TS1, TS2, row_count=10)
    d2 = data("file1", TS1, TS2, row_count=5)
    assert_duplicates([d2, d1], [d2])


@pytest.mark.skip(reason="not implemented yet")
def test_find_duplicate_files_three_overlapping():
    d1 = data("file1", TS1, TS3)
    d2 = data("file2", TS2, TS5)
    d3 = data("file3", TS4, TS6)
    assert_duplicates([d1, d2, d3], [d2])


@pytest.mark.skip(reason="not implemented yet")
def test_find_duplicate_files_three_overlapping_reverse():
    d1 = data("file1", TS1, TS3)
    d2 = data("file2", TS2, TS5)
    d3 = data("file3", TS4, TS6)
    assert_duplicates([d3, d2, d1], [d2])


@pytest.mark.skip(reason="not implemented yet")
def test_find_duplicate_files_three_overlapping_short():
    d1 = data("file1", TS1, TS4)
    d2 = data("file2", TS3, TS6)
    d3 = data("file3", TS2, TS5)
    assert_duplicates([d1, d2, d3], [d3])


def assert_no_duplicates(files: list[SingleFileData]):
    actual = single_device_data(files).find_duplicate_files()
    assert actual == []


def assert_duplicates(files: list[SingleFileData], expected_duplicates: list[SingleFileData]):
    actual = single_device_data(files).find_duplicate_files()
    assert actual == expected_duplicates
