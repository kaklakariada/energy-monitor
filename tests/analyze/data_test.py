import datetime
from pathlib import Path
from tokenize import Single

import pytest
from analyze.loader import SingleDeviceData, SingleFileData
import polars as pl

TS1 = datetime.datetime(year=2024, month=10, day=1)
TS2 = datetime.datetime(year=2024, month=10, day=2)
TS3 = datetime.datetime(year=2024, month=10, day=3)
TS4 = datetime.datetime(year=2024, month=10, day=4)
TS5 = datetime.datetime(year=2024, month=10, day=5)
TS6 = datetime.datetime(year=2024, month=10, day=6)


def single_device_data(files: list[SingleFileData]) -> SingleDeviceData:
    return SingleDeviceData(device="dev", df=pl.DataFrame(), file_data=files)


def data(file_name: str, first_timestamp: datetime.datetime, last_timestamp: datetime.datetime) -> SingleFileData:
    return SingleFileData(
        device="dev",
        file=Path("data") / file_name,
        df=pl.DataFrame(),
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


def test_find_duplicate_files_three_overlapping():
    d1 = data("file1", TS1, TS3)
    d2 = data("file2", TS2, TS5)
    d3 = data("file3", TS4, TS6)
    assert_duplicates([d1, d2, d3], [d2])


def test_find_duplicate_files_three_overlapping_reverse():
    d1 = data("file1", TS1, TS3)
    d2 = data("file2", TS2, TS5)
    d3 = data("file3", TS4, TS6)
    assert_duplicates([d3, d2, d1], [d2])


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
