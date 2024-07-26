from pathlib import Path
from unittest.mock import Mock, patch
import polars as pl
from analyzepolar.main import DeviceData, read_csv_dir, read_csvs, read_data


@patch("polars.scan_csv")
def test_read_csvs_merge(scan_csv_mock: Mock):
    scan_csv_mock.side_effect = [
        pl.LazyFrame({"timestamp": [0, 60, 120], "value": [1, 2, 3]}),
        pl.LazyFrame({"timestamp": [120, 180], "value": [4, 5]}),
    ]
    df = read_csvs([Path("file1"), Path("file2")], "device1").collect()
    assert df.columns == ["timestamp", "value", "device", "file"]
    assert df.dtypes == [pl.Datetime(time_zone="UTC"), pl.Int64, pl.String, pl.String]
    assert len(df) == 4
    assert df.select("file").rows() == [("file1",), ("file1",), ("file1",), ("file2",)]
    assert df.select("device").rows() == [("device1",), ("device1",), ("device1",), ("device1",)]


@patch("polars.scan_csv")
@patch("glob.glob")
def test_read_csv_dir(glob_mock: Mock, scan_csv_mock: Mock):
    scan_csv_mock.side_effect = [
        pl.LazyFrame({"timestamp": [0, 60, 120], "value": [1, 2, 3]}),
        pl.LazyFrame({"timestamp": [120, 180], "value": [4, 5]}),
    ]
    glob_mock.return_value = ["file1", "file2"]
    df = read_csv_dir(DeviceData(Path("dir"), "device1")).collect()
    assert df.columns == ["timestamp", "value", "device", "file"]
    assert df.dtypes == [pl.Datetime(time_zone="UTC"), pl.Int64, pl.String, pl.String]
    assert len(df) == 4
    assert df.select("file").rows() == [("file1",), ("file1",), ("file1",), ("file2",)]
    assert df.select("device").rows() == [("device1",), ("device1",), ("device1",), ("device1",)]


@patch("polars.scan_csv")
@patch("glob.glob")
def test_read_data(glob_mock: Mock, scan_csv_mock: Mock):
    scan_csv_mock.side_effect = [
        pl.LazyFrame({"timestamp": [0, 60, 120], "value": [1, 2, 3]}),
        pl.LazyFrame({"timestamp": [120, 180], "value": [4, 5]}),
    ]
    glob_mock.side_effect = [["dev1/file"], ["dev2/file"]]
    df = read_data([DeviceData(Path("dir1"), "device1"), DeviceData(Path("dir2"), "device2")]).collect()
    assert df.columns == ["timestamp", "value", "device", "file"]
    assert df.dtypes == [pl.Datetime(time_zone="UTC"), pl.Int64, pl.String, pl.String]
    assert len(df) == 5
    assert df.select(["file", "device", "value"]).rows() == [
        ("dev1/file", "device1", 1),
        ("dev1/file", "device1", 2),
        ("dev1/file", "device1", 3),
        ("dev2/file", "device2", 4),
        ("dev2/file", "device2", 5),
    ]
