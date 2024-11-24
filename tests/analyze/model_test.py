import glob
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest

from analyze.common import ALL_CSV_COLUMNS, PHASE_COLUMNS
from analyze.loader import DeviceDataSource
from analyze.model import PolarDeviceData


def test_load_empty():
    with pytest.raises(ValueError, match="No devices given"):
        PolarDeviceData.load([])


def test_load_single_device():
    data = _load(["device1"], 3)
    assert data.df.select(["file", "device"]).rows() == [
        ("data/device1.csv", "device1"),
        ("data/device1.csv", "device1"),
        ("data/device1.csv", "device1"),
    ]


def test_load():
    data = _load(["dev1", "dev2"], 2)
    assert data.df.select(["file", "device"]).rows() == [
        ("data/dev1.csv", "dev1"),
        ("data/dev1.csv", "dev1"),
        ("data/dev2.csv", "dev2"),
        ("data/dev2.csv", "dev2"),
    ]


def test_find_gaps():
    data = _load(["dev1", "dev2"], 2)
    assert len(list(data.gaps)) == 0


def test_load_phase_data_column_unsupported_column():
    data = _load(["dev1", "dev2"], 2)
    with pytest.raises(ValueError, match="Unsupported column 'unsupported'. Use one of"):
        data.phase_data_column("unsupported").collect()


@pytest.mark.parametrize(argnames="column", argvalues=PHASE_COLUMNS)
def test_load_phase_data_column(column: str):
    data = _load(["dev1", "dev2"], 2)
    df = data.phase_data_column(column).collect()
    assert df.columns == ["timestamp", "device", "file", "phase", column]
    assert len(df) == 2 * 2 * 3


def test_load_phase_data():
    data = _load(["dev1", "dev2"], 2)
    df = data.phase_data.collect()
    assert df.columns == ["timestamp", "device", "file", "phase"] + PHASE_COLUMNS
    assert len(df) == 2 * 2 * 3


def _load(devices: list[str], rows: int) -> PolarDeviceData:
    with patch.object(target=pl, attribute="scan_csv") as scan_csv_mock:
        with patch.object(target=glob, attribute="glob") as glob_mock:
            glob_mock.side_effect = [[f"data/{device}.csv"] for device in devices]
            scan_csv_mock.side_effect = [_generate_csv_data_device(device, rows) for device in devices]
            device_data = [DeviceDataSource(Path(f"data/{device}"), device) for device in devices]
            return PolarDeviceData.load(device_data)


def _generate_csv_data_device(device: str, rows: int) -> pl.LazyFrame:
    data = {col: _generate_column_data(col, device, rows) for col in ALL_CSV_COLUMNS}
    return pl.LazyFrame(data=data)


def _generate_column_data(col: str, device: str, rows: int) -> list[Any]:
    if col == "timestamp":
        return [i * 60 for i in range(rows)]
    return [i + (1 / id(device)) + ALL_CSV_COLUMNS.index(col) for i in range(rows)]
