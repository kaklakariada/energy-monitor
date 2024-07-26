from typing import Any

import numpy as np
import pandas as pd
import pytest

from analyzer.model import DeviceData, MultiDeviceData, Phase

ALL_CSV_COLUMNS = [
    "timestamp",
    "a_total_act_energy",
    "a_fund_act_energy",
    "a_total_act_ret_energy",
    "a_fund_act_ret_energy",
    "a_lag_react_energy",
    "a_lead_react_energy",
    "a_max_act_power",
    "a_min_act_power",
    "a_max_aprt_power",
    "a_min_aprt_power",
    "a_max_voltage",
    "a_min_voltage",
    "a_avg_voltage",
    "a_max_current",
    "a_min_current",
    "a_avg_current",
    "b_total_act_energy",
    "b_fund_act_energy",
    "b_total_act_ret_energy",
    "b_fund_act_ret_energy",
    "b_lag_react_energy",
    "b_lead_react_energy",
    "b_max_act_power",
    "b_min_act_power",
    "b_max_aprt_power",
    "b_min_aprt_power",
    "b_max_voltage",
    "b_min_voltage",
    "b_avg_voltage",
    "b_max_current",
    "b_min_current",
    "b_avg_current",
    "c_total_act_energy",
    "c_fund_act_energy",
    "c_total_act_ret_energy",
    "c_fund_act_ret_energy",
    "c_lag_react_energy",
    "c_lead_react_energy",
    "c_max_act_power",
    "c_min_act_power",
    "c_max_aprt_power",
    "c_min_aprt_power",
    "c_max_voltage",
    "c_min_voltage",
    "c_avg_voltage",
    "c_max_current",
    "c_min_current",
    "c_avg_current",
    "n_max_current",
    "n_min_current",
    "n_avg_current",
]

PHASE_DATA_COLUMNS = [
    "timestamp",
    "total_act_energy",
    "fund_act_energy",
    "total_act_ret_energy",
    "fund_act_ret_energy",
    "lag_react_energy",
    "lead_react_energy",
    "max_act_power",
    "min_act_power",
    "max_aprt_power",
    "min_aprt_power",
    "max_voltage",
    "min_voltage",
    "avg_voltage",
    "max_current",
    "min_current",
    "avg_current",
]


@pytest.fixture(name="empty_device_data")
def empty_device_data_fixture() -> DeviceData:
    return DeviceData.load_df("device", pd.DataFrame(columns=ALL_CSV_COLUMNS))


@pytest.fixture(name="filled_device_data")
def filled_device_data_fixture() -> DeviceData:
    return _create_device_data("device", 5)


def _create_device_data(device: str, rows: int) -> DeviceData:
    df = pd.DataFrame(columns=ALL_CSV_COLUMNS, data=_generate_csv_data(rows))
    return DeviceData.load_df(device, df)


def _generate_csv_data(rows: int) -> dict[str, Any]:
    return {col: _generate_column_data(col, rows) for col in ALL_CSV_COLUMNS}


def _generate_column_data(col: str, rows: int) -> list[Any]:
    if col == "timestamp":
        return [i * 60 for i in range(rows)]
    return [i + (i / 10) + ALL_CSV_COLUMNS.index(col) for i in range(rows)]


def test_validate_success() -> None:
    data = DeviceData.load_df("device", pd.DataFrame({"timestamp": [1, 2, 3]}))
    assert data is not None
    assert data.device == "device"
    assert data.df["timestamp"][0] == pd.Timestamp("1970-01-01 00:00:01", tz="UTC")
    assert data.df["timestamp"][1] == pd.Timestamp("1970-01-01 00:00:02", tz="UTC")


def test_validate_not_unique() -> None:
    with pytest.raises(AssertionError, match="Timestamps for device 'device' are not unique."):
        DeviceData.load_df("device", pd.DataFrame({"timestamp": [1, 2, 2]}))


def test_validate_not_sorted() -> None:
    with pytest.raises(AssertionError, match="Timestamps for device 'device' are not monotonically increasing."):
        DeviceData.load_df("device", pd.DataFrame({"timestamp": [1, 3, 2]}))


def test_data_no_gaps() -> None:
    data = DeviceData.load_df("device", pd.DataFrame({"timestamp": [0, 60, 90, 150]}))
    assert len(list(data.find_gaps())) == 0


def test_data_gaps() -> None:
    data = DeviceData.load_df("device", pd.DataFrame({"timestamp": [60, 121, 181]}))
    gaps = list(data.find_gaps())
    assert len(gaps) == 1
    assert gaps[0].start == pd.Timestamp("1970-01-01 00:01:00", tz="UTC")
    assert gaps[0].end == pd.Timestamp("1970-01-01 00:02:01", tz="UTC")
    assert gaps[0].duration == pd.Timedelta("0 days 00:01:01")


@pytest.mark.parametrize("phase", Phase.__members__.values())
def test_get_phase_empty_df(phase: Phase, empty_device_data: DeviceData) -> None:
    data = empty_device_data.get_phase_data(phase)
    assert data.device == "device"
    assert data.phase == phase
    assert len(data.df) == 0
    assert len(data.df.columns) == len(PHASE_DATA_COLUMNS)
    assert data.df.columns.tolist() == PHASE_DATA_COLUMNS
    assert data.df["timestamp"].dtype == "datetime64[ns, UTC]"


@pytest.mark.parametrize("phase", Phase.__members__.values())
def test_get_phase_filled_df(phase: str, filled_device_data: DeviceData) -> None:
    data = filled_device_data.get_phase_data(phase)
    assert data.device == "device"
    assert data.phase == phase
    assert len(data.df) == 5
    assert len(data.df.columns) == len(PHASE_DATA_COLUMNS)
    assert data.df.columns.tolist() == PHASE_DATA_COLUMNS
    assert data.df["timestamp"].dtype == "datetime64[ns, UTC]"


def test_get_total_active_energy(filled_device_data: DeviceData) -> None:
    df = filled_device_data.get_phase_data(Phase.A).total_active_energy
    assert len(df) == 5
    assert df.columns.tolist() == ["timestamp", "total_act_energy"]


def test_device_get_total_active_energy(filled_device_data: DeviceData) -> None:
    df = filled_device_data.get_total_active_energy()
    assert len(df) == 5
    assert df.columns.tolist() == ["a_total_act_energy", "b_total_act_energy", "c_total_act_energy"]
    assert df.index.name == "timestamp"
    assert df.index.dtype == "datetime64[ns, UTC]"


def test_multi_device_gaps() -> None:
    data1 = DeviceData.load_df("device1", pd.DataFrame({"timestamp": [60, 120, 180]}))
    data2 = DeviceData.load_df("device2", pd.DataFrame({"timestamp": [60, 121, 181]}))
    data = MultiDeviceData.create([data1, data2])
    gaps = list(data.find_gaps())
    assert len(gaps) == 1
    assert gaps[0].start == pd.Timestamp("1970-01-01 00:01:00", tz="UTC")
    assert gaps[0].end == pd.Timestamp("1970-01-01 00:02:01", tz="UTC")
    assert gaps[0].duration == pd.Timedelta("0 days 00:01:01")


def test_multi_device_total_active_energy() -> None:
    data = MultiDeviceData.create([_create_device_data("device1", 5), _create_device_data("device2", 5)])
    df = data.get_total_active_energy()
    assert len(df) == 5
    assert df.columns.tolist() == [
        "device1_a",
        "device1_b",
        "device1_c",
        "device2_a",
        "device2_b",
        "device2_c",
        "total",
    ]
    assert df.index.name == "timestamp"
    assert df.index.dtype == "datetime64[ns, UTC]"
    assert df.iloc[0]["total"].sum() == np.float64(102.0)
