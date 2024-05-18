import datetime
from typing import Any, NamedTuple


MEASUREMENT_NAMES = {
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
}

ALL_FIELD_NAMES = {"timestamp"} | MEASUREMENT_NAMES


class CsvRow(NamedTuple):
    timestamp: datetime.datetime
    """blubb"""
    a_total_act_energy: float
    a_fund_act_energy: float
    a_total_act_ret_energy: float
    a_fund_act_ret_energy: float
    a_lag_react_energy: float
    a_lead_react_energy: float
    a_max_act_power: float
    a_min_act_power: float
    a_max_aprt_power: float
    a_min_aprt_power: float
    a_max_voltage: float
    a_min_voltage: float
    a_avg_voltage: float
    a_max_current: float
    a_min_current: float
    a_avg_current: float
    b_total_act_energy: float
    b_fund_act_energy: float
    b_total_act_ret_energy: float
    b_fund_act_ret_energy: float
    b_lag_react_energy: float
    b_lead_react_energy: float
    b_max_act_power: float
    b_min_act_power: float
    b_max_aprt_power: float
    b_min_aprt_power: float
    b_max_voltage: float
    b_min_voltage: float
    b_avg_voltage: float
    b_max_current: float
    b_min_current: float
    b_avg_current: float
    c_total_act_energy: float
    c_fund_act_energy: float
    c_total_act_ret_energy: float
    c_fund_act_ret_energy: float
    c_lag_react_energy: float
    c_lead_react_energy: float
    c_max_act_power: float
    c_min_act_power: float
    c_max_aprt_power: float
    c_min_aprt_power: float
    c_max_voltage: float
    c_min_voltage: float
    c_avg_voltage: float
    c_max_current: float
    c_min_current: float
    c_avg_current: float
    n_max_current: float
    n_min_current: float
    n_avg_current: float

    @classmethod
    def from_dict(cls, row: dict[str | Any, str | Any]) -> "CsvRow":
        def value(key: str) -> float:
            return float(row[key])

        values: dict[str, Any] = {key: value(key) for key in MEASUREMENT_NAMES}
        timestamp = datetime.datetime.fromtimestamp(int(row["timestamp"]))
        return cls(timestamp=timestamp, **values)
