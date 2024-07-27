from enum import Enum


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

PHASE_COLUMNS = [
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

PHASE_DATA_COLUMNS = ["timestamp"] + PHASE_COLUMNS


class Phase(Enum):
    A = "a"
    B = "b"
    C = "c"
