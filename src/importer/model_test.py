import csv
import datetime
import math

from config import config
from importer.model import CsvRow, NotifyStatusEvent, Phase, RawCsvRow


def test_parse_event():
    data = {
        "src": "shelly-12345",
        "dst": "client-693035",
        "method": "NotifyStatus",
        "params": {
            "ts": 1716560784.74,
            "em:0": {
                "id": 0,
                "a_act_power": 10.5,
                "a_aprt_power": 19.4,
                "a_current": 0.083,
                "a_freq": 50.0,
                "a_pf": 0.54,
                "a_voltage": 234.0,
                "b_act_power": 7.1,
                "b_aprt_power": 29.6,
                "b_current": 0.126,
                "b_freq": 50.0,
                "b_pf": 0.24,
                "b_voltage": 234.4,
                "c_act_power": 3.0,
                "c_aprt_power": 9.6,
                "c_current": 0.041,
                "c_freq": 50.0,
                "c_pf": 0.3,
                "c_voltage": 234.5,
                "n_current": None,
                "total_act_power": 20.641,
                "total_aprt_power": 58.593,
                "total_current": 0.25,
            },
        },
    }
    event = NotifyStatusEvent.from_dict(data)
    assert str(event.timestamp) == "2024-05-24 16:26:24.740000+02:00"
    assert event.src == "shelly-12345"
    assert event.status.id == 0
    assert event.status.phases[0].phase_name == "a"
    assert event.status.phases[1].phase_name == "b"
    assert event.status.phases[2].phase_name == "c"
    assert math.isclose(event.status.phases[0].act_power, 10.5)
    assert event.status.n_current is None
    assert math.isclose(event.status.total_act_power, 20.641)


def test_parse_csv_row():
    content = [
        "timestamp,a_total_act_energy,a_fund_act_energy,a_total_act_ret_energy,a_fund_act_ret_energy,a_lag_react_energy,a_lead_react_energy,a_max_act_power,a_min_act_power,a_max_aprt_power,a_min_aprt_power,a_max_voltage,a_min_voltage,a_avg_voltage,a_max_current,a_min_current,a_avg_current,b_total_act_energy,b_fund_act_energy,b_total_act_ret_energy,b_fund_act_ret_energy,b_lag_react_energy,b_lead_react_energy,b_max_act_power,b_min_act_power,b_max_aprt_power,b_min_aprt_power,b_max_voltage,b_min_voltage,b_avg_voltage,b_max_current,b_min_current,b_avg_current,c_total_act_energy,c_fund_act_energy,c_total_act_ret_energy,c_fund_act_ret_energy,c_lag_react_energy,c_lead_react_energy,c_max_act_power,c_min_act_power,c_max_aprt_power,c_min_aprt_power,c_max_voltage,c_min_voltage,c_avg_voltage,c_max_current,c_min_current,c_avg_current,n_max_current,n_min_current,n_avg_current",
        "1649906400,0.0069,0.1552,0.0000,0.0000,0.0386,0.0006,10.7,9.8,30.7,18.0,236.062,235.114,235.552,0.130,0.077,0.079,0.0052,0.0231,0.0000,0.0000,0.0000,0.1032,1.8,0.9,27.3,9.6,236.294,235.410,235.862,0.115,0.040,0.043,0.0063,0.0646,0.0000,0.0000,0.0000,0.0023,5.4,3.3,26.8,9.9,236.479,235.559,235.971,0.112,0.042,0.048,0.000,0.000,0.000",
    ]

    reader = csv.DictReader(content)
    row = CsvRow.from_dict(next(reader))
    assert str(row.timestamp) == "2022-04-14 03:20:00+00:00"
    assert len(row.phases) == 3
    assert row.phases[0].phase_name == Phase.A
    assert row.phases[1].phase_name == Phase.B
    assert row.phases[2].phase_name == Phase.C
    assert math.isclose(row.phases[0].avg_current, 0.079)
    assert math.isclose(row.n_avg_current, 0.0)
