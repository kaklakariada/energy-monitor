import datetime

from influxdb_client import Point

from importer.db.influx_converter import EventPointConverter, PointConverter
from importer.model import (
    CsvRow,
    EnergyMeterPhase,
    EnergyMeterStatus,
    NotifyStatusEvent,
    Phase,
    PhaseData,
)

TIMESTAMP = datetime.datetime.fromisoformat("2024-05-19T17:43:59Z")
UNIX_TIMESTAMP = 1716140639
DEVICE = "dev"


def test_convert_csv_row():
    points = _convert(
        CsvRow(
            timestamp=TIMESTAMP, phases=[_phase_data(Phase.A)], n_max_current=1.1, n_min_current=2.2, n_avg_current=3.3
        )
    )
    assert len(points) == 2
    assert (
        points[0].to_line_protocol()
        == f"em,device={DEVICE},phase=a,source=csv avg_current=1.1,avg_voltage=4.4,fund_act_energy=5.5,fund_act_ret_energy=6.6,lag_react_energy=7.7,lead_react_energy=8.8,max_act_power=9.9,max_aprt_power=13.13,max_current=2.2,max_voltage=15.15,min_act_power=12.12,min_aprt_power=14.14,min_current=3.3,min_voltage=16.16,total_act_energy=10.1,total_act_ret_energy=11.11 {UNIX_TIMESTAMP}"
    )
    assert (
        points[1].to_line_protocol()
        == f"em,device={DEVICE},phase=neutral,source=csv avg_current=3.3,max_current=1.1,min_current=2.2 {UNIX_TIMESTAMP}"
    )


def _convert(row: CsvRow | NotifyStatusEvent) -> list[Point]:
    return list(PointConverter().convert(DEVICE, row))


def _phase_data(phase_name: Phase) -> PhaseData:
    return PhaseData(
        phase_name=phase_name,
        avg_current=1.1,
        max_current=2.2,
        min_current=3.3,
        avg_voltage=4.4,
        fund_act_energy=5.5,
        fund_act_ret_energy=6.6,
        lag_react_energy=7.7,
        lead_react_energy=8.8,
        max_act_power=9.9,
        total_act_energy=10.10,
        total_act_ret_energy=11.11,
        min_act_power=12.12,
        max_aprt_power=13.13,
        min_aprt_power=14.14,
        max_voltage=15.15,
        min_voltage=16.16,
    )


def test_convert_event() -> None:
    points = _convert(
        NotifyStatusEvent(
            src="src",
            timestamp=TIMESTAMP,
            status=EnergyMeterStatus(
                id=1,
                phases=[_event_phase("a")],
                errors=[],
                n_current=1.1,
                n_errors=[],
                total_act_power=1.1,
                total_aprt_power=2.2,
                total_current=3.3,
                user_calibrated_phase=[],
            ),
        )
    )
    assert len(points) == 3
    assert (
        points[0].to_line_protocol()
        == f"em,device={DEVICE},phase=a,source=live act_power=1.1,aprt_power=2.2,current=3.3,freq=4.4,pf=5.5,voltage=6.6 {UNIX_TIMESTAMP}"
    )
    assert points[1].to_line_protocol() == f"em,device={DEVICE},phase=neutral,source=live current=1.1 {UNIX_TIMESTAMP}"
    assert (
        points[2].to_line_protocol()
        == f"em,device={DEVICE},phase=total,source=live act_power=1.1,aprt_power=2.2,current=3.3 {UNIX_TIMESTAMP}"
    )


def _event_phase(phase: str) -> EnergyMeterPhase:
    return EnergyMeterPhase(
        phase_name=phase,
        act_power=1.1,
        aprt_power=2.2,
        current=3.3,
        freq=4.4,
        pf=5.5,
        voltage=6.6,
        errors=[],
    )
