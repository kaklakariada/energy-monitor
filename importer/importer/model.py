import datetime
from enum import Enum
from pathlib import Path
from typing import Any, NamedTuple, Optional

from config import config

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


class RawCsvRow(NamedTuple):
    timestamp: int
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
    def from_dict(cls, row: dict[str | Any, str | Any]) -> "RawCsvRow":
        def value(key: str) -> float:
            return float(row[key])

        values: dict[str, Any] = {key: value(key) for key in MEASUREMENT_NAMES}
        timestamp = int(row["timestamp"])
        return cls(timestamp=timestamp, **values)


class Phase(Enum):
    A = "a"
    B = "b"
    C = "c"


class PhaseData(NamedTuple):
    phase_name: Phase
    """Phase name, a, b or c"""
    total_act_energy: float
    fund_act_energy: float
    total_act_ret_energy: float
    fund_act_ret_energy: float
    lag_react_energy: float
    lead_react_energy: float
    max_act_power: float
    min_act_power: float
    max_aprt_power: float
    min_aprt_power: float
    max_voltage: float
    min_voltage: float
    avg_voltage: float
    max_current: float
    min_current: float
    avg_current: float

    @classmethod
    def from_dict(cls, phase: Phase, row: dict[str, float]) -> "PhaseData":
        return cls(phase, **row)


class CsvRow(NamedTuple):
    timestamp: datetime.datetime
    phases: list[PhaseData]
    n_max_current: float
    n_min_current: float
    n_avg_current: float

    @classmethod
    def from_dict(cls, row: dict[str | Any, str | Any]) -> "CsvRow":
        return cls._from_raw(RawCsvRow.from_dict(row))

    @classmethod
    def _from_raw(cls, row: RawCsvRow) -> "CsvRow":
        phases: list[PhaseData] = []
        for phase in [Phase.A, Phase.B, Phase.C]:
            phase_data = {key[2:]: getattr(row, key) for key in MEASUREMENT_NAMES if key.startswith(f"{phase.value}_")}
            phases.append(PhaseData.from_dict(phase, phase_data))
        return cls(
            timestamp=datetime.datetime.fromtimestamp(row.timestamp, tz=datetime.timezone.utc),
            phases=phases,
            n_max_current=row.n_max_current,
            n_min_current=row.n_min_current,
            n_avg_current=row.n_avg_current,
        )


class DeviceInfo(NamedTuple):
    name: str
    id: str
    mac: str
    slot: int
    key: str
    batch: str
    fw_sbits: str
    model: str
    gen: int
    fw_id: str
    ver: str
    app: str
    auth_en: bool
    auth_domain: Optional[str]
    profile: str

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "DeviceInfo":
        return DeviceInfo(**data)


class EnergyMeterPhase(NamedTuple):
    phase_name: str
    """Phase name, a, b or c"""
    current: float
    """Current measurement value, [A]"""
    voltage: float
    """Voltage measurement value, [V]"""
    act_power: float
    """Active power measurement value, [W]"""
    aprt_power: float
    """Apparent power measurement value, [VA]"""
    pf: float
    """Power factor measurement value"""
    freq: float
    """Network frequency measurement value"""
    errors: list[str]
    """Error conditions occurred. May contain out_of_range:active_power, out_of_range:apparent_power, out_of_range:voltage, out_of_range:current,(shown if at least one error is present)"""


class EnergyMeterStatusRaw(NamedTuple):
    id: int
    """Id of the EM component instance"""
    a_current: float
    """Phase A current measurement value, [A]"""
    a_voltage: float
    """Phase A voltage measurement value, [V]"""
    a_act_power: float
    """Phase A active power measurement value, [W]"""
    a_aprt_power: float
    """Phase A apparent power measurement value, [VA]"""
    a_pf: float
    """Phase A power factor measurement value"""
    a_freq: float
    """Phase A network frequency measurement value"""
    a_errors: list[str]
    """Phase A error conditions occurred. May contain out_of_range:active_power, out_of_range:apparent_power, out_of_range:voltage, out_of_range:current,(shown if at least one error is present)"""
    b_current: float
    """Phase B current measurement value, [A]"""
    b_voltage: float
    """Phase B voltage measurement value, [V]"""
    b_act_power: float
    """Phase B active power measurement value, [W]"""
    b_aprt_power: float
    """Phase B apparent power measurement value, [VA]"""
    b_pf: float
    """Phase B power factor measurement value"""
    b_freq: float
    """Phase B network frequency measurement value"""
    b_errors: list[str]
    """Phase B error conditions occurred. May contain out_of_range:active_power, out_of_range:apparent_power, out_of_range:voltage, out_of_range:current,(shown if at least one error is present)"""
    c_current: float
    """Phase C current measurement value, [A]"""
    c_voltage: float
    """Phase C voltage measurement value, [V]"""
    c_act_power: float
    """Phase C active power measurement value, [W]"""
    c_aprt_power: float
    """Phase C apparent power measurement value, [VA]"""
    c_pf: float
    """Phase C power factor measurement value"""
    c_freq: float
    """Phase C network frequency measurement value"""
    c_errors: list[str]
    """Phase C error conditions occurred. May contain out_of_range:active_power, out_of_range:apparent_power, out_of_range:voltage, out_of_range:current,(shown if at least one error is present)"""
    n_current: Optional[float]
    """Neutral current measurement value, [A] (if supported)"""
    n_errors: list[str]
    """Neutral error conditions occurred. May contain out_of_range:current,(shown if error is present)"""
    total_current: float
    """Sum of the current on all phases(excluding neutral readings if available)"""
    total_act_power: float
    """Sum of the active power on all phases"""
    total_aprt_power: float
    """Sum of the apparent power on all phases"""
    user_calibrated_phase: list[Any]
    """Indicates which phase was user calibrated"""
    errors: list[str]
    """EM component error conditions. May contain power_meter_failure or phase_sequence. Present in status only if not empty."""

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "EnergyMeterStatusRaw":
        for error_field in {"a_errors", "b_errors", "c_errors", "n_errors", "errors"}:
            if error_field not in data:
                data[error_field] = []
        return EnergyMeterStatusRaw(**data)


class EnergyMeterStatus(NamedTuple):
    id: int
    """Id of the EM component instance"""
    phases: list[EnergyMeterPhase]
    """List of all phases status"""
    n_current: Optional[float]
    """Neutral current measurement value, [A] (if supported)"""
    n_errors: list[str]
    """Neutral error conditions occurred. May contain out_of_range:current,(shown if error is present)"""
    total_current: float
    """Sum of the current on all phases(excluding neutral readings if available)"""
    total_act_power: float
    """Sum of the active power on all phases"""
    total_aprt_power: float
    """Sum of the apparent power on all phases"""
    user_calibrated_phase: list[Any]
    """Indicates which phase was user calibrated"""
    errors: list[str]
    """EM component error conditions. May contain power_meter_failure or phase_sequence. Present in status only if not empty."""

    @staticmethod
    def from_raw(raw_data: EnergyMeterStatusRaw) -> "EnergyMeterStatus":
        data = raw_data._asdict()
        phases: list[EnergyMeterPhase] = []
        for phase_name in ["a", "b", "c"]:
            phase = EnergyMeterPhase(
                phase_name=phase_name,
                current=data[f"{phase_name}_current"],
                voltage=data[f"{phase_name}_voltage"],
                act_power=data[f"{phase_name}_act_power"],
                aprt_power=data[f"{phase_name}_aprt_power"],
                pf=data[f"{phase_name}_pf"],
                freq=data[f"{phase_name}_freq"],
                errors=data[f"{phase_name}_errors"],
            )
            phases.append(phase)
            del data[f"{phase_name}_current"]
            del data[f"{phase_name}_voltage"]
            del data[f"{phase_name}_act_power"]
            del data[f"{phase_name}_aprt_power"]
            del data[f"{phase_name}_pf"]
            del data[f"{phase_name}_freq"]
            del data[f"{phase_name}_errors"]
        return EnergyMeterStatus(phases=phases, **data)


class EnergyMeterData(NamedTuple):
    device_info: DeviceInfo
    id: int
    """Id of the EMData component instance"""
    a_total_act_energy: float
    """Total active energy on phase A, Wh"""
    a_total_act_ret_energy: float
    """Total active returned energy on phase A, Wh"""
    b_total_act_energy: float
    """Total active energy on phase B, Wh"""
    b_total_act_ret_energy: float
    """Total active returned energy on phase B, Wh"""
    c_total_act_energy: float
    """Total active energy on phase C, Wh"""
    c_total_act_ret_energy: float
    """Total active returned energy on phase C, Wh"""
    total_act: float
    """Total active energy on all phases, Wh"""
    total_act_ret: float
    """Total active returned energy on all phases, Wh"""
    errors: list[str]
    """Error condition occurred. May contain database_error, (shown if the error is present)."""

    @staticmethod
    def from_dict(device_info: DeviceInfo, data: dict[str, Any]) -> "EnergyMeterData":
        if "errors" not in data:
            data["errors"] = []
        return EnergyMeterData(device_info=device_info, **data)


class DataBlocks(NamedTuple):
    timestamp: datetime.datetime
    ts: int
    period: int
    records: int

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "DataBlocks":
        timestamp = datetime.datetime.fromtimestamp(data["ts"])
        return DataBlocks(timestamp=timestamp, **data)


class EnergyMeterRecords(NamedTuple):
    data_blocks: list[DataBlocks]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "EnergyMeterRecords":
        return EnergyMeterRecords([DataBlocks.from_dict(block) for block in data["data_blocks"]])


class SystemStatus(NamedTuple):
    mac: str
    restart_required: bool
    time: str
    unixtime: int
    uptime: int
    ram_size: int
    ram_free: int
    fs_size: int
    fs_free: int
    cfg_rev: int
    kvs_rev: int
    schedule_rev: int
    webhook_rev: int
    available_updates: dict[str, Any]
    reset_reason: int

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "SystemStatus":
        assert "time" in data
        assert "unixtime" in data
        return SystemStatus(**data)


class ShellyStatus(NamedTuple):
    temperature: float
    sys: SystemStatus
    em: EnergyMeterStatus
    emdata: EnergyMeterData

    @staticmethod
    def from_dict(device_info: DeviceInfo, data: dict[str, Any]) -> "ShellyStatus":
        return ShellyStatus(
            temperature=data["temperature:0"]["tC"],
            sys=SystemStatus.from_dict(data["sys"]),
            em=EnergyMeterStatus.from_raw(EnergyMeterStatusRaw.from_dict(data["em:0"])),
            emdata=EnergyMeterData.from_dict(device_info, data["emdata:0"]),
        )


class NotifyStatusEvent(NamedTuple):
    timestamp: datetime.datetime
    src: str
    status: EnergyMeterStatus

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "NotifyStatusEvent":
        params = data["params"]
        timestamp = datetime.datetime.fromtimestamp(float(params["ts"]), tz=config.timezone)
        em_data = params["em:0"]
        em_data["user_calibrated_phase"] = []
        raw = EnergyMeterStatusRaw.from_dict(em_data)
        status = EnergyMeterStatus.from_raw(raw)
        return NotifyStatusEvent(timestamp=timestamp, src=data["src"], status=status)
