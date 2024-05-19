import csv
import datetime
import json
import logging
from typing import Any, Generator, NamedTuple, Optional
from importer.model import ALL_FIELD_NAMES, CsvRow, RawCsvRow
import requests


class RpcError(Exception):
    pass


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
    name: str
    """Phase name, A, B or C"""
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
    phase_a: EnergyMeterPhase
    """Phase A status"""
    phase_b: EnergyMeterPhase
    """Phase B status"""
    phase_c: EnergyMeterPhase
    """Phase C status"""
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
    def from_raw(data: EnergyMeterStatusRaw) -> "EnergyMeterStatus":
        data = data._asdict()
        for phase in {"a", "b", "c"}:
            data[f"phase_{phase}"] = EnergyMeterPhase(
                name=phase,
                current=data[f"{phase}_current"],
                voltage=data[f"{phase}_voltage"],
                act_power=data[f"{phase}_act_power"],
                aprt_power=data[f"{phase}_aprt_power"],
                pf=data[f"{phase}_pf"],
                freq=data[f"{phase}_freq"],
                errors=data[f"{phase}_errors"],
            )
            del data[f"{phase}_current"]
            del data[f"{phase}_voltage"]
            del data[f"{phase}_act_power"]
            del data[f"{phase}_aprt_power"]
            del data[f"{phase}_pf"]
            del data[f"{phase}_freq"]
            del data[f"{phase}_errors"]
        data["phases"] = [data[f"phase_{phase}"] for phase in {"a", "b", "c"}]
        return EnergyMeterStatus(**data)


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
    time: Optional[str]
    unixtime: Optional[int]
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


class Shelly:
    ip: str
    device_info: DeviceInfo

    def __init__(self, ip):
        self.ip = ip
        self.device_info = self._get_device_info()
        logging.info(f"Connected to '{self.device_info.name}' at {self.ip}")

    def _get_device_info(self) -> dict[str, Any]:
        data = self._rpc_call("Shelly.GetDeviceInfo", {"ident": True})
        return DeviceInfo.from_dict(data)

    @property
    def rpc_url(self):
        return f"http://{self.ip}/rpc"

    def get_status(self) -> ShellyStatus:
        data = self._rpc_call("Shelly.GetStatus", {})
        return ShellyStatus.from_dict(self.device_info, data)

    def get_system_status(self) -> SystemStatus:
        data = self._rpc_call("Sys.GetStatus", {})
        return SystemStatus.from_dict(data)

    def get_emdata_status(self) -> EnergyMeterData:
        data = self._rpc_call("EMData.GetStatus", {"id": 0})
        return EnergyMeterData.from_dict(self.device_info, data)

    def get_em_status(self) -> EnergyMeterStatus:
        data = self._rpc_call("EM.GetStatus", {"id": 0})
        raw_data = EnergyMeterStatusRaw.from_dict(data)
        return EnergyMeterStatus.from_raw(raw_data)

    def get_emdata_records(self, timestamp: int = 0) -> EnergyMeterRecords:
        data = self._rpc_call("EMData.GetRecords", {"id": 0, "ts": timestamp})
        return EnergyMeterRecords.from_dict(data)

    def get_data(
        self, timestamp: datetime.datetime, end_timestamp: datetime.datetime = None, id: int = 0
    ) -> Generator[CsvRow, None, None]:
        """Get energy meter data from Shelly.

        Args:
            timestamp (datetime.datetime): Timestamp of the first record.
                Any record with data having timestamp between ts and end_ts will be retrieved.
            end_timestamp (datetime.datetime, optional): Timestamp of the last record to get (if available).
                If response is too big, it will be chunked. Default is to get all available records without limit.
            id (int, optional): Id of the EMData component instance. Defaults to 0.
        Returns:
            Generator[str, None, None]: Generator of CSV rows.
        """
        url = f"http://{self.ip}/emdata/{id}/data.csv?add_keys=true&ts={timestamp.timestamp()}"
        if end_timestamp:
            url += f"&end_ts={end_timestamp.timestamp()}"
        response = requests.get(url, stream=True, timeout=3)
        response.raise_for_status()
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        assert set(reader.fieldnames) == ALL_FIELD_NAMES
        raw_rows = (RawCsvRow.from_dict(row) for row in reader)
        rows = (CsvRow.from_raw(row) for row in raw_rows)
        return rows

    def _rpc_call(self, method: str, params: dict[str, str]):
        data = {"id": 1, "method": method, "params": params}
        data = json.dumps(data)
        logging.debug(f"Sending POST with data {data} to {self.rpc_url}")
        response = requests.post(self.rpc_url, data=data, headers={"Content-Type": "application/json"}, timeout=3)
        response.raise_for_status()
        response = response.json()
        if "error" in response:
            raise RpcError(f"Error in response: {response['error']}")
        return response["result"]

    def __str__(self):
        return f"Shelly {self.device_info.name} at {self.ip}"
