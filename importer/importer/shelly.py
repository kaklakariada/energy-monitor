import csv
import datetime
import json
import logging
from typing import Any, Generator, NamedTuple, Optional
from importer.model import (
    ALL_FIELD_NAMES,
    CsvRow,
    DeviceInfo,
    EnergyMeterData,
    EnergyMeterRecords,
    EnergyMeterStatus,
    EnergyMeterStatusRaw,
    RawCsvRow,
    ShellyStatus,
    SystemStatus,
)
import requests


class RpcError(Exception):
    pass


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
