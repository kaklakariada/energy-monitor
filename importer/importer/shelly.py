import csv
import datetime
import json
import logging
from pathlib import Path
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

logger = logging.getLogger("shelly")


class RpcError(Exception):
    pass


class Shelly:
    ip: str
    device_info: DeviceInfo

    def __init__(self, ip) -> None:
        self.ip = ip
        self.device_info = self._get_device_info()
        logger.info(f"Connected to '{self.device_info.name}' at {self.ip}")

    def _get_device_info(self) -> dict[str, Any]:
        data = self._rpc_call("Shelly.GetDeviceInfo", {"ident": True})
        return DeviceInfo.from_dict(data)

    @property
    def device_name(self):
        return self.device_info.name

    @property
    def device_id(self):
        return self.device_info.id

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
        self, timestamp: datetime.datetime, end_timestamp: Optional[datetime.datetime] = None, id: int = 0
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
        response = self._get_data_response(timestamp, end_timestamp, id)
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        assert set(reader.fieldnames) == ALL_FIELD_NAMES
        raw_rows = (RawCsvRow.from_dict(row) for row in reader)
        rows = (CsvRow.from_raw(row) for row in raw_rows)
        return rows

    def download_csv_data(
        self,
        target_file: Path,
        timestamp: Optional[datetime.datetime],
        end_timestamp: Optional[datetime.datetime] = None,
        id: int = 0,
    ) -> None:
        response = self._get_data_response(timestamp=timestamp, end_timestamp=end_timestamp, id=id)
        logger.info(f"Writing CSV data to {target_file}...")
        _create_dir(target_file.parent)
        size = 0
        with open(target_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    byte_count = file.write(chunk)
                    size += byte_count
        logger.info(f"Wrote {size} bytes of CSV data to {target_file}")

    def _get_data_response(
        self, timestamp: Optional[datetime.datetime], end_timestamp: Optional[datetime.datetime], id: int
    ):
        url = f"http://{self.ip}/emdata/{id}/data.csv?add_keys=true"
        if timestamp:
            url += f"&ts={timestamp.timestamp()}"
        if end_timestamp:
            url += f"&end_ts={end_timestamp.timestamp()}"
        response = requests.get(url, stream=True, timeout=3)
        response.raise_for_status()
        return response

    def _rpc_call(self, method: str, params: dict[str, str]):
        data = {"id": 1, "method": method, "params": params}
        data = json.dumps(data)
        logger.debug(f"Sending POST with data {data} to {self.rpc_url}")
        response = requests.post(self.rpc_url, data=data, headers={"Content-Type": "application/json"}, timeout=3)
        response.raise_for_status()
        response = response.json()
        if "error" in response:
            raise RpcError(f"Error in response: {response['error']}")
        return response["result"]

    def __str__(self):
        return f"Shelly {self.device_info.name} at {self.ip}"


def _create_dir(dir: Path) -> None:
    if not dir.exists():
        dir.mkdir(parents=True)
