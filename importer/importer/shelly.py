import csv
import datetime
import json
import logging
import random
import threading
import traceback
from pathlib import Path
from typing import Any, Callable, Generator, NamedTuple, Optional

import requests
from websockets.sync.client import connect as connect_websocket

from importer.config_model import DeviceConfig
from importer.model import (
    ALL_FIELD_NAMES,
    CsvRow,
    DeviceInfo,
    EnergyMeterData,
    EnergyMeterRecords,
    EnergyMeterStatus,
    EnergyMeterStatusRaw,
    NotifyStatusEvent,
    RawCsvRow,
    ShellyStatus,
    SystemStatus,
)

logger = logging.getLogger("shelly")


class RpcError(Exception):
    pass


class Shelly:
    ip: str
    device_info: DeviceInfo

    def __init__(self, config: DeviceConfig) -> None:
        self.ip = config.ip
        self.device_info = self._get_device_info()
        logger.debug(f"Connected to '{self.device_info.name}' at {self.ip}")

    def _get_device_info(self) -> DeviceInfo:
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
        assert reader.fieldnames is not None
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
        logger.debug(f"Writing CSV data to {target_file}...")
        _create_dir(target_file.parent)
        size = 0
        with open(target_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    byte_count = file.write(chunk)
                    size += byte_count
        logger.debug(f"Wrote {size} bytes of CSV data to {target_file}")

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

    def _rpc_call(self, method: str, params: dict[str, Any]):
        data = json.dumps({"id": 1, "method": method, "params": params})
        logger.debug(f"Sending POST with data {data} to {self.rpc_url}")
        response = requests.post(self.rpc_url, data=data, headers={"Content-Type": "application/json"}, timeout=3)
        response.raise_for_status()
        json_data = response.json()
        if "error" in json_data:
            raise RpcError(f"Error in response: {json_data['error']}")
        return json_data["result"]

    def subscribe(self, callback: Callable[[NotifyStatusEvent], None]) -> "NotificationSubscription":
        subscription = NotificationSubscription(self, callback)
        subscription.subscribe()
        return subscription

    def __str__(self):
        return f"Shelly {self.device_info.name} at {self.ip}"


class NotificationSubscription:
    _logger: logging.Logger
    _shelly: Shelly
    _callback: Callable[[NotifyStatusEvent], None]
    _client_id: str
    _running: bool
    _thread: threading.Thread

    def __init__(self, shelly: Shelly, callback: Callable[[NotifyStatusEvent], None]) -> None:
        self._shelly = shelly
        self._callback = callback
        self._client_id = f"client-{random.randint(0, 1000000)}"
        self._logger = logging.getLogger(f"ws-{self._client_id}")

    def subscribe(self):
        callback = self._handle_exception(self._subscribe_thread)
        self._thread = threading.Thread(target=callback, name=f"subscription-{self._client_id}")
        self._running = True
        self._thread.start()

    def _handle_exception(self, func: Callable[[], None]) -> Callable[[], None]:
        def callback():
            try:
                func()
            except Exception as e:
                self._logger.error(f"Error processing subscription: {e}")
                traceback.print_exception(e)

        return callback

    def _subscribe_thread(self):
        ws_url = f"ws://{self._shelly.ip}/rpc"
        self._logger.debug(f"Connecting to {ws_url} as client {self._client_id}...")
        with connect_websocket(ws_url) as websocket:
            websocket.send('{"id": 1, "src": "' + self._client_id + '"}')
            while self._running:
                response = websocket.recv()
                data = json.loads(response)
                try:
                    method = data["method"]
                    if method == "NotifyEvent":
                        self._logger.debug(f"Ignoring event {data}")
                    elif method == "NotifyStatus":
                        if "em:0" in data["params"]:
                            status = NotifyStatusEvent.from_dict(self._shelly.device_info, data)
                            self._callback(status)
                        else:
                            self._logger.debug(f"Ignoring event {data}")
                    else:
                        raise RpcError(f"Unexpected event method {method} in data {data}")
                except Exception as e:
                    self._logger.error(f"Error processing data {data}: {e}")
                    traceback.print_exception(e)
        self._thread

    def stop(self):
        self._running = False
        self._logger.info(f"Waiting for thread {self._client_id} / device {self._shelly.device_name} to stop...")
        self._thread.join()


def _create_dir(dir: Path) -> None:
    if not dir.exists():
        dir.mkdir(parents=True)
