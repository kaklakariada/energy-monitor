import csv
import datetime
import json
import logging
import threading
import traceback
from pathlib import Path
from typing import Any, Callable, Generator, NamedTuple, Optional

import requests
import tqdm
from websockets.sync.client import Connection
from websockets.sync.client import connect as connect_websocket

from importer.config_model import DeviceConfig
from importer.logger import MAIN_LOGGER
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

logger = MAIN_LOGGER.getChild("shelly")


class RpcError(Exception):
    pass


NotificationCallback = Callable[["Shelly", NotifyStatusEvent], None]


class CsvDownloadResult(NamedTuple):
    device_name: str
    target_file: Path
    size: int
    duration: datetime.timedelta


class Shelly:
    ip: str
    name: str
    device_info: Optional[DeviceInfo]

    def __init__(self, config: DeviceConfig) -> None:
        self.ip = config.ip
        self.name = config.name
        self.device_info = None
        logger.debug(f"Connected to '{self.name}' at {self.ip}")

    def get_device_info(self) -> DeviceInfo:
        if self.device_info is None:
            data = self._rpc_call("Shelly.GetDeviceInfo", {"ident": True})
            self.device_info = DeviceInfo.from_dict(data)
        return self.device_info

    @property
    def device_name(self):
        return self.name

    @property
    def device_id(self):
        return self.get_device_info().id

    @property
    def rpc_url(self):
        return f"http://{self.ip}/rpc"

    def get_status(self) -> ShellyStatus:
        data = self._rpc_call("Shelly.GetStatus", {})
        return ShellyStatus.from_dict(self.get_device_info(), data)

    def get_system_status(self) -> SystemStatus:
        data = self._rpc_call("Sys.GetStatus", {})
        return SystemStatus.from_dict(data)

    def get_emdata_status(self) -> EnergyMeterData:
        data = self._rpc_call("EMData.GetStatus", {"id": 0})
        return EnergyMeterData.from_dict(self.get_device_info(), data)

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
        rows = (CsvRow.from_dict(row) for row in reader)
        return rows

    def download_csv_data(
        self,
        target_file: Path,
        timestamp: Optional[datetime.datetime],
        end_timestamp: Optional[datetime.datetime] = None,
        id: int = 0,
    ) -> CsvDownloadResult:
        response = self._get_data_response(timestamp=timestamp, end_timestamp=end_timestamp, id=id)
        logger.debug(f"Writing CSV data to {target_file}...")
        _create_dir(target_file.parent)
        size = 0
        start_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        progress_bar = tqdm.tqdm(total=_estimated_total_size(timestamp, end_timestamp), unit="iB", unit_scale=True)
        with open(target_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    byte_count = file.write(chunk)
                    progress_bar.update(byte_count)
                    size += byte_count
        progress_bar.close()
        duration = datetime.datetime.now(tz=datetime.timezone.utc) - start_timestamp
        logger.debug(f"Wrote {size} bytes of CSV data to {target_file} in {duration}")
        return CsvDownloadResult(target_file=target_file, size=size, duration=duration, device_name=self.name)

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

    def subscribe(self, callback: NotificationCallback) -> "NotificationSubscription":
        subscription = NotificationSubscription(self, callback)
        subscription._subscribe()
        return subscription

    def __str__(self):
        return f"Shelly {self.name} at {self.ip}"


RECEIVE_TIMEOUT = datetime.timedelta(seconds=5)


class NotificationSubscription:
    _logger: logging.Logger
    _shelly: Shelly
    _callback: NotificationCallback
    _client_id: str
    _running: bool
    _thread: threading.Thread

    def __init__(self, shelly: Shelly, callback: NotificationCallback) -> None:
        self._shelly = shelly
        self._callback = callback
        self._client_id = f"client-{self._shelly.name}"
        self._logger = logger.getChild(f"ws-{self._client_id}")

    def _subscribe(self):
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
                self._receive_loop(websocket)
        self._logger.info(f"Stopped thread {self._client_id} / device {self._shelly.device_name}")

    def _receive_loop(self, websocket: Connection) -> None:
        try:
            response = websocket.recv(RECEIVE_TIMEOUT.total_seconds())
        except TimeoutError as e:
            return
        data = json.loads(response)
        try:
            self._process_data(data)
        except Exception as e:
            self._logger.error(f"Error processing data {data}: {e}")
            traceback.print_exception(e)

    def _process_data(self, data: dict[str, Any]):
        method = data["method"]
        if method == "NotifyEvent":
            self._logger.debug(f"Ignoring NotifyEvent {data}")
        elif method == "NotifyStatus":
            if "em:0" in data["params"]:
                status = NotifyStatusEvent.from_dict(data)
                self._callback(self._shelly, status)
            else:
                self._logger.debug(f"Ignoring NotifyStatus event with missing 'em:0' param: {data}")
        else:
            raise RpcError(f"Unexpected event method {method} in data {data}")

    def request_stop(self):
        self._running = False
        self._logger.info(f"Sent stop signal to thread {self._client_id} / device {self._shelly.device_name}...")

    def join_thread(self):
        self._logger.info(
            f"Waiting for thread {self._client_id} / device {self._shelly.device_name} to stop (timeout: {RECEIVE_TIMEOUT})..."
        )
        self._thread.join()

    def __enter__(self) -> "NotificationSubscription":
        return self

    def __exit__(self, _exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        self.request_stop()
        self.join_thread()


def _create_dir(dir: Path) -> None:
    if not dir.exists():
        dir.mkdir(parents=True)


def _estimated_total_size(
    timestamp: Optional[datetime.datetime],
    end_timestamp: Optional[datetime.datetime] = None,
) -> Optional[float]:
    if timestamp is None:
        return None
    header_size = 866
    bytes_per_record = 334
    end_timestamp = end_timestamp or datetime.datetime.now(tz=datetime.timezone.utc)
    delta_minutes = (end_timestamp - timestamp).total_seconds() / 60
    return header_size + (delta_minutes * bytes_per_record)
