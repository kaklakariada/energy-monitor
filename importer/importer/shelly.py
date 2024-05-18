import json
import logging
from typing import Any, NamedTuple, Optional
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


class EnergyMeterStatus(NamedTuple):
    device_info: DeviceInfo
    id: int
    a_total_act_energy: float
    a_total_act_ret_energy: float
    b_total_act_energy: float
    b_total_act_ret_energy: float
    c_total_act_energy: float
    c_total_act_ret_energy: float
    total_act: float
    total_act_ret: float


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


class Shelly:
    ip: str
    device_info: DeviceInfo

    def __init__(self, ip):
        self.ip = ip
        self.device_info = self._get_device_info()
        logging.info(f"Connected to '{self.device_info.name}' at {self.ip}")

    def _get_device_info(self) -> dict[str, Any]:
        response = self.rpc_call("Shelly.GetDeviceInfo", {"ident": True})
        return DeviceInfo(**response)

    @property
    def rpc_url(self):
        return f"http://{self.ip}/rpc"

    def get_status(self) -> dict[str, Any]:
        return self.rpc_call("Shelly.GetStatus", {})

    def get_system_status(self) -> dict[str, Any]:
        data = self.rpc_call("Sys.GetStatus", {})
        return SystemStatus(**data)

    def get_em_status(self) -> EnergyMeterStatus:
        data = self.rpc_call("EMData.GetStatus", {"id": 0})
        return EnergyMeterStatus(device_info=self.device_info, **data)

    def rpc_call(self, method: str, params: dict[str, str]):
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
        return f"Shelly {self.name} at {self.ip}"
