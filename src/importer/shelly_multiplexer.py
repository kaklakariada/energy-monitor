import datetime
import tarfile
from concurrent import futures
from pathlib import Path
from typing import Any, NamedTuple, Optional

from importer.config_model import DeviceConfig
from importer.logger import MAIN_LOGGER
from importer.model import ShellyStatus
from importer.shelly import (
    CsvDownloadResult,
    NotificationCallback,
    NotificationSubscription,
    Shelly,
)

logger = MAIN_LOGGER.getChild("shelly").getChild("multi")


class CsvDownloadTask(NamedTuple):
    device: Shelly
    target_file: Path


class ShellyMultiplexer:
    devices: list[Shelly] = []

    def __init__(self, config: list[DeviceConfig]) -> None:
        self.devices = [Shelly(device) for device in config]
        logger.debug(f"Connected to {len(self.devices)} devices")

    def get_status(self) -> dict[str, ShellyStatus]:
        return {device.name: device.get_status() for device in self.devices}

    def download_csv_data(
        self,
        target_dir: Path,
        timestamp: Optional[datetime.datetime],
        end_timestamp: Optional[datetime.datetime] = None,
    ) -> list[CsvDownloadResult]:

        def _download_one(task: CsvDownloadTask) -> CsvDownloadResult:
            return task.device.download_csv_data(
                target_file=task.target_file, timestamp=timestamp, end_timestamp=end_timestamp
            )

        file_name_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        tasks = [
            CsvDownloadTask(device, target_dir / device.name / f"{device.name}_{file_name_timestamp}.csv")
            for device in self.devices
        ]
        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            result = list(executor.map(_download_one, tasks))
        for status in result:
            logger.info(f"Downloaded {status.size} bytes from {status.device_name} to {status.target_file}")
        _create_backup_file(
            target_file=target_dir / f"backup_{file_name_timestamp}.tar.bz2",
            archive_dir=Path(f"backup_{file_name_timestamp}"),
            directories=[task.target_file.parent for task in tasks],
        )
        return result

    def subscribe(self, callback: NotificationCallback) -> "MultiNotificationSubscription":
        subscription = MultiNotificationSubscription(self, callback)
        subscription.subscribe()
        return subscription


def _create_backup_file(target_file: Path, archive_dir: Path, directories: list[Path]) -> None:
    with tarfile.open(target_file, "w:bz2", compresslevel=9) as tar:
        for directory in directories:
            logger.info(f"Adding {directory} to backup file {target_file}...")
            tar.add(directory, arcname=archive_dir / directory.name, recursive=True)
    logger.info(f"Backup file created: {target_file}")


class MultiNotificationSubscription:
    _multiplexer: ShellyMultiplexer
    _callback: NotificationCallback
    _subscriptions: list[NotificationSubscription]

    def __init__(self, multiplexer: ShellyMultiplexer, callback: NotificationCallback) -> None:
        self._multiplexer = multiplexer
        self._callback = callback

    def subscribe(self) -> None:
        logger.debug(f"Subscribing to {len(self._multiplexer.devices)} devices...")
        self._subscriptions = [device.subscribe(self._callback) for device in self._multiplexer.devices]

    def stop(self) -> None:
        logger.debug(f"Stopping {len(self._subscriptions)} subscriptions...")
        for subscription in self._subscriptions:
            subscription.request_stop()
        for subscription in self._subscriptions:
            subscription.join_thread()

    def __enter__(self) -> "MultiNotificationSubscription":
        return self

    def __exit__(self, _exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        self.stop()
