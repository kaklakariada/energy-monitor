import asyncio
import datetime
import logging
import time
from importer.config import config
from importer.model import NotifyStatusEvent
from importer.shelly import Shelly

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("downloader")


def main():
    subscribe()


def subscribe():
    shelly = Shelly(config.devices[0].ip)

    def callback(data: NotifyStatusEvent):
        logger.info(f"Received data: {data}")

    shelly.subscribe(callback)


def download_data():
    target_dir = config.data_dir
    now = datetime.datetime.now()
    start_timestamp = now - datetime.timedelta(days=1)
    # start_timestamp = None
    for device in config.devices:
        shelly = Shelly(device.ip)
        target_file = target_dir / device.name / f"{now.isoformat()}.csv"
        shelly.download_csv_data(timestamp=start_timestamp, end_timestamp=None, target_file=target_file)


if __name__ == "__main__":
    main()
