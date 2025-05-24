import logging

from analyze.loader import DeviceDataSource
from analyze.logger import POLAR_ANALYZER_LOGGER
from analyze.model import PolarDeviceData
from config import config

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")

_logger = POLAR_ANALYZER_LOGGER.getChild("main")


def main():
    data = PolarDeviceData.load([DeviceDataSource(f.dir, f.device) for f in config.files])
    for device in data.device_data:
        print(device.find_duplicate_files())
    # df = data.total_energy(every="1mo", group_by=None).collect()
    # print(df)
    # print(data.statistics.to_string())


if __name__ == "__main__":
    main()
