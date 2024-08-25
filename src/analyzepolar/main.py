import logging
from pathlib import Path

from analyzepolar.loader import DeviceDataSource
from analyzepolar.logger import POLAR_ANALYZER_LOGGER
from analyzepolar.model import PolarDeviceData

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")

_logger = POLAR_ANALYZER_LOGGER.getChild("main")


def main():
    data = PolarDeviceData.load(
        [DeviceDataSource(Path("data/unten"), "unten"), DeviceDataSource(Path("data/oben"), "oben")]
    )
    df = data.total_energy(every="1d", group_by=None).collect()
    print(df)
    print(data.statistics.to_string())


if __name__ == "__main__":
    main()
