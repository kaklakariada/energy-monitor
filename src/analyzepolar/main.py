import logging
from pathlib import Path

from analyzepolar.loader import DeviceDataSource
from analyzepolar.logger import POLAR_ANALYZER_LOGGER
from analyzepolar.model import PolarDeviceData

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")

_logger = POLAR_ANALYZER_LOGGER.getChild("main")


def main():
    data = PolarDeviceData.load(
        [DeviceDataSource(Path("data/unten"), "unten"), DeviceDataSource(Path("data/oben"), "oben")]
    )
    df = data.total_energy(every="1d", group_by=None).collect()
    print(df)
    for gap in data.gaps:
        print(gap)


if __name__ == "__main__":
    main()
