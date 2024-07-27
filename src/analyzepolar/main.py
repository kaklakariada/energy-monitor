import logging
from pathlib import Path

from analyzepolar.loader import DeviceData, read_data
from analyzepolar.logger import POLAR_ANALYZER_LOGGER
from analyzepolar.model import PolarDeviceData

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")

_logger = POLAR_ANALYZER_LOGGER.getChild("main")


def main():
    data = PolarDeviceData.load([DeviceData(Path("data/unten"), "unten"), DeviceData(Path("data/oben"), "oben")])
    print(data.df.unpivot(on=[], index=None, variable_name=None, value_name=None))


if __name__ == "__main__":
    main()
