import logging
from pathlib import Path

from analyzepolar.loader import DeviceData, read_data
from analyzepolar.logger import POLAR_ANALYZER_LOGGER

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(threadName)s - %(levelname)s - %(name)s - %(message)s")
_logger = POLAR_ANALYZER_LOGGER.getChild("main")


def main():
    df_csv = read_data([DeviceData(Path("data/unten"), "unten"), DeviceData(Path("data/oben"), "oben")])
    print(df_csv.collect())


if __name__ == "__main__":
    main()
