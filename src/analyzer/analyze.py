import logging
from typing import Any, Generator, NamedTuple

import pandas as pd

from analyzer.logger import ANALYZER_LOGGER
from analyzer.model import MultiDeviceData, Phase
from config import config

logger = ANALYZER_LOGGER.getChild("analyze")


def main():
    data = MultiDeviceData.load(config.files)
    # phase = data.get_phase_data(config.files[0].device, phase=Phase.A)
    # print(phase.df.head())
    # print(phase.df.tail())
    # print("Describe\n" + str(phase.df.describe()))  # statistics
    # print("Correlation matrix\n" + str(phase.df.corr()))  # correlation matrix
    # phase.df.info()
    df = data.get_total_active_energy()
    print(df.head(20))
    print(df.tail())
    print("Describe\n" + str(df.describe()))  # statistics
    df.info()
    print("Null values\n" + str(df.isnull().sum()))  # null values


def phase_data():
    # df = load_all_files(config.files)
    # df_phase = load_phase_data(df)
    # print("Head\n" + str(df_phase.head()))
    # print("Tail\n" + str(df_phase.tail()))
    # print("Describe\n" + str(df_phase.describe()))  # statistics
    # print("Correlation matrix\n" + str(df_phase.iloc[0:-1, 2:-1].corr()))  # correlation matrix
    # print("Info:\n")
    # df_phase.info(verbose=True)
    pass


if __name__ == "__main__":
    main()
