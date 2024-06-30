from typing import Any, Generator, NamedTuple

import pandas as pd

from analyzer.model import MultiDeviceData
from config import config
from importer.config_model import AnalyzedFile


def main():
    data = MultiDeviceData.load(config.files)
    for gap in data.find_gaps():
        print(f"- Gap: {gap.start} - {gap.end} ({gap.duration})")


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
