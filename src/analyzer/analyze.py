from typing import Any, Generator, NamedTuple
import pandas as pd

from analyzer.loader import load_all_files, load_phase_data, load_single_file
from config import config
from importer.config_model import AnalyzedFile


class DataGap(NamedTuple):
    device: str
    start: pd.Timestamp
    end: pd.Timestamp

    @property
    def duration(self) -> pd.Timedelta:
        return self.end - self.start


def main():
    all_data()
    # phase_data()


def all_data():
    analyze_gaps(config.files[0])
    analyze_gaps(config.files[1])


def analyze_gaps(file: AnalyzedFile):
    df = load_single_file(file)

    first = df.iloc[0].timestamp
    last = df.iloc[-1].timestamp
    print(f"Device: {file.device}, First: {first}, Last: {last}")
    for gap in find_gaps(file.device, df):
        print(f"- Gap: {gap.start} - {gap.end} ({gap.duration})")


def find_gaps(device: str, df: pd.DataFrame) -> Generator[DataGap, Any, Any]:
    sorted = df.sort_values(by="timestamp", inplace=False)
    prev: pd.Timestamp = None
    for index, row in sorted.iterrows():
        if prev is not None:
            diff = row.timestamp - prev
            if diff.total_seconds() > 60:
                yield DataGap(device, prev, row.timestamp)
        prev = row.timestamp


def phase_data():
    df = load_all_files(config.files)
    df_phase = load_phase_data(df)
    print("Head\n" + str(df_phase.head()))
    print("Tail\n" + str(df_phase.tail()))
    print("Describe\n" + str(df_phase.describe()))  # statistics
    print("Correlation matrix\n" + str(df_phase.iloc[0:-1, 2:-1].corr()))  # correlation matrix
    print("Info:\n")
    df_phase.info(verbose=True)


if __name__ == "__main__":
    main()
