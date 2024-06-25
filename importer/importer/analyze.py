from pathlib import Path

import pandas as pd

from config import config
from importer.config_model import AnalyzedFile


def main():
    df = load_data(config.files)
    phase_columns = [
        "total_act_energy",
        "fund_act_energy",
        "total_act_ret_energy",
        "fund_act_ret_energy",
        "lag_react_energy",
        "lead_react_energy",
        "max_act_power",
        "min_act_power",
        "max_aprt_power",
        "min_aprt_power",
        "max_voltage",
        "min_voltage",
        "avg_voltage",
        "max_current",
        "min_current",
        "avg_current",
    ]

    phase = "a"

    selected_columns = [
        "device",
        "timestamp",
    ]
    selected_columns.extend([f"{phase}_{column}" for column in phase_columns])
    df_phase = df[selected_columns]
    df_phase = df_phase.rename(columns={f"{phase}_{column}": column for column in phase_columns}, inplace=False)
    df_phase.insert(1, "phase", phase)
    print(df_phase.head())
    print(df_phase.describe())  # statistics
    print(df_phase.iloc[0:-1, 2:-1].corr())  # correlation matrix
    print(df_phase.info())  # data types and missing values


def load_data(input_files: list[AnalyzedFile]) -> pd.DataFrame:
    dfs = [load_dataframe(file.device, file.file) for file in input_files]
    result = pd.concat(dfs, axis=0)
    result.reset_index(drop=True, inplace=True)
    return result


def load_dataframe(device: str, file: Path) -> pd.DataFrame:
    df = pd.read_csv(file)
    df["timestamp"] = pd.to_datetime(df["timestamp"], origin="unix", unit="s", utc=True)
    df.insert(0, "device", device)
    return df


if __name__ == "__main__":
    main()
