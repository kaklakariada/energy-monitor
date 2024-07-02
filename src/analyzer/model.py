from enum import Enum
from typing import Any, Generator, Iterable, NamedTuple

import pandas as pd

from analyzer.logger import ANALYZER_LOGGER
from importer.config_model import AnalyzedFile

LOGGER = ANALYZER_LOGGER.getChild("model")

PHASE_COLUMNS = [
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


class DataGap(NamedTuple):
    device: str
    start: pd.Timestamp
    end: pd.Timestamp

    @property
    def duration(self) -> pd.Timedelta:
        return self.end - self.start


class Phase(Enum):
    A = "a"
    B = "b"
    C = "c"


class PhaseData(NamedTuple):
    device: str
    phase: Phase
    df: pd.DataFrame

    @property
    def total_active_energy(self) -> pd.Series:
        return self.df[["timestamp", "total_act_energy"]]


class DeviceData(NamedTuple):
    device: str
    df: pd.DataFrame

    @classmethod
    def load_file(cls, file: AnalyzedFile) -> "DeviceData":
        df = pd.read_csv(file.path)
        LOGGER.debug(f"Loaded data for device '{file.device}' from '{file.path}' with {len(df)} rows.")
        return cls.load_df(file.device, df)

    @classmethod
    def load_df(cls, device: str, df: pd.DataFrame) -> "DeviceData":
        return cls(device, _prepare_device_data(device, df))

    def find_gaps(self) -> Generator[DataGap, Any, Any]:
        prev: pd.Timestamp = None
        for _, row in self.df.iterrows():
            if prev is not None:
                diff = row.timestamp - prev
                if diff.total_seconds() > 60:
                    gap = DataGap(self.device, prev, row.timestamp)
                    LOGGER.debug(
                        f"Found gap for device '{gap.device}' of {gap.duration} between {gap.start} and {gap.end}."
                    )
                    yield gap
            prev = row.timestamp

    def get_phase_data(self, phase: Phase) -> PhaseData:
        selected_columns = ["timestamp"]
        selected_columns.extend(_phase_columns(phase))
        df_phase = self.df[selected_columns]
        df_phase = df_phase.rename(
            columns={f"{phase.value}_{column}": column for column in PHASE_COLUMNS}, inplace=False
        )
        return PhaseData(device=self.device, phase=phase, df=df_phase)

    def get_total_active_energy(self) -> pd.DataFrame:
        data = self.df[["timestamp", "a_total_act_energy", "b_total_act_energy", "c_total_act_energy"]]
        return data.set_index("timestamp", drop=True, append=False, inplace=False, verify_integrity=True)


def _phase_columns(phase: Phase) -> list[str]:
    return [f"{phase.value}_{column}" for column in PHASE_COLUMNS]


class MultiDeviceData(NamedTuple):
    dfs: dict[str, DeviceData]

    @classmethod
    def load(cls, files: list[AnalyzedFile]) -> "MultiDeviceData":
        return MultiDeviceData.create((DeviceData.load_file(file) for file in files))

    @classmethod
    def create(cls, data: Iterable[DeviceData]) -> "MultiDeviceData":
        return cls({d.device: d for d in data})

    def find_gaps(self) -> Generator[DataGap, Any, Any]:
        for _, data in self.dfs.items():
            yield from data.find_gaps()

    def get_phase_data(self, device: str, phase: Phase) -> PhaseData:
        return self.dfs[device].get_phase_data(phase)

    def _get_total_active_energy(self, device: str) -> pd.DataFrame:
        return (
            self.dfs[device]
            .get_total_active_energy()
            .rename(
                columns={f"{phase}_total_act_energy": f"{device}_{phase}" for phase in ["a", "b", "c"]}, inplace=False
            )
        )

    def get_total_active_energy(self) -> pd.DataFrame:
        devices = list(self.dfs.keys())
        result = self._get_total_active_energy(devices[0])
        for device in devices[1:]:
            result = result.join(
                other=self._get_total_active_energy(device),
                # on="timestamp",
                how="outer",
                lsuffix="",
                rsuffix="",
                validate="one_to_one",
            )
        columns = result.columns.tolist()
        result["total"] = result[columns].sum(axis=1)
        return result


def _prepare_device_data(device: str, df: pd.DataFrame) -> pd.DataFrame:
    _validate_device_data(device, df)
    df["timestamp"] = pd.to_datetime(df["timestamp"], origin="unix", unit="s", utc=True)
    # df.insert(0, "device", device)
    return df


def _validate_device_data(device: str, df: pd.DataFrame) -> None:
    assert df[
        "timestamp"
    ].is_monotonic_increasing, f"Timestamps for device '{device}' are not monotonically increasing."
    assert df["timestamp"].is_unique, f"Timestamps for device '{device}' are not unique."
