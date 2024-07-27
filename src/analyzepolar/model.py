from dataclasses import dataclass
from typing import NamedTuple
import polars as pl

from analyzepolar.loader import DeviceData, read_data
from analyzer.common import PHASE_COLUMNS, Phase

_PHASE_TYPE = pl.Enum(["a", "b", "c"])


@dataclass(frozen=True)
class PolarDeviceData:
    _df: pl.LazyFrame

    @classmethod
    def load(cls, devices: list[DeviceData]) -> "PolarDeviceData":
        df = read_data(devices)
        return cls(df)

    @property
    def df(self) -> pl.DataFrame:
        return self._df.collect()

    def phase_data(self, column: str) -> pl.LazyFrame:
        if column not in PHASE_COLUMNS:
            raise ValueError(f"Unsupported column '{column}'. Use one of {PHASE_COLUMNS}")
        column_names = [f"{phase.value}_{column}" for phase in Phase.__members__.values()]
        df = self._df.unpivot(
            on=column_names,
            index=["timestamp", "device", "file"],
            variable_name="phase",
            value_name=column,
        )
        df = df.with_columns(
            pl.col("phase")
            .str.extract(r"([abc])_\w+", group_index=1)
            .cast(dtype=_PHASE_TYPE, strict=True)
            .alias("phase")
        )
        return df
