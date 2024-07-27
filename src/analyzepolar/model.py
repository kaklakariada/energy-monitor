from dataclasses import dataclass
from functools import reduce
from typing import NamedTuple, Optional
import polars as pl

from analyzepolar.loader import DeviceData, read_data
from analyzer.common import PHASE_COLUMNS, Phase

_PHASE_TYPE = pl.Enum(["a", "b", "c"])
PhaseData = tuple[str, pl.LazyFrame]


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

    @property
    def phase_data(self) -> pl.LazyFrame:

        def merge(a: PhaseData, b: PhaseData) -> PhaseData:
            assert b[0] != "*"
            result = a[1].join(
                other=b[1],
                on=["timestamp", "device", "file", "phase"],
                how="left",
                validate="1:1",
                allow_parallel=True,
                coalesce=None,
            )
            return ("*", result)

        dfs = [(column, self.phase_data_column(column)) for column in PHASE_COLUMNS]
        return reduce(merge, dfs)[1]

    def phase_data_column(self, column: str) -> pl.LazyFrame:
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
