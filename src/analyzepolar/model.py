import datetime
from dataclasses import dataclass
from functools import reduce
from typing import Optional

import polars as pl

from analyzepolar.loader import DeviceData, read_data
from analyzer.common import PHASE_COLUMNS, Phase

_PHASE_TYPE = pl.Enum(["a", "b", "c"])
PhaseData = tuple[str, pl.LazyFrame]


@dataclass(frozen=False)
class PolarDeviceData:
    _df: pl.LazyFrame
    _collected: Optional[pl.DataFrame] = None

    @classmethod
    def load(cls, devices: list[DeviceData]) -> "PolarDeviceData":
        df = read_data(devices)
        return cls(df)

    @property
    def df(self) -> pl.DataFrame:
        if self._collected is None:
            self._collected = self._df.collect()
        return self._collected

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
        df = self.df.lazy().unpivot(
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

    def total_energy(
        self,
        every: str | datetime.timedelta,
        group_by: Optional[list[str]] = ["device", "phase"],
        start_by: pl._typing.StartBy = "window",
    ) -> pl.LazyFrame:
        df = self.phase_data_column("total_act_energy")
        if group_by is None:
            df = df.group_by("timestamp").agg(pl.sum("total_act_energy"))
            df = df.sort(by="timestamp", descending=False)
        df = df.group_by_dynamic(
            index_column="timestamp", every=every, period=None, group_by=group_by, start_by=start_by
        ).agg(pl.sum("total_act_energy"))
        df = df.with_columns(pl.col("timestamp").dt.date().alias("date"))
        return df

    def daily_total_energy(self) -> pl.LazyFrame:
        return self.total_energy(every="1d")

    def plot_all(self, column: str):
        df = self.phase_data
        return df.collect().plot.line(
            x="timestamp",
            y=column,
            by=["device", "phase"],
            autorange=None,
            grid=True,
            hover=True,
            responsive=False,
            title=f"{column} over time for all devices and phases",
            xlabel="Timestamp",
            ylabel=column,
            sort_date=True,
            downsample=True,
        )

    def plot(self, column: str, phase: Phase, device: str):
        df = self.phase_data
        df = df.filter((pl.col("device").eq(device) & pl.col("phase").eq(phase.value)))
        return df.collect().plot.line(
            x="timestamp",
            y=column,
            autorange=None,
            grid=True,
            hover=True,
            responsive=False,
            title=f"{column} over time for device '{device}' and phase {phase.name}",
            xlabel="Timestamp",
            ylabel=column,
            sort_date=True,
            downsample=True,
        )

    def plot_total_energy(
        self, every: str | datetime.timedelta, device: Optional[str] = None, phase: Optional[Phase] = None
    ):
        df = self.total_energy(every=every)
        title = f"Total energy every {every} for each device and phase"
        if device is not None and phase is not None:
            df = df.filter((pl.col("device").eq(device) & pl.col("phase").eq(phase.value)))
            title = f"Total energy every {every} for device '{device}' and phase {phase.name}"
        return df.collect().plot.bar(
            x="timestamp",
            y="total_act_energy",
            autorange=None,
            grid=True,
            hover=True,
            responsive=False,
            title=title,
            xlabel="Timestamp",
            ylabel="Total energy",
            sort_date=True,
            downsample=True,
        )
