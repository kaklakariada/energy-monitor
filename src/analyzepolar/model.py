from dataclasses import dataclass
from typing import NamedTuple
import polars as pl

from analyzepolar.loader import DeviceData, read_data


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
