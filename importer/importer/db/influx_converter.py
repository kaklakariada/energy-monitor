import datetime
import itertools
import logging
from typing import Iterable

from influxdb_client import Point, WritePrecision

from importer.model import CsvRow, EnergyMeterPhase, NotifyStatusEvent, PhaseData

logger = logging.getLogger("db.converter")


class PointConverter:

    def __init__(self, device: str):
        self.device = device

    def point(self, phase_name: str, timestamp: datetime.datetime) -> Point:
        return (
            Point("em")
            .tag("device", self.device)
            .tag("source", "csv")
            .tag("phase", phase_name)
            .time(timestamp, write_precision=WritePrecision.S)
        )

    def convert_csv_row_point(self, row: CsvRow) -> Iterable[Point]:
        phases = (self._create_phase_point(row, phase) for phase in row.phases)
        neutral = [self._create_neutral_point(row)]
        all_phases = itertools.chain(phases, neutral)
        return all_phases

    def _create_phase_point(self, row: CsvRow, phase: PhaseData) -> Point:
        point = self.point(phase.phase_name, row.timestamp)
        for field in phase._fields:
            point.field(field, getattr(phase, field))
        return point

    def _create_neutral_point(self, row: CsvRow) -> Point:
        point = self.point("neutral", row.timestamp)
        for field in ["n_max_current", "n_min_current", "n_avg_current"]:
            point.field(field[2:], getattr(row, field))
        return point


class EventPointConverter:

    def point(self, device: str, phase_name: str, timestamp: datetime.datetime) -> Point:
        return (
            Point("em")
            .tag("device", device)
            .tag("source", "live")
            .tag("phase", phase_name)
            .time(timestamp, write_precision=WritePrecision.S)
        )

    def convert_event(self, device: str, event: NotifyStatusEvent) -> Iterable[Point]:
        points = [self._create_event_phase_point(device, event, phase) for phase in event.status.phases]
        if event.status.n_current is not None:
            points.append(
                self.point(device=device, phase_name="neutral", timestamp=event.timestamp).field(
                    "current", event.status.n_current
                )
            )
        total = self.point(device=device, phase_name="total", timestamp=event.timestamp)
        for field in ["current", "act_power", "aprt_power"]:
            total.field(field, getattr(event.status, f"total_{field}"))
        points.append(total)
        return points

    def _create_event_phase_point(self, device: str, row: NotifyStatusEvent, phase: EnergyMeterPhase) -> Point:
        point = self.point(device, phase.phase_name, row.timestamp)
        for field in phase._fields:
            if field not in ("errors", "phase_name"):
                point.field(field, getattr(phase, field))
        return point
