import datetime
import itertools
import logging
from typing import Iterable

from influxdb_client import Point, WritePrecision

from importer.model import CsvRow, EnergyMeterPhase, NotifyStatusEvent, Phase, PhaseData

logger = logging.getLogger("db.converter")


class PointConverter:

    def convert(self, device: str, row: CsvRow) -> Iterable[Point]:
        phases = (self._create_phase_point(device, row, phase) for phase in row.phases)
        neutral = [self._create_neutral_point(device, row)]
        all_phases = itertools.chain(phases, neutral)
        return all_phases

    def _point(self, device: str, phase_name: str, timestamp: datetime.datetime) -> Point:
        return (
            Point("em")
            .tag("device", device)
            .tag("source", "csv")
            .tag("phase", str(phase_name))
            .time(timestamp, write_precision=WritePrecision.S)
        )

    def _create_phase_point(self, device: str, row: CsvRow, phase: PhaseData) -> Point:
        point = self._point(device, phase.phase_name.value, row.timestamp)
        for field in phase._fields:
            if field not in ("phase_name"):
                point.field(field, getattr(phase, field))
        return point

    def _create_neutral_point(self, device: str, row: CsvRow) -> Point:
        point = self._point(device, "neutral", row.timestamp)
        for field in ["n_max_current", "n_min_current", "n_avg_current"]:
            point.field(field[2:], getattr(row, field))
        return point


class EventPointConverter:

    def convert_event(self, device: str, event: NotifyStatusEvent) -> Iterable[Point]:
        points = [self._create_event_phase_point(device, event, phase) for phase in event.status.phases]
        if event.status.n_current is not None:
            points.append(
                self._point(device=device, phase_name="neutral", timestamp=event.timestamp).field(
                    "current", event.status.n_current
                )
            )
        total = self._point(device=device, phase_name="total", timestamp=event.timestamp)
        for field in ["current", "act_power", "aprt_power"]:
            total.field(field, getattr(event.status, f"total_{field}"))
        points.append(total)
        return points

    def _point(self, device: str, phase_name: str, timestamp: datetime.datetime) -> Point:
        return (
            Point("em")
            .tag("device", device)
            .tag("source", "live")
            .tag("phase", phase_name)
            .time(timestamp, write_precision=WritePrecision.S)
        )

    def _create_event_phase_point(self, device: str, row: NotifyStatusEvent, phase: EnergyMeterPhase) -> Point:
        point = self._point(device, phase.phase_name, row.timestamp)
        for field in phase._fields:
            if field not in ("errors", "phase_name"):
                point.field(field, getattr(phase, field))
        return point
