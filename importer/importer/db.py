import datetime
import itertools
import logging
import time
from typing import Generator, Iterable

from influxdb_client import (
    InfluxDBClient,
    Point,
    WriteApi,
    WriteOptions,
    WritePrecision,
)
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings, WriteType

from importer.model import CsvRow, EnergyMeterPhase, NotifyStatusEvent, PhaseData

logger = logging.getLogger("db")


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


class LoggingBatchCallback(object):
    def __init__(self) -> None:
        self.logger = logger
        self.logger.info("Created LoggingBatchCallback")

    def success(self, conf: tuple[str, str, str], data: str):
        self.logger.debug(f"Written batch: {conf}, data: {data}")
        # self.logger.info(f"Written batch: {conf}, data: {len(data.splitlines())} lines")

    def error(self, conf: tuple[str, str, str], data: str, exception: InfluxDBError):
        self.logger.error(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf: tuple[str, str, str], data: str, exception: InfluxDBError):
        self.logger.warning(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


class DbClient:
    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        logger.info(f"Connecting to {self.url} / org {self.org} / bucket {self.bucket}...")
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)

    def __del__(self):
        logger.info("Closing client...")
        self.client.close()
        del self.client

    def ensure_bucket_exists(self):
        buckets_api = self.client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(self.bucket)
        if bucket is None:
            buckets_api.create_bucket(bucket_name=self.bucket, org_id=self.org)
            logger.info(f"Created bucket {self.bucket}")
        else:
            logger.info(f"Bucket {self.bucket} already exists")

    def insert_rows(self, device: str, rows: Iterable[CsvRow]):
        converter = PointConverter(device)
        callback = LoggingBatchCallback()

        with self.client.write_api(
            write_options=WriteOptions(write_type=WriteType.batching),
            point_settings=PointSettings(device=device),
            success_callback=callback.success,
            error_callback=callback.error,
            retry_callback=callback.retry,
        ) as write_api:
            row_count = 0
            point_count = 0
            start_time = time.time()
            for row in rows:
                row_count += 1
                for point in converter.convert_csv_row_point(row):
                    assert point is not None
                    result = write_api.write(org=self.org, bucket=self.bucket, record=point)
                    point_count += 1
                    assert result is None
            duration = time.time() - start_time
            logger.debug(f"Wrote {point_count} points for {row_count} rows in {duration:.2f} seconds")

    def batch_writer(self) -> "BatchWriter":
        callback = LoggingBatchCallback()
        write_api = self.client.write_api(
            write_options=WriteOptions(
                write_type=WriteType.batching,
                batch_size=1_000,
                flush_interval=1_000,
            ),
            success_callback=callback.success,
            error_callback=callback.error,
            retry_callback=callback.retry,
        )
        return BatchWriter(converter=EventPointConverter(), write_api=write_api, bucket=self.bucket)

    def query(self, query):
        query_api = self.client.query_api()
        return query_api.query(query)

    def close(self) -> None:
        self.client.close()


class BatchWriter:
    converter: EventPointConverter
    write_api: WriteApi
    bucket: str

    def __init__(self, converter: EventPointConverter, write_api: WriteApi, bucket: str):
        self.converter = converter
        self.write_api = write_api
        self.bucket = bucket

    def insert_status_event(self, device: str, event: NotifyStatusEvent):
        count = 0
        for point in self.converter.convert_event(device, event):
            assert point is not None
            result = self.write_api.write(bucket=self.bucket, record=point)
            assert result is None
            count += 1
        self.flush()
        logger.debug(f"Wrote {count} points for event")

    def flush(self):
        self.write_api.flush()

    def close(self):
        self.write_api.close()
