import itertools
import logging
import time
from typing import Generator, Iterable

from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings, WriteType

from importer.model import CsvRow, PhaseData

logger = logging.getLogger("db")


class PointConverter:

    def __init__(self, device: str):
        self.device = device

    def convert_point(self, row: CsvRow) -> Iterable[Point]:
        phases = (self.create_phase_point(row, phase) for phase in row.phases)
        neutral = [self.create_neutral_point(row)]
        all_phases = itertools.chain(phases, neutral)
        return all_phases

    def create_phase_point(self, row: CsvRow, phase: PhaseData) -> Point:
        point = Point("em").tag("phase", phase.phase_name).time(row.timestamp, write_precision=WritePrecision.S)
        for field in phase._fields:
            point.field(field, getattr(phase, field))
        return point

    def create_neutral_point(self, row: CsvRow) -> Point:
        point = Point("em").tag("phase", "neutral").time(row.timestamp, write_precision=WritePrecision.S)
        for field in ["n_max_current", "n_min_current", "n_avg_current"]:
            point.field(field[2:], getattr(row, field))
        return point


class LoggingBatchCallback(object):
    def __init__(self) -> None:
        self.logger = logging.Logger("db.batch")

    def success(self, conf: tuple[str, str, str], data: str):
        self.logger.debug(f"Written batch: {conf}, data: {data}")

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
        self.client.close()
        self.client = None

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
            count = 0
            start_time = time.time()
            for row in rows:
                for point in converter.convert_point(row):
                    assert point is not None
                    result = write_api.write(org=self.org, bucket=self.bucket, record=point)
                    count += 1
                    assert result is None
            duration = time.time() - start_time
            logger.info(f"Wrote {count} points for {len(rows)} rows in {duration:.2f} seconds")

    def query(self, query):
        query_api = self.client.query_api()
        return query_api.query(query)

    def close(self):
        self.client.close()
