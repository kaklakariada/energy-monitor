import logging
import time
from typing import Generator, Iterable, Optional

from influxdb_client import InfluxDBClient, WriteApi, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings, WriteType

from importer.db.influx_converter import PointConverter
from importer.logger import MAIN_LOGGER
from importer.model import CsvRow, NotifyStatusEvent

logger = MAIN_LOGGER.getChild("db")


class LoggingBatchCallback:
    def __init__(self) -> None:

        self.logger = logger.getChild("batch")
        self.logger.info("Created LoggingBatchCallback")

    def success(self, conf: tuple[str, str, str], data: str):
        self.logger.debug(f"Written batch: {conf}, data: {len(data.splitlines())} lines")

    def error(self, conf: tuple[str, str, str], data: str, exception: InfluxDBError):
        self.logger.error(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf: tuple[str, str, str], data: str, exception: InfluxDBError):
        self.logger.warning(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


converter = PointConverter()


class DbClient:
    _client: Optional[InfluxDBClient]

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        logger.info(f"Connecting to {self.url} / org {self.org} / bucket {self.bucket}...")
        self._client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self._logging_callback = LoggingBatchCallback()

    def _get_client(self) -> InfluxDBClient:
        if self._client is None:
            raise ValueError("Client is closed")
        return self._client

    def ensure_bucket_exists(self):
        buckets_api = self._get_client().buckets_api()
        bucket = buckets_api.find_bucket_by_name(self.bucket)
        if bucket is None:
            buckets_api.create_bucket(bucket_name=self.bucket, org_id=self.org)
            logger.info(f"Created bucket {self.bucket}")
        else:
            logger.info(f"Bucket {self.bucket} already exists")

    def insert_rows(self, device: str, rows: Iterable[CsvRow]):
        with self._get_client().write_api(
            write_options=WriteOptions(write_type=WriteType.batching),
            point_settings=PointSettings(device=device),
            success_callback=self._logging_callback.success,
            error_callback=self._logging_callback.error,
            retry_callback=self._logging_callback.retry,
        ) as write_api:
            row_count = 0
            point_count = 0
            start_time = time.time()
            for row in rows:
                row_count += 1
                for point in converter.convert(device, row):
                    assert point is not None
                    result = write_api.write(org=self.org, bucket=self.bucket, record=point)
                    point_count += 1
                    assert result is None
            duration = time.time() - start_time
            logger.debug(f"Wrote {point_count} points for {row_count} rows in {duration:.2f} seconds")

    def batch_writer(self) -> "BatchWriter":
        write_api = self._get_client().write_api(
            write_options=WriteOptions(
                write_type=WriteType.batching,
                batch_size=1_000,
                flush_interval=1_000,
            ),
            success_callback=self._logging_callback.success,
            error_callback=self._logging_callback.error,
            retry_callback=self._logging_callback.retry,
        )
        return BatchWriter(converter=PointConverter(), write_api=write_api, bucket=self.bucket)

    def query(self, query):
        query_api = self._get_client().query_api()
        return query_api.query(query)

    def close(self) -> None:
        if self._client is None:
            return
        logger.info("Closing client...")
        self._client.close()
        self._client = None

    def __enter__(self) -> "DbClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self):
        self.close()


class BatchWriter:
    _converter: PointConverter
    _write_api: Optional[WriteApi]
    _bucket: str

    def __init__(self, converter: PointConverter, write_api: WriteApi, bucket: str):
        self._converter = converter
        self._write_api = write_api
        self._bucket = bucket

    def _get_write_api(self) -> WriteApi:
        if self._write_api is None:
            raise ValueError("BatchWriter is closed")
        return self._write_api

    def insert_status_event(self, device: str, event: NotifyStatusEvent):
        write_api = self._get_write_api()
        count = 0
        for point in self._converter.convert(device, event):
            assert point is not None
            result = write_api.write(bucket=self._bucket, record=point)
            assert result is None
            count += 1
        self.flush()

    def flush(self):
        self._get_write_api().flush()

    def close(self):
        if self._write_api is None:
            return
        self._write_api.close()
        self._write_api = None

    def __enter__(self) -> "BatchWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
