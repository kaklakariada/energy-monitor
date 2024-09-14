import datetime

from config import config


def format_local_timestamp(ts: datetime.datetime) -> str:
    return ts.astimezone(config.timezone).strftime("%Y-%m-%d %H:%M")
