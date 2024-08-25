from datetime import datetime, timedelta

import pytest

from importer.main import _get_start_timestamp

NOW = datetime.fromisoformat("2024-05-19T17:43:59")


@pytest.mark.parametrize(
    "delta, expected",
    [
        ("all", None),
        ("ALL", None),
        ("max", NOW - timedelta(days=60, hours=12)),
        ("MAX", NOW - timedelta(days=60, hours=12)),
        ("3w", NOW - timedelta(weeks=3)),
        ("1d", NOW - timedelta(days=1)),
        ("2h", NOW - timedelta(hours=2)),
        ("3W", NOW - timedelta(weeks=3)),
        ("1D", NOW - timedelta(days=1)),
        ("2H", NOW - timedelta(hours=2)),
    ],
)
def test_get_start_timestamp(delta: str, expected: datetime) -> None:
    assert _get_start_timestamp(delta, NOW) == expected


def test_get_start_timestamp_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid time delta format: 'invalid'"):
        _get_start_timestamp("invalid", NOW)
