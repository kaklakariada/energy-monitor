import datetime
import math
from typing import Optional

import pytest

from importer.shelly import _estimated_total_size

NOW = datetime.datetime.now(tz=datetime.timezone.utc)
BEGIN = NOW - datetime.timedelta(hours=3)
END = NOW - datetime.timedelta(hours=2)


@pytest.mark.parametrize(
    "timestamp, end_timestamp, expected",
    [
        (None, None, None),
        (None, datetime.datetime.now(), None),
        (BEGIN, None, 60986),
        (BEGIN, END, 20906),
    ],
)
def test_estimated_total_size(
    timestamp: Optional[datetime.datetime], end_timestamp: Optional[datetime.datetime], expected: Optional[float]
):
    result = _estimated_total_size(timestamp, end_timestamp)
    if expected is None:
        assert result is None
    else:
        assert result is not None
        assert math.isclose(result, expected, abs_tol=1000)
