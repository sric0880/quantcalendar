from datetime import datetime

import pytest
from numpy import datetime64


def _to_datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0):
    return datetime(year, month, day, hour, minute, second, microsecond=microsecond)


def _to_datetime64(year, month, day, hour=0, minute=0, second=0, microsecond=0):
    return datetime64(
        datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond=microsecond,
        )
    )


@pytest.fixture(
    scope="session",
    params=[_to_datetime64, _to_datetime],
    ids=["Python datetime", "Numpy datetime64"],
)
def to_datetime64(request):
    return request.param
