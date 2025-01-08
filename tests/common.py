from datetime import datetime, timezone


def ts(year, month, day, hour=0, minute=0, second=0):
    dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return int(dt.timestamp())


def tp(year, month, day, hour=0, minute=0, second=0):
    return datetime(
        year, month, day, hour, minute, second, tzinfo=timezone.utc
    ).timestamp()
