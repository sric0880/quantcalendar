from datetime import datetime, timezone


def to_seconds(year, month, day, hour=0, minute=0, second=0):
    """return integer seconds"""
    dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return int(dt.timestamp())
