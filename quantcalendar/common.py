from enum import Enum


class IntervalType(Enum):
    SECOND = "s"
    MINUTE = "m"
    HOUR = "H"
    DAILY = "D"
    WEEKLY = "W"
    MONTH = "M"
    YEAR = "Y"
