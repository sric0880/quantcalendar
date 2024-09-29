from zoneinfo import ZoneInfo

from .calendar import MongoDBCalendar

__all__ = ["CalendarAstock"]


class CalendarAstock(MongoDBCalendar):
    """
    中国A股日历
    """
    COLLECTION_NAME = "cn_stock"
    session = (34200, 54000)
    session_details = ((34200, 41400), (46800, 54000))
    tz = ZoneInfo("Asia/Shanghai")
