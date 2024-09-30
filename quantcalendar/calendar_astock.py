from zoneinfo import ZoneInfo

from .calendar import MongoDBCalendar

__all__ = ["CalendarAstock"]


class CalendarAstock(MongoDBCalendar):
    """
    中国A股日历
    """

    COLLECTION_NAME = "cn_stock"
    session_details = ((34200, 41400), (46800, 54000))
    tz = ZoneInfo("Asia/Shanghai")
    # 1m - 5m - 15m - 30m - 1H - 2H
    intervals = (60, 300, 900, 1800, 3600, 7200)


if __name__ == "__main__":
    import quantdata as qd

    with qd.mongo_connect("127.0.0.1") as conn:
        print("connect mongodb")
        cal = CalendarAstock(conn)
        print(cal)
    print("disconnect mongodb")
