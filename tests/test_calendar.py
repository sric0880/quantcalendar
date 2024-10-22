import os
import re
from datetime import datetime, date, time

import pandas as pd
import pytest
import quantdata as qd

from quantcalendar.calendar_astock import CalendarAstock
from quantcalendar.calendar_ctp import CalendarCTP
from quantcalendar.calendar_7x24 import Time7x24Calendar
from quantcalendar.calendar import (
    I1H,
    I2H,
    I3H,
    I4H,
    DAILY,
    WEEKLY,
    MONTHLY,
)


@pytest.fixture(scope="module")
def mongo_client():
    conn = qd.mongo_connect("127.0.0.1")
    print("connect mongodb")
    yield conn
    qd.mongo_close(conn)
    print("disconnect mongodb")


# fmt: off
def test_get_tradedays(mongo_client):
    cal = Time7x24Calendar()
    assert cal.get_tradedays_between(datetime(2024, 9, 13), datetime(2024, 9, 13)) == [datetime(2024, 9, 13)]
    assert cal.get_tradedays_between(datetime(2024, 9, 13), datetime(2024, 9, 14)) == [datetime(2024, 9, 13), datetime(2024, 9, 14)]
    assert cal.get_tradedays_between(datetime(2024, 9, 13), datetime(2024, 9, 12)) == []

    month_ends = [ datetime(2024, 1, 31), datetime(2024, 2, 29), datetime(2024, 3, 31), ]
    month_begins = [ datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1), ]
    week_ends = [ datetime(2024, 1, 7), datetime(2024, 1, 14), datetime(2024, 1, 21), ]
    week_begins = [ datetime(2024, 1, 1), datetime(2024, 1, 8), datetime(2024, 1, 15), ]
    week_days = [ datetime(2023, 12, 27), datetime(2024, 1, 3), datetime(2024, 1, 10), ]
    assert cal.get_tradedays_month_end(datetime(2024, 1, 1), datetime(2024, 3, 31)) == month_ends
    assert cal.get_tradedays_month_end(datetime(2024, 1, 1), count=3) == month_ends
    assert cal.get_tradedays_month_end(datetime(2024, 1, 31), count=3) == month_ends
    assert cal.get_tradedays_month_begin(datetime(2024, 1, 1), datetime(2024, 3, 1)) == month_begins
    assert cal.get_tradedays_month_begin(datetime(2024, 1, 1), count=3) == month_begins
    assert cal.get_tradedays_month_begin(datetime(2023, 12, 31), count=3) == month_begins
    assert cal.get_tradedays_week_end(datetime(2024, 1, 1), datetime(2024, 1, 21)) == week_ends
    assert cal.get_tradedays_week_end(datetime(2024, 1, 1), count=3) == week_ends
    assert cal.get_tradedays_week_end(datetime(2024, 1, 7), count=3) == week_ends
    assert cal.get_tradedays_week_begin(datetime(2024, 1, 1), datetime(2024, 1, 15)) == week_begins
    assert cal.get_tradedays_week_begin(datetime(2024, 1, 1), count=3) == week_begins
    assert cal.get_tradedays_week_begin(datetime(2023, 12, 26), count=3) == week_begins
    assert cal.get_tradedays_week_day(3, datetime(2023, 12, 27), datetime(2024, 1, 10)) == week_days
    assert cal.get_tradedays_week_day(3, datetime(2023, 12, 27), count=3) == week_days

    cal = CalendarAstock(mongo_client)
    assert cal.get_tradedays_gte(datetime(2023, 6, 30))[0] == datetime(2023, 6, 30)
    assert cal.get_tradedays_lte(datetime(2024, 9, 17))[-1] == datetime(2024, 9, 13)
    assert cal.get_tradedays_lte(datetime(2024, 9, 14))[-1] == datetime(2024, 9, 13)
    assert cal.get_tradedays_lte(datetime(2024, 9, 13))[-1] == datetime(2024, 9, 13)
    assert cal.get_tradedays_between(datetime(2024, 9, 13), datetime(2024, 9, 17)) == [datetime(2024, 9, 13)]
    assert cal.get_tradedays_between(datetime(2024, 9, 13), datetime(2024, 9, 18)) == [datetime(2024, 9, 13), datetime(2024, 9, 18)]
    month_ends = [ datetime(2024, 1, 31), datetime(2024, 2, 29), datetime(2024, 3, 29), ]
    month_begins = [ datetime(2024, 1, 2), datetime(2024, 2, 1), datetime(2024, 3, 1), ]
    week_ends = [ datetime(2024, 1, 5), datetime(2024, 1, 12), datetime(2024, 1, 19), ]
    week_begins = [ datetime(2024, 1, 2), datetime(2024, 1, 8), datetime(2024, 1, 15), ]
    week_days = [ datetime(2024, 2, 7), datetime(2024, 2, 21), datetime(2024, 2, 28), ]
    assert cal.get_tradedays_month_end(datetime(2024, 1, 1))
    assert cal.get_tradedays_month_end(datetime(2024, 1, 1), datetime(2024, 3, 31)) == month_ends
    assert cal.get_tradedays_month_end(datetime(2024, 1, 1), count=3) == month_ends
    assert cal.get_tradedays_month_end(datetime(2024, 1, 31), count=3) == month_ends
    assert cal.get_tradedays_month_begin(datetime(2024, 1, 1), datetime(2024, 3, 1)) == month_begins
    assert cal.get_tradedays_month_begin(datetime(2024, 1, 1), count=3) == month_begins
    assert cal.get_tradedays_month_begin(datetime(2024, 1, 2), count=3) == month_begins
    assert cal.get_tradedays_month_begin(datetime(2023, 12, 31), count=3) == month_begins
    assert cal.get_tradedays_week_end(datetime(2024, 1, 1), datetime(2024, 1, 21)) == week_ends
    assert cal.get_tradedays_week_end(datetime(2024, 1, 1), count=3) == week_ends
    assert cal.get_tradedays_week_end(datetime(2024, 1, 5), count=3) == week_ends
    assert cal.get_tradedays_week_end(datetime(2024, 1, 7))[0] == datetime(2024, 1, 12)
    assert cal.get_tradedays_week_begin(datetime(2024, 1, 1), datetime(2024, 1, 15)) == week_begins
    assert cal.get_tradedays_week_begin(datetime(2024, 1, 1), count=3) == week_begins
    assert cal.get_tradedays_week_begin(datetime(2023, 12, 26), count=3) == week_begins
    assert cal.get_tradedays_week_begin(datetime(2024, 6, 11))[0] == datetime(2024, 6, 11)
    assert cal.get_tradedays_week_day(3, datetime(2024, 2, 7), datetime(2024, 2, 28)) == week_days
    assert cal.get_tradedays_week_day(3, datetime(2024, 2, 7), count=3) == week_days


def test_calendar_7x24_bartime():
    cal = Time7x24Calendar()
    bartime_testcases = [
        (datetime(2024, 9, 13), datetime(2024, 9, 13), 60),
        (datetime(2024, 9, 13), datetime(2024, 9, 13), 300),
        (datetime(2024, 9, 13), datetime(2024, 9, 13), 900),
        (datetime(2024, 9, 13, 0, 0, 1), datetime(2024, 9, 13, 0, 1), 60),
        (datetime(2024, 9, 13, 0, 1, 0), datetime(2024, 9, 13, 0, 1), 60),
        (datetime(2024, 9, 13, 0, 0, 1), datetime(2024, 9, 13, 0, 5), 300),
        (datetime(2024, 9, 13, 0, 0, 1), datetime(2024, 9, 13, 0, 15), 900),
        (datetime(2024, 9, 13, 23, 59, 1), datetime(2024, 9, 14), 60),
        (datetime(2024, 9, 13, 23), datetime(2024, 9, 13, 23), I1H),
        (datetime(2024, 9, 30), datetime(2024, 10, 1), MONTHLY),
        (datetime(2024, 9, 30, 1), datetime(2024, 10, 1), MONTHLY),
        (datetime(2024, 10, 1), datetime(2024, 10, 1), MONTHLY),
        (datetime(2024, 10, 1, 1), datetime(2024, 11, 1), MONTHLY),
        (datetime(2024, 9, 15), datetime(2024, 9, 16), WEEKLY),
        (datetime(2024, 9, 15, 1), datetime(2024, 9, 16), WEEKLY),
        (datetime(2024, 9, 16), datetime(2024, 9, 16), WEEKLY),
        (datetime(2024, 9, 16, 1), datetime(2024, 9, 23), WEEKLY),
        (datetime(2024, 9, 13), datetime(2024, 9, 13), DAILY),
        (datetime(2024, 9, 13, 1), datetime(2024, 9, 14), DAILY),
    ]
    for query, answer, interval in bartime_testcases:
        assert cal.get_current_bartime(query, interval) == answer

    bartimes = cal.get_bartimes(MONTHLY, datetime(2024, 9, 13), count=20)
    assert bartimes[0] == datetime(2024, 10, 1)
    assert bartimes[1] == datetime(2024, 11, 1)

    bartimes = cal.get_bartimes(WEEKLY, datetime(2024, 9, 13), count=20)
    assert bartimes[0] == datetime(2024, 9, 16)

    bartimes = cal.get_bartimes(DAILY, datetime(2024, 9, 13), count=20)
    assert len(bartimes) == 20
    assert bartimes[0] == datetime(2024, 9, 13)
    assert bartimes[-1] == datetime(2024, 10, 2)

    bartimes = cal.get_bartimes(1800, datetime(2024, 9, 13, 1, 0, 1), end=datetime(2024, 9, 14))
    assert bartimes[0] == datetime(2024, 9, 13, 1, 30)

    bartimes = cal.get_bartimes(I4H, datetime(2024, 9, 13), end=datetime(2024, 9, 14))
    assert len(bartimes) == 6
    assert bartimes == [
        datetime(2024, 9, 13),
        datetime(2024, 9, 13, 4),
        datetime(2024, 9, 13, 8),
        datetime(2024, 9, 13, 12),
        datetime(2024, 9, 13, 16),
        datetime(2024, 9, 13, 20),
    ]


def test_calendar_astock(mongo_client):
    cal = CalendarAstock(mongo_client)
    assert cal.is_trading(datetime(2024, 9, 20, 9, 0)) == False
    assert cal.is_trading(datetime(2024, 9, 20, 9, 30)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 11, 30)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 12, 0)) == False
    assert cal.is_trading(datetime(2024, 9, 20, 13, 0)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 15, 0)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 15, 1)) == False
    assert cal.is_trading(datetime(2024, 9, 17, 10, 0)) == False
    assert cal.get_open_close_dt(datetime(2024, 9, 13)) == (datetime(2024, 9, 13, 9, 30), datetime(2024, 9, 13, 15))
    assert cal.get_open_close_dt(datetime(2024, 9, 14)) == (datetime(2024, 9, 18, 9, 30), datetime(2024, 9, 18, 15))
    assert cal.get_open_close_dt(datetime(2024, 9, 18, 10)) == (datetime(2024, 9, 19, 9, 30), datetime(2024, 9, 18, 15))
    # test bartime
    bartime_testcases = [
        (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), 60),
        (datetime(2024, 9, 20, 15, 0, 1), datetime(2024, 9, 23, 9, 31), 60),
        (datetime(2024, 9, 23, 9, 30), datetime(2024, 9, 23, 9, 35), 300),
        (datetime(2024, 9, 20, 8, 30), datetime(2024, 9, 20, 10, 30), I1H),
        (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), I1H),
        (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), I2H),
        (datetime(2024, 10, 1), datetime(2024, 10, 8, 15), DAILY),
        (datetime(2024, 10, 6), datetime(2024, 10, 11, 15), WEEKLY),
        (datetime(2024, 10, 11), datetime(2024, 10, 31, 15), MONTHLY),
    ]
    for query, answer, interval in bartime_testcases:
        assert cal.get_current_bartime(query, interval) == answer


def _ctp_close_time(product_id, year, month, day):
    if product_id is None or product_id in ("T", "TS", "TF", "TL"):
        return datetime.combine(date(year, month, day), time(15, 15))
    else:
        return datetime.combine(date(year, month, day), time(15))


_ctp_get_open_close_queries = [
    datetime(2024, 9, 12, 20, 59, 59),
    datetime(2024, 9, 12, 21),
    datetime(2024, 9, 13, 15),
    datetime(2024, 9, 13, 21, 0, 1),
    datetime(2024, 9, 18, 8, 59, 59),
    datetime(2024, 9, 18, 9, 0, 0),
]


def _ctp_get_close_answers(product_id):
    return [
        _ctp_close_time(product_id, 2024, 9, 13),
        _ctp_close_time(product_id, 2024, 9, 13),
        _ctp_close_time(product_id, 2024, 9, 13),
        _ctp_close_time(product_id, 2024, 9, 18),
        _ctp_close_time(product_id, 2024, 9, 18),
        _ctp_close_time(product_id, 2024, 9, 18),
    ]


_ctp_get_open_answers = {
    None: [
        datetime(2024, 9, 12, 21),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 21),
    ],
    "IH": [
        datetime(2024, 9, 13, 9, 30),
        datetime(2024, 9, 13, 9, 30),
        datetime(2024, 9, 18, 9, 30),
        datetime(2024, 9, 18, 9, 30),
        datetime(2024, 9, 18, 9, 30),
        datetime(2024, 9, 18, 9, 30),
    ],
    "AG": [
        datetime(2024, 9, 12, 21),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 9),
        datetime(2024, 9, 18, 21),
    ],
}


@pytest.mark.parametrize("product_id", [None, "IH", "AG"])
def test_calendar_ctp(mongo_client, product_id):
    cal = CalendarCTP(mongo_client)
    cal = cal.get(product_id)
    # 2023-06-22 端午节
    # 2024-09-14 中秋节
    # 夜盘
    assert cal.is_trading(datetime(2023, 6, 30)) == (product_id != "IH")
    assert cal.is_trading(datetime(2023, 6, 30, 23, 59, 59)) == (product_id != "IH")
    assert cal.is_trading(datetime(2023, 7, 1, 2, 29, 0)) == (product_id != "IH")
    assert cal.is_trading(datetime(2023, 6, 21, 20, 30, 0)) == False
    assert cal.is_trading(datetime(2023, 6, 22)) == False
    assert cal.is_trading(datetime(2024, 9, 13, 21)) == False

    # 日盘
    assert cal.is_trading(datetime(2023, 6, 21, 9, 0, 0)) == (product_id != "IH")
    assert cal.is_trading(datetime(2023, 6, 21, 9, 30, 0))
    assert cal.is_trading(datetime(2023, 6, 21, 10, 20, 0)) == (product_id != "AG")
    assert cal.is_trading(datetime(2023, 6, 21, 10, 15, 0))
    assert cal.is_trading(datetime(2023, 6, 21, 10, 30, 0))
    assert cal.is_trading(datetime(2023, 6, 21, 14, 55, 0))
    assert cal.is_trading(datetime(2023, 6, 21, 15, 0, 0))
    assert cal.is_trading(datetime(2023, 6, 22, 9, 0, 0)) == False
    assert cal.is_trading(datetime(2023, 6, 22, 9, 30, 0)) == False

    for q, ans in zip(
        _ctp_get_open_close_queries,
        list(
            zip(_ctp_get_open_answers[product_id], _ctp_get_close_answers(product_id))
        ),
    ):
        assert cal.get_open_close_dt(q) == ans


def test_calendar_ctp_bartime(mongo_client):
    cal = CalendarCTP(mongo_client)
    path = "tests/bartime_answers"

    for pickle_file in os.listdir(path):
        mat = re.match(r"(\w+)_(\d+)([smHDWMY]).pickle", pickle_file)
        if mat:
            answer = pd.read_pickle(os.path.join(path, pickle_file))
            symbol, i, int_type = mat.groups()
            i = int(i)
            if int_type == "m":
                i *= 60
            elif int_type == "H":
                i *= 3600
            elif int_type == "D":
                i = DAILY
            _cal = cal.get(symbol)
            for q, ans, value in zip(
                answer.index,
                answer,
                answer.index.map(lambda x: _cal.get_current_bartime(x, i)),
            ):
                assert ans == value, f"{pickle_file} {q}: {ans} != {value}"
            print(f"{pickle_file} pass")
            # test bartime
            bartime_testcases = [
                (
                    datetime(2024, 10, 1),
                    _ctp_close_time(_cal.product_id, 2024, 10, 8),
                    DAILY,
                ),
                (
                    datetime(2024, 10, 6),
                    _ctp_close_time(_cal.product_id, 2024, 10, 11),
                    WEEKLY,
                ),
                (
                    datetime(2024, 10, 11),
                    _ctp_close_time(_cal.product_id, 2024, 10, 31),
                    MONTHLY,
                ),
            ]
            for query, answer, interval in bartime_testcases:
                assert _cal.get_current_bartime(query, interval) == answer
# fmt: on
