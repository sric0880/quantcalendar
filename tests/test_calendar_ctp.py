import os
import pickle
import re
from datetime import date, datetime, time

import pandas as pd
import pytest
import quantdata as qd
from datetime_helper import to_seconds

from quantcalendar import CalendarCTP, bar_unit, timestamp_s


@pytest.fixture(scope="module", autouse=True)
def mongo_client():
    with qd.open_mongodb(host="127.0.0.1"):
        days = qd.mongo_get_data("quantcalendar", "cn_future")
        dates_arr = [(timestamp_s(day["_id"]), day["status"]) for day in days]
        sessions = qd.mongo_get_data("quantcalendar", "cn_future_sessions")
        sessions = [(s["_id"].encode("ascii"), s["market_time"]) for s in sessions]
        CalendarCTP.Init(dates_arr, sessions)


def _ctp_close_time(product_id, year, month, day):
    if product_id == "" or product_id in ("T", "TS", "TF", "TL"):
        return timestamp_s(datetime.combine(date(year, month, day), time(15, 15)))
    else:
        return timestamp_s(datetime.combine(date(year, month, day), time(15)))


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
    "": [
        to_seconds(2024, 9, 12, 21),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 21),
    ],
    "IH": [
        to_seconds(2024, 9, 13, 9, 30),
        to_seconds(2024, 9, 13, 9, 30),
        to_seconds(2024, 9, 18, 9, 30),
        to_seconds(2024, 9, 18, 9, 30),
        to_seconds(2024, 9, 18, 9, 30),
        to_seconds(2024, 9, 18, 9, 30),
    ],
    "AG": [
        to_seconds(2024, 9, 12, 21),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 9),
        to_seconds(2024, 9, 18, 21),
    ],
}


def test_has_night():
    assert CalendarCTP("").has_night()
    assert CalendarCTP("AG").has_night()
    assert not CalendarCTP("IH").has_night()


# fmt: off
@pytest.mark.parametrize("product_id", ["", "IH", "AG"])
def test_trading_time(product_id, to_datetime64):
    cal = CalendarCTP(product_id)
    print(cal)
    # 2023-06-22 端午节
    # 2024-09-14 中秋节
    # 夜盘
    assert cal.is_trading(to_datetime64(2023, 6, 30)) == (product_id != "IH")
    assert cal.is_trading(to_datetime64(2023, 6, 30, 23, 59, 59)) == (product_id != "IH")
    assert cal.is_trading(to_datetime64(2023, 7, 1, 2, 29, 0)) == (product_id != "IH")
    assert cal.is_trading_day(to_datetime64(2023, 7, 1, 2, 29, 0)) == (product_id != "IH")
    assert cal.is_trading(to_datetime64(2023, 6, 21, 20, 30, 0)) == False
    assert cal.is_trading(to_datetime64(2023, 6, 22)) == False
    assert cal.is_trading(to_datetime64(2024, 9, 13, 21)) == False

    # 日盘
    assert cal.is_trading(to_datetime64(2023, 6, 21, 9, 0, 0)) == (product_id != "IH")
    assert cal.is_trading(to_datetime64(2023, 6, 21, 9, 30, 0))
    assert cal.is_trading(to_datetime64(2023, 6, 21, 10, 20, 0)) == (product_id != "AG")
    assert cal.is_trading(to_datetime64(2023, 6, 21, 10, 15, 0))
    assert cal.is_trading(to_datetime64(2023, 6, 21, 10, 30, 0))
    assert cal.is_trading(to_datetime64(2023, 6, 21, 14, 55, 0))
    assert cal.is_trading(to_datetime64(2023, 6, 21, 15, 0, 0))
    assert cal.is_trading(to_datetime64(2023, 6, 22, 9, 0, 0)) == False
    assert cal.is_trading(to_datetime64(2023, 6, 22, 9, 30, 0)) == False

    _ctp_get_open_close_queries = [
        to_datetime64(2024, 9, 12, 20, 59, 59),
        to_datetime64(2024, 9, 12, 21),
        to_datetime64(2024, 9, 13, 15),
        to_datetime64(2024, 9, 13, 21, 0, 1),
        to_datetime64(2024, 9, 18, 8, 59, 59),
        to_datetime64(2024, 9, 18, 9, 0, 0),
    ]

    for q, ans in zip(
        _ctp_get_open_close_queries,
        list(
            zip(_ctp_get_open_answers[product_id], _ctp_get_close_answers(product_id))
        ),
    ):
        assert cal.get_next_open_close(q) == ans


def to_product_id(symbol: str):
    i = 0
    for a in symbol:
        if a.isdigit():
            break
        i += 1
    return symbol[:i]


def test_next_bartime(to_datetime64):
    path = "tests/bartime_answers"

    for pickle_file in os.listdir(path):
        mat = re.match(r"(\w+)_(\d+)([smHDWMY]).pickle", pickle_file)
        if mat:
            answer = pd.read_pickle(os.path.join(path, pickle_file))
            symbol, i, int_type = mat.groups()
            product_id = to_product_id(symbol)
            i = int(i)
            if int_type == "m":
                i *= bar_unit.min
            elif int_type == "H":
                i *= bar_unit.hour
            elif int_type == "D":
                i = bar_unit.day
            cal = CalendarCTP(symbol)
            for q, ans, value in zip(
                answer.index,
                answer.map(lambda x: int(pd.Timestamp.timestamp(x))),
                answer.index.map(lambda x: cal.get_bartime_next(i, x.to_datetime64())),
            ):
                assert ans == value, f"{pickle_file} {q}: {ans} != {value}"
            print(f"{pickle_file} pass")
            # test bartime
            bartime_testcases = [
                (
                    to_datetime64(2024, 10, 1),
                    _ctp_close_time(product_id, 2024, 10, 8),
                    bar_unit.day,
                ),
                (
                    to_datetime64(2024, 10, 6),
                    _ctp_close_time(product_id, 2024, 10, 11),
                    bar_unit.week,
                ),
                (
                    to_datetime64(2024, 10, 11),
                    _ctp_close_time(product_id, 2024, 10, 31),
                    bar_unit.mon,
                ),
            ]
            for query, answer, interval in bartime_testcases:
                assert cal.get_bartime_next(interval, query) == answer


def hourly_bartimes(year, mon, day):
    return [
        to_seconds(year, mon, day),
        to_seconds(year, mon, day, 1),
        to_seconds(year, mon, day, 2),
        to_seconds(year, mon, day, 9, 30),
        to_seconds(year, mon, day, 10, 45),
        to_seconds(year, mon, day, 13, 45),
        to_seconds(year, mon, day, 14, 45),
        to_seconds(year, mon, day, 15),
        to_seconds(year, mon, day, 22),
        to_seconds(year, mon, day, 23) ]

def test_get_bartimes(to_datetime64):
    cal = CalendarCTP("")
    bartimes = cal.get_bartimes_gte(bar_unit.mon, to_datetime64(2024, 9, 13), count=2)
    assert bartimes[0] == to_seconds(2024, 9, 30, 15, 15)
    assert bartimes[1] == to_seconds(2024, 10, 31, 15, 15)

    # 2024年9月30这周只有一天交易日，是周初，也是周末
    bartimes = cal.get_bartimes_gte(bar_unit.week, to_datetime64(2024, 9, 30), count=2)
    assert bartimes[0] == to_seconds(2024, 9, 30, 15, 15)
    assert bartimes[1] == to_seconds(2024, 10, 11, 15, 15)
    bartimes = cal.get_bartimes_gte(bar_unit.week, to_datetime64(2024, 10, 1), count=2)
    assert bartimes[0] == to_seconds(2024, 10, 11, 15, 15)
    assert bartimes[1] == to_seconds(2024, 10, 18, 15, 15)

    bartimes = cal.get_bartimes_gte(bar_unit.day, to_datetime64(2024, 9, 30), count=30)
    assert len(bartimes) == 30
    assert bartimes[0] == to_seconds(2024, 9, 30, 15, 15)
    assert bartimes[1] == to_seconds(2024, 10, 8, 15, 15)

    cal = CalendarCTP("ag")
    # 周4
    bartimes = cal.get_bartimes_between(bar_unit.hour, to_datetime64(2024, 9, 12), to_datetime64(2024, 9, 13))
    assert bartimes == hourly_bartimes(2024, 9, 12)

    # 周五
    bartimes = cal.get_bartimes_between(bar_unit.hour, to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 14))
    assert bartimes == hourly_bartimes(2024, 9, 13)[:-2]

    # 周一
    bartimes = cal.get_bartimes_between(bar_unit.hour, to_datetime64(2024, 9, 23), to_datetime64(2024, 9, 24))
    assert bartimes == hourly_bartimes(2024, 9, 23)[3:]

    # 跨越国庆节
    bartimes = cal.get_bartimes_between(4*bar_unit.hour, to_datetime64(2024, 9, 30), to_datetime64(2024, 10, 9))
    assert len(bartimes) == 4
    assert bartimes[0] == to_seconds(2024, 9, 30, 13, 45)
    assert bartimes[1] == to_seconds(2024, 9, 30, 15)
    assert bartimes[2] == to_seconds(2024, 10, 8, 13, 45)
    assert bartimes[3] == to_seconds(2024, 10, 8, 15)


def test_pickle():
    cal = CalendarCTP("AG")
    bs = pickle.dumps({"calendar": cal})
    dct = pickle.loads(bs)
    assert isinstance(dct, dict) and "calendar" in dct
    assert dct["calendar"].is_trading(datetime(2023, 6, 30)) == True
