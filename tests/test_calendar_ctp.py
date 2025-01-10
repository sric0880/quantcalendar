import os
import re
from datetime import date, datetime, time, timezone

import pandas as pd
import pytest
import quantdata as qd

from quantcalendar import CalendarCTP, bar_unit, to_seconds, to_timepoint


@pytest.fixture(scope="module", autouse=True)
def mongo_client():
    conn = qd.mongo_connect("127.0.0.1", tz_aware=True)  # utc
    print("connect mongodb")
    days = qd.mongo_get_data(conn["quantcalendar"], "cn_future")
    data = [(int(day["_id"].timestamp()), day["status"]) for day in days]
    sessions = qd.mongo_get_data(conn["quantcalendar"], "cn_future_sessions")
    sessions = [(s["_id"].encode("ascii"), s["market_time"]) for s in sessions]
    CalendarCTP.InitData(data, sessions)
    qd.mongo_close(conn)
    print("disconnect mongodb")


def _ctp_close_time(product_id, year, month, day):
    if product_id == "" or product_id in ("T", "TS", "TF", "TL"):
        return int(
            datetime.combine(
                date(year, month, day), time(15, 15), tzinfo=timezone.utc
            ).timestamp()
        )
    else:
        return int(
            datetime.combine(
                date(year, month, day), time(15), tzinfo=timezone.utc
            ).timestamp()
        )


_ctp_get_open_close_queries = [
    to_timepoint(2024, 9, 12, 20, 59, 59),
    to_timepoint(2024, 9, 12, 21),
    to_timepoint(2024, 9, 13, 15),
    to_timepoint(2024, 9, 13, 21, 0, 1),
    to_timepoint(2024, 9, 18, 8, 59, 59),
    to_timepoint(2024, 9, 18, 9, 0, 0),
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
def test_calendar_ctp(product_id):
    cal = CalendarCTP(product_id)
    print(cal)
    # 2023-06-22 端午节
    # 2024-09-14 中秋节
    # 夜盘
    assert cal.is_trading(to_timepoint(2023, 6, 30)) == (product_id != "IH")
    assert cal.is_trading(to_timepoint(2023, 6, 30, 23, 59, 59)) == (product_id != "IH")
    assert cal.is_trading(to_timepoint(2023, 7, 1, 2, 29, 0)) == (product_id != "IH")
    assert cal.is_trading(to_timepoint(2023, 6, 21, 20, 30, 0)) == False
    assert cal.is_trading(to_timepoint(2023, 6, 22)) == False
    assert cal.is_trading(to_timepoint(2024, 9, 13, 21)) == False

    # 日盘
    assert cal.is_trading(to_timepoint(2023, 6, 21, 9, 0, 0)) == (product_id != "IH")
    assert cal.is_trading(to_timepoint(2023, 6, 21, 9, 30, 0))
    assert cal.is_trading(to_timepoint(2023, 6, 21, 10, 20, 0)) == (product_id != "AG")
    assert cal.is_trading(to_timepoint(2023, 6, 21, 10, 15, 0))
    assert cal.is_trading(to_timepoint(2023, 6, 21, 10, 30, 0))
    assert cal.is_trading(to_timepoint(2023, 6, 21, 14, 55, 0))
    assert cal.is_trading(to_timepoint(2023, 6, 21, 15, 0, 0))
    assert cal.is_trading(to_timepoint(2023, 6, 22, 9, 0, 0)) == False
    assert cal.is_trading(to_timepoint(2023, 6, 22, 9, 30, 0)) == False

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


def test_calendar_ctp_bartime():
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
                answer.index.map(lambda x: cal.get_bartime_next(x.timestamp(), i)),
            ):
                assert ans == value, f"{pickle_file} {q}: {ans} != {value}"
            print(f"{pickle_file} pass")
            # test bartime
            bartime_testcases = [
                (
                    to_timepoint(2024, 10, 1),
                    _ctp_close_time(product_id, 2024, 10, 8),
                    bar_unit.day,
                ),
                (
                    to_timepoint(2024, 10, 6),
                    _ctp_close_time(product_id, 2024, 10, 11),
                    bar_unit.week,
                ),
                (
                    to_timepoint(2024, 10, 11),
                    _ctp_close_time(product_id, 2024, 10, 31),
                    bar_unit.mon,
                ),
            ]
            for query, answer, interval in bartime_testcases:
                assert cal.get_bartime_next(query, interval) == answer


# fmt: on
