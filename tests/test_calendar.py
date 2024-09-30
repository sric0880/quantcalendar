import os
import re
from datetime import datetime

import pandas as pd
import pytest
import quantdata as qd

from quantcalendar.calendar_astock import CalendarAstock
from quantcalendar.calendar_ctp import CalendarCTP


@pytest.fixture(scope="module")
def mongo_client():
    conn = qd.mongo_connect("127.0.0.1")
    print("connect mongodb")
    yield conn
    qd.mongo_close(conn)
    print("disconnect mongodb")


def test_calendar_astock(mongo_client):
    cal = CalendarAstock(mongo_client)
    assert cal.get_tradedays_gte(datetime(2023, 6, 30))[0] == datetime(2023, 6, 30)
    assert cal.get_tradedays_lte(datetime(2024, 9, 17))[-1] == datetime(2024, 9, 13)
    assert cal.get_tradedays_lte(datetime(2024, 9, 14))[-1] == datetime(2024, 9, 13)
    assert cal.get_tradedays_lte(datetime(2024, 9, 13))[-1] == datetime(2024, 9, 13)
    assert cal.is_trading(datetime(2024, 9, 20, 9, 0)) == False
    assert cal.is_trading(datetime(2024, 9, 20, 9, 30)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 11, 30)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 12, 0)) == False
    assert cal.is_trading(datetime(2024, 9, 20, 13, 0)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 15, 0)) == True
    assert cal.is_trading(datetime(2024, 9, 20, 15, 1)) == False
    assert cal.is_trading(datetime(2024, 9, 17, 10, 0)) == False
    assert cal.get_session_dt(datetime(2024, 9, 13)) == (
        datetime(2024, 9, 13, 9, 30),
        datetime(2024, 9, 13, 15),
    )
    assert cal.get_session_dt(datetime(2024, 9, 14)) == (
        datetime(2024, 9, 18, 9, 30),
        datetime(2024, 9, 18, 15),
    )
    assert cal.get_session_dt(datetime(2024, 9, 18, 10)) == (
        datetime(2024, 9, 19, 9, 30),
        datetime(2024, 9, 18, 15),
    )
    # test next bartime
    # bartime_testcases = [
    #     (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), 1, IntervalType.MINUTE),
    #     (
    #         datetime(2024, 9, 20, 15, 0, 1),
    #         datetime(2024, 9, 23, 9, 31),
    #         1,
    #         IntervalType.MINUTE,
    #     ),
    #     (
    #         datetime(2024, 9, 23, 9, 30),
    #         datetime(2024, 9, 23, 9, 35),
    #         5,
    #         IntervalType.MINUTE,
    #     ),
    #     (
    #         datetime(2024, 9, 20, 8, 30),
    #         datetime(2024, 9, 20, 10, 30),
    #         1,
    #         IntervalType.HOUR,
    #     ),
    #     (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), 1, IntervalType.HOUR),
    # ]
    # for query, answer, i, int_type in bartime_testcases:
    #     assert cal.get_next_bartime(query, None, i, int_type) == answer


@pytest.mark.parametrize("product_id", [None, "ag2401"])
def test_calendar_ctp(mongo_client, product_id):
    cal = CalendarCTP(mongo_client)
    cal = cal.get(product_id)
    assert cal.is_trading(datetime(2023, 6, 30))
    assert cal.is_trading(datetime(2023, 6, 30, 23, 59, 59))
    assert cal.is_trading(datetime(2023, 7, 1, 2, 29, 0))
    assert cal.is_trading(datetime(2023, 6, 21, 14, 55, 0))
    assert cal.is_trading(datetime(2023, 6, 21, 15, 0, 0))
    assert cal.is_trading(datetime(2023, 6, 21, 20, 30, 0)) == False
    assert cal.is_trading(datetime(2023, 6, 22)) == False

    assert cal.get_session_dt(datetime(2024, 9, 12, 21)) == (
        datetime(2024, 9, 12, 21),
        datetime(2024, 9, 13, 15),
    )
    assert cal.get_session_dt(datetime(2024, 9, 13)) == (
        datetime(2024, 9, 12, 21),
        datetime(2024, 9, 13, 15),
    )
    assert cal.get_session_dt(datetime(2024, 9, 13, 15)) == (
        datetime(2024, 9, 12, 21),
        datetime(2024, 9, 13, 15),
    )
    assert cal.get_session_dt(datetime(2024, 9, 13, 21, 0, 1)) == (
        datetime(2024, 9, 18, 21),
        datetime(2024, 9, 18, 15),
    )


def test_calendar_ctp_next_bartime(mongo_client):
    cal = CalendarCTP(mongo_client)
    path = "tests/next_bartime_answers"
    for pickle_file in os.listdir(path):
        answer = pd.read_pickle(os.path.join(path, pickle_file))
        print(answer)
        mat = re.match(r"(\w+)_(\d+)([smHDWMY]).pickle", pickle_file)
        if mat:
            symbol, i, int_type = mat.groups()
            i = int(i)
            int_type = IntervalType(int_type)
            print(symbol, i, int_type)
            _cal = cal.get(symbol)
            assert answer == answer.index.map(
                lambda x: _cal.get_next_bartime(x, i, int_type)
            )
