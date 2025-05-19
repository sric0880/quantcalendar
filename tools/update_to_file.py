from datetime import datetime

import download_tqsdk
import download_tushare
import fire

import pandas as pd


def output_python(updating):
    file_header = """
from pandas import Timestamp

# Auto generated file, don't modify.
# Generate: sh my_update.sh

"""

    content = """
{collection} = {dct}
"""
    with open("calendar_data.py", "w") as f:
        f.write(file_header)
        for col, data in updating.items():
            f.write(content.format(collection=col, dct=data))
            print(f"{col} updated, last row: {data[-1]}")

def output_cpp(updating):
    file_header = """
// Auto generated file, don't modify.
// Generate: sh my_update.sh

#include <vector>
#include "quantcalendar/dates.h"
#include "quantcalendar/calendar.h"

"""
    def date_arr_format(data):
        arrs = []
        for d in data:
            ts = int(d["_id"].timestamp())
            status = d["status"]
            arrs.append(f"{{{ts}, {status}}}")
        return ",".join(arrs)

    def sessions_format(data):
        arrs = []
        for d in data:
            symbol = d["_id"]
            market_time = d["market_time"]
            market_time_str = ",".join([f"{{{o}, {c}}}" for o, c in market_time])
            arrs.append(f"{{\"{symbol}\", {{{market_time_str}}}}}")
        return ",".join(arrs)


    declarations = {
        "cn_stock" : "const static std::vector<qmc::date_status_item>",
        "cn_future": "const static std::vector<qmc::date_status_item>",
        "cn_future_sessions": "const static std::vector<qmc::CalendarCTP::session_item>"
    }
    data_formatter = {
        "cn_stock" : date_arr_format,
        "cn_future": date_arr_format,
        "cn_future_sessions": sessions_format
    }
    content = """
{declare} {var}{{{data}}};
"""
    with open("calendar_data.h", "w") as f:
        f.write(file_header)
        for col, data in updating.items():
            f.write(content.format(declare=declarations[col], var=col, data=data_formatter[col](data)))
            print(f"{col} updated, last row: {data[-1]}")


def update(
    end_year: int,
    host="127.0.0.1",
    port=27017,
    user="root",
    password="admin",
    tushare_token: str = None,
    tq_user: str = None,
    tq_pwd: str = None,
    out: str = "python"
):
    end_dt = datetime(end_year, 12, 31)
    updating = {}
    if tushare_token:
        collection_name, data = download_tushare.download(tushare_token)
        updating[collection_name] = data

    if tq_user and tq_pwd:
        for collection_name, data in download_tqsdk.download(tq_user, tq_pwd, end_dt):
            updating[collection_name] = data

    if out == "python":
        output_python(updating)
    elif out == "c++":
        output_cpp(updating)

if __name__ == "__main__":
    fire.Fire(update)
