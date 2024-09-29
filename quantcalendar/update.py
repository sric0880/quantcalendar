from datetime import datetime

import fire
import quantdata as qd

from .calendar import DB_NAME_CALENDAR
from .tools import download_tqsdk, download_tushare


def update(
    end_year: int,
    host="127.0.0.1",
    port=27017,
    user="root",
    password="admin",
    tushare_token: str = None,
    tq_user: str = None,
    tq_pwd: str = None,
):
    end_dt = datetime(end_year, 12, 31)
    updating = {}
    if tushare_token:
        collection_name, data = download_tushare.download(tushare_token)
        updating[collection_name] = data

    if tq_user and tq_pwd:
        for collection_name, data in download_tqsdk.download(tq_user, tq_pwd, end_dt):
            updating[collection_name] = data

    with qd.mongo_connect(host, port, user, password) as mg:
        db = mg[DB_NAME_CALENDAR]
        for col, data in updating.items():
            db.drop_collection(col)
            db[col].insert_many(data)
            print(f"{col} updated, last row: {data[-1]}")


if __name__ == "__main__":
    fire.Fire(update)
