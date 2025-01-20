from datetime import datetime

import download_tqsdk
import download_tushare
import fire
import quantdata as qd


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

    with qd.open_mongodb(host=host, port=port, user=user, password=password) as mg:
        db = mg["quantcalendar"]
        for col, data in updating.items():
            db.drop_collection(col)
            db[col].insert_many(data)
            print(f"{col} updated, last row: {data[-1]}")


if __name__ == "__main__":
    fire.Fire(update)
