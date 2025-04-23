from datetime import datetime

import download_tqsdk
import download_tushare
import fire


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


if __name__ == "__main__":
    fire.Fire(update)
