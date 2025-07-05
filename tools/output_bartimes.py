import json
import os

import fire

from quantcalendar import CalendarAstock, CalendarCTP


def time_fmt(time: int):
    hour = time // 3600
    min = (time % 3600) // 60
    sec = time - 3600 * hour - 60 * min
    return "{:02d}:{:02d}:{:02d}".format(hour, min, sec)


def export(calendar: str, symbol: str = "", is_iso_format: bool = True):
    if calendar == "astock":
        cal = CalendarAstock()
    elif calendar == "ctp":
        cal = CalendarCTP(symbol)
    else:
        print(f"{calendar} not suppported")
        return
    dct = {}
    try:
        os.mkdir("bartimes")
    except FileExistsError:
        pass
    with open(f"bartimes/{calendar}_{symbol}_bartimes.json", "w") as f:
        for itv in cal.get_intervals():
            item = []
            for seconds in cal.get_bartimes(itv):
                item.append(time_fmt(seconds) if is_iso_format else seconds)
            dct[f"{int(itv/60)}m"] = item
        json.dump(dct, f, indent=4)


all_calendars = {
    "astock": [""],
    "ctp": [
        "",
        "ao",
        "PF",
        "sc",
        "fu",
        "hc",
        "FG",
        "l",
        "au",
        "i",
        "ss",
        "zn",
        "pp",
        "cs",
        "lc",
        "ni",
        "c",
        "sn",
        "nr",
        "lu",
        "a",
        "ag",
        "rb",
        "b",
        "si",
        "eg",
        "jd",
        "wr",
        "bb",
        "p",
        "jm",
        "eb",
        "CY",
        "sp",
        "rr",
        "j",
        "MA",
        "cu",
        "br",
        "AP",
        "PK",
        "PR",
        "PX",
        "SA",
        "SF",
        "SH",
        "SM",
        "TA",
        "UR",
        "bu",
        "TL",
        "al",
        "T",
        "TS",
        "ru",
        "TF",
        "lh",
        "SR",
        "v",
        "pb",
        "bc",
        "OI",
        "IC",
        "ZC",
        "y",
        "IM",
        "m",
        "lg",
        "fb",
        "CF",
        "CJ",
        "JR",
        "LR",
        "PM",
        "RI",
        "RM",
        "RS",
        "WH",
        "ps",
        "IF",
        "ec",
        "pg",
        # "bz", TODO: new product
        "IH",
        # "ad", TODO: new product
    ],
}


def export_all(is_iso_format: bool = True):
    for calendar, symbols in all_calendars.items():
        for symbol in symbols:
            export(calendar, symbol, is_iso_format)


if __name__ == "__main__":
    fire.Fire({"export": export, "export_all": export_all})
