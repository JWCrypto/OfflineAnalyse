import json
import logging
from datetime import datetime
from os import path, listdir
from time import sleep

import pandas as pd
import requests

TRADE_DATA_URL = "https://api.kraken.com/0/public/Trades"
DATA_DIR = path.join(path.dirname(path.dirname(__file__)), "data")

RETRY = 15


class HistoricalData:
    def __init__(self):
        pass

    def download_all(self, from_c, to_c):
        since_str = self.load_resume()
        current_year = None
        current_month = None

        current_dfs = []

        while True:
            df, since, since_str = self.download(from_c, to_c, since_str)

            current_dfs.append(df)

            next_month = since.month
            next_year = since.year

            if current_year is None or current_month is None:
                current_year = next_year
                current_month = next_month

            if next_month != current_month or next_year != current_year:
                data_fname = path.join(DATA_DIR,
                                       f"trade_{from_c}2{to_c}_{current_year}{current_month:02d}_{since_str}.csv")
                pd.concat(current_dfs).to_csv(data_fname, index=False)
                current_dfs = []
                current_year = next_year
                current_month = next_month
                logging.info(f"Trade history ({current_year}/{current_month:02d}) for {from_c} to {to_c} download")

    def download(self, from_c, to_c, since):
        sleep_time = 1 * 10

        retry = 0
        content = ""

        while retry < RETRY:
            pair = f"X{from_c}Z{to_c}"
            payload = {
                'pair': pair,
                'since': since,
            }

            try:
                res = requests.get(TRADE_DATA_URL, params=payload)

                content = json.loads(res.text)
                since = int(content['result']['last'])
                data = content['result'][pair]

                return self.to_dataframe(data), datetime.fromtimestamp(since / 1000000000), since

            except requests.RequestException:
                retry += 1
            except KeyError:
                retry += 1
                if content['error'][0].find("Rate limit exceeded"):
                    sleep(sleep_time)
                    sleep_time *= 2
                print(content)

        logging.error("Data missing: %s" % since)

    @staticmethod
    def to_dataframe(data):
        df = pd.DataFrame(data,
                          columns=["price", "volume", "time", "action", "type", "miscellaneous"])

        df["price"] = pd.to_numeric(df["price"], errors='coerce')
        df["volume"] = pd.to_numeric(df["volume"], errors='coerce')
        df["time"] = pd.to_datetime(df["time"], errors='coerce')
        df["action"] = df["action"].astype("category")
        df["type"] = df["type"].astype("category")

        return df.dropna()

    @staticmethod
    def load_resume():
        since = 0

        for fname in listdir(DATA_DIR):
            if fname.endswith(".csv"):
                since = max(since, int(fname.split("_")[-1][:-4]))

        return since
