from unittest import TestCase

from utils import HistoricalData


class TestHistoricalData(TestCase):
    def test_downloader(self):
        d = HistoricalData()
        df, since, since_str = d.download(from_c="XBT", to_c="EUR", since=0)

        self.assertEqual(1383839436659595694, since_str)
        self.assertEqual(2013, since.year)
        self.assertEqual(11, since.month)

    def test_to_dataframe(self):
        d = HistoricalData()

        data = [["97.00000", "1.00000000", 1378856831.546, "s", "m", ""],
                ["99.90000", "0.10000000", 1378859634.7626, "b", "m", ""],
                ["99.90000", "0.10000000", 1378859669.3147, "b", "m", ""],
                ["98.20000", "0.10000000", 1378869758.1198, "b", "l", ""],
                ["96.91000", "0.50000000", 1378875023.0442, "s", "m", ""],
                ["96.90000", "0.50000000", 1378875023.0529, "s", "m", ""],
                ["96.80000", "0.25000000", 1378885271.7763, "s", "l", ""],
                ["96.75000", "0.50000000", 1378891354.3994, "b", "l", ""],
                ["96.00000", "0.01000000", 1379070996.3774, "s", "m", ""],
                ["96.99000", "0.00987833", 1379071035.0723, "b", "m", ""],
                ["96.00000", "0.01100000", 1379071113.4515, "s", "m", ""],
                ["96.30000", "0.10384000", 1379085887.0434, "s", "l", ""],
                ["96.20000", "0.10395000", 1379086063.4595, "s", "l", ""],
                ["96.00000", "0.79221000", 1379086126.0679, "s", "l", ""],
                ["96.00000", "0.18679000", 1379177019.7967, "s", "l", ""],
                ["96.00000", "0.10416000", 1379177019.8048, "s", "l", ""],
                ["95.00000", "2.00000000", 1379191972.5572, "s", "l", ""],
                ["95.00000", "0.10000000", 1379223876.1675, "b", "m", ""],
                ["95.00000", "0.10000000", 1379224436.4174, "b", "m", ""]]

        df = d.to_dataframe(data)

        self.assertEqual(19, df["price"].count())
        self.assertEqual(2013, df["time"][0].year)
        self.assertEqual(9, df["time"][0].month)
        self.assertEqual(10, df["time"][0].day)
        self.assertEqual(23, df["time"][0].hour)
        self.assertEqual(47, df["time"][0].minute)
        self.assertEqual(11, df["time"][0].second)
        self.assertEqual(546000, df["time"][0].microsecond)

    def test_download_all(self):
        d = HistoricalData()
        d.download_all("XBT", "EUR")

    def test_load_resume(self):
        d = HistoricalData()
        since = int(d.load_resume())

        self.assertEqual(1385912691332052251, since)
