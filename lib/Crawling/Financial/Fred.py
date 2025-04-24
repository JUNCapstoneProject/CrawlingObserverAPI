from fredapi import Fred
import pandas as pd

from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Exceptions.exceptions import *


class FredCrawler(CrawlerInterface):
    def __init__(self, name, api_key):
        super().__init__(name)
        self.key = api_key
        self.fred = Fred(api_key=self.key)
        # 가져올 FRED 데이터 목록 (지표명 : FRED Series ID)
        self.series_dict = {
            "Nominal GDP": "GDP",
            "Real GDP": "GDPC1",
            "Consumer Price Index (CPI)": "CPIAUCSL",
            "Unemployment Rate": "UNRATE",
            "Federal Funds Rate": "FEDFUNDS",
            "2-Year Treasury Yield": "DGS2",
            "Trade Balance": "BOPGSTB",
            "Producer Price Index (PPI)": "PPIACO",
            "PPI - Vehicle": "PCU336110336110",
            "PPI - Electric": "PCU335999335999",
            # "Purchasing Managers' Index (PMI)": "PMI",
            "Personal Consumption Expenditures (PCE)": "PCE",
            "Consumer Confidence Index (CCI)": "UMCSENT",
        }
        self.tag = "macro"

    def crawl(self):
        """FRED 데이터를 지표별로 분리하여 최근과 그 전 데이터 각각 반환"""
        results = []

        for name, series_id in self.series_dict.items():
            try:
                data_series = self.fred.get_series(series_id)
                data_series = data_series.dropna().ffill()
                data_series.index = pd.to_datetime(data_series.index)

                if len(data_series) < 2:
                    raise DataNotFoundException(
                        "데이터가 2개 미만입니다.", source=series_id
                    )

                latest_date = data_series.index[-1]
                previous_date = data_series.index[-2]
                latest_value = data_series.iloc[-1]
                previous_value = data_series.iloc[-2]

                latest_row = {
                    "index_name": name,
                    "country": "US",
                    "index_value": str(latest_value),
                    "posted_at": latest_date.to_pydatetime(),
                }

                previous_row = {
                    "index_name": name,
                    "country": "US",
                    "index_value": str(previous_value),
                    "posted_at": previous_date.to_pydatetime(),
                }

                # 최신 데이터용 result
                results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 200},
                        "df": pd.DataFrame([latest_row]),
                    }
                )

                # 이전 데이터용 result
                results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 200},
                        "df": pd.DataFrame([previous_row]),
                    }
                )

            except CrawlerException as e:
                results.append(
                    {
                        "tag": self.tag,
                        "log": {
                            "crawling_type": self.tag,
                            "status_code": e.status_code,
                        },
                        "fail_log": {"index_name": name, "err_message": str(e)},
                    }
                )

            except Exception as e:
                results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 500},
                        "fail_log": {
                            "index_name": name,
                            "err_message": f"{series_id} - 알 수 없는 오류: {str(e)}",
                        },
                    }
                )

        return results
