from ..Interfaces.Crawler import CrawlerInterface
from fredapi import Fred
import pandas as pd

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
        self.tag = "macroeconomics"

    def crawl(self):
        """FRED 데이터를 가져와서 pandas DataFrame으로 출력"""
        macro_data = {}
        status_code = 200
        fail_messages = []

        for name, series_id in self.series_dict.items():
            try:
                data = self.fred.get_series(series_id)
                macro_data[name] = data
            except Exception as e:
                msg = f"{name}({series_id}) - {str(e)}"
                print(f"⚠️ {msg}")
                fail_messages.append(msg)
                status_code = 500

        # 공통 로그 반환
        log_data = {
            "crawling_type": self.tag,
            "status_code": status_code
        }

        if status_code == 500:
            return {
                "tag": self.tag,
                "log": log_data,
                "fail_log": {
                    "err_message": "\n".join(fail_messages)
                }
            }

        # 성공 시
        df = pd.DataFrame(macro_data).dropna(how="all").ffill()
        latest_date = df.index[-1]
        latest_data = df.loc[latest_date]

        macro_rows = pd.DataFrame([{
            "index_name": k,
            "country": "US",
            "index_value": str(v),
            "posted_at": latest_date.to_pydatetime()
        } for k, v in latest_data.items()])

        return {
            "tag": self.tag,
            "log": log_data,
            "df": macro_rows
        }