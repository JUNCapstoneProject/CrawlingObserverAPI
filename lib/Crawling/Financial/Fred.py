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
            "Producer Price Index (PPI)": "PCUOMFGOMFG",
            "Personal Consumption Expenditures (PCE)": "PCE",
            "Consumer Confidence Index (CCI)": "UMCSENT",
        }

    def crawl(self):
        """FRED 데이터를 가져와서 pandas DataFrame으로 출력"""
        macro_data = {}

        for name, series_id in self.series_dict.items():
            try:
                data = self.fred.get_series(series_id)
                macro_data[name] = data
            except Exception as e:
                print(f"⚠️ Error fetching {name} ({series_id}): {e}")

        # DataFrame 변환
        macro_df = pd.DataFrame(macro_data)
        macro_df.index.name = "Date"

        # 🔥 최근 값만 가져오기: NaN이 아닌 가장 최근 데이터 선택
        macro_df = macro_df.dropna(how="all")  # 모든 컬럼이 NaN인 행 제거
        macro_df = macro_df.ffill()  # 결측값을 가장 가까운 이전 값으로 채움

        # df.to_csv("fred_macro_data.csv")
        # print("✅ 데이터 저장 완료: `fred_macro_data.csv`")

        # # 터미널 출력
        # print("\nFRED 거시경제 데이터 (최근 5개)")
        # print(df.tail())  # 최근 5개 행만 출력

        print(f"{self.__class__.__name__}: 데이터 수집 완료")  
        
        return {"df": macro_df, "tag": "macro"}