import yfinance as yf
from ..Interfaces.Crawler import CrawlerInterface
from ..config.LoadConfig import load_config
import time
import pandas as pd

class YFinanceStockCrawler(CrawlerInterface):

    def __init__(self, name):
        super().__init__(name)
        self.batch_size = 100
        self.symbols = load_config("symbols_test.json")
        self.tag = "stock"

    def crawl(self):
        """ yfinance에서 주가 데이터를 가져와 반환하는 함수 """
        try:
            total_symbols = len(self.symbols)
            stock_data = []  # 결과 저장 리스트
            status_code = 200

            for batch_number, start_idx in enumerate(range(0, total_symbols, self.batch_size), start=1):
                batch = self.symbols[start_idx:start_idx + self.batch_size]  # 배치 단위로 나누기
                
                try:
                    tickers = yf.Tickers(" ".join(batch))  # 여러 종목 데이터 가져오기

                    for symbol in batch:
                        stock = tickers.tickers.get(symbol)
                        
                        if not stock:
                            continue

                        # 최근 1개월 (1mo) 주가 데이터 가져오기
                        df = stock.history(period="1mo")[['Open', 'High', 'Low', 'Close', 'Volume']]
                        if df.empty:
                            continue
                        
                        df = df.reset_index()
                        df["Symbol"] = symbol  # 종목 코드 추가
                        stock_data.append(df)

                    time.sleep(2)  # API Rate Limit 방지

                except Exception as e:
                    print(f"Error in batch {batch_number}: {e}")
                    status_code = 500

            # 모든 데이터를 하나의 DataFrame으로 병합
            stock_prices_df = pd.concat(stock_data, axis=0) if stock_data else pd.DataFrame()

            # print(f"{self.__class__.__name__}: 주가 데이터 수집 완료")
            
            return {
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": status_code,
                },
                "df": stock_prices_df
            }

        except Exception as e:
            # print(f"❌ YFinanceStockCrawler: 전체 크롤링 과정에서 오류 발생 - {e}")
            return {
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": 500,
                },
                "fail_log": {
                    "err_message": str(e)
                }
            }