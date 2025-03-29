import yfinance as yf
from ..Interfaces.Crawler import CrawlerInterface
from ..config.LoadConfig import load_config

import time

class YFinanceStockCrawler(CrawlerInterface):

    def __init__(self, name):
        super().__init__(name)
        self.batch_size = 100
        self.symbols = load_config("symbols_test.json")
        self.tag = "stock"

    def crawl(self):
        """ yfinance에서 종목별 주가 데이터를 분리해서 반환하는 함수 """
        results = []

        try:
            total_symbols = len(self.symbols)

            for batch_number, start_idx in enumerate(range(0, total_symbols, self.batch_size), start=1):
                batch = self.symbols[start_idx:start_idx + self.batch_size]

                try:
                    tickers = yf.Tickers(" ".join(batch))

                    for symbol in batch:
                        try:
                            stock = tickers.tickers.get(symbol)
                            if not stock:
                                raise ValueError("해당 종목을 찾을 수 없음")

                            df = stock.history(period="1d", interval="1m", prepost=True)[['Open', 'High', 'Low', 'Close', 'Volume']]
                            if df.empty:
                                raise ValueError("Empty DataFrame (데이터 없음)")

                            df = df.tail(1).reset_index()
                            df = df.rename(columns={"Datetime": "posted_at"})
                            df["Symbol"] = symbol

                            results.append({
                                "tag": self.tag,
                                "log": {
                                    "crawling_type": self.tag,
                                    "status_code": 200
                                },
                                "df": df
                            })

                        except Exception as symbol_error:
                            results.append({
                                "tag": self.tag,
                                "log": {
                                    "crawling_type": self.tag,
                                    "status_code": 500
                                },
                                "fail_log": {
                                    "err_message": str(symbol_error)
                                }
                            })

                    time.sleep(2)

                except Exception as batch_error:
                    for symbol in batch:
                        results.append({
                            "tag": self.tag,
                            "log": {
                                "crawling_type": self.tag,
                                "status_code": 500
                            },
                            "fail_log": {
                                "err_message": f"배치 오류: {str(batch_error)}"
                            }
                        })

        except Exception as global_error:
            return [{
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": 500
                },
                "fail_log": {
                    "err_message": f"전역 오류: {str(global_error)}"
                }
            }]

        return results
