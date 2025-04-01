import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import time

from ..Interfaces.Crawler import CrawlerInterface
from ..config.LoadConfig import load_config


class YFinanceStockCrawler(CrawlerInterface):

    def __init__(self, name):
        super().__init__(name)
        self.batch_size = 30       # 한 스레드에 넘길 배치 크기
        self.max_workers = 20      # 동시 실행 스레드 수
        self.symbols = load_config("symbols.json")
        self.tag = "stock"

    def crawl(self):
        results = []
        total_symbols = len(self.symbols)

        # 배치로 나누기
        batches = [self.symbols[i:i + self.batch_size] for i in range(0, total_symbols, self.batch_size)]

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._crawl_batch, batch): batch for batch in batches}

            for future in as_completed(futures):
                batch = futures[future]
                try:
                    batch_results = future.result()
                    results.extend(batch_results)
                except Exception as e:
                    for symbol in batch:
                        results.append({
                            "tag": self.tag,
                            "log": {"crawling_type": self.tag, "status_code": 500},
                            "fail_log": {"err_message": f"배치 처리 중 예외: {str(e)}"}
                        })

        return results

    def _crawl_batch(self, batch):
        batch_results = []

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

                    batch_results.append({
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 200},
                        "df": df
                    })

                except Exception as symbol_error:
                    batch_results.append({
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 500},
                        "fail_log": {"err_message": str(symbol_error)}
                    })

                time.sleep(0.01)  # 너무 빠른 요청 방지

        except Exception as batch_error:
            for symbol in batch:
                batch_results.append({
                    "tag": self.tag,
                    "log": {"crawling_type": self.tag, "status_code": 500},
                    "fail_log": {"err_message": f"배치 전체 실패: {str(batch_error)}"}
                })

        return batch_results
