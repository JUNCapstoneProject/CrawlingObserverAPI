from ..Interfaces.Crawler import CrawlerInterface
from ..config.LoadConfig import load_config
import yfinance as yf
import pandas as pd
import time

class YFinanceCrawler(CrawlerInterface):

    def __init__(self, name):
        super().__init__(name)
        self.batch_size = 100
        self.symbols = load_config("symbols_test.json")
        self.tag = "financials"

    def crawl(self):
        """ yfinance에서 재무제표 데이터를 가져와 반환하는 함수 """
        try:
            financial_data = {
                "income_statement": [],
                "balance_sheet": [],
                "cash_flow": []
            }

            for start in range(0, len(self.symbols), self.batch_size):
                batch = self.symbols[start:start + self.batch_size]
                tickers = yf.Tickers(" ".join(batch))

                for symbol in batch:
                    stock = tickers.tickers.get(symbol)
                    if not stock:
                        continue

                    try:
                        if not stock.financials.empty:
                            financial_data["income_statement"].append(
                                stock.financials.T.reset_index().assign(Symbol=symbol)
                            )
                        if not stock.balance_sheet.empty:
                            financial_data["balance_sheet"].append(
                                stock.balance_sheet.T.reset_index().assign(Symbol=symbol)
                            )
                        if not stock.cashflow.empty:
                            financial_data["cash_flow"].append(
                                stock.cashflow.T.reset_index().assign(Symbol=symbol)
                            )
                    except Exception as inner_e:
                        print(f"오류 발생 ({symbol}): {inner_e}")

                time.sleep(2)

        except Exception as e:
            # 전체 실패 시
            return [{
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": 500
                },
                "fail_log": {
                    "err_message": str(e)
                }
            }]

        # 정상 수집 완료
        results = []
        for tag in ["income_statement", "balance_sheet", "cash_flow"]:
            df_list = financial_data[tag]
            df = pd.concat(df_list, axis=0) if df_list else pd.DataFrame()
            results.append({
                "tag": tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": 200
                },
                "df": df  # 이후 dispatcher에서 처리
            })

        return results