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

    def crawl(self):
        """ yfinance에서 재무제표 데이터를 가져와 반환하는 함수 """
        try:
            total_symbols = len(self.symbols)
            # total_batches = (total_symbols + self.batch_size - 1) // self.batch_size  # 총 배치 수 계산

            financial_data = {
                "Income Statement": [],
                "Balance Sheet": [],
                "Cash Flow Statement": []
            }

            for batch_number, start_idx in enumerate(range(0, total_symbols, self.batch_size), start=1):
                batch = self.symbols[start_idx:start_idx + self.batch_size]  # 100개씩 나누기
                # print(f"⏳ Processing batch {batch_number}/{total_batches} ({len(batch)} symbols)")

                try:
                    tickers = yf.Tickers(" ".join(batch))  # `yfinance`에서 여러 개 요청 가능
                    
                    for symbol in batch:
                        stock = tickers.tickers.get(symbol)

                        if not stock or stock.financials.empty:
                            continue

                        # 🔥 종목명을 인덱스로 설정하여 DataFrame 리스트에 추가 (가장 최근 데이터만)
                        financial_data["Income Statement"].append(
                            stock.financials.T.reset_index().iloc[:1].assign(Symbol=symbol)
                        )
                        financial_data["Balance Sheet"].append(
                            stock.balance_sheet.T.reset_index().iloc[:1].assign(Symbol=symbol)
                        )
                        financial_data["Cash Flow Statement"].append(
                            stock.cashflow.T.reset_index().iloc[:1].assign(Symbol=symbol)
                        )

                    time.sleep(2)  # Rate Limit 방지
            
                except Exception as e:
                    print(f"⚠️ Error in batch {batch_number}: {e}")

            # 🔥 수정된 부분: pd.concat() 사용하여 리스트를 단일 DataFrame으로 변환
            income_statement_df = pd.concat(financial_data["Income Statement"], axis=0) if financial_data["Income Statement"] else pd.DataFrame()
            balance_sheet_df = pd.concat(financial_data["Balance Sheet"], axis=0) if financial_data["Balance Sheet"] else pd.DataFrame()
            cash_flow_statement_df = pd.concat(financial_data["Cash Flow Statement"], axis=0) if financial_data["Cash Flow Statement"] else pd.DataFrame()

            print(f"{self.__class__.__name__}: 데이터 수집 완료")  

            return [
                {"df": income_statement_df, "tag": "income_statement"},
                {"df": balance_sheet_df, "tag": "balance_sheet"},
                {"df": cash_flow_statement_df, "tag": "cash_flow"},
            ]

        except Exception as e:
            print(f"❌ YFinanceCrawler: 전체 크롤링 과정에서 오류 발생 - {e}")
            return []
