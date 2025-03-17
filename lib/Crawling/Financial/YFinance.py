from ..Interfaces.Crawler import CrawlerInterface
from ..config.LoadConfig import load_config
import yfinance as yf
import pandas as pd
import time
# import json

class YFinanceCrawler(CrawlerInterface):

    def __init__(self, name):
        super().__init__(name)
        self.batch_size = 100

    def crawl(self):
        """ yfinance에서 재무제표 데이터를 가져와 반환하는 함수 """
        # symbols = load_config("symbols.json")
        symbols = load_config("symbols_test.json")

        total_symbols = len(symbols)
        # print(total_symbols) # 약 5900개
        total_batches = (total_symbols + self.batch_size - 1) // self.batch_size  # 총 배치 수 계산

        # 결과 저장할 딕셔너리
        financial_data = {
            "Income Statement": {},
            "Balance Sheet": {},
            "Cash Flow Statement": {}
        }

        # # 지원되지 않는 심볼 저장
        # unsupported_symbols = []

        # 배치별로 주가 데이터 가져오기
        for batch_number, start_idx in enumerate(range(0, total_symbols, self.batch_size), start=1):
            batch = symbols[start_idx:start_idx + self.batch_size]  # 100개씩 나누기
            # print(f"⏳ Processing batch {batch_number}/{total_batches} ({len(batch)} symbols)")

            try:
                # 여러 심볼을 한 번에 가져오기
                tickers = yf.Tickers(" ".join(batch))  # `yfinance`에서 여러 개 요청 가능
                
                for symbol in batch:
                    stock = tickers.tickers.get(symbol)

                    # 🔥 심볼이 존재하지 않거나 데이터가 없는 경우 제외
                    if not stock or stock.financials.empty:
                        # unsupported_symbols.append(symbol)
                        continue

                    if stock:
                        # 손익계산서 (Income Statement)
                        financial_data["Income Statement"][symbol] = stock.financials
                        
                        # 대차대조표 (Balance Sheet)
                        financial_data["Balance Sheet"][symbol] = stock.balance_sheet
                        
                        # 현금흐름표 (Cash Flow Statement)
                        financial_data["Cash Flow Statement"][symbol] = stock.cashflow

                time.sleep(2)  # Rate Limit 방지 (2초 대기)
            
            except Exception as e:
                print(f"⚠️ Error in batch {start_idx // self.batch_size + 1}: {e}")


        # # 데이터프레임으로 변환 후 CSV 저장
        # for report_name, report_data in financial_data.items():
        #     df = pd.concat(report_data, axis=1)
        #     df.to_csv(f"yfinance_{report_name.replace(' ', '_')}.csv")

        # # ❌ 지원되지 않는 심볼을 파일로 저장 (JSON)
        # if unsupported_symbols:
        #     with open("unsupported_symbols.json", "w", encoding="utf-8") as f:
        #         json.dump(unsupported_symbols, f, indent=4)

        #     print(f"\n⚠️ {len(unsupported_symbols)}개의 심볼이 지원되지 않음. `unsupported_symbols.json` 파일로 저장됨.")

        # 개별 데이터프레임 생성
        income_statement_df = pd.DataFrame.from_dict(financial_data["Income Statement"], orient="index", columns=["Income Statement"])
        balance_sheet_df = pd.DataFrame.from_dict(financial_data["Balance Sheet"], orient="index", columns=["Balance Sheet"])
        cash_flow_statement_df = pd.DataFrame.from_dict(financial_data["Cash Flow Statement"], orient="index", columns=["Cash Flow Statement"])

        print(f"{self.__class__.__name__}: 데이터 수집 완료")  

        return income_statement_df, balance_sheet_df, cash_flow_statement_df