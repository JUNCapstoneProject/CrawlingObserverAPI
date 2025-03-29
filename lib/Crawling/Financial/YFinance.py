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
        results = []

        for start in range(0, len(self.symbols), self.batch_size):
            batch = self.symbols[start:start + self.batch_size]

            try:
                tickers = yf.Tickers(" ".join(batch))
            except Exception as e:
                # tickers 객체 자체가 실패한 경우 → batch 전체 실패 처리
                for symbol in batch:
                    for tag in ["income_statement", "balance_sheet", "cash_flow"]:
                        results.append({
                            "tag": tag,
                            "log": {
                                "crawling_type": "financials",
                                "status_code": 500
                            },
                            "fail_log": {
                                "err_message": f"yf.Tickers 실패: {str(e)}"
                            }
                        })
                continue  # 다음 배치로 넘어감

            for symbol in batch:
                try:
                    stock = tickers.tickers.get(symbol)
                    if not stock:
                        raise ValueError("해당 symbol에 대한 데이터 없음")

                    # 🔹 income_statement
                    try:
                        if not stock.financials.empty:
                            df = stock.financials.T.reset_index().rename(columns={"index": "posted_at"})
                            df["Symbol"] = symbol
                            df["posted_at"] = pd.to_datetime(df["posted_at"])
                            df["financial_type"] = "income_statement"
                            latest = df.sort_values("posted_at").iloc[[-1]]
                            results.append({
                                "tag": "income_statement",
                                "log": {
                                    "crawling_type": "financials",
                                    "status_code": 200
                                },
                                "df": latest.reset_index(drop=True)
                            })
                        else:
                            raise ValueError("income_statement 데이터 없음")

                    except Exception as e:
                        results.append({
                            "tag": "income_statement",
                            "log": {
                                "crawling_type": "financials",
                                "status_code": 500
                            },
                            "fail_log": {
                                "err_message": str(e)
                            }
                        })

                    # 🔹 balance_sheet
                    try:
                        if not stock.balance_sheet.empty:
                            df = stock.balance_sheet.T.reset_index().rename(columns={"index": "posted_at"})
                            df["Symbol"] = symbol
                            df["posted_at"] = pd.to_datetime(df["posted_at"])
                            df["financial_type"] = "balance_sheet"
                            latest = df.sort_values("posted_at").iloc[[-1]]
                            results.append({
                                "tag": "balance_sheet",
                                "log": {
                                    "crawling_type": "financials",
                                    "status_code": 200
                                },
                                "df": latest.reset_index(drop=True)
                            })
                        else:
                            raise ValueError("balance_sheet 데이터 없음")

                    except Exception as e:
                        results.append({
                            "tag": "balance_sheet",
                            "log": {
                                "crawling_type": "financials",
                                "status_code": 500
                            },
                            "fail_log": {
                                "err_message": str(e)
                            }
                        })

                    # 🔹 cash_flow
                    try:
                        if not stock.cashflow.empty:
                            df = stock.cashflow.T.reset_index().rename(columns={"index": "posted_at"})
                            df["Symbol"] = symbol
                            df["posted_at"] = pd.to_datetime(df["posted_at"])
                            df["financial_type"] = "cash_flow"
                            latest = df.sort_values("posted_at").iloc[[-1]]
                            results.append({
                                "tag": "cash_flow",
                                "log": {
                                    "crawling_type": "financials",
                                    "status_code": 200
                                },
                                "df": latest.reset_index(drop=True)
                            })
                        else:
                            raise ValueError("cash_flow 데이터 없음")

                    except Exception as e:
                        results.append({
                            "tag": "cash_flow",
                            "log": {
                                "crawling_type": "financials",
                                "status_code": 500
                            },
                            "fail_log": {
                                "err_message": str(e)
                            }
                        })

                except Exception as symbol_level_error:
                    # 종목 자체가 불러와지지 않았거나 완전한 실패일 경우
                    for tag in ["income_statement", "balance_sheet", "cash_flow"]:
                        results.append({
                            "tag": tag,
                            "log": {
                                "crawling_type": "financials",
                                "status_code": 500
                            },
                            "fail_log": {
                                "err_message": f"심볼 수준 실패: {str(symbol_level_error)}"
                            }
                        })

            time.sleep(2)

        return results

