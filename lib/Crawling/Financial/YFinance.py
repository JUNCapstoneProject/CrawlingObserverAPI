import yfinance as yf
import pandas as pd
import time

from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Exceptions.exceptions import *


from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.session import get_session
from typing import List


def get_symbols_from_db(limit: int = None) -> List[str]:
    with get_session() as session:
        query = session.query(Company.ticker).order_by(Company.company_id.asc())
        if limit is not None:
            query = query.limit(limit)
        results = query.all()
        return [ticker[0] for ticker in results]


class YFinanceCrawler(CrawlerInterface):

    def __init__(self, name):
        super().__init__(name)
        self.batch_size = 100
        self.symbols = get_symbols_from_db(limit=5)
        self.tag = "financials"

    def crawl(self):
        results = []

        for start in range(0, len(self.symbols), self.batch_size):
            batch = self.symbols[start : start + self.batch_size]

            try:
                tickers = yf.Tickers(" ".join(batch))
            except Exception as e:
                for symbol in batch:
                    for tag in ["income_statement", "balance_sheet", "cash_flow"]:
                        results.append(
                            {
                                "tag": tag,
                                "log": {
                                    "crawling_type": self.tag,
                                    "status_code": ExternalAPIException.status_code,
                                },
                                "fail_log": {
                                    "err_message": str(
                                        ExternalAPIException(
                                            "yf.Tickers 실패", source=symbol
                                        )
                                    )
                                },
                            }
                        )
                continue

            for symbol in batch:
                try:
                    stock = tickers.tickers.get(symbol)
                    if not stock:
                        raise DataNotFoundException(
                            f"{symbol}: 종목 데이터 없음", source=symbol
                        )

                    for fin_type, accessor in [
                        ("income_statement", lambda s: s.quarterly_financials),
                        ("balance_sheet", lambda s: s.quarterly_balance_sheet),
                        ("cash_flow", lambda s: s.quarterly_cashflow),
                    ]:
                        try:
                            try:
                                df_raw = accessor(stock)
                            except Exception as e:
                                raise ParsingException(
                                    f"{symbol}: {fin_type} 파싱 실패", source=symbol
                                ) from e

                            df_latest = self.extract_recent_quarters(
                                df_raw, symbol, fin_type
                            )

                            for _, row in df_latest.iterrows():
                                row_dict = {
                                    k: (None if pd.isna(v) else v)
                                    for k, v in row.to_dict().items()
                                }
                                results.append(
                                    {
                                        "tag": fin_type,
                                        "log": {
                                            "crawling_type": self.tag,
                                            "status_code": 200,
                                        },
                                        "df": [row_dict],
                                    }
                                )

                        except CrawlerException as e:
                            results.append(
                                {
                                    "tag": fin_type,
                                    "log": {
                                        "crawling_type": self.tag,
                                        "status_code": e.status_code,
                                    },
                                    "fail_log": {"err_message": str(e)},
                                }
                            )
                        except Exception as e:
                            results.append(
                                {
                                    "tag": fin_type,
                                    "log": {
                                        "crawling_type": self.tag,
                                        "status_code": 500,
                                    },
                                    "fail_log": {
                                        "err_message": f"{symbol}/{fin_type} 처리 중 알 수 없는 오류: {str(e)}"
                                    },
                                }
                            )

                except CrawlerException as e:
                    for tag in ["income_statement", "balance_sheet", "cash_flow"]:
                        results.append(
                            {
                                "tag": tag,
                                "log": {
                                    "crawling_type": self.tag,
                                    "status_code": e.status_code,
                                },
                                "fail_log": {"err_message": str(e)},
                            }
                        )
                except Exception as e:
                    for tag in ["income_statement", "balance_sheet", "cash_flow"]:
                        results.append(
                            {
                                "tag": tag,
                                "log": {"crawling_type": self.tag, "status_code": 500},
                                "fail_log": {
                                    "err_message": f"{symbol} 처리 중 알 수 없는 오류: {str(e)}"
                                },
                            }
                        )

            time.sleep(2)

        return results

    def extract_recent_quarters(
        self, df: pd.DataFrame, symbol: str, financial_type: str
    ) -> pd.DataFrame:
        if df.empty:
            raise DataNotFoundException(
                f"[{symbol}] {financial_type} 원본 데이터가 비어 있음", source=symbol
            )

        df = df.T.reset_index().rename(columns={"index": "posted_at"})
        df["Symbol"] = symbol
        df["posted_at"] = pd.to_datetime(df["posted_at"])
        df["financial_type"] = financial_type

        df_sorted = df.sort_values("posted_at", ascending=False)
        return df_sorted.head(3).reset_index(drop=True)
