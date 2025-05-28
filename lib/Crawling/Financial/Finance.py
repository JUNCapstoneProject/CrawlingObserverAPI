import yfinance as yf
import pandas as pd
import concurrent.futures
from typing import List
from curl_cffi import requests as curl_requests


from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Exceptions.exceptions import *
from lib.Config.config import Config
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.session import get_session
from lib.Logger.logger import Logger

SECTION_FIELDS = {
    "balance_sheet": {
        "Cash Equivalents",
        "Cash Financial",
        "Inventory",
        "Other Short Term Investments",
        "Current Assets",
        "Current Liabilities",
        "Cash Cash Equivalents And Short Term Investments",
        "Accounts Receivable",
        "Retained Earnings",
    },
    "cash_flow": {"Capital Expenditure"},
    "income_statement": {
        "Other Non Operating Income Expenses",
        "Cost Of Revenue",
        "Gross Profit",
        "Operating Income",
        "EBITDA",
        "Reconciled Depreciation",
        "Selling General And Administration",
    },
}


def get_symbols_from_db(limit: int = None) -> List[str]:
    with get_session() as session:
        query = session.query(Company.ticker).order_by(Company.company_id.asc())
        if limit is not None:
            query = query.limit(limit)
        results = query.all()
        return [ticker[0] for ticker in results]


class FinancialCrawler(CrawlerInterface):

    def __init__(self, name):
        super().__init__(name)
        self.batch_size = 100
        self.symbols = get_symbols_from_db(limit=Config.get("symbol_size.total", 5))
        self.tag = "financials"
        self.logger = Logger(self.__class__.__name__)

    def crawl(self):
        results = []

        for start in range(0, len(self.symbols), self.batch_size):
            batch_idx = start // self.batch_size
            batch = self.symbols[start : start + self.batch_size]

            # 배치 인덱스가 10의 배수일 때만 로그 출력
            if batch_idx % 10 == 0:
                self.logger.log(
                    "DEBUG",
                    f"[배치 진행] [{batch_idx}]번째 배치 시작",
                )

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
                self.logger.log("ERROR", f"yf.Tickers 실패 (batch={batch}): {str(e)}")
                continue

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_symbol = {
                    executor.submit(self.fetch_symbol_data, symbol, tickers): symbol
                    for symbol in batch
                }

                for future in concurrent.futures.as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        symbol_results = future.result()
                        results.extend(symbol_results)
                    except Exception as exc:
                        for tag in ["income_statement", "balance_sheet", "cash_flow"]:
                            results.append(
                                {
                                    "tag": tag,
                                    "log": {
                                        "crawling_type": self.tag,
                                        "status_code": 500,
                                    },
                                    "fail_log": {
                                        "err_message": f"{symbol} 병렬 처리 중 오류 발생: {str(exc)}"
                                    },
                                }
                            )

        for result in results:
            if "log" in result:
                result["log"]["target_url"] = "yfinance_library"
        return results

    def fetch_symbol_data(self, symbol: str, tickers) -> List[dict]:
        results = []
        stock = yf.Ticker(symbol)
        if not stock:
            raise DataNotFoundException(f"{symbol}: 종목 데이터 없음", source=symbol)

        for fin_type, accessors in [
            (
                "income_statement",
                [lambda s: s.quarterly_financials, lambda s: s.financials],
            ),
            (
                "balance_sheet",
                [lambda s: s.quarterly_balance_sheet, lambda s: s.balance_sheet],
            ),
            ("cash_flow", [lambda s: s.quarterly_cashflow, lambda s: s.cashflow]),
        ]:
            df_raw = None
            for accessor in accessors:
                try:
                    df = accessor(stock)
                    if df is not None and not df.empty:
                        df_raw = df
                        break
                except Exception as e:
                    continue  # 다음 fallback accessor 시도

            if df_raw is None or df_raw.empty:
                results.append(
                    {
                        "tag": fin_type,
                        "log": {"crawling_type": self.tag, "status_code": 404},
                        "fail_log": {
                            "err_message": f"{symbol}/{fin_type}: 분기 및 연간 데이터 모두 없음"
                        },
                    }
                )
                self.logger.log(
                    "ERROR", f"{symbol}/{fin_type}: 분기 및 연간 데이터 모두 없음"
                )
                continue

            try:
                df_latest = self.extract_recent_quarters(df_raw, symbol, fin_type)
                for _, row in df_latest.iterrows():
                    row_dict = {
                        k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()
                    }
                    row_dict = self.fill_missing_fields(row_dict, fin_type)
                    results.append(
                        {
                            "tag": fin_type,
                            "log": {"crawling_type": self.tag, "status_code": 200},
                            "df": [row_dict],
                        }
                    )
            except Exception as e:
                results.append(
                    {
                        "tag": fin_type,
                        "log": {"crawling_type": self.tag, "status_code": 500},
                        "fail_log": {
                            "err_message": f"{symbol}/{fin_type} 처리 중 오류: {str(e)}"
                        },
                    }
                )
                self.logger.log("ERROR", f"{symbol}/{fin_type} 처리 중 오류: {str(e)}")

        return results

    def extract_recent_quarters(
        self, df: pd.DataFrame, symbol: str, financial_type: str
    ) -> pd.DataFrame:
        if df.empty:
            raise DataNotFoundException(
                f"[{financial_type} 원본 데이터가 비어 있음", source=symbol
            )

        df = df.T.reset_index().rename(columns={"index": "posted_at"})
        df["Symbol"] = symbol
        df["posted_at"] = pd.to_datetime(df["posted_at"])
        df["financial_type"] = financial_type

        return (
            df.sort_values("posted_at", ascending=False).head(5).reset_index(drop=True)
        )

    def fill_missing_fields(self, row: dict, section: str) -> dict:
        fields = SECTION_FIELDS.get(section, set())

        if "Cash Equivalents" in fields and (row.get("Cash Equivalents") is None):
            ceq = row.get("Cash And Cash Equivalents")
            if ceq is not None:
                row["Cash Equivalents"] = ceq

        if "Cash Financial" in fields and (row.get("Cash Financial") is None):
            ce = row.get("Cash Equivalents") or row.get("Cash And Cash Equivalents")
            osti = row.get("Other Short Term Investments")
            if ce is not None and osti is not None:
                row["Cash Financial"] = ce + osti

        if "Cash Cash Equivalents And Short Term Investments" in fields and (
            row.get("Cash Cash Equivalents And Short Term Investments") is None
        ):
            ce = row.get("Cash Equivalents") or row.get("Cash And Cash Equivalents")
            osti = row.get("Other Short Term Investments")
            if ce is not None and osti is not None:
                row["Cash Cash Equivalents And Short Term Investments"] = ce + osti

        if "Cost Of Revenue" in fields and (row.get("Cost Of Revenue") is None):
            tr, gp = row.get("Total Revenue"), row.get("Gross Profit")
            if tr is not None and gp is not None:
                row["Cost Of Revenue"] = tr - gp

        if "Gross Profit" in fields and (row.get("Gross Profit") is None):
            tr, cor = row.get("Total Revenue"), row.get("Cost Of Revenue")
            if tr is not None and cor is not None:
                row["Gross Profit"] = tr - cor

        if "EBITDA" in fields and (row.get("EBITDA") is None):
            oi, rd = row.get("Operating Income"), row.get("Reconciled Depreciation")
            if oi is not None and rd is not None:
                row["EBITDA"] = oi + rd

        for field in fields:
            if field not in row:
                row[field] = None

        return row
