import yfinance as yf
import pandas as pd
import time
import concurrent.futures
from typing import List

from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Exceptions.exceptions import *
from lib.Config.config import Config
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.session import get_session


# ✅ 재무제표 종류별 필드 매핑
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
        self.symbols = get_symbols_from_db(limit=Config.get("symbol_size", 5))
        self.tag = "financials"

    def crawl(self):
        results = []

        for start in range(0, len(self.symbols), self.batch_size):
            batch = self.symbols[start : start + self.batch_size]

            try:
                tickers = yf.Tickers(" ".join(batch))
            except Exception:
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

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
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

            time.sleep(2)

        return results

    def fetch_symbol_data(self, symbol: str, tickers) -> List[dict]:
        """
        하나의 심볼(symbol)에 대해 income/balance/cashflow를 수집하고 누락 필드 보완까지 처리
        """
        results = []
        stock = tickers.tickers.get(symbol)
        if not stock:
            raise DataNotFoundException(f"{symbol}: 종목 데이터 없음", source=symbol)

        for fin_type, accessor in [
            ("income_statement", lambda s: s.quarterly_financials),
            ("balance_sheet", lambda s: s.quarterly_balance_sheet),
            ("cash_flow", lambda s: s.quarterly_cashflow),
        ]:
            try:
                df_raw = accessor(stock)
                df_latest = self.extract_recent_quarters(df_raw, symbol, fin_type)

                for _, row in df_latest.iterrows():
                    row_dict = {
                        k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()
                    }
                    row_dict = self.fill_missing_fields(row_dict, fin_type)

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

            except Exception as e:
                results.append(
                    {
                        "tag": fin_type,
                        "log": {
                            "crawling_type": self.tag,
                            "status_code": 500,
                        },
                        "fail_log": {
                            "err_message": f"{symbol}/{fin_type} 처리 중 오류: {str(e)}"
                        },
                    }
                )
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

    def fill_missing_fields(self, row: dict, section: str) -> dict:
        """
        section (balance_sheet, income_statement, cash_flow) 별로 필요한 필드만 계산해서 채운다.
        """

        fields = SECTION_FIELDS.get(section, set())

        # Cash Equivalents
        if "Cash Equivalents" in fields:
            if "Cash Equivalents" not in row or row["Cash Equivalents"] is None:
                cash_and_cash_eq = row.get("Cash And Cash Equivalents")
                if cash_and_cash_eq is not None:
                    row["Cash Equivalents"] = cash_and_cash_eq

        # Cash Financial
        if "Cash Financial" in fields:
            if "Cash Financial" not in row or row["Cash Financial"] is None:
                ce = row.get("Cash Equivalents") or row.get("Cash And Cash Equivalents")
                osti = row.get("Other Short Term Investments")
                if ce is not None and osti is not None:
                    row["Cash Financial"] = ce + osti

        # Cash Cash Equivalents And Short Term Investments
        if "Cash Cash Equivalents And Short Term Investments" in fields:
            if (
                "Cash Cash Equivalents And Short Term Investments" not in row
                or row["Cash Cash Equivalents And Short Term Investments"] is None
            ):
                ce = row.get("Cash Equivalents") or row.get("Cash And Cash Equivalents")
                osti = row.get("Other Short Term Investments")
                if ce is not None and osti is not None:
                    row["Cash Cash Equivalents And Short Term Investments"] = ce + osti

        # Cost Of Revenue
        if "Cost Of Revenue" in fields:
            if "Cost Of Revenue" not in row or row["Cost Of Revenue"] is None:
                tr = row.get("Total Revenue")
                gp = row.get("Gross Profit")
                if tr is not None and gp is not None:
                    row["Cost Of Revenue"] = tr - gp

        # Gross Profit
        if "Gross Profit" in fields:
            if "Gross Profit" not in row or row["Gross Profit"] is None:
                tr = row.get("Total Revenue")
                cor = row.get("Cost Of Revenue")
                if tr is not None and cor is not None:
                    row["Gross Profit"] = tr - cor

        # EBITDA
        if "EBITDA" in fields:
            if "EBITDA" not in row or row["EBITDA"] is None:
                oi = row.get("Operating Income")
                rd = row.get("Reconciled Depreciation")
                if oi is not None and rd is not None:
                    row["EBITDA"] = oi + rd

        # 나머지 필드 없으면 None
        for field in fields:
            if field not in row:
                row[field] = None

        return row
