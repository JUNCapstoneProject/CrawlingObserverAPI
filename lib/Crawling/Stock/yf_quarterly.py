from datetime import date
import math
import yfinance as yf
import pandas as pd
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Quarterly
from lib.Logger.logger import get_logger  # Logger import 추가


class YF_Quarterly:

    def __init__(self, _company_map):
        self._missing: list[str] = []
        self._company_map = _company_map
        self.failed_tickers: dict[str, set[str]] = {}
        self.logger = get_logger("YF_Quarterly")  # Logger 인스턴스 생성

    def _add_fail(self, ticker: str, message: str):
        if message not in self.failed_tickers:
            self.failed_tickers[message] = set()
        self.failed_tickers[message].add(ticker)

    def check_missing(self) -> bool:
        with get_session() as session:
            existing = (
                session.query(Company.ticker)
                .join(
                    Stock_Quarterly,
                    Company.company_id == Stock_Quarterly.company_id,
                )
                .distinct()
            )

            existing_tickers = {r[0] for r in existing}
            missing = set(self._company_map.keys()) - existing_tickers

            if missing:
                self._missing = list(missing)
                return True

            return False

    def fetch_fundamentals(self, ticker: str) -> Optional[dict[date, dict]]:
        try:
            tkr = yf.Ticker(ticker)
            shares_dict = self._get_quarterly_shares(tkr)
            if not shares_dict:
                return None

            info = tkr.info
            eps = info.get("trailingEps")
            per = info.get("trailingPE")
            dividend_yield = info.get("dividendYield") or 0.0

            if (
                per is None
                or isinstance(per, str)
                or not isinstance(per, (int, float))
                or math.isinf(per)
                or math.isnan(per)
            ):
                per = None

            result = {
                q_date: {
                    "shares": shares,
                    "eps": eps,
                    "per": per,
                    "dividend_yield": dividend_yield,
                }
                for q_date, shares in shares_dict.items()
            }

            return result or None

        except Exception as e:
            self._add_fail(ticker, f"fundamentals 수집 실패: {e}")
            return None

    def _get_quarterly_shares(self, tkr) -> Optional[dict[date, int]]:
        try:
            df = tkr.get_shares_full(start="2015-01-01", end=str(date.today()))
            if df is None or df.empty:
                return None

            if isinstance(df, pd.Series):
                df = df.to_frame(name="Shares")

            df = df[~df.index.duplicated(keep="first")]
            df.index = df.index.tz_localize(None)
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Date"}, inplace=True)

            from pandas.tseries.offsets import QuarterEnd

            df["quarter"] = df["Date"].apply(lambda x: (x + QuarterEnd(0)).date())

            df.sort_values("Date", inplace=True)
            quarter_grouped = df.groupby("quarter").last()

            result = {}
            for q_date, row in quarter_grouped.iterrows():
                shares = row["Shares"]
                if pd.notna(shares):
                    result[q_date] = int(shares)

            return result if result else None

        except Exception as e:
            self._add_fail(tkr.ticker, f"shares 추출 실패: {e}")
            return None

    def crawl(self):
        try:
            today = date.today()

            if today.day != 2 and not self.check_missing():
                self.logger.debug("모든 분기 데이터가 존재함")
                return

            target = self._missing or list(self._company_map.keys())
            self.logger.debug(f"분기 데이터 수집 시작 - {len(target)} 종목")

            records = []

            def crawl_one(ticker: str) -> list[Stock_Quarterly]:
                data = self.fetch_fundamentals(ticker)
                if not data:
                    self._add_fail(ticker, "분기 데이터 없음")
                    return []

                company = self._company_map.get(ticker)
                if not company:
                    self._add_fail(ticker, "company 정보 없음")
                    return []

                records = []
                for q_date, values in data.items():
                    shares = values.get("shares")
                    if shares is None:
                        self._add_fail(ticker, f"shares 없음 - {q_date}")
                        continue

                    record = Stock_Quarterly(
                        company_id=company["company_id"],
                        shares=shares,
                        eps=values.get("eps"),
                        per=values.get("per"),
                        dividend_yield=values.get("dividend_yield"),
                        posted_at=q_date,
                    )
                    records.append(record)

                if not records:
                    self._add_fail(ticker, f"모든 분기 데이터가 유효하지 않음")

                return records

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(crawl_one, ticker): ticker for ticker in target
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        records.extend(result)

            if not records:
                self.logger.warning("저장할 데이터 없음")
                return

            try:
                with get_session() as session:
                    # 테이블 전체 비우기
                    session.execute(text("TRUNCATE TABLE stock_quarterly"))

                    # 새로 수집한 전체 데이터를 삽입
                    session.bulk_save_objects(records)
                    session.commit()
                self.logger.debug(f"분기 데이터 저장 완료 - {len(records)}건")

            except Exception as e:
                self.logger.error(f"분기 데이터 저장 중 예외 발생: {e}")

        except Exception as e:
            raise RuntimeError(f"분기데이터 수집 중 예외 발생: {e}")
