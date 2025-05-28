from datetime import date
import math
import yfinance as yf
import pandas as pd
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.Crawling.Stock.YFinance_stock import YFinanceStockCrawler
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Quarterly
from lib.Logger.logger import Logger  # Logger import 추가


class YF_Quarterly(YFinanceStockCrawler):

    def __init__(self, _company_map):
        self._missing: list[str] = []
        self._company_map = _company_map
        self.logger = Logger("YF_Quarterly")  # Logger 인스턴스 생성

    def check_missing(self) -> bool:
        try:
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

                self.logger.log("DEBUG", "모든 종목의 분기 데이터가 존재")
                return False

        except Exception as e:
            self.logger.log("ERROR", f"분기 데이터 확인 중 오류: {e}")
            return True

    def crawl(self):
        try:
            today = date.today()

            if today.day != 2 and not self.check_missing():
                self.logger.log("DEBUG", "분기 데이터 수집 완료")
                return

            target = self._missing or list(self._company_map.keys())
            self.logger.log("DEBUG", f"분기 데이터 수집 시작 - {len(target)} 종목")

            records = []

            def crawl_one(ticker: str) -> list[Stock_Quarterly]:
                data = self.fetch_fundamentals(ticker)
                if not data:
                    self.logger.log("WARN", f"{ticker} 분기 데이터 없음")
                    return []

                company = self._company_map.get(ticker)
                if not company:
                    self.logger.log("WARN", f"{ticker}에 대한 company 정보 없음")
                    return []

                records = []
                for q_date, values in data.items():
                    shares = values.get("shares")
                    if shares is None:
                        self.logger.log("WARN", f"{ticker} - {q_date} shares 없음")
                        continue

                    record = Stock_Quarterly(
                        company_id=company["company_id"],
                        shares=shares,
                        eps=values.get("eps"),
                        per=values.get("per"),
                        dividend_yield=values.get("dividend_yield"),
                        posted_at=q_date,
                    )
                    self.logger.log("DEBUG", f"{ticker} - record 생성 완료: {record}")
                    records.append(record)

                if not records:
                    self.logger.log(
                        "WARN", f"{ticker}의 모든 분기 데이터가 유효하지 않음"
                    )

                return records

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(crawl_one, ticker): ticker for ticker in target
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        records.extend(result)

            try:
                with get_session() as session:
                    session.bulk_save_objects(records)
                    session.commit()
                self.logger.log(
                    "DEBUG", f"분기 데이터 벌크 저장 완료 - {len(records)}건"
                )
            except Exception as e:
                self.logger.log("ERROR", f"벌크 저장 실패: {e}")

        except Exception as e:
            self.logger.log("ERROR", f"분기데이터 수집 중 예외 발생: {e}")

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
            self.logger.log("ERROR", f"{ticker} fundamentals 수집 실패: {e}")
            return None

    def _get_recent_quarter_ends(self, base_date: date, count=4) -> list[date]:
        quarters = []
        year = base_date.year
        month = base_date.month

        while len(quarters) < count:
            if month >= 10:
                quarter_end = date(year, 9, 30)
            elif month >= 7:
                quarter_end = date(year, 6, 30)
            elif month >= 4:
                quarter_end = date(year, 3, 31)
            else:
                quarter_end = date(year - 1, 12, 31)

            quarters.append(quarter_end)
            month = quarter_end.month - 3
            if month <= 0:
                month += 12
                year -= 1

        return quarters

    def _get_quarterly_shares(self, tkr) -> Optional[dict[date, int]]:
        try:
            df = tkr.get_shares_full(start="2019-01-01", end=str(date.today()))
            if df is None or df.empty:
                return None

            if isinstance(df, pd.Series):
                df = df.to_frame(name="Shares")

            df = df[~df.index.duplicated(keep="first")]
            df.index = df.index.tz_localize(None)
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Date"}, inplace=True)

            result = {}
            quarter_ends = self._get_recent_quarter_ends(date.today(), count=4)

            for qd in quarter_ends:
                df["date_diff"] = df["Date"].apply(
                    lambda x: abs((x - pd.Timestamp(qd)).days)
                )
                nearest_row = df.loc[df["date_diff"].idxmin()]

                if nearest_row["date_diff"] > 30:
                    continue

                shares = nearest_row["Shares"]
                if pd.notna(shares):
                    result[qd] = int(shares)

            return result if result else None

        except Exception as e:
            return None
