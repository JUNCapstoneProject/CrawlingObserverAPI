from datetime import date, timedelta
from sqlalchemy import func
import yfinance as yf
import pandas as pd

from lib.Crawling.Stock.YFinance_stock import YFinanceStockCrawler
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Daily, Stock_Quarterly

from lib.Logger.logger import Logger  # Logger import


class YF_Daily(YFinanceStockCrawler):

    def __init__(self, _company_map):
        self.logger = Logger(self.__class__.__name__)
        self._missing: list[str] = []
        self._shares_map = self._load_shares_map()  # dict[str, dict[date, int]]
        self._company_map = _company_map

    def _load_shares_map(self) -> dict[str, dict[date, int]]:
        """
        티커별로 분기별 shares 정보를 날짜(key)-shares(value) 딕셔너리로 반환
        """
        with get_session() as session:
            rows = (
                session.query(
                    Company.ticker, Stock_Quarterly.posted_at, Stock_Quarterly.shares
                )
                .join(Stock_Quarterly, Company.company_id == Stock_Quarterly.company_id)
                .filter(Stock_Quarterly.shares.isnot(None))
                .order_by(Company.ticker, Stock_Quarterly.posted_at)
                .all()
            )

            shares_map = {}
            for ticker, posted_at, shares in rows:
                ticker = ticker.upper()
                if ticker not in shares_map:
                    shares_map[ticker] = {}
                shares_map[ticker][posted_at] = shares

            return shares_map

    def _get_quarter_shares_for_date(
        self, ticker: str, target_date: date
    ) -> int | None:
        """
        주어진 날짜가 속하는 분기의 shares 반환
        DB에 저장된 분기별 shares 날짜 리스트에서,
        target_date가 [posted_at_i, posted_at_i+1) 구간에 속하면 해당 shares 반환.
        가장 마지막 분기 이후면 마지막 분기 shares 반환.
        """
        ticker = ticker.upper()
        shares_dict = self._shares_map.get(ticker)
        if not shares_dict:
            return None

        # 모든 key를 date 타입으로 변환
        date_key_map = {}
        for k in shares_dict.keys():
            if isinstance(k, date) and not hasattr(k, "hour"):
                date_key_map[k] = k
            else:
                date_key_map[k.date()] = k

        sorted_dates = sorted(date_key_map.keys())
        for i, start_date in enumerate(sorted_dates):
            end_date = sorted_dates[i + 1] if i + 1 < len(sorted_dates) else date.max
            if start_date <= target_date < end_date:
                return shares_dict[date_key_map[start_date]]

        # target_date가 마지막 분기 이후면 마지막 shares 반환
        if target_date >= sorted_dates[-1]:
            return shares_dict[date_key_map[sorted_dates[-1]]]

        return None

    def get_previous_trading_day(self) -> str:
        try:
            df = yf.Ticker("AAPL").history(period="7d", interval="1d")
            if df.empty:
                return None
            return df.index[-1].date().isoformat()
        except Exception as e:
            self.logger.log("ERROR", f"전 거래일 계산 실패: {e}")
            return None

    def check_missing(self, prev_day: str) -> list[str]:
        """
        1. 최근 거래일 데이터가 없는 종목 → 무조건 수집
        2. 최근 거래일 데이터는 있지만, 1년치 데이터 부족 → 수집
        """
        one_year_ago = date.today() - timedelta(days=366)

        try:
            required_days = len(yf.Ticker("AAPL").history(period="1y", interval="1d"))
        except Exception as e:
            self.logger.log("WARN", f"거래일 수 계산 실패 → 전체 수집: {e}")
            return list(self._company_map.keys())

        try:
            with get_session() as session:
                recent_rows = (
                    session.query(Company.ticker)
                    .join(Stock_Daily, Company.company_id == Stock_Daily.company_id)
                    .filter(func.date(Stock_Daily.posted_at) == prev_day)
                    .distinct()
                )
                recent_updated = {row[0] for row in recent_rows}

                one_year_rows = (
                    session.query(Company.ticker, func.count(Stock_Daily.posted_at))
                    .join(Stock_Daily, Company.company_id == Stock_Daily.company_id)
                    .filter(Stock_Daily.posted_at >= one_year_ago)
                    .group_by(Company.ticker)
                    .having(func.count(Stock_Daily.posted_at) >= required_days)
                    .all()
                )
                one_year_complete = {r[0] for r in one_year_rows}

                result = [
                    t
                    for t in self._company_map.keys()
                    if t not in recent_updated or t not in one_year_complete
                ]
                self._missing = result
                return result

        except Exception as e:
            self.logger.log("ERROR", f"일간 데이터 확인 중 오류: {e}")
            self._missing = list(self._company_map.keys())
            return self._missing

    def crawl(self):
        prev_day = self.get_previous_trading_day()

        if not prev_day:
            self.logger.log("ERROR", "기준 거래일 없음 - 수집 중단")
            return

        missing = self.check_missing(prev_day)

        if not missing:
            self.logger.log("DEBUG", "모든 종목의 일간 데이터가 존재 → 종료")
            return

        self.logger.log("DEBUG", f"일간 데이터 수집 시작 - {len(missing)} 종목")

        try:
            df = yf.download(
                tickers=missing,
                period="1y",
                interval="1d",
                auto_adjust=True,
                group_by="ticker",
                threads=True,
                progress=False,
            )
        except Exception as e:
            self.logger.log("ERROR", f"yf.download 실패: {e}")
            return

        records = []

        for ticker in missing:
            try:
                if ticker not in df.columns.levels[0]:
                    self.logger.log("WARN", f"{ticker} 데이터 없음 → 스킵")
                    continue

                df_tkr = df[ticker].dropna(subset=["Close", "Open", "High", "Low"])
                company = self._company_map.get(ticker)
                if not company:
                    self.logger.log("WARN", f"{ticker} 회사 정보 없음 - 스킵")
                    continue

                for dt, row in df_tkr.iterrows():
                    shares = self._get_quarter_shares_for_date(ticker, dt.date())
                    if shares is None:
                        self.logger.log(
                            "WARN", f"{ticker} {dt.date()} 분기 shares 없음 - 스킵"
                        )
                        continue

                    try:
                        market_cap = round(row["Close"] * shares)
                        record = Stock_Daily(
                            company_id=company["company_id"],
                            open=row["Open"],
                            close=row["Close"],
                            adj_close=row.get("Adj Close", row["Close"]),
                            high=row["High"],
                            low=row["Low"],
                            volume=(
                                int(row["Volume"])
                                if not pd.isna(row["Volume"])
                                else None
                            ),
                            market_cap=int(market_cap),
                            posted_at=dt.date(),
                        )
                        records.append(record)
                    except Exception as e_inner:
                        self.logger.log(
                            "WARN", f"{ticker} {dt.date()} 처리 오류: {e_inner}"
                        )

            except Exception as e:
                self.logger.log("ERROR", f"{ticker} 처리 실패: {e}")

        try:
            with get_session() as session:
                session.bulk_save_objects(records)
                session.commit()
            self.logger.log("DEBUG", f"일간 데이터 벌크 저장 완료 - {len(records)} 건")
        except Exception as e:
            self.logger.log("ERROR", f"벌크 저장 실패: {e}")
