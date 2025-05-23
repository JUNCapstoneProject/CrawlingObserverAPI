from lib.Crawling.Stock.YFinance_stock import YFinanceStockCrawler
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Daily
from lib.Distributor.secretary.models.stock import Stock_Quarterly
from lib.Logger.logger import Logger


from sqlalchemy import func
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed


class YF_Daily(YFinanceStockCrawler):

    def __init__(self, _company_map):
        self.logger = Logger(self.__class__.__name__)
        self._missing: list[str] = []
        self._shares_map = self._load_shares_map()
        self._company_map = _company_map

    def _load_shares_map(self) -> dict[str, int]:
        with get_session() as session:
            rows = (
                session.query(Company.ticker, Stock_Quarterly.shares)
                .join(Stock_Quarterly, Company.company_id == Stock_Quarterly.company_id)
                .filter(Stock_Quarterly.shares.isnot(None))
                .all()
            )
            return {ticker: shares for ticker, shares in rows}

    def get_previous_trading_day(self):
        try:
            df = yf.Ticker("AAPL").history(period="7d", interval="1d")
            if df.empty:
                return None
            return df.index[-1].date().isoformat()
        except Exception as e:
            self.logger.log("ERROR", f"전 거래일 계산 실패: {e}")
            return None

    def check_missing(self, prev_day) -> bool:
        if not prev_day:
            self.logger.log("DEBUG", "전 거래일 판단 불가 → 전체 수집")
            self._missing = list(self._company_map.keys())
            return True

        try:
            with get_session() as session:
                existing = (
                    session.query(Company.ticker)
                    .join(Stock_Daily, Company.company_id == Stock_Daily.company_id)
                    .filter(func.date(Stock_Daily.posted_at) == prev_day)
                    .distinct()
                )
                updated = {row[0] for row in existing}
                all_tickers = set(self._company_map.keys())
                self._missing = list(all_tickers - updated)

                if self._missing:
                    return True
                else:
                    self.logger.log("DEBUG", "모든 종목의 일간 데이터가 존재")
                    return False
        except Exception as e:
            self.logger.log("ERROR", f"일간 데이터 확인 중 오류: {e}")
            return True

    def crawl(self):
        prev_day = self.get_previous_trading_day()

        if not self.check_missing(prev_day):
            self.logger.log("DEBUG", "일간 데이터 수집 완료")
            return

        if not prev_day:
            self.logger.log("ERROR", "기준 거래일 없음 - 수집 중단")
            return

        self.logger.log("DEBUG", f"일간 데이터 수집 시작 - {len(self._missing)} 종목")

        # ✅ yf.download 사용
        try:
            df = yf.download(
                tickers=self._missing,
                period="7d",
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

        for ticker in self._missing:
            try:
                if ticker not in df.columns.levels[0]:
                    self.logger.log("WARN", f"{ticker} 데이터 없음 → 스킵")
                    continue

                df_tkr = df[ticker]
                row = df_tkr.loc[df_tkr.index.strftime("%Y-%m-%d") == prev_day]
                if row.empty:
                    self.logger.log("WARN", f"{ticker}의 {prev_day} 데이터 없음 → 스킵")
                    continue

                adj_close = row["Close"].iloc[0]
                shares = self._shares_map.get(ticker.upper())
                if shares is None or not shares or adj_close is None:
                    self.logger.log(
                        "WARN", f"{ticker} shares 또는 adj_close 없음 → 스킵"
                    )
                    continue

                market_cap = round(adj_close * shares)
                company = self._company_map.get(ticker)
                if not company:
                    self.logger.log("WARN", f"{ticker} 회사 정보 없음 - 스킵")
                    continue

                record = Stock_Daily(
                    company_id=company["company_id"],
                    adj_close=float(adj_close),
                    market_cap=int(market_cap),
                    posted_at=prev_day,
                )
                records.append(record)

            except Exception as e:
                self.logger.log("ERROR", f"{ticker} 처리 실패: {e}")

        try:
            with get_session() as session:
                session.bulk_save_objects(records)
                session.commit()
            self.logger.log("DEBUG", f"일간 데이터 벌크 저장 완료 - {len(records)} 건")
        except Exception as e:
            self.logger.log("ERROR", f"벌크 저장 실패: {e}")
