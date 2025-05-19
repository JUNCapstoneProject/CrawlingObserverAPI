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

    def check_missing(self) -> bool:
        prev_day = self.get_previous_trading_day()
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
        if not self.check_missing():
            self.logger.log("DEBUG", "일간 데이터 수집 완료")
            return

        prev_day = self.get_previous_trading_day()
        if not prev_day:
            self.logger.log("ERROR", "기준 거래일 없음 - 수집 중단")
            return

        self.logger.log("DEBUG", f"일간 데이터 수집 시작 - {len(self._missing)} 종목")

        success_count = 0
        total = len(self._missing)

        def crawl_one(ticker: str) -> bool:
            try:
                data = self.fetch_daily_data(ticker, prev_day)
                if not data:
                    self.logger.log("ERROR", f"{ticker} 데이터 없음")
                    return False
                self.save_to_db(ticker, data, prev_day)
                return True
            except Exception as e:
                self.logger.log("ERROR", f"{ticker} 저장 실패: {e}")
                return False

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(crawl_one, t): t for t in self._missing}
            for future in as_completed(futures):
                if future.result():
                    success_count += 1

        self.logger.log(
            "DEBUG", f"일간 데이터 수집 완료 - {success_count}/{total} 종목"
        )

    def fetch_daily_data(self, ticker: str, target_date: str):
        try:
            df = yf.Ticker(ticker).history(period="7d", interval="1d", auto_adjust=True)
            if df.empty or target_date not in df.index.strftime("%Y-%m-%d"):
                self.logger.log("WARN", f"{ticker}의 {target_date} 데이터 없음 → 스킵")
                return None

            adj_close = df[df.index.strftime("%Y-%m-%d") == target_date]["Close"].iloc[
                0
            ]
            shares = self._shares_map.get(ticker.upper())

            if shares is None or adj_close is None:
                self.logger.log("WARN", f"{ticker} shares 또는 adj_close 없음 → 스킵")
                return None

            market_cap = round(adj_close * shares)

            return {
                "adj_close": float(adj_close),
                "market_cap": int(market_cap),
            }

        except Exception as e:
            self.logger.log("ERROR", f"{ticker} 일간 데이터 수집 실패: {e}")
            return None

    def save_to_db(self, ticker: str, data: dict, posted_at: str) -> None:
        company = self._company_map.get(ticker)
        if not company:
            self.logger.log("WARN", f"{ticker}에 대한 company 정보 없음 - 저장 스킵")
            return

        company_id = company["company_id"]

        # 필수 필드 검증
        required_keys = ["adj_close", "market_cap"]
        for key in required_keys:
            if data.get(key) is None:
                self.logger.log("WARN", f"{ticker} → '{key}' 값 없음 - 저장 스킵")
                return

        try:
            with get_session() as session:
                record = Stock_Daily(
                    company_id=company_id,
                    adj_close=data["adj_close"],
                    market_cap=data["market_cap"],
                    posted_at=posted_at,
                )
                session.add(record)
                session.commit()
        except Exception as e:
            self.logger.log("ERROR", f"{ticker} 저장 실패: {e}")
