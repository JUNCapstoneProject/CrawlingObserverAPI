import yfinance as yf
from datetime import date, timedelta
from sqlalchemy import func, delete
import pandas as pd

from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Market
from lib.Logger.logger import get_logger


MARKET_INDEX_TICKER = {
    "ASE": "^XAX",  # NYSE American (AMEX)
    "BTS": "^DJI",  # Bulletin Board → 대체로 다우존스 사용
    "NCM": "^IXIC",  # NASDAQ Capital Market
    "NGM": "^IXIC",  # NASDAQ Global Market
    "NMS": "^IXIC",  # NASDAQ Global Select
    "NYQ": "^NYA",  # NYSE
    "OEM": "^DJI",  # OTC Emerging Markets
    "OQB": "^DJI",  # OTCQB
    "OQX": "^DJI",  # OTCQX
    "PCX": "^XAX",  # NYSE Arca (PCX)
    "PNK": "^DJI",  # OTC Pink
}


class YF_Market:
    def __init__(self):
        self.market_map = self._get_market_index_tickers()
        self.logger = get_logger(self.__class__.__name__)

    def _get_market_index_tickers(self) -> dict[str, str]:
        with get_session() as session:
            markets = (
                session.query(Company.market)
                .filter(Company.market.isnot(None))
                .distinct()
                .all()
            )
        valid = {m[0] for m in markets if m[0] in MARKET_INDEX_TICKER}
        return {m: MARKET_INDEX_TICKER[m] for m in valid}

    def get_recent_trading_day(self) -> date | None:
        try:
            df = yf.Ticker("^IXIC").history(period="7d", interval="1d")
            if df.empty:
                return None
            return df.index[-1].date()
        except Exception as e:
            raise RuntimeError(f"최근 거래일 확인 실패: {e}")

    def check_missing_symbols(self, days=30) -> list[str]:
        try:
            recent_day = self.get_recent_trading_day()
        except Exception as e:
            raise

        if not recent_day:
            return list(self.market_map.keys())

        cutoff = date.today() - timedelta(days=days + 1)

        try:
            with get_session() as session:
                recent = (
                    session.query(Stock_Market.symbol)
                    .filter(func.date(Stock_Market.date) == recent_day)
                    .distinct()
                    .all()
                )
                complete = (
                    session.query(Stock_Market.symbol, func.count(Stock_Market.date))
                    .filter(Stock_Market.date >= cutoff)
                    .group_by(Stock_Market.symbol)
                    .having(func.count(Stock_Market.date) >= days)
                    .all()
                )

            recent_symbols = {r[0] for r in recent}
            complete_symbols = {r[0] for r in complete}
            all_symbols = set(self.market_map.keys())

            return [
                sym
                for sym in all_symbols
                if sym not in recent_symbols or sym not in complete_symbols
            ]
        except Exception as e:
            raise RuntimeError(f"데이터 확인 실패: {e}")

    def download_and_save_all(self, market_codes: list[str], days: int = 30):
        missing_data_codes = set()
        total_saved = 0  # ← 누적 저장 건수

        try:
            for market_code in market_codes:
                index_symbol = self.market_map[market_code]
                df = yf.Ticker(index_symbol).history(period=f"{days}d", interval="1d")

                if df.empty:
                    missing_data_codes.add(market_code)
                    continue

                df = df.dropna(subset=["Open", "Close", "High", "Low"])

                records = []
                for dt, row in df.iterrows():
                    records.append(
                        Stock_Market(
                            symbol=market_code,
                            date=dt.to_pydatetime(),
                            open=round(row["Open"], 2),
                            close=round(row["Close"], 2),
                            adj_close=round(row.get("Adj Close", row["Close"]), 2),
                            high=round(row["High"], 2),
                            low=round(row["Low"], 2),
                            volume=(
                                int(row["Volume"])
                                if not pd.isna(row["Volume"])
                                else None
                            ),
                        )
                    )

                with get_session() as session:
                    session.execute(
                        delete(Stock_Market).where(Stock_Market.symbol == market_code)
                    )
                    session.bulk_save_objects(records)
                    session.commit()

                total_saved += len(records)  # ← 누적

            if missing_data_codes:
                self.logger.warning(
                    f"총 {len(missing_data_codes)}개 시장 데이터 없음:\n"
                    + "\n".join(
                        ", ".join(group)
                        for i in range(0, len(missing_data_codes), 10)
                        for group in [missing_data_codes[i : i + 10]]
                    )
                )
            self.logger.debug(f"시장 데이터 저장 완료 - 총 {total_saved}건")

        except Exception as e:
            raise RuntimeError(f"{market_code} 처리 중 오류: {e}")

    def crawl(self, days: int = 30):
        targets = self.check_missing_symbols(days)
        if not targets:
            self.logger.debug(f"모든 시장 데이터가 존재함")

        self.logger.debug(f"시장 데이터 캐싱 시작 - {len(targets)}개")

        self.download_and_save_all(targets, days)
