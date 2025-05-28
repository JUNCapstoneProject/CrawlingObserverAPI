import yfinance as yf
from datetime import date, timedelta
from sqlalchemy import func, delete
import pandas as pd

from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Market

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


class MarketDataManager:
    def __init__(self):
        self.market_map = self._get_market_index_tickers()

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

    def download_and_save(self, market_code: str, days: int = 30):
        index_symbol = self.market_map[market_code]  # e.g., NMS → ^IXIC

        try:
            df = yf.Ticker(index_symbol).history(period=f"{days}d", interval="1d")

            if df.empty:
                return

            df = df.dropna(subset=["Open", "Close", "High", "Low"])

            records = []
            for dt, row in df.iterrows():
                records.append(
                    Stock_Market(
                        symbol=market_code,  # 지수 심볼이 아닌 market_code 저장
                        date=dt.to_pydatetime(),
                        open=round(row["Open"], 2),
                        close=round(row["Close"], 2),
                        adj_close=round(row.get("Adj Close", row["Close"]), 2),
                        high=round(row["High"], 2),
                        low=round(row["Low"], 2),
                        volume=(
                            int(row["Volume"]) if not pd.isna(row["Volume"]) else None
                        ),
                    )
                )

            with get_session() as session:
                session.execute(
                    delete(Stock_Market).where(Stock_Market.symbol == market_code)
                )
                session.bulk_save_objects(records)
                session.commit()

        except Exception as e:
            pass

    def update_all(self, days: int = 30):
        try:
            targets = self.check_missing_symbols(days)
        except Exception as e:
            raise

        for symbol in targets:
            try:
                self.download_and_save(symbol, days)
            except Exception as e:
                pass
