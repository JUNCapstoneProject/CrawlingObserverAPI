from datetime import date
from sqlalchemy import func
import yfinance as yf
import pandas as pd

from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Daily, Stock_Quarterly
from lib.Logger.logger import get_logger


class YF_Daily:
    def __init__(self, _company_map):
        self.logger = get_logger(self.__class__.__name__)
        self._company_map = _company_map
        self._missing: list[str] = []
        self._shares_map = {}
        self.failed_tickers: dict[str, set[str]] = {}

    def _add_fail(self, ticker: str, message: str):
        if message not in self.failed_tickers:
            self.failed_tickers[message] = set()
        self.failed_tickers[message].add(ticker)

    def _load_shares_map(self) -> dict[str, dict[date, int]]:
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
                shares_map.setdefault(ticker, {})[posted_at] = shares
            return shares_map

    def _get_quarter_shares_for_date(
        self, ticker: str, target_date: date
    ) -> int | None:
        ticker = ticker.upper()
        shares_dict = self._shares_map.get(ticker)
        if not shares_dict:
            return None

        date_key_map = {
            k if isinstance(k, date) and not hasattr(k, "hour") else k.date(): k
            for k in shares_dict
        }

        sorted_dates = sorted(date_key_map.keys())
        for i, start_date in enumerate(sorted_dates):
            end_date = sorted_dates[i + 1] if i + 1 < len(sorted_dates) else date.max
            if start_date <= target_date < end_date:
                return shares_dict[date_key_map[start_date]]

        if target_date >= sorted_dates[-1]:
            return shares_dict[date_key_map[sorted_dates[-1]]]

        return None

    def get_previous_trading_day(self) -> str:
        df = yf.Ticker("AAPL").history(period="7d", interval="1d")
        return df.index[-1].date().isoformat() if not df.empty else None

    def check_missing(self, prev_day: str) -> list[str]:
        try:
            with get_session() as session:
                recent_rows = (
                    session.query(Company.ticker)
                    .join(Stock_Daily, Company.company_id == Stock_Daily.company_id)
                    .filter(func.date(Stock_Daily.posted_at) == prev_day)
                    .distinct()
                    .all()
                )
                recent_updated = {row[0] for row in recent_rows}
                self._missing = [
                    t for t in self._company_map if t not in recent_updated
                ]
                return self._missing
        except Exception:
            self._missing = list(self._company_map.keys())
            return self._missing

    def crawl(self):
        try:
            prev_day = self.get_previous_trading_day()
            self._shares_map = self._load_shares_map()
            if not prev_day:
                self.logger.warning("기준 거래일 없음")
                return

            missing = self.check_missing(prev_day)
            if not missing:
                self.logger.debug("모든 일간 데이터가 존재함")
                return

            self.logger.debug(f"일간 데이터 수집 시작 - {len(missing)} 종목")

            try:
                df = yf.download(
                    tickers=missing,
                    period="10y",
                    interval="1d",
                    auto_adjust=True,
                    group_by="ticker",
                    threads=True,
                    progress=False,
                )
            except Exception as e:
                raise RuntimeError(f"yf.download 실패: {e}")

            all_records = []

            for ticker in missing:
                if ticker not in df.columns.levels[0]:
                    self._add_fail(ticker, "데이터 없음")
                    continue

                df_tkr = df[ticker].dropna(subset=["Close", "Open", "High", "Low"])
                df_tkr = df_tkr[~df_tkr.index.duplicated(keep="last")]

                company = self._company_map.get(ticker)
                if not company:
                    self._add_fail(ticker, "회사 정보 없음")
                    continue

                for dt, row in df_tkr.iterrows():
                    shares = self._get_quarter_shares_for_date(ticker, dt.date())
                    if shares is None:
                        self._add_fail(ticker, "shares 없음")
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
                        all_records.append(record)
                    except Exception as e_inner:
                        self._add_fail(ticker, f"{dt.date()} 처리 실패: {e_inner}")

            if not all_records:
                self.logger.warning("저장할 데이터 없음")
                return

            # 중복 제거 (company_id + posted_at 기준)
            with get_session() as session:
                existing = set(
                    session.query(Stock_Daily.company_id, Stock_Daily.posted_at)
                    .filter(
                        Stock_Daily.posted_at.in_([r.posted_at for r in all_records])
                    )
                    .all()
                )

            filtered_records = [
                r for r in all_records if (r.company_id, r.posted_at) not in existing
            ]

            try:
                with get_session() as session:
                    session.bulk_save_objects(filtered_records)
                    session.commit()
                self.logger.debug(f"일간 데이터 저장 완료 - {len(filtered_records)} 건")
            except Exception as e_db:
                self.logger.error(f"일간 데이터 저장 중 예외 발생: {e_db}")

            if self.failed_tickers:
                total = sum(len(tickers) for tickers in self.failed_tickers.values())
                self.logger.warning(
                    f"총 {total}개 일간 데이터 수집 실패:\n"
                    + "\n".join(
                        f"{reason}:\n"
                        + "\n".join(
                            ", ".join(sorted_tickers[i : i + 10])
                            for i in range(0, len(sorted_tickers), 10)
                        )
                        for reason, tickers in self.failed_tickers.items()
                        for sorted_tickers in [sorted(tickers)]
                    )
                )

        except Exception as e:
            raise RuntimeError(f"일간 데이터 수집 중 예외 발생: {e}")
