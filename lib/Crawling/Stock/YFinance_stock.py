from __future__ import annotations

# ─── Built-in Modules ─────────────────────────────────────────────────────
import time
from typing import List
import random

# ─── Third-party Modules ─────────────────────────────────────────────────
import logging
import pandas as pd
import yfinance as yf
from curl_cffi import requests as curl_requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Project-specific Modules ────────────────────────────────────────────
from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Distributor.secretary.models.stock import Stock_Daily
from lib.Distributor.secretary.session import get_session
from lib.Config.config import Config
from lib.Crawling.utils.yfhandler import _YFForwardHandler
from lib.Crawling.utils.GetSymbols import get_company_map_from_db

session = curl_requests.Session(impersonate="chrome")


class YFinanceStockCrawler(CrawlerInterface):

    def __init__(self, name: str):
        """Initializes the crawler."""
        super().__init__(name)
        self.tag = "stock"
        self.batch_size = 30
        self.max_workers = 5
        self._company_map = get_company_map_from_db(
            Config.get("symbol_size.total", 6000)
        )
        self.failed_tickers: dict[str, str] = {}

        # Configure yfinance logger
        yf_logger = logging.getLogger("yfinance")
        yf_logger.handlers.clear()
        yf_logger.setLevel(logging.INFO)
        yf_logger.addHandler(_YFForwardHandler(self.logger))
        yf_logger.propagate = False

    def _add_fail(self, ticker: str, msg: str):
        if ticker not in self.failed_tickers:
            self.failed_tickers[ticker] = []
        self.failed_tickers[ticker].append(msg)

    def _refresh_price_data_cache(self):
        from .yf_quarterly import YF_Quarterly
        from .yf_daily import YF_Daily
        from .yf_market import YF_Market

        self.logger.debug("주가 데이터 캐싱 시작")

        crawlers = [
            YF_Market(),
            YF_Quarterly(self._company_map),
            YF_Daily(self._company_map),
        ]

        for crawler in crawlers:
            crawler.crawl()
            crawler.logger.log_summary()

        self.logger.debug("주가 데이터 캐싱 완료")

    def get_adj_close_map(self) -> dict[int, float]:
        from sqlalchemy import func, and_
        from sqlalchemy.orm import aliased

        with get_session() as session:
            # 회사별 가장 최신 날짜 추출하는 서브쿼리
            subq = (
                session.query(
                    Stock_Daily.company_id,
                    func.max(Stock_Daily.posted_at).label("latest_date"),
                )
                .group_by(Stock_Daily.company_id)
                .subquery()
            )

            # Stock_Daily 테이블을 별칭(alias)으로 설정
            sd = aliased(Stock_Daily)

            # 최신 날짜 데이터만 조인하여 추출
            rows = (
                session.query(sd.company_id, sd.adj_close)
                .join(
                    subq,
                    and_(
                        sd.company_id == subq.c.company_id,
                        sd.posted_at == subq.c.latest_date,
                    ),
                )
                .all()
            )

            # None 필터링 후 딕셔너리로 반환
            return {
                company_id: float(adj_close)
                for company_id, adj_close in rows
                if adj_close is not None
            }

    def crawl(self):
        try:
            self._refresh_price_data_cache()
            self._adj_map = self.get_adj_close_map()

            tickers = list(self._company_map.keys())
            if not tickers:
                self.logger.warning("ticker 목록 없음")
                return None

            batches = [
                tickers[i : i + self.batch_size]
                for i in range(0, len(tickers), self.batch_size)
            ]
            results = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_batch = {
                    executor.submit(self._crawl_batch, batch): batch
                    for _, batch in enumerate(batches)
                }

                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]

                    try:
                        results.extend(future.result())

                    except Exception as e:
                        for sym in batch:
                            results.append(
                                {
                                    "tag": self.tag,
                                    "log": {
                                        "crawling_type": self.tag,
                                        "status_code": 500,
                                    },
                                    "fail_log": {"err_message": f"Batch error: {e}"},
                                }
                            )

            for result in results:
                if "log" in result:
                    result["log"]["target_url"] = "yfinance_library"

            if self.failed_tickers:
                self.logger.warning(
                    f"총 {len(self.failed_tickers)}개 주가 데이터 수집 실패:\n"
                    + "\n".join(
                        f"{ticker}: {', '.join(reasons)}"
                        for ticker, reasons in sorted(self.failed_tickers.items())
                    )
                )

            return results

        except Exception as e:
            self.logger.error(f"{e}")

    def _crawl_batch(self, batch: List[str]):
        batch_results = []

        for ticker in batch:
            try:
                company = self._company_map.get(ticker)
                if not company:
                    self._add_fail(ticker, "company 정보 없음")
                    continue

                company_id = company["company_id"]
                stock = yf.Ticker(ticker, session=session)

                df_min = self._process_minute_data(stock, ticker, company_id)

                batch_results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 200},
                        "df": df_min,
                    }
                )

            except Exception as e:
                batch_results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 500},
                        "fail_log": {"err_message": f"{ticker}: {str(e)}"},
                    }
                )
                self._add_fail(ticker, str(e))

            time.sleep(random.uniform(0.1, 0.4))

        return batch_results

    def _process_minute_data(self, stock, ticker, company_id) -> pd.DataFrame:
        """Processes minute-level stock data with company_id, full-day volume, and change."""
        df_min = stock.history(period="1d", interval="1m", prepost=True)[
            ["Open", "High", "Low", "Close", "Volume"]
        ]

        if df_min.empty:
            self._add_fail(ticker, "1분 데이터 없음")
            return

        volume_sum = int(df_min["Volume"].sum())

        latest = df_min.tail(1).reset_index()
        latest.rename(columns={"Datetime": "posted_at"}, inplace=True)

        latest["company_id"] = company_id

        adj_close = self._adj_map.get(company_id)
        latest["Change"] = 0.0
        if adj_close and not latest["Close"].isna().all():
            try:
                latest["Change"] = round(
                    (latest["Close"].iloc[0] / adj_close - 1) * 100, 2
                )
            except ZeroDivisionError:
                latest["Change"] = 0.0

        latest["Volume"] = volume_sum

        return latest[
            [
                "company_id",
                "posted_at",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Change",
            ]
        ]
