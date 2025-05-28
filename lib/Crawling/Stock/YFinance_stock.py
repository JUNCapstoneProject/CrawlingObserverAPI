from __future__ import annotations

# ─── Built-in Modules ─────────────────────────────────────────────────────
import time
from typing import List, Optional, Dict
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
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.session import get_session
from lib.Exceptions.exceptions import (
    BatchProcessingException,
    CrawlerException,
    DataNotFoundException,
)
from lib.Config.config import Config
from lib.Logger.logger import Logger
from lib.Crawling.utils.yfhandler import _YFForwardHandler

session = curl_requests.Session(impersonate="chrome")


def get_company_map_from_db(limit: Optional[int] = None) -> Dict[str, dict]:
    with get_session() as session:
        q = session.query(Company.ticker, Company.company_id, Company.cik).order_by(
            Company.company_id.asc()
        )
        if limit:
            q = q.limit(limit)

        return {
            ticker.upper(): {"company_id": company_id, "cik": cik}
            for ticker, company_id, cik in q.all()
            if cik
        }


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
        self.logger = Logger(self.__class__.__name__)

        # Configure yfinance logger
        yf_logger = logging.getLogger("yfinance")
        yf_logger.handlers.clear()
        yf_logger.setLevel(logging.INFO)
        yf_logger.addHandler(_YFForwardHandler(self.logger))
        yf_logger.propagate = False

    def _refresh_price_data_cache(self):
        from .yf_quarterly import YF_Quarterly
        from .yf_daily import YF_Daily
        from .yf_market import MarketDataManager

        self.logger.log("DEBUG", "분기 및 일간 데이터 확인 시작")

        try:
            manager = MarketDataManager()
            manager.update_all()
        except Exception as e:
            self.logger.log("ERROR", f"[MarketDataManager]: {e}")

        try:
            YF_Quarterly(self._company_map).crawl()
        except Exception as e:
            self.logger.log("ERROR", f"[YF_Quarterly]: {e}")

        try:
            YF_Daily(self._company_map).crawl()
        except Exception as e:
            self.logger.log("ERROR", f"[YF_Daily]: {e}")

    def crawl(self):
        try:
            self._refresh_price_data_cache()
            self._adj_map = self.get_adj_close_map()
        except Exception as e:
            self.logger.log("ERROR", f"{e}")

        tickers = list(self._company_map.keys())
        if not tickers:
            return None

        batches = [
            tickers[i : i + self.batch_size]
            for i in range(0, len(tickers), self.batch_size)
        ]
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self._crawl_batch, batch): batch
                for i, batch in enumerate(batches)
            }

            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    results.extend(future.result())
                except BatchProcessingException as e:
                    for sym in batch:
                        results.append(
                            {
                                "tag": self.tag,
                                "log": {
                                    "crawling_type": self.tag,
                                    "status_code": e.status_code,
                                },
                                "fail_log": {"err_message": str(e)},
                            }
                        )
                except Exception as e:
                    for sym in batch:
                        results.append(
                            {
                                "tag": self.tag,
                                "log": {"crawling_type": self.tag, "status_code": 500},
                                "fail_log": {"err_message": f"Batch error: {e}"},
                            }
                        )

        for result in results:
            if "log" in result:
                result["log"]["target_url"] = "yfinance_library"
        return results

    def _crawl_batch(self, batch: List[str]):
        batch_results = []

        for ticker in batch:
            try:
                company = self._company_map.get(ticker)
                if not company:
                    raise DataNotFoundException("company_map 누락", source=ticker)

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

            except CrawlerException as e:
                batch_results.append(
                    {
                        "tag": self.tag,
                        "log": {
                            "crawling_type": self.tag,
                            "status_code": e.status_code,
                        },
                        "fail_log": {"err_message": str(e)},
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

            time.sleep(random.uniform(0.1, 0.4))

        return batch_results

    def _process_minute_data(self, stock, ticker, company_id) -> pd.DataFrame:
        """Processes minute-level stock data with company_id, full-day volume, and change."""
        df_min = stock.history(period="1d", interval="1m", prepost=True)[
            ["Open", "High", "Low", "Close", "Volume"]
        ]

        if df_min.empty:
            raise DataNotFoundException(
                "Empty DataFrame (No trading data)", source=ticker
            )

        # ✅ 하루 누적 거래량
        volume_sum = int(df_min["Volume"].sum())

        # ✅ 가장 마지막 시점의 OHLC만 사용
        latest = df_min.tail(1).reset_index()
        latest.rename(columns={"Datetime": "posted_at"}, inplace=True)

        # ✅ 회사 ID 포함
        latest["company_id"] = company_id

        # ✅ Change 계산
        adj_close = self._adj_map.get(company_id)
        latest["Change"] = 0.0
        if adj_close and not latest["Close"].isna().all():
            try:
                latest["Change"] = round(
                    (latest["Close"].iloc[0] / adj_close - 1) * 100, 2
                )
            except ZeroDivisionError:
                latest["Change"] = 0.0

        # ✅ 누적 Volume 반영
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
