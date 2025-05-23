from datetime import date
import math
import yfinance as yf
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.Crawling.Stock.YFinance_stock import YFinanceStockCrawler
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.models.stock import Stock_Quarterly
from lib.Logger.logger import Logger


class YF_Quarterly(YFinanceStockCrawler):

    def __init__(self, _company_map):
        self.logger = Logger(self.__class__.__name__)
        self._missing: list[str] = []
        self._company_map = _company_map

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

            # ❗조건 판단: 매월 2일이거나 누락 종목 존재 시만 실행
            if today.day != 2 and not self.check_missing():
                self.logger.log("DEBUG", "분기 데이터 수집 완료")
                return

            target = self._missing or list(self._company_map.keys())
            self.logger.log("DEBUG", f"분기 데이터 수집 시작 - {len(target)} 종목")

            # success_count = 0
            # total = len(target)

            records = []

            def crawl_one(ticker: str) -> Optional[Stock_Quarterly]:
                data = self.fetch_fundamentals(ticker)
                if not data:
                    self.logger.log("ERROR", f"{ticker} 분기 데이터 수집 실패")
                    return None

                company = self._company_map.get(ticker)
                if not company:
                    self.logger.log("WARN", f"{ticker}에 대한 company 정보 없음")
                    return None

                if data.get("shares") is None:
                    self.logger.log("WARN", f"{ticker} → 발행 주식수 누락")
                    return None

                return Stock_Quarterly(
                    company_id=company["company_id"],
                    shares=data["shares"],
                    eps=data["eps"],
                    per=data["per"],
                    dividend_yield=data["dividend_yield"],
                )

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(crawl_one, ticker): ticker for ticker in target
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        records.append(result)

            # DB에 벌크 저장
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

    def fetch_fundamentals(self, ticker: str) -> Optional[dict]:
        try:
            tkr = yf.Ticker(ticker)

            shares = self._get_shares_outstanding(tkr)
            if not shares:
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

            return {
                "shares": int(shares),
                "eps": eps,
                "per": per,
                "dividend_yield": dividend_yield,
            }

        except Exception as e:
            self.logger.log("ERROR", f"{ticker} fundamentals 수집 실패: {e}")
            return None

    def _get_shares_outstanding(self, tkr) -> Optional[int]:
        try:
            shares = None
            if hasattr(tkr, "fast_info"):
                shares = tkr.fast_info.get("sharesOutstanding")
            if not shares:
                try:
                    shares = tkr.get_info()["sharesOutstanding"]
                except AttributeError:
                    shares = tkr.info.get("sharesOutstanding")
            if shares:
                return int(shares)
            else:
                self.logger.log("WARN", f"{tkr.ticker} - shares 조회 실패")
        except Exception as e:
            self.logger.log("WARN", f"{tkr.ticker} - shares 조회 실패: {e}")
        return None
