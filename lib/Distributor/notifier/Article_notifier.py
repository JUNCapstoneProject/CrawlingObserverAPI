import copy
import yfinance as yf
from sqlalchemy import text, update

from lib.Distributor.notifier.Notifier import NotifierBase
from lib.Distributor.socket.messages.request import news_item, news_requests_message
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.news import NewsTag
from lib.Distributor.secretary.models.reports import ReportTag
from lib.Crawling.config.MarketMap import MARKET_INDEX_TICKER


class ArticleNotifier(NotifierBase):
    def __init__(self):
        super().__init__("ArticleNotifier")

    def run(self):
        rows = self._fetch_unanalyzed_rows("notifier_articles_vw")
        if not rows:
            self.logger.info("처리할 뉴스 없음")
            return
        for row in rows:
            try:
                requests_message = copy.deepcopy(news_requests_message)
                item = self._build_item(row)
                requests_message["body"]["item"] = item

                if not item:
                    self.logger.debug(f"no item in: {row.get('ticker')}")
                    continue

                if self.socket_condition:
                    result = self.client.request_tcp(requests_message)

                    status_code = result.get("status_code")
                    message = result.get("message")

                    if status_code != 200:
                        if status_code == 400:
                            msg = f"데이터 입력 오류 (400)"
                        elif status_code == 500:
                            msg = f"시스템 오류 (500)"
                        else:
                            msg = f"알 수 없는 상태 코드({status_code})"

                        self.logger.error(f"{msg} → {message}: {row['ticker']}")
                        continue

                    self.logger.debug(f"{result}")

                    analysis = result.get("item", {}).get("result")
                    if analysis:
                        self._update_analysis(row["tag_id"], analysis, row["source"])
                    else:
                        self.logger.warning(f"분석 결과 없음 → {row['crawling_id']}")

                else:
                    analysis = "notifier 테스트"
                    # self._update_analysis(row["tag_id"], analysis, row["source"])

            except Exception as e:
                self.logger.error(
                    f"예외 발생 → {e}: {row.get('ticker')}, {row.get('crawling_id')}"
                )

    def _build_item(self, row):
        try:
            item = copy.deepcopy(news_item)
            ticker = row.get("ticker")
            if not ticker:
                self.logger.warning(f"ticker missing for {row.get('crawling_id')}")
                return None

            content = row.get("content") or ""
            if not content.strip():
                self.logger.warning(f"content empty for {row.get('crawling_id')}")
                return None

            stock_history = self._get_stock_history(ticker)
            market_history = self._get_market_history(ticker)
            income_statement = self._get_income_statement(ticker)
            info = self._get_info(ticker)

            if (
                not any(stock_history.values())
                or not any(market_history.values())
                or not any(income_statement.values())
                or not any(info.values())
            ):
                return None

            item["data"]["news_data"] = [content]
            item["data"]["stock_history"] = stock_history
            item["data"]["market_history"] = market_history
            item["data"]["income_statement"] = income_statement
            item["data"]["info"] = info

            return item

        except Exception as e:
            self.logger.error(
                f"예외 발생 → {e}: {row.get('ticker')}, {row.get('crawling_id')}"
            )

    def _get_stock_history(self, ticker: str) -> dict:
        try:
            with get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT * 
                        FROM notifier_stock_vw 
                        WHERE ticker = :tag 
                        AND posted_at >= CURDATE() - INTERVAL 30 DAY
                        ORDER BY posted_at
                        """
                    ),
                    {"tag": ticker},
                )

                result = {
                    k: []
                    for k in [
                        "stock",
                        "Date",
                        "Open",
                        "Close",
                        "Adj Close",
                        "High",
                        "Low",
                        "Volume",
                        "Market Cap",
                    ]
                }

                found = False
                for r in rows:
                    found = True
                    result["stock"].append(ticker)
                    result["Date"].append(
                        r._mapping.get("posted_at").strftime("%Y-%m-%d")
                        if r._mapping.get("posted_at")
                        else ""
                    )
                    result["Open"].append(float(r._mapping.get("open") or 0))
                    result["Close"].append(float(r._mapping.get("close") or 0))
                    result["Adj Close"].append(float(r._mapping.get("adj_close") or 0))
                    result["High"].append(float(r._mapping.get("high") or 0))
                    result["Low"].append(float(r._mapping.get("low") or 0))
                    result["Volume"].append(float(r._mapping.get("volume") or 0))
                    result["Market Cap"].append(
                        float(r._mapping.get("market_cap") or 0)
                    )

                if not found:
                    self.logger.debug(f"no data for {ticker}")

                return result

        except Exception as e:
            self.logger.error(f"예외 발생 {ticker}: {e}")
            return {
                k: []
                for k in [
                    "stock",
                    "Date",
                    "Open",
                    "Close",
                    "Adj Close",
                    "High",
                    "Low",
                    "Volume",
                    "Market Cap",
                ]
            }

    def _get_market_history(self, ticker: str) -> dict:
        try:
            ticker_obj = self.yf_with_backoff(ticker)
            exchange = ticker_obj.info.get("exchange", "").upper()
            index_symbol = MARKET_INDEX_TICKER.get(exchange) or exchange

            if not exchange:
                self.logger.warning(f"Index symbol not found for {ticker}")
                return {
                    k: []
                    for k in [
                        "m_Symbol",
                        "Date",
                        "Open",
                        "Close",
                        "Adj Close",
                        "High",
                        "Low",
                        "Volume",
                    ]
                }

            with get_session() as session:
                rows = session.execute(
                    text(
                        "SELECT * FROM notifier_market_vw WHERE symbol = :tag ORDER BY date"
                    ),
                    {"tag": exchange},
                )

                result = {
                    k: []
                    for k in [
                        "m_Symbol",
                        "Date",
                        "Open",
                        "Close",
                        "Adj Close",
                        "High",
                        "Low",
                        "Volume",
                    ]
                }
                found = False

                for r in rows:
                    found = True
                    r = r._mapping
                    result["m_Symbol"].append(index_symbol)
                    result["Date"].append(
                        r.get("date").strftime("%Y-%m-%d") if r.get("date") else ""
                    )
                    result["Open"].append(float(r.get("open") or 0))
                    result["Close"].append(float(r.get("close") or 0))
                    result["Adj Close"].append(float(r.get("adj_close") or 0))
                    result["High"].append(float(r.get("high") or 0))
                    result["Low"].append(float(r.get("low") or 0))
                    result["Volume"].append(float(r.get("volume") or 0))

                if not found:
                    self.logger.debug(f"no data for {exchange}")

                return result

        except Exception as e:
            self.logger.error(f"예외 발생 {ticker}: {e}")
            return {
                k: []
                for k in ["Date", "Open", "Close", "Adj Close", "High", "Low", "Volume"]
            }

    def _get_income_statement(self, ticker: str) -> dict:
        try:
            with get_session() as session:
                row = (
                    session.execute(
                        text(
                            """
                        SELECT * FROM notifier_financial_vw
                        WHERE ticker = :tag
                        ORDER BY crawling_id DESC
                        LIMIT 1
                    """
                        ),
                        {"tag": ticker},
                    )
                    .mappings()
                    .first()
                )
                if not row:
                    self.logger.debug(f"no row for {ticker}")
                    return {}
                return {
                    "Total Revenue": [
                        (
                            float(row["total_revenue"])
                            if row["total_revenue"] is not None
                            else None
                        )
                    ],
                    "Normalized Income": [
                        (
                            float(row["normalized_income"])
                            if row["normalized_income"] is not None
                            else None
                        )
                    ],
                }
        except Exception as e:
            self.logger.error(f"예외 발생 {ticker}: {e}")
            return {}

    def yf_with_backoff(self, ticker_str: str, max_retries: int = 4) -> yf.Ticker:
        import time

        for retry in range(max_retries + 1):
            try:
                ticker = yf.Ticker(ticker_str)
                _ = ticker.info
                return ticker
            except Exception as e:
                if "401" in str(e):
                    wait = 5 + retry * 5
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError(f"{ticker_str} 요청 실패 (최대 재시도 초과)")

    def _get_info(self, ticker: str) -> dict:
        try:
            ticker_obj = self.yf_with_backoff(ticker)
            ptb = ticker_obj.info.get("priceToBook")
            if ptb is None:
                self.logger.debug(f"no priceToBook for {ticker}")
            return {"priceToBook": [ptb] if ptb is not None else []}
        except Exception as e:
            self.logger.error(f"에러 발생 {ticker}: {e}")
            return {"priceToBook": []}

    def _update_analysis(self, tag_id: str, analysis: str, source: str) -> None:
        model_map = {
            "news": NewsTag,
            "report": ReportTag,
        }

        model = model_map.get(source)
        if not model:
            self.logger.error(f"unknown source: {source}")
            return

        try:
            with get_session() as session:
                stmt = (
                    update(model)
                    .where(model.tag_id == tag_id)
                    .values(ai_analysis=analysis)
                )
                result = session.execute(stmt)

                if result.rowcount > 0:
                    session.commit()
                else:
                    self.logger.warning(f"{tag_id} not matched in {source}")
        except Exception as e:
            self.logger.error(f"{tag_id}: {e}")
