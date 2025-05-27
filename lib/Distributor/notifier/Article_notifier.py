import copy
import yfinance as yf
from sqlalchemy import text, update

from lib.Distributor.notifier.Notifier import NotifierBase
from lib.Distributor.socket.messages.request import news_item
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.news import NewsTag
from lib.Distributor.secretary.models.reports import ReportTag


class ArticleNotifier(NotifierBase):
    def __init__(self):
        super().__init__("ArticleNotifier")

    def run(self):
        rows = self._fetch_unanalyzed_rows("notifier_articles_vw")
        if not rows:
            self.logger.log("WAIT", "[Article] 처리할 뉴스 없음")
            return

        for row in rows:
            try:
                item = self._build_item(row)
                if not item:
                    self.logger.log("DEBUG", f"[Article] no item in: {row.get('tag')}")
                    continue

                if self.socket_condition:
                    result = self.client.request_tcp(item)

                    status_code = result.get("status_code")
                    message = result.get("message")

                    if status_code != 200:
                        log_level = "ERROR"
                        if status_code == 400:
                            msg = f"[Article] 데이터 입력 오류 (400)"
                        elif status_code == 500:
                            msg = f"[Article] 시스템 오류 (500)"
                        else:
                            msg = f"[Article] 알 수 없는 상태 코드({status_code})"

                        self.logger.log(
                            log_level,
                            f"{msg} → {message}: {row['tag']}",
                        )
                        continue  # 에러일 경우 이후 로직 실행하지 않음

                    # 성공(200)일 때만 분석 결과 확인
                    analysis = result.get("item", {}).get("result")
                    if analysis:
                        self._update_analysis(row["tag_id"], analysis, row["source"])
                    else:
                        self.logger.log(
                            "WARN",
                            f"[Article] 분석 결과 없음 → {row['crawling_id']}",
                        )

                else:
                    analysis = "notifier 테스트"
                    # self._update_analysis(row["tag_id"], analysis, row["source"])

            except Exception as e:
                self.logger.log(
                    "ERROR",
                    f"[Article] 예외 발생 → {e}: {row.get('tag')}, {row.get('crawling_id')}",
                )

        self.logger.log_summary()

    def _build_item(self, row):
        try:
            item = copy.deepcopy(news_item)
            tag = row.get("tag")
            if not tag:
                self.logger.log(
                    "WARN", f"[BuildItem] tag missing for {row.get('crawling_id')}"
                )
                return None

            content = row.get("content") or ""
            if not content.strip():
                self.logger.log(
                    "WARN",
                    f"[BuildItem] content empty for {row.get('crawling_id')}",
                )
                return None

            stock_history = self._get_stock_history(tag)
            market_history = self._get_market_history(tag)
            income_statement = self._get_income_statement(tag)
            info = self._get_info(tag)

            # ✅ 모든 항목이 비어 있으면 중단
            if (
                not any(stock_history.values())
                or not any(market_history.values())
                or not any(income_statement.values())
                or not any(info.values())
            ):
                return None

            item["data"]["news_data"] = content
            item["data"]["stock_history"] = stock_history
            item["data"]["market_history"] = market_history
            item["data"]["income_statement"] = income_statement
            item["data"]["info"] = info

            return item

        except Exception as e:
            self.logger.log("ERROR", f"[BuildItem] {row.get('crawling_id')}: {e}")
            return None

    def _get_stock_history(self, tag: str) -> dict:
        try:
            with get_session() as session:
                rows = session.execute(
                    text("SELECT * FROM stock_vw WHERE ticker = :tag"), {"tag": tag}
                )
                result = {
                    k: []
                    for k in [
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
                    for k in result:
                        if k == "Date":
                            result[k].append(str(r._mapping.get("posted_at")))
                        else:
                            key = k.replace(" ", "_").lower()
                            val = r._mapping.get(key)
                            result[k].append(float(val) if val is not None else None)
                if not found:
                    self.logger.log("DEBUG", f"[StockHistory] no data for {tag}")
                return result
        except Exception as e:
            self.logger.log("ERROR", f"[StockHistory] {tag}: {e}")
            return {
                k: []
                for k in [
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

    def _get_market_history(self, tag: str) -> dict:
        try:
            info = yf.Ticker(tag).info
            exchange = info.get("exchange", "").upper()
            index = self._map_exchange_to_index(exchange)

            ticker_obj = self.yf_with_backoff(index, "MarketHistory")
            df = ticker_obj.history(period="7d", interval="1d")

            result = {
                k: []
                for k in ["Date", "Open", "Close", "Adj Close", "High", "Low", "Volume"]
            }
            if df.empty:
                self.logger.log("DEBUG", f"[MarketHistory] {tag} → {index}: empty")
                return result

            last_row = df.tail(1).iloc[0]
            result["Date"].append(str(df.tail(1).index[0].date()))
            for k in result:
                if k != "Date":
                    result[k].append(float(last_row.get(k, 0)))
            return result

        except Exception as e:
            self.logger.log("ERROR", f"[MarketHistory] {tag}: {e}")
            return {
                k: []
                for k in ["Date", "Open", "Close", "Adj Close", "High", "Low", "Volume"]
            }

    def _map_exchange_to_index(self, exchange: str) -> str:
        mapping = {
            "NMS": "^IXIC",
            "NASDAQ": "^IXIC",
            "NASDAQGS": "^IXIC",
            "NASDAQGM": "^IXIC",
            "NYQ": "^GSPC",
            "NYSE": "^GSPC",
            "AMEX": "^XAX",
            "ARCA": "^GSPC",
        }
        return mapping.get(exchange, "^GSPC")

    def _get_income_statement(self, tag: str) -> dict:
        try:
            with get_session() as session:
                row = (
                    session.execute(
                        text(
                            """
                        SELECT * FROM notifier_financial_vw
                        WHERE company = :tag
                        ORDER BY crawling_id DESC
                        LIMIT 1
                    """
                        ),
                        {"tag": tag},
                    )
                    .mappings()
                    .first()
                )
                if not row:
                    self.logger.log("DEBUG", f"[IncomeStatement] no row for {tag}")
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
            self.logger.log("ERROR", f"[IncomeStatement] {tag}: {e}")
            return {}

    def yf_with_backoff(
        self, ticker_str: str, purpose: str, max_retries: int = 4
    ) -> yf.Ticker:
        """
        yfinance Ticker 객체 반환. 401 Unauthorized 발생 시 백오프 재시도
        :param self: 로거가 포함된 클래스 인스턴스
        :param ticker_str: 티커 문자열 (ex: "AAPL", "^GSPC")
        :param purpose: 로그용 태그 (ex: "MarketHistory", "PriceToBook")
        :param max_retries: 최대 재시도 횟수
        :return: yf.Ticker 객체
        """
        import time
        import yfinance as yf

        for retry in range(max_retries + 1):
            try:
                ticker = yf.Ticker(ticker_str)
                _ = ticker.info  # 이 시점에서 요청 발생
                return ticker
            except Exception as e:
                if "401" in str(e):
                    wait = 5 + retry * 5
                    self.logger.log(
                        "ERROR",
                        f"[{purpose}] 401 Unauthorized for {ticker_str} → {wait}s 대기 (재시도 {retry})",
                    )
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError(f"[{purpose}] {ticker_str} 요청 실패 (최대 재시도 초과)")

    def _get_info(self, tag: str) -> dict:
        try:
            ticker_obj = self.yf_with_backoff(tag, "PriceToBook")
            ptb = ticker_obj.info.get("priceToBook")
            if ptb is None:
                self.logger.log("DEBUG", f"[PriceToBook] no priceToBook for {tag}")
            return {"priceToBook": [ptb] if ptb is not None else []}
        except Exception as e:
            self.logger.log("ERROR", f"[PriceToBook] {tag}: {e}")
            return {"priceToBook": []}

    def _update_analysis(self, tag_id: str, analysis: str, source: str) -> None:
        model_map = {
            "news": NewsTag,
            "report": ReportTag,
        }

        model = model_map.get(source)
        if not model:
            self.logger.log("ERROR", f"[Update] unknown source: {source}")
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
                    self.logger.log(
                        "DEBUG", f"[Update] tag_id {tag_id} updated in {source}"
                    )
                else:
                    self.logger.log(
                        "WARN", f"[Update] tag_id {tag_id} not matched in {source}"
                    )
        except Exception as e:
            self.logger.log("ERROR", f"[Update] tag_id {tag_id}: {e}")
