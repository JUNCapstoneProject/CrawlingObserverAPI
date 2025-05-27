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
            item = self._build_item(row)
            if not item:
                self.logger.log("WARN", f"[Article] no item in: {row.get('tag')}")
                continue

            try:
                if self.socket_condition:
                    result = self.client.request_tcp(item)

                    status_code = result.get("status_code")
                    message = result.get("message")  # 오류 메시지 or 일반 메시지
                    analysis = result.get("item", {}).get("result")  # 실제 분석 결과

                    if status_code == 200:
                        if analysis:
                            self._update_analysis(
                                row["tag_id"], analysis, row["source"]
                            )
                        else:
                            self.logger.log(
                                "WARN",
                                f"[Article] 분석 결과 없음 → {row['crawling_id']}",
                            )
                    elif status_code == 400:
                        self.logger.log(
                            "ERROR",
                            f"[Article] 데이터 입력 오류 (400) → {message}: {row['tag']}, {row['crawling_id']}",
                        )
                    elif status_code == 500:
                        self.logger.log(
                            "ERROR",
                            f"[Article] 시스템 오류 (500) → {message}: {row['tag']}, {row['crawling_id']}",
                        )
                    else:
                        self.logger.log(
                            "ERROR",
                            f"[Article] 알 수 없는 상태 코드({status_code}) → {message}: {row['tag']}, {row['crawling_id']}",
                        )

                else:
                    analysis = "notifier 테스트"
                    # self._update_analysis(row["tag_id"], analysis, row["source"])

            except Exception as e:
                self.logger.log(
                    "ERROR",
                    f"[Article] 예외 발생 → {e}: {row['tag']}, {row['crawling_id']}",
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
                self.logger.log(
                    "WARN",
                    f"[BuildItem] one or more parts missing → {row.get('crawling_id')}",
                )
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
            df = yf.Ticker(index).history(period="7d", interval="1d")

            result = {
                k: []
                for k in ["Date", "Open", "Close", "Adj Close", "High", "Low", "Volume"]
            }
            if df.empty:
                self.logger.log("DEBUG", f"[MarketHistory] {tag} → {index}: empty")
                return result

            # ✅ 가장 최근 1일치만 사용
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

    def _get_info(self, tag: str) -> dict:
        try:
            ptb = yf.Ticker(tag).info.get("priceToBook")
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
