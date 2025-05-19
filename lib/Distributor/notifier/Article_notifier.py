import copy
import yfinance as yf
from sqlalchemy import text

from lib.Distributor.notifier.Notifier import NotifierBase
from lib.Distributor.socket.messages.request import news_item
from lib.Distributor.secretary.session import get_session


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
                self.logger.log("WARN", f"[Article] skipping: {row.get('crawling_id')}")
                continue

            try:
                result = self.client.request_tcp(item)
                analysis = result.get("message")
                if analysis:
                    self._update_analysis(
                        row["crawling_id"], analysis, ["news", "reports"]
                    )
                else:
                    self.logger.log(
                        "WARN", f"[Article] no result for {row['crawling_id']}"
                    )
            except Exception as e:
                self.logger.log(
                    "ERROR", f"[Article] {e}: {row['tag']}, {row['crawling_id']}"
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

            content = (row.get("title") or "") + (row.get("content") or "")
            if not content.strip():
                self.logger.log(
                    "WARN",
                    f"[BuildItem] title+content empty for {row.get('crawling_id')}",
                )
                return None

            item["data"]["news_data"] = content
            item["data"]["stock_history"] = self._get_stock_history(tag)
            item["data"]["market_history"] = self._get_market_history(tag)
            item["data"]["income_statement"] = self._get_income_statement(tag)
            item["data"]["info"] = self._get_info(tag)
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
