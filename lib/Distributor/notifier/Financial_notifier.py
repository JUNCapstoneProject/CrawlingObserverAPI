import json
import copy
from sqlalchemy import text, update

from lib.Distributor.notifier.Notifier import NotifierBase
from lib.Distributor.socket.messages.request import (
    finance_item,
    finance_requests_message,
)
from lib.Distributor.secretary.session import get_session


class FinancialNotifier(NotifierBase):
    def __init__(self):
        super().__init__("FinancialNotifier")

    def run(self):
        rows = self._fetch_unanalyzed_rows("notifier_financial_vw")
        if not rows:
            self.logger.info("처리할 재무 데이터 없음")
            return

        for row in rows:
            try:
                requests_message = copy.deepcopy(finance_requests_message)
                item = self._build_item(row)
                requests_message["body"]["item"] = item

                # with open("temp_request_message.json", "w", encoding="utf-8") as f:
                #     json.dump(requests_message, f, ensure_ascii=False, indent=4)

                if not item:
                    self.logger.debug(f"no item in: {row.get('ticker')}")
                    continue

                if self.socket_condition:
                    result = self.client.request_tcp(requests_message)
                    status_code = result.get("status_code")
                    message = result.get("message")

                    if status_code != 200:
                        if status_code == 400:
                            msg = "데이터 입력 오류 (400)"
                        elif status_code == 500:
                            msg = "시스템 오류 (500)"
                        else:
                            msg = f"알 수 없는 상태 코드({status_code})"

                        self.logger.error(f"{msg} → {message}: {row['ticker']}")
                        continue

                    raw_result = result.get("item", {}).get("result")
                    if raw_result is not None:
                        try:
                            # 문자열, float 등 어떤 형식이든 int 변환 시도
                            index = int(float(raw_result))
                            self._update_analysis(
                                row["crawling_id"], index, "financial"
                            )
                        except (ValueError, TypeError):
                            self.logger.warning(f"분석 인덱스 변환 실패 → {raw_result}")
                    else:
                        self.logger.warning(f"분석 결과 없음 → {row['crawling_id']}")

            except Exception as e:
                self.logger.error(
                    f"예외 발생 -> {e}: {row.get('ticker')}, {row.get('crawling_id')}"
                )

    def _build_item(self, row):
        try:
            item = copy.deepcopy(finance_item)
            ticker = row["ticker"]

            recent_rows = self._fetch_recent_quarter_rows(ticker)

            item["data"]["balance_sheet"] = self._build_section_fieldwise_padded(
                recent_rows, self._bs_map()
            )
            item["data"]["income_statement"] = self._build_section_fieldwise_padded(
                recent_rows, self._is_map()
            )
            item["data"]["cash_flow"] = self._build_section_fieldwise_padded(
                recent_rows, self._cf_map()
            )
            item["data"]["chart"] = self._get_chart_data(ticker)

            def all_empty(section):
                return not any(v for v in section.values() if v)

            if (
                all_empty(item["data"]["balance_sheet"])
                or all_empty(item["data"]["income_statement"])
                or all_empty(item["data"]["cash_flow"])
                or not item["data"]["chart"]["timestamp"]
            ):
                return None

            return item

        except Exception as e:
            self.logger.error(f"{e}: ticker={row.get('ticker', '?')}")
            return None

    def _fetch_recent_quarter_rows(self, ticker: str) -> list[dict]:
        try:
            with get_session() as session:
                rows = (
                    session.execute(
                        text(
                            """
                        SELECT * FROM notifier_financial_vw
                        WHERE ticker = :tag
                        ORDER BY posted_at DESC
                        LIMIT 5
                        """
                        ),
                        {"tag": ticker},
                    )
                    .mappings()
                    .all()
                )
                return rows or []
        except Exception as e:
            self.logger.error(f"{ticker}: {e}")
            return []

    def _build_section_fieldwise_padded(
        self, rows: list[dict], mapping: dict[str, str]
    ) -> dict | None:
        section = {}

        for field, req_key in mapping.items():
            values = []
            for row in rows:
                if row is None:
                    continue
                val = row.get(field)
                if val is not None:
                    try:
                        values.append(float(val))
                    except (ValueError, TypeError):
                        values.append(val)

                if len(values) == 4:
                    break

            if len(values) < 4:
                # 하나라도 4개 미만이면 전체를 None으로
                return {}

            section[req_key] = values

        return section

    def _get_chart_data(self, ticker: str) -> dict:
        try:
            with get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT posted_at, open, close
                        FROM notifier_stock_vw
                        WHERE ticker = :ticker
                        ORDER BY posted_at DESC
                        LIMIT 300
                        """
                    ),
                    {"ticker": ticker},
                )

                chart = {"timestamp": [], "o": [], "c": []}
                found = False
                for r in rows:
                    found = True
                    chart["timestamp"].append(str(r.posted_at))
                    chart["o"].append(float(r.open) if r.open is not None else None)
                    chart["c"].append(float(r.close) if r.close is not None else None)

                if not found:
                    self.logger.debug(f"no data for {ticker}")

                for key in chart:
                    chart[key] = list(reversed(chart[key]))

                return chart

        except Exception as e:
            self.logger.error(f"{ticker}: {e}")
            return {"timestamp": [], "o": [], "c": []}

    def _bs_map(self) -> dict[str, str]:
        return {
            "current_assets": "Current Assets",
            "current_liabilities": "Current Liabilities",
            "cash_and_cash_equivalents": "Cash And Cash Equivalents",
            "accounts_receivable": "Accounts Receivable",
            "cash_cash_equivalents_and_short_term_investments": "Cash Cash Equivalents And Short Term Investments",
            "cash_equivalents": "Cash Equivalents",
            "cash_financial": "Cash Financial",
            "other_short_term_investments": "Other Short Term Investments",
            "stockholders_equity": "Stockholders Equity",
            "total_assets": "Total Assets",
            "retained_earnings": "Retained Earnings",
            "inventory": "Inventory",
        }

    def _is_map(self) -> dict[str, str]:
        return {
            "total_revenue": "Total Revenue",
            "cost_of_revenue": "Cost Of Revenue",
            "gross_profit": "Gross Profit",
            "sgna": "Selling General And Administration",
            "operating_income": "Operating Income",
            "other_non_operating_income_expenses": "Other Non Operating Income Expenses",
            "reconciled_depreciation": "Reconciled Depreciation",
            "ebitda": "EBITDA",
        }

    def _cf_map(self) -> dict[str, str]:
        return {
            "operating_cash_flow": "Operating Cash Flow",
            "investing_cash_flow": "Investing Cash Flow",
            "capital_expenditure": "Capital Expenditure",
        }
