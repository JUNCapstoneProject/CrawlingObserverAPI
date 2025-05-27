import copy
from sqlalchemy import text, update

from lib.Distributor.notifier.Notifier import NotifierBase
from lib.Distributor.socket.messages.request import finance_item
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.financials import FinancialStatement


class FinancialNotifier(NotifierBase):
    def __init__(self):
        super().__init__("FinancialNotifier")

    def run(self):
        rows = self._fetch_unanalyzed_rows("notifier_financial_vw")
        if not rows:
            self.logger.log("WAIT", "[Finance] 처리할 재무 데이터 없음")
            return

        for row in rows:
            try:
                item = self._build_item(row)
                if not item:
                    self.logger.log(
                        "WARN", f"[Finance] no item in: {row.get('company')}"
                    )
                    continue

                if self.socket_condition:
                    result = self.client.request_tcp(item)

                    status_code = result.get("status_code")
                    message = result.get("message")  # 에러 원인 또는 일반 메시지
                    analysis = result.get("item", {}).get("result")

                    if status_code == 200:
                        if analysis:
                            self._update_analysis(
                                row["crawling_id"], analysis, ["financials"]
                            )
                        else:
                            self.logger.log(
                                "WARN",
                                f"[Finance] 분석 결과 없음 → {row['crawling_id']}",
                            )
                    elif status_code == 400:
                        self.logger.log(
                            "ERROR",
                            f"[Finance] 데이터 입력 오류 (400) → {row['crawling_id']}: {message}",
                        )
                    elif status_code == 500:
                        self.logger.log(
                            "ERROR",
                            f"[Finance] 시스템 오류 (500) → {row['crawling_id']}: {message}",
                        )
                    else:
                        self.logger.log(
                            "ERROR",
                            f"[Finance] 알 수 없는 상태 코드({status_code}) → {row['crawling_id']}: {message}",
                        )

                else:
                    analysis = "notifier 테스트"
                    # self._update_analysis(row["crawling_id"], analysis, ["financials"])

            except Exception as e:
                self.logger.log(
                    "ERROR", f"[Finance] 예외 발생 → {row.get('crawling_id')}: {e}"
                )

        self.logger.log_summary()

    def _build_item(self, row):
        try:
            item = copy.deepcopy(finance_item)

            def set_val(path, key, value):
                if value is not None:
                    try:
                        path[key].append(float(value))
                    except (ValueError, TypeError):
                        path[key].append(value)

            bs_map = {
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

            is_map = {
                "total_revenue": "Total Revenue",
                "cost_of_revenue": "Cost Of Revenue",
                "gross_profit": "Gross Profit",
                "sgna": "Selling General And Administration",
                "operating_income": "Operating Income",
                "other_non_operating_income_expenses": "Other Non Operating Income Expenses",
                "reconciled_depreciation": "Reconciled Depreciation",
                "ebitda": "EBITDA",
            }

            cf_map = {
                "operating_cash_flow": "Operating Cash Flow",
                "investing_cash_flow": "Investing Cash Flow",
                "capital_expenditure": "Capital Expenditure",
            }

            for field, req_key in bs_map.items():
                set_val(item["data"]["balance_sheet"], req_key, row.get(field))

            for field, req_key in is_map.items():
                set_val(item["data"]["income_statement"], req_key, row.get(field))

            for field, req_key in cf_map.items():
                set_val(item["data"]["cash_flow"], req_key, row.get(field))

            item["data"]["chart"] = self._get_chart_data(row["company"])

            # ✅ 모든 필드가 비어 있으면 분석 제외
            def all_empty(section):
                return not any(v for v in section.values() if v)

            if (
                all_empty(item["data"]["balance_sheet"])
                or all_empty(item["data"]["income_statement"])
                or all_empty(item["data"]["cash_flow"])
                or not item["data"]["chart"]["timestamp"]  # 차트는 timestamp로 판단
            ):
                return None

            return item

        except Exception as e:
            self.logger.log(
                "ERROR",
                f"[BuildItem] {e}: tag={row.get('tag', '?')}, id={row.get('crawling_id', '?')}",
            )
            return None

    def _get_chart_data(self, company: str) -> dict:
        try:
            with get_session() as session:
                rows = session.execute(
                    text(
                        """
                        SELECT posted_at, open, close
                        FROM stock_vw
                        WHERE ticker = :ticker
                        ORDER BY posted_at ASC
                    """
                    ),
                    {"ticker": company},
                )

                chart = {"timestamp": [], "o": [], "c": []}
                found = False
                for r in rows:
                    found = True
                    chart["timestamp"].append(str(r.posted_at))
                    chart["o"].append(float(r.open) if r.open is not None else None)
                    chart["c"].append(float(r.close) if r.close is not None else None)

                if not found:
                    self.logger.log("DEBUG", f"[ChartData] no data for {company}")

                return chart

        except Exception as e:
            self.logger.log("ERROR", f"[ChartData] {company}: {e}")
            return {"timestamp": [], "o": [], "c": []}

    def _update_analysis(
        self, crawling_id: str, analysis: str, tables: list[str]
    ) -> None:
        try:
            with get_session() as session:
                stmt = (
                    update(FinancialStatement)
                    .where(FinancialStatement.crawling_id == crawling_id)
                    .values(ai_analysis=analysis)
                )
                result = session.execute(stmt)

                if result.rowcount > 0:
                    session.commit()
                    self.logger.log(
                        "DEBUG", f"[Update] crawling_id {crawling_id} updated"
                    )
                else:
                    self.logger.log(
                        "WARN", f"[Update] crawling_id {crawling_id} not found"
                    )
        except Exception as e:
            self.logger.log("ERROR", f"[Update] crawling_id {crawling_id}: {e}")
