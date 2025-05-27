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
        rows = self._fetch_unanalyzed_rows("notifier_articles_vw")
        if not rows:
            self.logger.log("WAIT", "[Article] 처리할 뉴스 없음")
            return

        for row in rows:
            try:
                item = self._build_item(row)
                if not item:
                    self.logger.log("WARN", f"[Article] no item in: {row.get('tag')}")
                    continue

                if self.socket_condition:
                    result = self.client.request_tcp(item)

                    self.logger.log("DEBUG", f"type(result): {type(result)}")
                    self.logger.log("DEBUG", f"result content: {result}")

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
                            f"{msg} → {message}: {row['company']}",
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
