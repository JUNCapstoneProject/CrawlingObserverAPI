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
            self.logger.info("처리할 재무 데이터 없음")
            return

        for row in rows:
            try:
                item = self._build_item(row)
                if not item:
                    self.logger.debug(f"no item in: {row.get('company')}")
                    continue

                if self.socket_condition:
                    result = self.client.request_tcp(item)
                    status_code = result.get("status_code")
                    message = result.get("message")

                    if status_code != 200:
                        if status_code == 400:
                            msg = "데이터 입력 오류 (400)"
                        elif status_code == 500:
                            msg = "시스템 오류 (500)"
                        else:
                            msg = f"알 수 없는 상태 코드({status_code})"

                        self.logger.error(f"{msg} → {message}: {row['company']}")
                        continue

                    analysis = result.get("item", {}).get("result")
                    if analysis:
                        self._update_analysis(
                            row["crawling_id"], analysis, ["financials"]
                        )
                    else:
                        self.logger.warning(f"분석 결과 없음 → {row['crawling_id']}")

                else:
                    analysis = "notifier 테스트"
                    # self._update_analysis(row["crawling_id"], analysis, ["financials"])

            except Exception as e:
                self.logger.error(
                    f"예외 발생 -> {e}: {row.get('company')}, {row.get('crawling_id')}"
                )

    def _build_item(self, row):
        try:
            item = copy.deepcopy(finance_item)
            company = row["company"]

            recent_rows = self._fetch_recent_quarter_rows(company)  # 5개 row만 반환

            item["data"]["balance_sheet"] = self._build_section_fieldwise_padded(
                recent_rows, self._bs_map()
            )
            item["data"]["income_statement"] = self._build_section_fieldwise_padded(
                recent_rows, self._is_map()
            )
            item["data"]["cash_flow"] = self._build_section_fieldwise_padded(
                recent_rows, self._cf_map()
            )
            item["data"]["chart"] = self._get_chart_data(company)

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
            self.logger.error(f"{e}: company={row.get('company', '?')}")
            return None

    def _fetch_recent_quarter_rows(self, company: str) -> list[dict]:
        try:
            with get_session() as session:
                rows = (
                    session.execute(
                        text(
                            """
                        SELECT * FROM notifier_financial_vw
                        WHERE company = :tag
                        ORDER BY posted_at DESC
                        LIMIT 5
                        """
                        ),
                        {"tag": company},
                    )
                    .mappings()
                    .all()
                )

                return rows or []

        except Exception as e:
            self.logger.error(f"{company}: {e}")
            return []

    def _build_section_fieldwise_padded(
        self, rows: list[dict], mapping: dict[str, str]
    ) -> dict:
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

            # 부족하면 앞에서부터 None으로 패딩
            padded = [None] * (4 - len(values)) + values
            section[req_key] = padded

        return section

    def _get_chart_data(self, company: str) -> dict:
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
                    self.logger.debug(f"no data for {company}")

                # 최신순 정렬이므로 다시 시간순 정렬
                for key in chart:
                    chart[key] = list(reversed(chart[key]))

                return chart

        except Exception as e:
            self.logger.error(f"{company}: {e}")
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
                else:
                    self.logger.warning(f"crawling_id {crawling_id} not found")

        except Exception as e:
            self.logger.error(f"crawling_id {crawling_id}: {e}")

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
