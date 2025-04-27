from lib.Distributor.secretary.models.news import News, NewsTag
from lib.Distributor.secretary.models.macro import MacroIndex, MacroEconomics
from lib.Distributor.secretary.models.reports import Report, ReportTag
from lib.Distributor.secretary.models.stock import Stock
from lib.Distributor.secretary.models.financials import (
    FinancialStatement,
    IncomeStatement,
    BalanceSheet,
    CashFlow,
)


def store_news(db, crawling_id, data):
    for row in data:
        db.add(
            News(
                crawling_id=crawling_id,
                title=row.get("title"),
                author=row.get("author"),
                organization=row.get("organization"),
                posted_at=row.get("posted_at"),
                content=row.get("content"),
                hits=row.get("hits"),
                ai_analysis=row.get("ai_analysis"),
            )
        )
        tag = row.get("tag")
        if tag:
            db.add(NewsTag(crawling_id=crawling_id, tag=tag))


def store_reports(db, crawling_id, data):
    for row in data:
        db.add(
            Report(
                crawling_id=crawling_id,
                title=row.get("title"),
                author=row.get("author"),
                hits=row.get("hits"),
                posted_at=row.get("posted_at"),
                content=row.get("content"),
                ai_analysis=row.get("ai_analysis"),
            )
        )
        tag = row.get("tag")
        if tag:  # None 또는 빈 문자열이 아닌 경우
            db.add(ReportTag(crawling_id=crawling_id, tag=tag))


def store_macro(db, crawling_id, data):
    for row in data:
        index_name = row.get("index_name")

        # index_name으로 MacroIndex 조회 or 생성
        index = db.query(MacroIndex).filter_by(index_name=index_name).first()
        if not index:
            index = MacroIndex(index_name=index_name)
            db.add(index)
            db.flush()  # index_id 확보
            db.refresh(index)

        db.add(
            MacroEconomics(
                crawling_id=crawling_id,
                country=row.get("country"),
                index_id=index.index_id,
                index_value=float(row.get("index_value")),
                posted_at=row.get("posted_at"),
            )
        )


def store_stock(db, crawling_id, data):
    from collections import defaultdict
    from datetime import datetime, timedelta, time
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    import pandas as pd

    if isinstance(data, pd.DataFrame):
        records = data.dropna(how="all").to_dict(orient="records")
    else:
        records = [row for row in (data or []) if row.get("posted_at")]

    if not records:
        return

    grouped = defaultdict(list)
    for row in records:
        grouped[row["Symbol"]].append(row)

    prev_close_map: dict[str, float | None] = {}

    for ticker, rows in grouped.items():
        # 이번 배치에서 가장 이른 시각
        first_ts: datetime = min(r["posted_at"] for r in rows)
        cutoff = datetime.combine(first_ts.date() - timedelta(days=1), time(16, 0))

        prev = (
            db.query(Stock.close)
            .filter(Stock.ticker == ticker, Stock.posted_at <= cutoff)
            .order_by(Stock.posted_at.desc())
            .first()
        )

        prev_close_map[ticker] = prev.close if prev else None

    for ticker, rows in grouped.items():
        rows.sort(key=lambda r: r["posted_at"])  # 시계열 순서
        prev_close = prev_close_map[ticker]

        for row in rows:
            cur_close = row.get("Close")

            # 전날 종가 대비 변동률(%) 계산
            change = 0.0
            if prev_close and cur_close:
                try:
                    change = round((float(cur_close) / float(prev_close) - 1) * 100, 2)
                except ZeroDivisionError:
                    change = 0.0
            prev_close = cur_close or prev_close  # 다음 루프용 캐시

            stmt = (
                mysql_insert(Stock).values(
                    crawling_id=crawling_id,
                    ticker=ticker,
                    posted_at=row["posted_at"],
                    open=row.get("Open"),
                    high=row.get("High"),
                    low=row.get("Low"),
                    close=cur_close,
                    volume=row.get("Volume"),
                    change=change,
                    adj_close=row.get("Adj Close"),
                    market_cap=row.get("MarketCap"),
                )
                # UNIQUE(ticker, posted_at) 가 충돌하면 내용 갱신
                .on_duplicate_key_update(
                    crawling_id=crawling_id,
                    open=row.get("Open"),
                    high=row.get("High"),
                    low=row.get("Low"),
                    close=cur_close,
                    volume=row.get("Volume"),
                    change=change,
                    adj_close=row.get("Adj Close"),
                    market_cap=row.get("MarketCap"),
                    posted_at=row["posted_at"],
                )
            )
            db.execute(stmt)

    db.commit()


def store_financials_common(db, crawling_id, row):
    try:
        # 각 재무제표 공통 정보 (meta)
        db.add(
            FinancialStatement(
                crawling_id=crawling_id,
                company=row.get("Symbol"),
                financial_type=row.get("financial_type"),
                posted_at=row.get("posted_at"),
                ai_analysis=row.get("ai_analysis"),
            )
        )
        db.flush()
    except Exception as e:
        print(f"[ERROR] insert 실패: {e}")


def store_income_statement(db, crawling_id, data):
    if data:
        store_financials_common(db, crawling_id, data[0])

    for row in data:
        db.add(
            IncomeStatement(
                crawling_id=crawling_id,
                # ✅ 반드시 필요한 필드
                total_revenue=row.get("Total Revenue"),
                operating_income=row.get("Operating Income"),
                net_income=row.get("Net Income"),
                ebitda=row.get("EBITDA"),
                # ⚠️ 일반적으로 포함되지만 누락될 수 있음
                diluted_eps=row.get("Diluted EPS"),
                gross_profit=row.get("Gross Profit"),
                cost_of_revenue=row.get("Cost Of Revenue"),
                sgna=row.get("Selling General And Administration"),
                reconciled_depreciation=row.get("Reconciled Depreciation"),
                other_non_operating_income_expenses=row.get(
                    "Other Non Operating Income Expenses"
                ),
                interest_expense=row.get("Interest Expense"),
                interest_income=row.get("Interest Income"),
                # ❌ 자주 누락되거나 특정 상황에서만 존재
                special_income_charges=row.get("Special Income Charges"),
                restructuring_and_mergern_acquisition=row.get(
                    "Restructuring And Mergern Acquisition"
                ),
                rent_expense_supplemental=row.get("Rent Expense Supplemental"),
                average_dilution_earnings=row.get("Average Dilution Earnings"),
            )
        )


def store_balance_sheet(db, crawling_id, data):
    if data:
        store_financials_common(db, crawling_id, data[0])

    for row in data:
        db.add(
            BalanceSheet(
                crawling_id=crawling_id,
                # ✅ 반드시 필요한 필드
                total_assets=row.get("Total Assets"),
                total_liabilities=row.get("Total Liabilities Net Minority Interest"),
                stockholders_equity=row.get("Stockholders Equity"),
                # ⚠️ 일반적으로 포함
                current_assets=row.get("Current Assets"),
                current_liabilities=row.get("Current Liabilities"),
                retained_earnings=row.get("Retained Earnings"),
                cash_and_cash_equivalents=row.get("Cash And Cash Equivalents"),
                accounts_receivable=row.get("Accounts Receivable"),
                inventory=row.get("Inventory"),
                cash_cash_equivalents_and_short_term_investments=row.get(
                    "Cash Cash Equivalents And Short Term Investments"
                ),
                # ❌ 자주 누락됨
                cash_equivalents=row.get("Cash Equivalents"),
                cash_financial=row.get("Cash Financial"),
                other_short_term_investments=row.get("Other Short Term Investments"),
                goodwill=row.get("Goodwill"),
                preferred_stock=row.get("Preferred Stock"),
                line_of_credit=row.get("Line Of Credit"),
                treasury_stock=row.get("Treasury Stock"),
            )
        )


def store_cash_flow(db, crawling_id, data):
    if data:
        store_financials_common(db, crawling_id, data[0])

    for row in data:
        db.add(
            CashFlow(
                crawling_id=crawling_id,
                # ✅ 반드시 필요한 필드
                operating_cash_flow=row.get("Operating Cash Flow"),
                investing_cash_flow=row.get("Investing Cash Flow"),
                financing_cash_flow=row.get("Financing Cash Flow"),
                free_cash_flow=row.get("Free Cash Flow"),
                # ⚠️ 일반적으로 포함
                capital_expenditure=row.get("Capital Expenditure"),
                depreciation_and_amortization=row.get("Depreciation And Amortization"),
                stock_based_compensation=row.get("Stock Based Compensation"),
                income_tax_paid=row.get("Income Tax Paid Supplemental Data"),
                # ❌ 자주 누락됨
                net_intangibles_purchase_and_sale=row.get(
                    "Net Intangibles Purchase And Sale"
                ),
                sale_of_business=row.get("Sale Of Business"),
                net_foreign_currency_exchange_gain_loss=row.get(
                    "Net Foreign Currency Exchange Gain Loss"
                ),
            )
        )
