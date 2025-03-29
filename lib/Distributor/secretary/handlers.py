from .models.news import News, NewsTag
from .models.macro import MacroIndex, MacroEconomics
from .models.reports import Report, ReportTag
from .models.stock import Stock
from .models.financials import (
    FinancialStatement,
    IncomeStatement,
    BalanceSheet,
    CashFlow,
)

def store_news(db, crawling_id, data):
    for row in data:
        db.add(News(
            crawling_id=crawling_id,
            title=row.get("title"),
            author=row.get("author"),
            organization=row.get("organization"),
            posted_at=row.get("posted_at"),
            content=row.get("content"),
            hits=row.get("hits"),
            ai_analysis=row.get("ai_analysis")
        ))
        for tag in row.get("tag", []):
            db.add(NewsTag(
                crawling_id=crawling_id,
                tag=tag
            ))

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

        db.add(MacroEconomics(
            crawling_id=crawling_id,
            country=row.get("country"),
            index_id=index.index_id,
            index_value=float(row.get("index_value")),
            posted_at=row.get("posted_at")
        ))

def store_reports(db, crawling_id, data):
    for row in data:
        db.add(Report(
            crawling_id=crawling_id,
            title=row.get("title"),
            author=row.get("author"),
            hits=row.get("hits"),
            posted_at=row.get("posted_at"),
            content=row.get("content"),
            ai_analysis=row.get("ai_analysis")
        ))
        for tag in row.get("tag", []):
            db.add(ReportTag(
                crawling_id=crawling_id,
                tag=tag
            ))

def store_stock(db, crawling_id, data):
    for row in data:
        db.add(Stock(
            crawling_id=crawling_id,
            ticker=row.get("ticker"),
            posted_at=row.get("posted_at"),
            open=row.get("open"),
            high=row.get("high"),
            low=row.get("low"),
            close=row.get("close"),
            volume=row.get("volume")
        ))

def store_financials_common(db, crawling_id, row):
    # 각 재무제표 공통 정보 (meta)
    db.add(FinancialStatement(
        crawling_id=crawling_id,
        company=row.get("company"),
        financial_type=row.get("financial_type"),
        posted_at=row.get("posted_at"),
        ai_analysis=row.get("ai_analysis")
    ))

def store_income_statement(db, crawling_id, data):
    for row in data:
        store_financials_common(db, crawling_id, row)

        db.add(IncomeStatement(
            crawling_id=crawling_id,
            total_revenue=row.get("total_revenue"),
            gross_profit=row.get("gross_profit"),
            cost_of_revenue=row.get("cost_of_revenue"),
            sgna=row.get("sgna"),
            operating_income=row.get("operating_income"),
            other_non_operating_income=row.get("other_non_operating_income"),
            reconciled_depreciation=row.get("reconciled_depreciation"),
            ebitda=row.get("ebitda")
        ))

def store_balance_sheet(db, crawling_id, data):
    for row in data:
        store_financials_common(db, crawling_id, row)

        db.add(BalanceSheet(
            crawling_id=crawling_id,
            current_assets=row.get("current_assets"),
            current_liabilities=row.get("current_liabilities"),
            cash_and_cash_equivalents=row.get("cash_and_cash_equivalents"),
            accounts_receivable=row.get("accounts_receivable"),
            inventory=row.get("inventory"),
            cash_cash_equivalents_and_short_term_investments=row.get("cash_cash_equivalents_and_short_term_investments"),
            cash_equivalents=row.get("cash_equivalents"),
            cash_financial=row.get("cash_financial"),
            other_short_term_investments=row.get("other_short_term_investments"),
            stockholders_equity=row.get("stockholders_equity"),
            total_assets=row.get("total_assets"),
            retained_earnings=row.get("retained_earnings")
        ))

def store_cash_flow(db, crawling_id, data):
    for row in data:
        store_financials_common(db, crawling_id, row)

        db.add(CashFlow(
            crawling_id=crawling_id,
            operating_cash_flow=row.get("operating_cash_flow"),
            capital_expenditure=row.get("capital_expenditure"),
            investing_cash_flow=row.get("investing_cash_flow")
        ))
