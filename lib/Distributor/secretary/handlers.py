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
        tag = row.get("tag")
        if tag:
            db.add(NewsTag(
                crawling_id=crawling_id,
                tag=tag
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
        tag = row.get("tag")
        if tag:  # None 또는 빈 문자열이 아닌 경우
            db.add(ReportTag(
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

def store_stock(db, crawling_id, data):
    for row in data:
        db.add(Stock(
            crawling_id=crawling_id,
            ticker=row.get("Symbol"),
            posted_at=row.get("posted_at"),
            open=row.get("Open"),
            high=row.get("High"),
            low=row.get("Low"),
            close=row.get("Close"),
            volume=row.get("Volume")
        ))

def store_financials_common(db, crawling_id, row):
    try:
        # 각 재무제표 공통 정보 (meta)
        db.add(FinancialStatement(
            crawling_id=crawling_id,
            company=row.get("Symbol"),
            financial_type=row.get("financial_type"),
            posted_at=row.get("posted_at"),
            ai_analysis=row.get("ai_analysis")
        ))
        db.flush()
    except Exception as e:
       print(f"[ERROR] insert 실패: {e}")

def store_income_statement(db, crawling_id, data):
    if data:  # 데이터가 있다면 첫 번째 row 기준으로 공통 정보 저장
        store_financials_common(db, crawling_id, data[0])
    
    for row in data:

        db.add(IncomeStatement(
            crawling_id=crawling_id,
            total_revenue=row.get("Total Revenue"),
            gross_profit=row.get("Gross Profit"),
            cost_of_revenue=row.get("Cost Of Revenue"),
            sgna=row.get("Selling General And Administration"),
            operating_income=row.get("Operating Income"),
            other_non_operating_income_expenses=row.get("Other Non Operating Income Expenses"),
            reconciled_depreciation=row.get("Reconciled Depreciation"),
            ebitda=row.get("EBITDA")
        ))

def store_balance_sheet(db, crawling_id, data):
    if data:  # 데이터가 있다면 첫 번째 row 기준으로 공통 정보 저장
        store_financials_common(db, crawling_id, data[0])
    
    for row in data:
        db.add(BalanceSheet(
            crawling_id=crawling_id,
            current_assets=row.get("Current Assets"),
            current_liabilities=row.get("Current Liabilities"),
            cash_and_cash_equivalents=row.get("Cash And Cash Equivalents"),
            accounts_receivable=row.get("Accounts Receivable"),
            inventory=row.get("Inventory"),
            cash_cash_equivalents_and_short_term_investments=row.get("Cash Cash Equivalents And Short Term Investments"),
            cash_equivalents=row.get("Cash Equivalents"),
            cash_financial=row.get("Cash Financial"),
            other_short_term_investments=row.get("Other Short Term Investments"),
            stockholders_equity=row.get("Stockholders Equity"),
            total_assets=row.get("Total Assets"),
            retained_earnings=row.get("Retained Earnings")
        ))

def store_cash_flow(db, crawling_id, data):
    if data:  # 데이터가 있다면 첫 번째 row 기준으로 공통 정보 저장
        store_financials_common(db, crawling_id, data[0])
    
    for row in data:
        db.add(CashFlow(
            crawling_id=crawling_id,
            operating_cash_flow=row.get("Operating Cash Flow"),
            capital_expenditure=row.get("Capital Expenditure"),
            investing_cash_flow=row.get("Investing Cash Flow")
        ))
