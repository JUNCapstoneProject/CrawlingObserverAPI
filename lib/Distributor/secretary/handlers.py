from sqlalchemy import select, func

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
from lib.Distributor.secretary.title_translator import translate_title


def store_news(db, crawling_id, data):
    for row in data:
        title = row.get("title")
        if not title:
            continue

        if db.execute(select(News).where(News.title == title)).first():
            continue

        chosen_tag = row.get("_chosen_tag")
        if not chosen_tag:
            continue

        # 태그부터 저장하고 태그 id를 가져와서 뉴스에 tag_id로 넣기 (crawling_id는 태그에 넣지 않음)
        tag = db.query(NewsTag).filter_by(tag=chosen_tag).first()
        if not tag:
            tag = NewsTag(tag=chosen_tag)
            db.add(tag)
            db.flush()
            db.refresh(tag)

        transed_title = translate_title(title)

        news = News(
            crawling_id=crawling_id,
            title=title,
            transed_title=transed_title,
            author=row.get("author"),
            organization=row.get("organization"),
            posted_at=row.get("posted_at"),
            content=row.get("content"),
            hits=row.get("hits"),
            tag_id=tag.tag_id,
        )
        db.add(news)


def store_reports(db, crawling_id, data):
    for row in data:
        title = row.get("title")
        if not title:
            continue

        if db.execute(select(Report).where(Report.title == title)).first():
            continue

        chosen_tag = row.get("_chosen_tag")
        if not chosen_tag:
            continue

        # 태그부터 저장하고 태그 id를 가져와서 리포트에 tag_id로 넣기 (crawling_id는 태그에 넣지 않음)
        tag = db.query(ReportTag).filter_by(tag=chosen_tag).first()
        if not tag:
            tag = ReportTag(tag=chosen_tag)
            db.add(tag)
            db.flush()
            db.refresh(tag)

        transed_title = translate_title(title)

        report = Report(
            crawling_id=crawling_id,
            title=title,
            transed_title=transed_title,
            author=row.get("author"),
            hits=row.get("hits"),
            posted_at=row.get("posted_at"),
            content=row.get("content"),
            tag_id=tag.tag_id,
        )
        db.add(report)


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
        grouped[row["company_id"]].append(row)

    for company_id, rows in grouped.items():
        for row in rows:
            stmt = (
                mysql_insert(Stock)
                .values(
                    crawling_id=crawling_id,
                    company_id=company_id,
                    posted_at=row["posted_at"],
                    open=row.get("Open"),
                    high=row.get("High"),
                    low=row.get("Low"),
                    close=row.get("Close"),
                    volume=row.get("Volume"),
                    change=row.get("Change"),
                )
                .on_duplicate_key_update(
                    crawling_id=crawling_id,
                    open=row.get("Open"),
                    high=row.get("High"),
                    low=row.get("Low"),
                    close=row.get("Close"),
                    volume=row.get("Volume"),
                    change=row.get("Change"),
                    posted_at=row["posted_at"],
                )
            )
            db.execute(stmt)

    db.commit()


from datetime import datetime, date


# 날짜 형식 정규화 함수
def normalize_posted_at(raw):
    if isinstance(raw, datetime):
        return raw
    elif isinstance(raw, date):
        return datetime.combine(raw, datetime.min.time())
    elif isinstance(raw, str):
        try:
            return datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    else:
        raise ValueError(f"지원하지 않는 posted_at 형식: {raw!r}")


# 공통 메타 저장 함수 (중복 시 False 반환)
def store_financial_statement_meta(db, crawling_id: str, row: dict) -> bool:
    company = row.get("Symbol")
    financial_type = row.get("financial_type")
    posted_at = normalize_posted_at(row.get("posted_at"))

    exists = db.execute(
        select(FinancialStatement).where(
            FinancialStatement.company == company,
            FinancialStatement.posted_at == posted_at,
            FinancialStatement.financial_type == financial_type,
        )
    ).first()

    if exists:
        return False

    db.add(
        FinancialStatement(
            crawling_id=crawling_id,
            company=company,
            financial_type=financial_type,
            posted_at=posted_at,
            ai_analysis=row.get("ai_analysis"),
        )
    )
    return True


def store_income_statement(db, crawling_id: str, data: list[dict]) -> None:
    if not data:
        return

    if not store_financial_statement_meta(db, crawling_id, data[0]):
        return

    db.flush()

    db.bulk_insert_mappings(
        IncomeStatement,
        [
            {
                "crawling_id": crawling_id,
                "total_revenue": row.get("Total Revenue"),
                "operating_income": row.get("Operating Income"),
                "net_income": row.get("Net Income"),
                "ebitda": row.get("EBITDA"),
                "diluted_eps": row.get("Diluted EPS"),
                "gross_profit": row.get("Gross Profit"),
                "cost_of_revenue": row.get("Cost Of Revenue"),
                "sgna": row.get("Selling General And Administration"),
                "reconciled_depreciation": row.get("Reconciled Depreciation"),
                "other_non_operating_income_expenses": row.get(
                    "Other Non Operating Income Expenses"
                ),
                "normalized_income": row.get("Normalized Income"),
            }
            for row in data
        ],
    )


def store_balance_sheet(db, crawling_id: str, data: list[dict]) -> None:
    if not data:
        return

    if not store_financial_statement_meta(db, crawling_id, data[0]):
        return

    db.flush()

    db.bulk_insert_mappings(
        BalanceSheet,
        [
            {
                "crawling_id": crawling_id,
                "total_assets": row.get("Total Assets"),
                "total_liabilities": row.get("Total Liabilities Net Minority Interest"),
                "stockholders_equity": row.get("Stockholders Equity"),
                "current_assets": row.get("Current Assets"),
                "current_liabilities": row.get("Current Liabilities"),
                "retained_earnings": row.get("Retained Earnings"),
                "cash_and_cash_equivalents": row.get("Cash And Cash Equivalents"),
                "accounts_receivable": row.get("Accounts Receivable"),
                "inventory": row.get("Inventory"),
                "cash_cash_equivalents_and_short_term_investments": row.get(
                    "Cash Cash Equivalents And Short Term Investments"
                ),
                "cash_equivalents": row.get("Cash Equivalents"),
                "cash_financial": row.get("Cash Financial"),
                "other_short_term_investments": row.get("Other Short Term Investments"),
            }
            for row in data
        ],
    )


def store_cash_flow(db, crawling_id: str, data: list[dict]) -> None:
    if not data:
        return

    if not store_financial_statement_meta(db, crawling_id, data[0]):
        return

    db.flush()

    db.bulk_insert_mappings(
        CashFlow,
        [
            {
                "crawling_id": crawling_id,
                "operating_cash_flow": row.get("Operating Cash Flow"),
                "investing_cash_flow": row.get("Investing Cash Flow"),
                "financing_cash_flow": row.get("Financing Cash Flow"),
                "free_cash_flow": row.get("Free Cash Flow"),
                "capital_expenditure": row.get("Capital Expenditure"),
            }
            for row in data
        ],
    )
