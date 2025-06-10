from sqlalchemy import Column, DateTime, DECIMAL, ForeignKey, Integer
from sqlalchemy.dialects.mysql import VARCHAR

from lib.Distributor.secretary.models.core import Base


# 📘 financials 테이블
class FinancialStatement(Base):
    __tablename__ = "financials"

    financials_id = Column(
        Integer, primary_key=True, nullable=False, autoincrement=True
    )
    crawling_id = Column(
        VARCHAR(64),
        ForeignKey("crawling_logs.crawling_id"),
        nullable=False,
    )
    company = Column(VARCHAR(20), nullable=False)
    financial_type = Column(VARCHAR(255), nullable=False)
    posted_at = Column(DateTime, nullable=False)
    ai_analysis = Column(Integer, nullable=True)


class IncomeStatement(Base):
    __tablename__ = "income_statement"
    income_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    crawling_id = Column(
        VARCHAR(64),
        ForeignKey("financials.crawling_id"),
        nullable=False,
    )

    total_revenue = Column(DECIMAL(18, 2), nullable=True)
    operating_income = Column(DECIMAL(18, 2), nullable=True)
    net_income = Column(DECIMAL(18, 2), nullable=True)
    ebitda = Column(DECIMAL(18, 2), nullable=True)

    diluted_eps = Column(DECIMAL(18, 2), nullable=True)
    gross_profit = Column(DECIMAL(18, 2), nullable=True)
    cost_of_revenue = Column(DECIMAL(18, 2), nullable=True)
    sgna = Column(DECIMAL(18, 2), nullable=True)
    reconciled_depreciation = Column(DECIMAL(18, 2), nullable=True)
    other_non_operating_income_expenses = Column(DECIMAL(18, 2), nullable=True)
    normalized_income = Column(DECIMAL(18, 2), nullable=True)


class BalanceSheet(Base):
    __tablename__ = "balance_sheet"
    balance_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    crawling_id = Column(
        VARCHAR(64),
        ForeignKey("financials.crawling_id"),
        nullable=False,
    )

    total_assets = Column(DECIMAL(18, 2), nullable=True)
    total_liabilities = Column(DECIMAL(18, 2), nullable=True)
    stockholders_equity = Column(DECIMAL(18, 2), nullable=True)

    current_assets = Column(DECIMAL(18, 2), nullable=True)
    current_liabilities = Column(DECIMAL(18, 2), nullable=True)
    retained_earnings = Column(DECIMAL(18, 2), nullable=True)
    cash_and_cash_equivalents = Column(DECIMAL(18, 2), nullable=True)
    accounts_receivable = Column(DECIMAL(18, 2), nullable=True)
    inventory = Column(DECIMAL(18, 2), nullable=True)
    cash_cash_equivalents_and_short_term_investments = Column(
        DECIMAL(18, 2), nullable=True
    )

    cash_equivalents = Column(DECIMAL(18, 2), nullable=True)
    cash_financial = Column(DECIMAL(18, 2), nullable=True)
    other_short_term_investments = Column(DECIMAL(18, 2), nullable=True)


class CashFlow(Base):
    __tablename__ = "cash_flow"
    cashflow_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    crawling_id = Column(
        VARCHAR(64),
        ForeignKey("financials.crawling_id"),
        nullable=False,
    )

    operating_cash_flow = Column(DECIMAL(18, 2), nullable=True)
    investing_cash_flow = Column(DECIMAL(18, 2), nullable=True)
    financing_cash_flow = Column(DECIMAL(18, 2), nullable=True)
    free_cash_flow = Column(DECIMAL(18, 2), nullable=True)
    capital_expenditure = Column(DECIMAL(18, 2), nullable=True)
