from sqlalchemy import Column, DateTime, DECIMAL, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR

from .core import Base

# 📘 financials 테이블
class FinancialStatement(Base):
    __tablename__ = "financials"

    crawling_id = Column(VARCHAR(36), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    company = Column(VARCHAR(20), nullable=False)
    financial_type = Column(VARCHAR(255), nullable=False)
    posted_at = Column(DateTime, nullable=False)
    ai_analysis = Column(VARCHAR(512), nullable=True)


# 📘 income_statement 테이블
class IncomeStatement(Base):
    __tablename__ = "income_statement"

    crawling_id = Column(VARCHAR(36), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    total_revenue = Column(DECIMAL(18, 2), nullable=False)
    gross_profit = Column(DECIMAL(18, 2), nullable=False)
    cost_of_revenue = Column(DECIMAL(18, 2), nullable=False)
    sgna = Column(DECIMAL(18, 2), nullable=False)
    operating_income = Column(DECIMAL(18, 2), nullable=False)
    other_non_operating_income = Column(DECIMAL(18, 2), nullable=False)
    reconciled_depreciation = Column(DECIMAL(18, 2), nullable=False)
    ebitda = Column(DECIMAL(18, 2), nullable=False)


# 📘 balance_sheet 테이블
class BalanceSheet(Base):
    __tablename__ = "balance_sheet"

    crawling_id = Column(VARCHAR(36), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    current_assets = Column(DECIMAL(18, 2), nullable=False)
    current_liabilities = Column(DECIMAL(18, 2), nullable=False)
    cash_and_cash_equivalents = Column(DECIMAL(18, 2), nullable=False)
    accounts_receivable = Column(DECIMAL(18, 2), nullable=False)
    inventory = Column(DECIMAL(18, 2), nullable=False)
    cash_cash_equivalents_and_short_term_investments = Column(DECIMAL(18, 2), nullable=False)
    cash_equivalents = Column(DECIMAL(18, 2), nullable=False)
    cash_financial = Column(DECIMAL(18, 2), nullable=False)
    other_short_term_investments = Column(DECIMAL(18, 2), nullable=False)
    stockholders_equity = Column(DECIMAL(18, 2), nullable=False)
    total_assets = Column(DECIMAL(18, 2), nullable=False)
    retained_earnings = Column(DECIMAL(18, 2), nullable=False)


# 📘 cash_flow 테이블
class CashFlow(Base):
    __tablename__ = "cash_flow"

    crawling_id = Column(VARCHAR(36), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    operating_cash_flow = Column(DECIMAL(18, 2), nullable=False)
    capital_expenditure = Column(DECIMAL(18, 2), nullable=False)
    investing_cash_flow = Column(DECIMAL(18, 2), nullable=False)
