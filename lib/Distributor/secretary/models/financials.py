from sqlalchemy import Column, DateTime, DECIMAL, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR

from .core import Base

# 📘 financials 테이블
class FinancialStatement(Base):
    __tablename__ = "financials"

    crawling_id = Column(VARCHAR(64), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    company = Column(VARCHAR(20), nullable=False)
    financial_type = Column(VARCHAR(255), nullable=False)
    posted_at = Column(DateTime, nullable=False)
    ai_analysis = Column(VARCHAR(512), nullable=True)

class IncomeStatement(Base):
    __tablename__ = "income_statement"

    crawling_id = Column(VARCHAR(64), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)

    # ✅ 반드시 필요한 항목
    total_revenue = Column(DECIMAL(18, 2), nullable=False)
    operating_income = Column(DECIMAL(18, 2), nullable=False)
    net_income = Column(DECIMAL(18, 2), nullable=False)
    ebitda = Column(DECIMAL(18, 2), nullable=False)

    # ⚠️ 누락 가능성이 있는 일반 필드
    diluted_eps = Column(DECIMAL(18, 2), nullable=True)
    gross_profit = Column(DECIMAL(18, 2), nullable=True)
    cost_of_revenue = Column(DECIMAL(18, 2), nullable=True)
    sgna = Column(DECIMAL(18, 2), nullable=True)
    reconciled_depreciation = Column(DECIMAL(18, 2), nullable=True)
    other_non_operating_income_expenses = Column(DECIMAL(18, 2), nullable=True)
    interest_expense = Column(DECIMAL(18, 2), nullable=True)
    interest_income = Column(DECIMAL(18, 2), nullable=True)

    # ❌ 특정 기업 전용 필드
    special_income_charges = Column(DECIMAL(18, 2), nullable=True)
    restructuring_and_mergern_acquisition = Column(DECIMAL(18, 2), nullable=True)
    rent_expense_supplemental = Column(DECIMAL(18, 2), nullable=True)
    average_dilution_earnings = Column(DECIMAL(18, 2), nullable=True)



class BalanceSheet(Base):
    __tablename__ = "balance_sheet"

    crawling_id = Column(VARCHAR(64), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)

    # ✅ 필수 항목
    total_assets = Column(DECIMAL(18, 2), nullable=False)
    total_liabilities = Column(DECIMAL(18, 2), nullable=False)
    stockholders_equity = Column(DECIMAL(18, 2), nullable=False)

    # ⚠️ 일반적으로 포함
    current_assets = Column(DECIMAL(18, 2), nullable=True)
    current_liabilities = Column(DECIMAL(18, 2), nullable=True)
    retained_earnings = Column(DECIMAL(18, 2), nullable=True)
    cash_and_cash_equivalents = Column(DECIMAL(18, 2), nullable=True)
    accounts_receivable = Column(DECIMAL(18, 2), nullable=True)
    inventory = Column(DECIMAL(18, 2), nullable=True)
    cash_cash_equivalents_and_short_term_investments = Column(DECIMAL(18, 2), nullable=True)

    # ❌ 자주 누락되는 항목
    cash_equivalents = Column(DECIMAL(18, 2), nullable=True)
    cash_financial = Column(DECIMAL(18, 2), nullable=True)
    other_short_term_investments = Column(DECIMAL(18, 2), nullable=True)
    goodwill = Column(DECIMAL(18, 2), nullable=True)
    preferred_stock = Column(DECIMAL(18, 2), nullable=True)
    line_of_credit = Column(DECIMAL(18, 2), nullable=True)
    treasury_stock = Column(DECIMAL(18, 2), nullable=True)



class CashFlow(Base):
    __tablename__ = "cash_flow"

    crawling_id = Column(VARCHAR(64), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)

    # ✅ 반드시 필요한 항목
    operating_cash_flow = Column(DECIMAL(18, 2), nullable=False)
    investing_cash_flow = Column(DECIMAL(18, 2), nullable=False)
    financing_cash_flow = Column(DECIMAL(18, 2), nullable=False)
    free_cash_flow = Column(DECIMAL(18, 2), nullable=False)

    # ⚠️ 자주 포함되지만 누락 가능
    capital_expenditure = Column(DECIMAL(18, 2), nullable=True)
    depreciation_and_amortization = Column(DECIMAL(18, 2), nullable=True)
    stock_based_compensation = Column(DECIMAL(18, 2), nullable=True)
    income_tax_paid = Column(DECIMAL(18, 2), nullable=True)

    # ❌ 일부 기업에서만 제공
    net_intangibles_purchase_and_sale = Column(DECIMAL(18, 2), nullable=True)
    sale_of_business = Column(DECIMAL(18, 2), nullable=True)
    net_foreign_currency_exchange_gain_loss = Column(DECIMAL(18, 2), nullable=True)
