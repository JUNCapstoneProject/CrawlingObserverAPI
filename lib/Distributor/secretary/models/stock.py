from sqlalchemy import Integer, Column, DateTime, DECIMAL, BigInteger, ForeignKey, Float
from sqlalchemy.dialects.mysql import VARCHAR

from lib.Distributor.secretary.models.core import Base


class Stock(Base):
    __tablename__ = "stock"

    crawling_id = Column(
        VARCHAR(64),
        ForeignKey("crawling_logs.crawling_id"),
        primary_key=True,
        nullable=False,
    )
    company_id = Column(
        Integer,
        ForeignKey("company.company_id"),
        nullable=False,
    )
    # ticker = Column(VARCHAR(10), nullable=False, unique=True)
    posted_at = Column(DateTime, nullable=False)

    open = Column(DECIMAL(18, 2), nullable=False)
    high = Column(DECIMAL(18, 2), nullable=False)
    low = Column(DECIMAL(18, 2), nullable=False)
    close = Column(DECIMAL(18, 2), nullable=False)
    # adj_close = Column(DECIMAL(18, 2), nullable=False)
    volume = Column(BigInteger, nullable=False)
    change = Column(Float, nullable=True)

    # market_cap = Column(BigInteger, nullable=False)


class Stock_Quarterly(Base):
    __tablename__ = "stock_quarterly"

    sq_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    company_id = Column(
        Integer,
        ForeignKey("company.company_id"),
        nullable=False,
    )
    shares = Column(BigInteger, nullable=False)
    eps = Column(DECIMAL(8, 4), nullable=True)
    per = Column(DECIMAL(8, 2), nullable=True)
    dividend_yield = Column(DECIMAL(5, 4), nullable=False)


class Stock_Daily(Base):
    __tablename__ = "stock_daily"

    sd_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    company_id = Column(
        Integer,
        ForeignKey("company.company_id"),
        nullable=False,
    )
    adj_close = Column(DECIMAL(18, 2), nullable=False)
    market_cap = Column(BigInteger, nullable=False)
    posted_at = Column(DateTime, nullable=False)
