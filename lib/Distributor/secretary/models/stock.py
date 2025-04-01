from sqlalchemy import Column, DateTime, DECIMAL, BigInteger, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR

from .core import Base

class Stock(Base):
    __tablename__ = "stock"

    crawling_id = Column(VARCHAR(64), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    ticker = Column(VARCHAR(10), nullable=False)
    posted_at = Column(DateTime, nullable=False)

    open = Column(DECIMAL(18, 2), nullable=False)
    high = Column(DECIMAL(18, 2), nullable=False)
    low = Column(DECIMAL(18, 2), nullable=False)
    close = Column(DECIMAL(18, 2), nullable=False)

    volume = Column(BigInteger, nullable=False)