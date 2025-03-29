from sqlalchemy import Column, Integer, String, DateTime, DECIMAL, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR

from .core import Base

class MacroIndex(Base):
    __tablename__ = "macro_index"

    index_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    index_name = Column(VARCHAR(255), nullable=False)


class MacroEconomics(Base):
    __tablename__ = "macroeconomics"

    crawling_id = Column(VARCHAR(36), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    country = Column(VARCHAR(255), nullable=False)
    index_id = Column(Integer, ForeignKey("macro_index.index_id"), nullable=False)
    index_value = Column(DECIMAL(20, 4), nullable=False)
    posted_at = Column(DateTime, nullable=False)
