from sqlalchemy import Column, Integer, DateTime, Text, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR

from .core import Base

class Report(Base):
    __tablename__ = "reports"

    crawling_id = Column(VARCHAR(36), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    title = Column(VARCHAR(512), nullable=False)
    hits = Column(Integer)
    author = Column(VARCHAR(45), nullable=False)
    posted_at = Column(DateTime, nullable=False)
    content = Column(Text, nullable=False)
    ai_analysis = Column(VARCHAR(512), nullable=True)


class ReportTag(Base):
    __tablename__ = "reports_tag"

    crawling_id = Column(VARCHAR(36), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    tag = Column(VARCHAR(255), nullable=False)