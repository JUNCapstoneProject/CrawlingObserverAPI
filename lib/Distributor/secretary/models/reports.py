from sqlalchemy import Column, Integer, DateTime, Text, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, LONGTEXT

from lib.Distributor.secretary.models.core import Base


class Report(Base):
    __tablename__ = "reports"

    report_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    crawling_id = Column(
        VARCHAR(64),
        ForeignKey("crawling_logs.crawling_id"),
        nullable=False,
    )
    title = Column(VARCHAR(512), nullable=False)
    transed_title = Column(VARCHAR(512))
    hits = Column(Integer)
    author = Column(VARCHAR(45), nullable=False)
    posted_at = Column(DateTime, nullable=False)
    content = Column(LONGTEXT, nullable=False)
    tag_id = Column(Integer, ForeignKey("reports_tag.tag_id"), nullable=False)


class ReportTag(Base):
    __tablename__ = "reports_tag"

    tag_id = Column(Integer, primary_key=True, nullable=True, autoincrement=True)
    tag = Column(VARCHAR(50), ForeignKey("company.ticker"), nullable=False)
