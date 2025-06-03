from sqlalchemy import Column, Integer, String, DateTime, TEXT, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, LONGTEXT

from lib.Distributor.secretary.models.core import Base


class News(Base):
    __tablename__ = "news"

    crawling_id = Column(
        VARCHAR(64),
        ForeignKey("crawling_logs.crawling_id"),
        primary_key=True,
        nullable=False,
    )
    organization = Column(VARCHAR(255), nullable=False)
    title = Column(VARCHAR(512), nullable=False)
    transed_title = Column(VARCHAR(512))
    hits = Column(Integer)
    author = Column(VARCHAR(255), nullable=False)
    posted_at = Column(DateTime, nullable=False)
    content = Column(LONGTEXT, nullable=False)
    ai_analysis = Column(Integer, nullable=True)


class NewsTag(Base):
    __tablename__ = "news_tag"

    tag_id = Column(Integer, primary_key=True, nullable=True, autoincrement=True)
    crawling_id = Column(VARCHAR(64), ForeignKey("news.crawling_id"), nullable=False)
    tag = Column(VARCHAR(50), ForeignKey("company.ticker"), nullable=False)
