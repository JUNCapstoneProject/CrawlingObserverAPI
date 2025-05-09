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
    ai_analysis = Column(VARCHAR(512), nullable=True)


class NewsTag(Base):
    __tablename__ = "news_tag"

    crawling_id = Column(
        VARCHAR(64), ForeignKey("news.crawling_id"), primary_key=True, nullable=False
    )
    tag = Column(VARCHAR(255), nullable=False, primary_key=True)
