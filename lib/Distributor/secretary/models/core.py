from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, func, text
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class CrawlingLog(Base):
    __tablename__ = "crawling_logs"

    crawling_id = Column(VARCHAR(64), primary_key=True, server_default=text("UUID()"), nullable=False, autoincrement=False)
    crawling_type = Column(String(255), nullable=False)
    target_url = Column(String(2083))
    try_time = Column(DateTime, nullable=False, server_default=func.now())
    status_code = Column(Integer, nullable=False)

class FailLog(Base):
    __tablename__ = "fail_logs"

    crawling_id = Column(VARCHAR(64), ForeignKey("crawling_logs.crawling_id"), primary_key=True, nullable=False)
    err_message = Column(Text, nullable=False)
