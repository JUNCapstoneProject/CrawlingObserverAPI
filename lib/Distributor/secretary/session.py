from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from lib.Config.config import Config  # YAML 기반 설정 클래스

if not Config._config:
    Config.init()

# ✅ 설정에서 DB URL 불러오기
db_url = Config.get("database.url", default="sqlite:///:memory:")

# ✅ SQLAlchemy 세션 설정
engine = create_engine(db_url, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
