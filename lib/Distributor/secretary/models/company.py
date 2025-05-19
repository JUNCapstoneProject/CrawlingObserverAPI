from sqlalchemy import Column, Integer
from sqlalchemy.dialects.mysql import VARCHAR

from lib.Distributor.secretary.models.core import Base


class Company(Base):
    __tablename__ = "company"

    company_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    ticker = Column(VARCHAR(20), nullable=False, unique=True)
    cik = Column(VARCHAR(20), nullable=False, unique=True)
    name_kr = Column(VARCHAR(255), nullable=False)
    name_en = Column(VARCHAR(255), nullable=False)
    sector = Column(VARCHAR(50))
