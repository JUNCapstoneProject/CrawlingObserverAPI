# create_tables.py

from lib.Distributor.secretary.session import engine
from lib.Distributor.secretary.models.core import Base
from lib.Distributor.secretary.models.financials import *
from lib.Distributor.secretary.models.macro import *
from lib.Distributor.secretary.models.news import *
from lib.Distributor.secretary.models.reports import *
from lib.Distributor.secretary.models.stock import *
from lib.Distributor.secretary.models.company import *

# 테이블 생성
Base.metadata.create_all(engine)

print("✅ 모든 테이블이 성공적으로 생성되었습니다.")
