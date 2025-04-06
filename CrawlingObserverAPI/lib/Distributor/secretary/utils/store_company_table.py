# test/imsi.ipynb 에서 실행
import sys
import os
import json
from pathlib import Path

# sys.path에 lib 상위 경로 추가
sys.path.append(os.path.abspath(".."))

from lib.Distributor.secretary.session import SessionLocal
from lib.Distributor.secretary.models.company import Company

# JSON 파일 경로
json_path = "symbols.json"

try:
    # JSON 로드
    with open(json_path, "r", encoding="utf-8") as f:
        companies = json.load(f)

    print(f"📦 총 {len(companies)}개의 회사 데이터를 불러왔습니다.")

    # DB 세션 시작
    session = SessionLocal()
    insert_count = 0

    for entry in companies:
        ticker = entry.get("ticker")
        name_kr = entry.get("name_kr")
        name_en = entry.get("name_en")

        if not ticker or not name_en:
            print(f"⚠️ 누락된 값이 있어 건너뜀: {entry}")
            continue

        company = Company(ticker=ticker, name_kr=name_kr, name_en=name_en)
        session.merge(company)
        insert_count += 1

    session.commit()
    print(f"✅ {insert_count}개 회사가 DB에 저장되었습니다.")

except Exception as e:
    print("❌ 전체 예외 발생:", e)

    # 세션이 정의돼 있으면 롤백 시도
    try:
        session.rollback()
    except:
        pass

finally:
    try:
        session.close()
    except:
        pass
