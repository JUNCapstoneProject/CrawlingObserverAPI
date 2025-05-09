# 표준 라이브러리
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)  # 스레드 풀 및 작업 완료 처리

# 내부 모듈
from lib.Crawling.News import run as run_news  # 뉴스 크롤링 실행 함수
from lib.Crawling.Reports import run as run_reports  # 리포트 크롤링 실행 함수
from lib.Crawling.Financial import run as run_financial  # 금융 데이터 크롤링 실행 함수
from lib.Crawling.Stock import run as run_stock  # 주식 데이터 크롤링 실행 함수
from lib.Config.config import Config  # 설정 관리 클래스


def run():
    """크롤링 실행 (스레드 풀)"""
    crawler_switch = Config.get("crawler_switch", {})

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        # 뉴스 크롤링 실행
        if crawler_switch.get("news", True):
            futures.append(executor.submit(run_news))

        # 리포트 크롤링 실행
        if crawler_switch.get("reports", True):
            futures.append(executor.submit(run_reports))

        # 금융 데이터 크롤링 실행
        if crawler_switch.get("financial", True):
            futures.append(executor.submit(run_financial))

        # 주식 데이터 크롤링 실행
        if crawler_switch.get("stock", True):
            futures.append(executor.submit(run_stock))

        # 모든 크롤링 작업 완료 대기
        for future in as_completed(futures):
            future.result()  # 작업 결과 가져오기 (예외 발생 시 상위 호출자로 전달)
