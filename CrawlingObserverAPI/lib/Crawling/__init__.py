from concurrent.futures import ThreadPoolExecutor
from lib.Crawling.News import run as run_news
from lib.Crawling.Reports import run as run_reports
from lib.Crawling.Financial import run as run_financial
from lib.Crawling.Stock import run as run_stock

def run():
    """크롤링 실행 (스레드 풀)"""
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        
        # 뉴스 크롤링 실행
        futures.append(executor.submit(run_news))
        
        # 리포트 크롤링 실행
        futures.append(executor.submit(run_reports))
        
        # 금융 데이터 크롤링 실행
        futures.append(executor.submit(run_financial))
        
        # 주식 데이터 크롤링 실행
        futures.append(executor.submit(run_stock))

        # 모든 크롤링 작업 완료 대기
        for future in futures:
            future.result()  # 예외 발생 시 처리 가능