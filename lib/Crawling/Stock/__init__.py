from concurrent.futures import ThreadPoolExecutor
from .YFinance_stock import YFinanceStockCrawler

def run():

    # 설정 정보를 매핑하여 관리
    crawler_configs = {
        "YFinanceStock_1m": {"cls": YFinanceStockCrawler, "kwargs": {"interval": "1m"}},
        "YFinanceStock_5m": {"cls": YFinanceStockCrawler, "kwargs": {"interval": "5m"}},
        "YFinanceStock_15m": {"cls": YFinanceStockCrawler, "kwargs": {"interval": "15m"}}
    }

    # 크롤러 인스턴스 생성
    crawlers = [
        config["cls"](name, **config["kwargs"])
        for name, config in crawler_configs.items()
    ]

    # 병렬 실행
    with ThreadPoolExecutor(max_workers=len(crawlers)) as executor:
        executor.map(lambda c: c.run(), crawlers)