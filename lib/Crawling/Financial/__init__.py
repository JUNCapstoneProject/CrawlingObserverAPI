from concurrent.futures import ThreadPoolExecutor
from ..config.keys import API_KEYS
from .Fred import FredCrawler
from .YFinance import YFinanceCrawler


def run():

    # 설정 정보를 매핑하여 관리
    crawler_configs = {"Fred": FredCrawler, "YFinance": YFinanceCrawler}

    # 크롤러 인스턴스 생성 (API 키가 있는 경우에만 전달)
    crawlers = [
        cls(name, API_KEYS.get(name)) if API_KEYS.get(name) else cls(name)
        for name, cls in crawler_configs.items()
    ]

    # 병렬 실행
    with ThreadPoolExecutor(max_workers=len(crawlers)) as executor:
        executor.map(lambda c: c.run(), crawlers)
