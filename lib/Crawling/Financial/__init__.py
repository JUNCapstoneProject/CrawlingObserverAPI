from concurrent.futures import ThreadPoolExecutor
from .Fred import FredCrawler
from .Finance import FinancialCrawler
from lib.Config.config import Config


def run():

    # 설정 정보를 매핑하여 관리
    crawler_configs = {"Fred": FredCrawler, "Finance": FinancialCrawler}

    # 크롤러 인스턴스 생성 (API 키가 있는 경우에만 전달)
    crawlers = []
    for name, cls in crawler_configs.items():
        key = Config.get(f"API_KEYS.{name}")
        crawlers.append(cls(name, key) if key else cls(name))

    # 병렬 실행
    with ThreadPoolExecutor(max_workers=len(crawlers)) as executor:
        executor.map(lambda c: c.run(), crawlers)
