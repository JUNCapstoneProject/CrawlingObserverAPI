from .Yahoo import YahooNewsCrawler
from .Investing import InvestingNewsCrawler
from ..config.LoadConfig import load_config
from concurrent.futures import ThreadPoolExecutor

def run():
    selector_config = load_config("selector_config.json")

    # 설정 정보를 매핑하여 관리
    crawler_configs = {
        "YahooFinance": YahooNewsCrawler,
        "InvestingNews": InvestingNewsCrawler
    }

    # 크롤러 인스턴스 생성
    crawlers = [
        cls(name, selector_config[name])
        for name, cls in crawler_configs.items()
    ]

    # 병렬 실행
    with ThreadPoolExecutor(max_workers=len(crawlers)) as executor:
        executor.map(lambda c: c.run(), crawlers)