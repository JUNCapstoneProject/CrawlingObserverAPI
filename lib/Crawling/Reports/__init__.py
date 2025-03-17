from .Investiong_report import InvestingReportCrawler
from ..config.LoadConfig import load_config
from concurrent.futures import ThreadPoolExecutor

def run():
    selector_config = load_config("selector_config.json")

    # 설정 정보를 매핑하여 관리
    crawler_configs = {
        "InvestingReports": InvestingReportCrawler
    }

    # 크롤러 인스턴스 생성
    crawlers = [
        cls(name, selector_config[name])
        for name, cls in crawler_configs.items()
    ]

    with ThreadPoolExecutor(max_workers=len(crawlers)) as executor:
        executor.map(lambda c: c.run(), crawlers)