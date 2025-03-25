from ..Interfaces.CrawlerUsingRequest import CrawlerUsingRequest
from ..utils.save_data import save_to_json
from ..config.headers import HEADERS
from ..utils.random_delay import random_delay
from bs4 import BeautifulSoup
import cloudscraper

class InvestingReportCrawler(CrawlerUsingRequest):

    def __init__(self, name, config):
        super().__init__(name, config)
        self.scraper = cloudscraper.create_scraper()
        self.tag = "reports"

    """ 오버라이딩 코드들 """

    def fetch_page(self, url=None, max_retries=None):
        """Cloudflare 우회를 포함한 요청 함수. Dispatcher 연동을 위해 상태코드/URL 포함한 dict 반환."""
        if url is None:
            url = self.config["url"]

        if max_retries is None:
            max_retries = self.max_retries

        retries = 0
        last_status = 500  # 기본값: 실패 코드

        while retries < max_retries:
            try:
                response = self.scraper.get(url, headers=HEADERS, timeout=20)
                last_status = response.status_code

                if response.status_code == 200:
                    return {
                        "soup": BeautifulSoup(response.text, "html.parser"),
                        "status_code": 200,
                        "url": url
                    }

            except Exception as e:
                print(f"[{self.__class__.__name__}] 요청 실패: {e}")

            retries += 1
            if retries < max_retries:
                random_delay()

        # 모든 재시도 실패
        print(f"[{self.__class__.__name__}] 모든 재시도 실패: {url}")
        return {
            "soup": None,
            "status_code": last_status,
            "url": url
        }