from bs4 import BeautifulSoup
import cloudscraper
import datetime
import re

from lib.Crawling.Interfaces.CrawlerUsingRequest import CrawlerUsingRequest
from lib.Crawling.config.headers import HEADERS
from lib.Crawling.utils.random_delay import random_delay
from lib.Exceptions.exceptions import *




class InvestingReportCrawler(CrawlerUsingRequest):

    def __init__(self, name, config):
        super().__init__(name, config)
        self.scraper = cloudscraper.create_scraper()
        self.tag = "reports"
        self.custom_handlers = {
            "posted_at": self.custom_extract_posted_at
        }

    """ 오버라이딩 코드들 """

    def fetch_page(self, url=None, max_retries=None):
        """Cloudflare 우회를 포함한 요청 함수. 요청 성공 시 soup+url 반환, 실패 시 ExternalAPIException 발생."""
        url = url or self.config["url"]
        max_retries = max_retries or self.max_retries

        for attempt in range(1, max_retries + 1):
            try:
                response = self.scraper.get(url, headers=HEADERS, timeout=20)
                if response.status_code == 200:
                    return {
                        "soup": BeautifulSoup(response.text, "html.parser"),
                        "status_code": 200,
                        "url": url
                    }
            except Exception:
                pass  # 로그 시스템을 쓰는 경우 여기서 logging.warning(e) 가능

            if attempt < max_retries:
                random_delay()

        raise ExternalAPIException("Cloudflare 우회 요청 실패 - 최대 재시도 초과", source=url)
    
    def custom_extract_posted_at(self, soup, selectors):
        """날짜 문자열에서 직접 날짜 추출"""
        for selector in selectors:
            posted_at_element = soup.select_one(selector)
    
            if posted_at_element:
                date_text = posted_at_element.get_text(strip=True)
            
                # ✅ 정규식으로 "MM/DD/YYYY" 형식 추출
                date_match = re.search(r"(\d{2}/\d{2}/\d{4})", date_text)
                if date_match:
                    extracted_date = date_match.group(1)
                    
                    # ✅ MM/DD/YYYY → YYYY-MM-DD 변환
                    formatted_date = datetime.datetime.strptime(extracted_date, "%m/%d/%Y").strftime("%Y-%m-%d")
                    return formatted_date

        return None  # 날짜를 찾지 못한 경우