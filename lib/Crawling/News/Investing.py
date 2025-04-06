from bs4 import BeautifulSoup
import cloudscraper
import datetime
import re

from lib.Crawling.Interfaces.CrawlerUsingRequest import CrawlerUsingRequest
from lib.Crawling.config.headers import HEADERS
from lib.Crawling.utils.random_delay import random_delay


class InvestingNewsCrawler(CrawlerUsingRequest):

    def __init__(self, name, config):
        super().__init__(name, config)
        self.scraper = cloudscraper.create_scraper()
        self.tag = "news"
        self.custom_handlers = {
            "organization": self.custom_extract_organization,
            "posted_at": self.custom_extract_posted_at
        }

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
    
    def extract_mainContainer(self, soup):
        """게시 시간이 없는 컨테이너 제외하고 메인 뉴스 컨테이너 추출"""
        containers = []
        for selector in self.config["main_container_selectors"]:
            main_containers = soup.select(selector)
            for container in main_containers:
                # <time> 태그가 있는지 확인
                time_tag = container.select_one("time[data-test='article-publish-date']")
                if time_tag:
                    containers.append(container)  # 시간 정보가 있는 경우만 추가
        return containers if containers else None
    


    def custom_extract_organization(self, soup, selectors):
        """기사 출처 (퍼블리셔) 추출"""
        for selector in selectors:
            organization_element = soup.select_one(selector)
            if organization_element:
                return organization_element.find(text=True, recursive=False).strip()  # ✅ 첫 번째 텍스트 노드만 추출
        return None
    
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