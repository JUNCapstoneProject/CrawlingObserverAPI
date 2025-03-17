from ..Interfaces.CrawlerUsingRequest import CrawlerUsingRequest
from ..config.headers import HEADERS
from ..utils.random_delay import random_delay
from bs4 import BeautifulSoup
import cloudscraper
import datetime
import re

class InvestingCrawler(CrawlerUsingRequest):

    def __init__(self, name, config):
        super().__init__(name, config)
        self.scraper = cloudscraper.create_scraper()
    
    """ 오버라이딩 코드들 """
    
    def crawl_content(self, url):
        """기사 개별 페이지에서 본문 및 추가 정보 크롤링"""
        article_soup = self.fetch_page(url, 10)

        if not article_soup:
            return None
        
        content_container = self.extract_contentContainer(article_soup)
        if not content_container:
            return None  # 컨텐츠 컨테이너가 없으면 기사 크롤링 실패
        
        article_content = self.extract_fields(content_container, "contents")

        return article_content

    def fetch_page(self, url=None, max_retries=None):
        """Cloudflare 우회를 위한 페이지 가져오기 (자동 재시도 포함)"""
        if url is None:
            url = self.config["url"]

        if max_retries is None:
            max_retries = self.max_retries  # 기본 설정 유지

        retries = 0
        while retries < max_retries:
            try:
                # print(f"🔍 [시도 {retries + 1}/{self.max_retries}] {url} 요청 중...")

                response = self.scraper.get(url, headers=HEADERS, timeout=20)
                
                # HTTP 응답 코드 확인
                if response.status_code == 200:
                    # print(f"✅ [시도 {retries + 1}/{max_retries}] 크롤링 성공!")
                    return BeautifulSoup(response.text, "html.parser")

                # print(f"⚠️ [시도 {retries + 1}/{max_retries}] HTTP 오류 발생: {response.status_code}")
            
            except Exception as e:
                print(f"{self.__class__.__name__}: 요청 실패 {e}")

            # 재시도 설정
            retries += 1
            if retries < max_retries:
                # print("⏳ Retry at intervals...")
                random_delay()

        print(f"{self.__class__.__name__}: 모든 재시도 실패. url: {url}")
        return None
    
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

    def extract_organization(self, soup, selectors):
        """기사 출처 (퍼블리셔) 추출"""
        for selector in selectors:
            organization_element = soup.select_one(selector)
            if organization_element:
                return organization_element.find(text=True, recursive=False).strip()  # ✅ 첫 번째 텍스트 노드만 추출
        return None
    
    def extract_posted_at(self, soup, selectors):
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