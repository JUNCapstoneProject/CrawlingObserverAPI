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

    # 테스트용
    def save_data(self, articles):
        """뉴스 데이터를 news_data.json에 저장"""
        save_to_json(articles, "lib/Crawling/data/reports_data.json", append=True)
        print(f"{len(articles)}개의 리포트 데이터를 reports_data.json에 저장 완료")

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
                # print("Retry at intervals...")
                random_delay()

        print(f"{self.__class__.__name__}: 모든 재시도 실패. url: {url}")
        return None