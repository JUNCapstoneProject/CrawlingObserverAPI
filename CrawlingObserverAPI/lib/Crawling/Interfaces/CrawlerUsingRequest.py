import requests
from bs4 import BeautifulSoup
import pandas as pd

from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Crawling.config.headers import HEADERS
from lib.Crawling.utils.random_delay import random_delay
from lib.Crawling.Interfaces.Crawler_handlers import EXTRACT_HANDLERS


class CrawlerUsingRequest(CrawlerInterface):
    def __init__(self, name, selector_config):
        super().__init__(name)
        self.tag = None
        self.config = selector_config
        self.max_articles = 30 # 크롤링할 뉴스 수
        self.max_retries = 50  # 요청 재시도 횟수
        self.custom_handlers = {}
        self.use_pagination = bool(selector_config.get("next_page", False))


    # 사이트 보안 방식에 따라 자식 클래스에서 오버라이드
    def fetch_page(self, url=None):
        if url is None:
            url = self.config["url"]

        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.get(url, headers=HEADERS, timeout=20)
                response.raise_for_status()
                return {
                    "soup": BeautifulSoup(response.text, "html.parser"),
                    "status_code": response.status_code,
                    "url": url
                }

            except requests.exceptions.RequestException as e:
                print(f"[fetch_page] 요청 실패: {e}")
                retries += 1
                if retries < self.max_retries:
                    print("Retrying...")
                    random_delay()

        return {
            "soup": None,
            "status_code": 500,
            "url": url
        }
        
    def crawl(self):
        """뉴스 리스트 페이지에서 기사 정보 가져오기"""
        # print(f"🔍 {self.__class__.__name__} 크롤링 시작")
        results = []

        fetch_result = self.fetch_page()
        soup = fetch_result["soup"]
        status_code = fetch_result["status_code"]
        target_url = fetch_result["url"]

        if not soup:
            return [{
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": status_code,
                    "target_url": target_url
                },
                "fail_log": {
                    "err_message": "HTML 파싱 실패 또는 None 반환"
                }
            }]

        try:
            articles = self.crawl_main(soup)
            if not articles:
                raise Exception("기사 추출 실패 (crawl_main 결과 없음)")

            for article in articles:
                href = self.get_absolute_url(article.get("href"))

                if not href:
                    continue  # URL이 없으면 스킵

                result = {
                    "tag": self.tag,
                    "log": {
                        "crawling_type": self.tag,
                        "status_code": 200,
                        "target_url": href
                    }
                }

                if article.get("content"):  # 본문 내용이 있으면 성공
                    result["df"] = pd.DataFrame([article])  # 한 기사 = 한 row
                else:
                    result["fail_log"] = {
                        "err_message": "기사 내용 없음"
                    }

                results.append(result)

            return results

        except Exception as e:
            return [{
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": 500,
                    "target_url": target_url
                },
                "fail_log": {
                    "err_message": str(e)
                }
            }]
    
    def crawl_main(self, soup):
        articles, seen_urls = [], set()
        page_url = self.config["url"]
        page_count = 0

        while len(articles) < self.max_articles and page_url and page_count < 10:
            fetch_result = self.fetch_page(page_url)
            soup = fetch_result["soup"]
            if not soup:
                break

            containers = self.extract_mainContainer(soup)
            if not containers:
                print("[ERROR] 메인 컨테이너를 찾을 수 없음!")
                break

            for article in containers:
                if len(articles) >= self.max_articles:
                    break

                main_data = self.extract_fields(article, "main")
                url = self.get_absolute_url(main_data.get("href"))
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                article_content = self.crawl_content(url)
                if not article_content or not article_content.get("content"):
                    continue

                article_data = {
                    **main_data,
                    **article_content
                }
                articles.append(article_data)

            # ✅ 페이지네이션이 설정된 경우에만 다음 페이지 진행
            if self.use_pagination:
                page_url = self.get_next_page_url(soup)
                page_count += 1
            else:
                break

        return articles

    def crawl_content(self, url):
        """기사 개별 페이지에서 본문 및 추가 정보 크롤링"""
        fetch_result = self.fetch_page(url)
        article_soup = fetch_result["soup"]

        if not article_soup:
            return None
        
        content_container = self.extract_contentContainer(article_soup)
        if not content_container:
            return None  # 컨텐츠 컨테이너가 없으면 기사 크롤링 실패
        
        article_content = self.extract_fields(content_container, "contents")

        return article_content
    
    def extract_mainContainer(self, soup):
        """메인 뉴스 컨테이너 추출 (다중 선택자 지원)"""
        containers = []
        for selector in self.config["main_container_selectors"]:
            main_containers = soup.select(selector)
            if main_containers:
                containers.extend(main_containers)
        return containers if containers else None

    def extract_contentContainer(self, soup):
        """기사 본문이 포함된 최상위 컨테이너 추출"""
        for selector in self.config["content_container_selectors"]:
            content_container = soup.select_one(selector)
            if content_container:
                return content_container
        return None
    
    def extract_fields(self, soup, section):
        """커스텀 핸들러 우선 적용"""
        extracted_data = {}
        if section in self.config["selectors"]:
            for field, selectors in self.config["selectors"][section].items():
                # 커스텀 핸들러 우선 → 기본 핸들러 fallback
                handler = self.custom_handlers.get(field) or EXTRACT_HANDLERS.get(field)
                if handler:
                    extracted_data[field] = handler(soup, selectors)
                else:
                    print(f"[extract_fields] 핸들러 없음: {field}")
        return extracted_data
    
    def get_absolute_url(self, url):
        """절대 URL 변환"""
        return url if url.startswith("http") else self.config["base_url"] + url
    
    def get_next_page_url(self, soup):
        """다음 페이지 URL 추출"""
        try:
            next_selector = self.config.get("next_page")
            if not next_selector:
                return None
            next_link = soup.select_one(next_selector)
            if next_link and next_link.get("href"):
                # print(f"[get_next_page_url] 다음 페이지 URL 추출됨: {next_link['href']}")
                return self.get_absolute_url(next_link.get("href"))
        except Exception as e:
            print(f"[get_next_page_url] 다음 페이지 URL 추출 실패: {e}")
        return None
