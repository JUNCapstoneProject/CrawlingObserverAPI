from .Crawler import CrawlerInterface
import requests
from bs4 import BeautifulSoup
import pandas as pd
from ..config.headers import HEADERS
from ..utils.save_data import save_to_json
from ..utils.random_delay import random_delay

class CrawlerUsingRequest(CrawlerInterface):
    def __init__(self, name, selector_config):
        super().__init__(name)
        self.tag = None
        self.config = selector_config
        self.max_articles = 2 # 크롤링할 뉴스 수
        self.max_retries = 30  # 요청 재시도 횟수

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
        fetch_result = self.fetch_page()
        soup = fetch_result["soup"]
        status_code = fetch_result["status_code"]
        target_url = fetch_result["url"]

        if not soup:
            return {
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": status_code,
                    "target_url": target_url
                },
                "fail_log": {
                    "err_message": "HTML 파싱 실패 또는 None 반환"
                }
            }

        try:
            articles = self.crawl_main(soup)
            if not articles:
                raise Exception("기사 추출 실패 (crawl_main 결과 없음)")

            df = pd.DataFrame(articles)
            return {
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": status_code,
                    "target_url": target_url
                },
                "df": df
            }

        except Exception as e:
            return {
                "tag": self.tag,
                "log": {
                    "crawling_type": self.tag,
                    "status_code": 500,
                    "target_url": target_url
                },
                "fail_log": {
                    "err_message": str(e)
                }
            } 
    
    def crawl_main(self, soup):
        # print(f"🔍 {self.__class__.__name__} 메인페이지 크롤링 시작")
        # print(self.config)
        """메인 페이지에서 기사 목록 추출"""
        articles, seen_urls = [], set()
        containers = self.extract_mainContainer(soup)
        # print(containers)

        if not containers:
            print("[ERROR] 메인 컨테이너를 찾을 수 없음! 선택자 확인 필요")
            return None

        for article in containers:
            if len(articles) >= self.max_articles:
                break

            # ✅ JSON에서 정의된 `main` 필드 자동 추출
            main_data = self.extract_fields(article, "main")
            # print(main_data)

            url = self.get_absolute_url(main_data.get("href"))
            if not url or url in seen_urls:
                print("중복 링크 탐지")
                continue
            seen_urls.add(url)

            # ✅ 개별 기사 추가 크롤링 실행
            article_content = self.crawl_content(url)
            if not article_content:
                print("기사 내용 없음")
                continue

            # ✅ JSON에서 정의된 필드 기반으로 동적 데이터 생성
            article_data = {
                **main_data,  # ✅ main에서 가져온 데이터 추가
                **article_content  # ✅ content에서 가져온 데이터 추가
            }
            articles.append(article_data)

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
        """JSON 설정을 기반으로 `extract_` 함수 동적 호출"""
        extracted_data = {}
        if section in self.config["selectors"]:
            for field, selectors in self.config["selectors"][section].items():
                extractor_func_name = f"extract_{field}"  # 예: extract_author, extract_content
                extractor_func = getattr(self, extractor_func_name, None)

                if callable(extractor_func):
                    extracted_data[field] = extractor_func(soup, selectors)

        return extracted_data

    def extract_href(self, soup, selectors):
        """기사 링크 (href) 추출"""
        for selector in selectors:
            href_element = soup.select_one(selector)
            if href_element:
                return href_element["href"]
        return None

    def extract_organization(self, soup, selectors):
        """기사 출처 (organization) 추출"""
        for selector in selectors:
            organization_element = soup.select_one(selector)
            if organization_element:
                return organization_element.get_text(strip=True)
        return None

    def extract_author(self, soup, selectors):
        """기사 작성자 (author) 추출"""
        for selector in selectors:
            author_element = soup.select_one(selector)
            if author_element:
                return author_element.get_text(strip=True)
        return "Unknown"

    def extract_title(self, soup, selectors):
        """기사 제목 (title) 추출"""
        for selector in selectors:
            title_element = soup.select_one(selector)
            if title_element:
                return title_element.get_text(strip=True)
        return None

    def extract_posted_at(self, soup, selectors):
        """기사 날짜 (posted_at) 추출"""
        for selector in selectors:
            posted_at_element = soup.select_one(selector)
            if posted_at_element and posted_at_element.has_attr("datetime"):
                return posted_at_element["datetime"].split(" ")[0]
        return None

    def extract_content(self, soup, selectors):
        """기사 본문 (content) 추출"""
        content_texts = []
        for selector in selectors:
            content_elements = soup.select(selector)
            if content_elements:
                content_texts.extend([e.get_text(strip=True) for e in content_elements])
        return " ".join(content_texts).strip() if content_texts else None

    def extract_tag(self, soup, selectors):
        """관련 주식 (tag) 추출"""
        tag = []
        for selector in selectors:
            tag_elements = soup.select(selector)
            tag.extend([ticker.get_text(strip=True) for ticker in tag_elements])
        return tag if tag else None

    def get_absolute_url(self, url):
        """절대 URL 변환"""
        return url if url.startswith("http") else self.config["base_url"] + url
