import requests
from bs4 import BeautifulSoup
import pandas as pd


from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Crawling.config.headers import HEADERS
from lib.Crawling.Interfaces.Crawler_handlers import EXTRACT_HANDLERS
from lib.Config.config import Config
from lib.Crawling.utils.retry import retry_with_exponential_backoff


class CrawlerUsingRequest(CrawlerInterface):
    def __init__(self, name, selector_config):
        super().__init__(name)
        self.tag = None
        self.config = selector_config
        self.max_articles = Config.get("articles.size", 6)  # 크롤링할 뉴스 수
        self.max_retries = Config.get("articles.retry", 50)  # 요청 재시도 횟수
        self.custom_handlers = {}
        self.use_pagination = bool(selector_config.get("next_page", False))

    def fetch_page(self, url=None):
        if url is None:
            url = self.config["url"]

        def _fetch():
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            return {
                "soup": BeautifulSoup(response.text, "html.parser"),
                "status_code": response.status_code,
                "url": url,
            }

        # retry를 함수 호출 형태로 적용
        return retry_with_exponential_backoff(
            _fetch,
            max_retries=self.max_retries,
            base_delay=1.0,
            max_delay=30,
            class_name=self.__class__.__name__,
            logger=self.logger,
        )

    def crawl(self):
        results = []
        try:
            fetch_result = self.fetch_page()
            soup = fetch_result["soup"]
            target_url = fetch_result["url"]

            if not soup:
                self.logger.warning("HTML 파싱 실패 또는 soup None")

            articles = self.crawl_main(soup)
            if not articles:
                self.logger.warning("크롤링 결과 없음")

            for article in articles:
                href = self.get_absolute_url(article.get("href"))
                if not href:
                    continue

                result = {
                    "tag": self.tag,
                    "log": {
                        "crawling_type": self.tag,
                        "status_code": 200,
                        "target_url": href,
                    },
                }

                if article.get("content"):
                    result["df"] = pd.DataFrame([article])
                else:
                    self.logger.warning("기사 내용 없음")

                results.append(result)

            return results

        except Exception as e:
            status_code = (
                getattr(e.response, "status_code", 500)
                if isinstance(e, requests.HTTPError)
                else 500
            )
            target_url = (
                getattr(e.response, "url", None)
                if isinstance(e, requests.HTTPError)
                else getattr(e, "source", None)
            )
            self.logger.error(f"{type(e).__name__}: {str(e)}")

            return [
                {
                    "tag": self.tag,
                    "log": {
                        "crawling_type": self.tag,
                        "status_code": status_code,
                        "target_url": target_url,
                    },
                    "fail_log": {"err_message": f"{type(e).__name__}: {str(e)}"},
                }
            ]

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

                article_data = {**main_data, **article_content}
                articles.append(article_data)

            if self.use_pagination:
                page_url = self.get_next_page_url(soup)
                page_count += 1
            else:
                break

        return articles

    def crawl_content(self, url):
        fetch_result = self.fetch_page(url)
        article_soup = fetch_result["soup"]

        if not article_soup:
            return None

        content_container = self.extract_contentContainer(article_soup)
        if not content_container:
            return None

        return self.extract_fields(content_container, "contents")

    def extract_mainContainer(self, soup):
        containers = []
        for selector in self.config["main_container_selectors"]:
            main_containers = soup.select(selector)
            if main_containers:
                containers.extend(main_containers)
        return containers if containers else None

    def extract_contentContainer(self, soup):
        for selector in self.config["content_container_selectors"]:
            content_container = soup.select_one(selector)
            if content_container:
                return content_container
        return None

    def extract_fields(self, soup, section):
        extracted_data = {}
        if section in self.config["selectors"]:
            for field, selectors in self.config["selectors"][section].items():
                handler = self.custom_handlers.get(field) or EXTRACT_HANDLERS.get(field)
                if handler:
                    extracted_data[field] = handler(soup, selectors)
        return extracted_data

    def get_absolute_url(self, url):
        return url if url and url.startswith("http") else self.config["base_url"] + url

    def get_next_page_url(self, soup):
        try:
            next_selector = self.config.get("next_page")
            if not next_selector:
                return None
            next_link = soup.select_one(next_selector)
            if next_link and next_link.get("href"):
                return self.get_absolute_url(next_link.get("href"))
        except Exception:
            return None
