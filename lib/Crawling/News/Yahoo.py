from ..Interfaces.CrawlerUsingRequest import CrawlerUsingRequest

class YahooCrawler(CrawlerUsingRequest):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.tag = "YahooNews"
    
    """ 오버라이딩 코드들 """
    
    def extract_title(self, soup, selectors):
        """Yahoo Finance의 기사 제목 추출 (title 속성에서 가져옴)"""
        for selector in selectors:
            title_element = soup.select_one(selector)
            if title_element and title_element.has_attr("title"):
                return title_element["title"].strip()
        return None
    
    def extract_organization(self, soup, selectors):
        """기사 출처 (퍼블리셔) 추출"""
        for selector in selectors:
            organization_element = soup.select_one(selector)
            if organization_element:
                return organization_element.find(text=True, recursive=False).strip()  # ✅ 첫 번째 텍스트 노드만 추출
        return None
    
    def extract_posted_at(self, soup, selectors):
        """기사 날짜 (date) 추출"""
        for selector in selectors:
            posted_at_element = soup.select_one(selector)
            if posted_at_element and posted_at_element.has_attr("datetime"):
                return posted_at_element["datetime"].split("T")[0]
        return None