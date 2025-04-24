class CrawlerException(Exception):
    status_code = 500
    default_message = "크롤러 처리 중 예외 발생"

    def __init__(self, message=None, source=None):
        super().__init__(message or self.default_message)
        self.message = message or self.default_message
        self.source = source

    def __str__(self):
        return f"[{self.source}] {self.message}" if self.source else self.message


class InvalidConfigException(CrawlerException):
    status_code = 400
    default_message = "잘못된 크롤링 설정입니다."


class DataNotFoundException(CrawlerException):
    status_code = 404
    default_message = "데이터가 존재하지 않습니다."


class ExternalAPIException(CrawlerException):
    status_code = 502
    default_message = "외부 API 호출 실패"


class ParsingException(CrawlerException):
    status_code = 422
    default_message = "파싱 실패 (HTML 구조 변경 등)"


class BatchProcessingException(CrawlerException):
    status_code = 500
    default_message = "배치 처리 중 오류"
