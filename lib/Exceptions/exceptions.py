class CrawlerException(Exception):
    """기본 크롤러 예외 클래스"""

    status_code = 500
    default_message = "크롤러 처리 중 예외 발생"

    def __init__(self, message=None, source=None, status_code=None):
        super().__init__(message or self.default_message)
        self.message = message or self.default_message
        self.source = source
        self.status_code = status_code or self.status_code

    def __str__(self):
        source_info = f"[{self.source}] " if self.source else ""
        return f"{source_info}{self.message} (status_code={self.status_code})"


class InvalidConfigException(CrawlerException):
    """잘못된 설정 예외"""

    status_code = 400
    default_message = "잘못된 크롤링 설정입니다."


class DataNotFoundException(CrawlerException):
    """데이터 없음 예외"""

    status_code = 404
    default_message = "데이터가 존재하지 않습니다."


class ExternalAPIException(CrawlerException):
    """외부 API 호출 실패 예외"""

    status_code = 502
    default_message = "외부 API 호출 실패"


class ParsingException(CrawlerException):
    """파싱 실패 예외"""

    status_code = 422
    default_message = "파싱 실패 (HTML 구조 변경 등)"


class BatchProcessingException(CrawlerException):
    """배치 처리 중 오류 예외"""

    status_code = 500
    default_message = "배치 처리 중 오류"


def raise_exception(exception_class, message=None, source=None, status_code=None):
    """유연한 예외 생성 및 발생 함수"""
    raise exception_class(message=message, source=source, status_code=status_code)
