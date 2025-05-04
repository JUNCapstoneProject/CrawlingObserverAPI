import logging
import time
from collections import OrderedDict
from lib.Logger.logger import Logger


class _YFForwardHandler(logging.Handler):
    """yfinance 로그를 프로젝트 Logger로 포워딩 (중복은 10초 내 재출력 금지)"""

    _map = {10: "DEBUG", 20: "INFO", 30: "WARN", 40: "ERROR", 50: "ERROR"}
    _dedup_interval = 10  # 중복 허용 시간 (초)
    _cache_ttl = 60  # 캐시 보관 시간 (초)

    def __init__(self, crawler_logger: Logger):
        super().__init__()
        self.crawler_logger = crawler_logger
        self._seen_messages = OrderedDict()  # {msg: last_emit_time}

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        msg = " ".join(msg.split())  # 줄바꿈, 탭, 다중 공백 제거

        now = time.time()
        # 오래된 캐시 제거 (TTL 초과)
        expired_keys = [
            k for k, t in self._seen_messages.items() if now - t > self._cache_ttl
        ]
        for key in expired_keys:
            self._seen_messages.pop(key)

        # 10초 이내 중복 메시지 무시
        last_time = self._seen_messages.get(msg)
        if last_time and now - last_time < self._dedup_interval:
            return

        # 로그 기록 및 캐시 갱신
        self._seen_messages[msg] = now
        lvl = self._map.get(record.levelno, "INFO")
        self.crawler_logger.log(lvl, f"yfinance: {msg}")
