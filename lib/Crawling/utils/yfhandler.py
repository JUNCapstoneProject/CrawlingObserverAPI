import logging
import time
from collections import OrderedDict


class _YFForwardHandler(logging.Handler):
    _dedup_interval = 10
    _cache_ttl = 60

    def __init__(self, crawler_logger):
        super().__init__()
        self.crawler_logger = crawler_logger
        self._seen_messages = OrderedDict()

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        msg = " ".join(msg.split())

        now = time.time()
        expired_keys = [
            k for k, t in self._seen_messages.items() if now - t > self._cache_ttl
        ]
        for key in expired_keys:
            self._seen_messages.pop(key)

        last_time = self._seen_messages.get(msg)
        if last_time and now - last_time < self._dedup_interval:
            return

        self._seen_messages[msg] = now

        log_func = getattr(
            self.crawler_logger, record.levelname.lower(), self.crawler_logger.info
        )
        log_func(f"yfinance: {msg}")
