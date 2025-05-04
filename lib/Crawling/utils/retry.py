import time, random
from typing import Callable, Optional
from lib.Logger.logger import Logger


def retry_with_exponential_backoff(
    func: Callable,
    max_retries: int = 5,
    base_delay: float = 1.0,  # 1번째 재시도 대기시간
    max_delay: float = 60.0,  # 대기 상한선
    class_name=None,
    logger: Optional[Logger] = None,
    *args,
    **kwargs,
):
    """
    - 예외가 발생하면: delay = min(base_delay * 2**attempt, max_delay) + 지터
    - 지터(jitter)는 0.0 ~ 0.5초 사이 난수
    - 마지막 재시도까지 실패하면 예외 그대로 raise
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)

        except Exception as e:
            if attempt == max_retries - 1:  # 마지막 재시도도 실패 → 그대로 전달
                raise

            delay = min(base_delay * (2**attempt), max_delay)
            delay += random.uniform(0, 0.5)  # 작은 지터로 충돌 완화

            if logger:
                logger.log(
                    "DEBUG",
                    f"{class_name} 오류 감지 → {delay:.1f}s 대기 후 재시도 "
                    f"({attempt + 1}/{max_retries})",
                )

            time.sleep(delay)
