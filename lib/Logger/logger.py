# lib/Logger/logger.py
import re
import os
from datetime import datetime
from lib.Config.config import Config

COLOR_MAP = {
    "START": "\033[92m",  # 초록
    "FILE": "\033[96m",  # 밝은 청록
    "DB": "\033[94m",  # 파랑
    "WAIT": "\033[90m",  # 회색
    "WARN": "\033[93m",  # 노랑
    "ERROR": "\033[91m",  # 빨강
    "INFO": "\033[97m",  # 흰색
    "DEBUG": "\033[95m",  # 보라
    "RESET": "\033[0m",  # 색상 초기화
}


class Logger:
    use_color = Config.get("color_log", True)
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    def __init__(self, name: str):
        self.name = name

    def log(self, level: str, message: str):
        color = COLOR_MAP.get(level.upper(), "") if Logger.use_color else ""
        reset = COLOR_MAP["RESET"] if Logger.use_color else ""
        timestamp = datetime.now().strftime("%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] [{level:<6}] {self.name:<24} - {message}"

        # DEBUG 로그는 테스트 모드일 때만 출력
        if level.upper() != "DEBUG" or Config.get("is_test", False):
            print(f"{color}{formatted}{reset}")

        with open(Logger.log_file, "a", encoding="utf-8") as f:
            f.write(f"{formatted}\n")

    def log_sqlalchemy_error(self, error: Exception):
        from sqlalchemy.exc import IntegrityError

        if isinstance(error, IntegrityError):
            match = re.search(r"Column '(\w+)' cannot be null", str(error))
            null_col = match.group(1) if match else "unknown"
            self.log("ERROR", f"삽입 실패 (필드: {null_col}=NULL) → SKIP")
        else:
            self.log("ERROR", f"DB 오류 발생 → {type(error).__name__}: {str(error)}")
