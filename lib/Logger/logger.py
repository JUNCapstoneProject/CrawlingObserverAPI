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
    base_log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(base_log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    common_log_file = os.path.join(base_log_dir, f"log_common_{timestamp}.log")

    error_count = 0
    error_log_dir = os.path.join(base_log_dir, "errors")
    os.makedirs(error_log_dir, exist_ok=True)
    error_log_file = os.path.join(error_log_dir, f"log_error_{timestamp}.log")

    def __init__(self, name: str):
        self.name = name
        self.indiv_log_dir = os.path.join(Logger.base_log_dir, name)
        os.makedirs(self.indiv_log_dir, exist_ok=True)

        self.log_file = os.path.join(self.indiv_log_dir, f"log_{Logger.timestamp}.log")

    def log(self, level: str, message: str):
        color = COLOR_MAP.get(level.upper(), "") if Logger.use_color else ""
        reset = COLOR_MAP["RESET"] if Logger.use_color else ""
        timestamp = datetime.now().strftime("%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] [{level:<6}] {self.name:<24} - {message}"

        # ERROR → 파일 + 조건부 콘솔 출력
        if level.upper() == "ERROR":
            Logger.error_count += 1
            with open(Logger.error_log_file, "a", encoding="utf-8") as ef:
                ef.write(formatted + "\n")
            if Config.get("print_error_log", True):  # 🔸 콘솔 출력은 옵션에 따라
                print(f"{color}{formatted}{reset}")

        # DEBUG → 테스트 모드일 때만 콘솔 출력
        elif level.upper() == "DEBUG":
            if Config.get("is_test", False):
                print(f"{color}{formatted}{reset}")

        # 나머지 → 항상 콘솔 출력
        else:
            print(f"{color}{formatted}{reset}")

        # 모든 로그는 파일에 기록
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")
        with open(Logger.common_log_file, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")

    def log_summary(self):
        timestamp = datetime.now().strftime("%m-%d %H:%M:%S")
        level = "SUMMARY"
        message = (
            f"로그 저장 완료-`{Logger.base_log_dir}` ERROR 발생: {Logger.error_count}개"
        )

        if Logger.use_color:
            color = COLOR_MAP["ERROR"] if Logger.error_count > 0 else COLOR_MAP["WAIT"]
            reset = COLOR_MAP["RESET"]
        else:
            color = ""
            reset = ""

        formatted = f"[{timestamp}] [{level:<6}] {self.name:<24} - {message}"
        print(f"{color}{formatted}{reset}")
        Logger.error_count = 0
