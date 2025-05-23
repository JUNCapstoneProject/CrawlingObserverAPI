import os
from datetime import datetime
from lib.Config.config import Config

# 로그 색상 매핑
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
    """로그 관리 클래스"""

    # 클래스 변수 초기화
    use_color = Config.get("color_log", True)
    base_log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(base_log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    common_log_file = os.path.join(base_log_dir, f"log_common_{timestamp}.log")

    error_log_dir = os.path.join(base_log_dir, "errors")
    os.makedirs(error_log_dir, exist_ok=True)
    error_log_file = os.path.join(error_log_dir, f"log_error_{timestamp}.log")

    def __init__(self, name: str):
        """개별 로거 초기화"""
        self.name = name
        self.indiv_log_dir = os.path.join(Logger.base_log_dir, name)
        os.makedirs(self.indiv_log_dir, exist_ok=True)

        self.log_file = os.path.join(self.indiv_log_dir, f"log_{Logger.timestamp}.log")
        self.error_count = 0
        self.is_test = Config.get("is_test.toggle", False)  # 테스트 모드 여부

    def log(self, level: str, message: str):
        """로그 메시지 출력 및 저장"""
        color = COLOR_MAP.get(level.upper(), "") if Logger.use_color else ""
        reset = COLOR_MAP["RESET"] if Logger.use_color else ""
        timestamp = datetime.now().strftime("%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] [{level:<6}] {self.name:<24} - {message}"

        # 로그 처리
        if level.upper() == "ERROR":
            self._handle_error_log(formatted, color, reset)
        elif level.upper() == "DEBUG":
            self._handle_debug_log(formatted, color, reset)
        else:
            self._handle_general_log(formatted, color, reset)

        # 모든 로그를 파일에 기록
        self._write_to_file(formatted)

    def _handle_error_log(self, formatted: str, color: str, reset: str):
        """에러 로그 처리"""
        self.error_count += 1
        with open(Logger.error_log_file, "a", encoding="utf-8") as ef:
            ef.write(formatted + "\n")
        if Config.get("print_error_log", True):
            print(f"{color}{formatted}{reset}")

    def _handle_debug_log(self, formatted: str, color: str, reset: str):
        """디버그 로그 처리"""
        if self.is_test:
            print(f"{color}{formatted}{reset}")

    def _handle_general_log(self, formatted: str, color: str, reset: str):
        """일반 로그 처리"""
        print(f"{color}{formatted}{reset}")

    def _write_to_file(self, formatted: str):
        """로그를 파일에 기록"""
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")
        with open(Logger.common_log_file, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")

    def log_summary(self):
        """로그 요약 출력"""
        timestamp = datetime.now().strftime("%m-%d %H:%M:%S")
        level = "SUMMARY"
        message = (
            f"로그 저장 완료-`{Logger.base_log_dir}` ERROR 발생: {self.error_count}개"
        )

        color = COLOR_MAP["ERROR"] if self.error_count > 0 else COLOR_MAP["WAIT"]
        reset = COLOR_MAP["RESET"] if Logger.use_color else ""

        formatted = f"[{timestamp}] [{level:<6}] {self.name:<24} - {message}"
        print(f"{color}{formatted}{reset}")
        self.error_count = 0
