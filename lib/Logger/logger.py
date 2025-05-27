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

    use_color = Config.get("color_log", True)
    base_log_dir = os.path.join("logs")
    os.makedirs(base_log_dir, exist_ok=True)

    error_log_dir = os.path.join(base_log_dir, "errors")
    os.makedirs(error_log_dir, exist_ok=True)

    def __init__(self, name: str):
        """개별 로거 초기화"""
        self.name = name
        self.indiv_log_dir = os.path.join(Logger.base_log_dir, name)
        os.makedirs(self.indiv_log_dir, exist_ok=True)

        self.error_count = 0
        self.is_test = Config.get("is_test.toggle", False)

    def _get_log_file_path(self):
        """현재 시간 기준 파일 경로 생성 (시간 단위 분리)"""
        rotation_hours = Config.get("log_rotation", 4)
        now = datetime.now()
        slot = now.hour // rotation_hours * rotation_hours
        time_str = now.strftime("%Y%m%d") + f"_{slot:02d}H"

        log_file = os.path.join(self.indiv_log_dir, f"log_{time_str}.log")
        common_log_file = os.path.join(
            Logger.base_log_dir, f"log_common_{time_str}.log"
        )
        error_log_file = os.path.join(Logger.error_log_dir, f"log_error_{time_str}.log")

        return log_file, common_log_file, error_log_file

    def log(self, level: str, message: str):
        """로그 메시지 출력 및 저장"""
        color = COLOR_MAP.get(level.upper(), "") if Logger.use_color else ""
        reset = COLOR_MAP["RESET"] if Logger.use_color else ""
        timestamp = datetime.now().strftime("%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] [{level:<6}] {self.name:<24} - {message}"

        if level.upper() == "ERROR":
            self._handle_error_log(formatted, color, reset)
        elif level.upper() == "DEBUG":
            self._handle_debug_log(formatted, color, reset)
        else:
            self._handle_general_log(formatted, color, reset)

        self._write_to_file(formatted)

    def _handle_error_log(self, formatted: str, color: str, reset: str):
        """에러 로그 처리"""
        self.error_count += 1
        _, _, error_log_file = self._get_log_file_path()
        with open(error_log_file, "a", encoding="utf-8") as ef:
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
        """로그 파일 기록 (개별 + 공통)"""
        log_file, common_log_file, _ = self._get_log_file_path()
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")
        with open(common_log_file, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")

    def log_summary(self):
        """에러 카운트 포함 요약 메시지 출력"""
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
