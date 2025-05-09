import os
import datetime
import traceback
from lib.Logger.logger import Logger  # Logger 클래스 임포트

# 로거 인스턴스 생성
logger = Logger("Traceback")


def log_traceback():
    """현재 발생한 예외의 트레이스백을 로그 파일에 저장하는 함수"""
    # Logger/logs 경로로 이동
    base_dir = os.path.dirname(os.path.abspath(__file__))  # lib/Exceptions
    log_dir = os.path.abspath(os.path.join(base_dir, "..", "Logger", "logs"))
    os.makedirs(log_dir, exist_ok=True)

    # 로그 파일 이름 및 경로 설정
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"traceback_{timestamp}.log"
    filepath = os.path.join(log_dir, filename)

    # 트레이스백 내용을 파일에 기록
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"[{timestamp}] 예외 발생\n")
        f.write(traceback.format_exc())

    # 로거를 통해 로그 저장 경로 출력
    logger.log("ERROR", f"예외 트레이스백 로그 저장됨: {filepath}")
