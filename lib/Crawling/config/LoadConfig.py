import json
import os
from lib.Logger.logger import get_logger


# 로거 인스턴스 생성
logger = get_logger("LoadConfig")


def load_config(filename):
    """config/ 디렉토리의 JSON 파일을 절대 경로로 로드하는 함수

    Args:
        filename (str): 로드할 JSON 파일의 이름.

    Returns:
        dict: JSON 파일의 내용을 딕셔너리로 반환.

    Raises:
        FileNotFoundError: 파일이 존재하지 않을 경우 발생.
        json.JSONDecodeError: JSON 파일 파싱에 실패할 경우 발생.
    """
    # 현재 파일의 디렉토리를 기준으로 설정 파일 경로 생성
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, filename)

    try:
        with open(config_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON file: {config_path}")
        raise
