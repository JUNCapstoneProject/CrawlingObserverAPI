import json
import os


def load_config(filename):
    """config/ 디렉토리의 JSON 파일을 절대 경로로 로드하는 함수"""
    # 현재 파일(`load_config` 함수가 포함된 파일)의 절대 경로를 기준으로 설정
    base_dir = os.path.dirname(os.path.abspath(__file__))  # 현재 파일의 디렉토리
    config_path = os.path.join(base_dir, filename)  # 절대 경로 생성

    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)
