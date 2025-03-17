import json
import os

def load_config(filename):
    """config/ 디렉토리의 JSON 파일을 로드하는 함수"""
    config_path = os.path.join("lib", "Crawling", "config", filename)
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)