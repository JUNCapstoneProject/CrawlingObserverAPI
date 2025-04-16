import os
import json


def save_to_json(data, filename, append=False):
    """JSON 데이터를 파일에 저장하는 함수.

    :param data: 저장할 데이터 (리스트 또는 딕셔너리)
    :param filename: 저장할 파일 경로
    :param append: True이면 기존 데이터에 추가 (False이면 덮어쓰기)
    """
    # 파일 경로에서 디렉터리 부분 가져오기
    dir_path = os.path.dirname(filename)
    if dir_path:  # 디렉터리 경로가 존재하는 경우에만 생성
        os.makedirs(dir_path, exist_ok=True)

    # 기존 파일이 있고, append=True이면 기존 데이터 불러오기
    if append and os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as file:
            try:
                existing_data = json.load(file)  # 기존 데이터 로드
                if not isinstance(
                    existing_data, list
                ):  # 기존 데이터가 리스트가 아니면 리스트로 변환
                    existing_data = [existing_data]
            except json.JSONDecodeError:
                existing_data = []
    else:
        existing_data = []

    # 새로운 데이터를 리스트에 추가
    if isinstance(data, list):
        existing_data.extend(data)
    else:
        existing_data.append(data)

    # JSON 파일 저장
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(existing_data, file, ensure_ascii=False, indent=4)
