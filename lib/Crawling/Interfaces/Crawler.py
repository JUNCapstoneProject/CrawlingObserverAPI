from abc import ABC, abstractmethod
import time
import datetime
import traceback
import pandas as pd
import os
import json
import numpy as np
from uuid import uuid4
from ..config.LoadConfig import load_config

class CrawlerInterface(ABC):
    """ 모든 크롤러의 최상위 인터페이스 (공통 스케줄 포함) """

    def __init__(self, name):
        """
        :param name: __init__에서 동적으로 전달받을 크롤러 이름 (예: "YahooFinance", "InvestingNews", "Fred")
        """
        self.name = name  # 실행 코드에서 name을 직접 넘겨받음
        self.schedule = self.load_schedule(self.name)  # name을 이용해 스케줄 로드

    def load_schedule(self, name):
        """ JSON에서 크롤링 스케줄 불러오기 """
        schedule_config = load_config("schedule_config.json")
        return schedule_config.get(name, {})

    def is_crawling_time(self):
        # """ 현재 시간이 스케줄 범위 내에 있는지 확인 """
        # now = datetime.datetime.now()
        # today = now.strftime("%A")  # 현재 요일 (Monday, Tuesday 등)
        # current_hour = now.hour  # 현재 시간

        # if today in self.schedule:
        #     start_hour, end_hour, interval = self.schedule[today]
        #     if start_hour <= current_hour <= end_hour:
        #         return True, interval
        # return False, None
        return True, 10 # 테스트용 임시

    def run(self):
        """ 스케줄 확인 후 크롤링 실행 (JSON 저장 + 에러 traceback 출력) """
        print(f"DEBUG: {self.__class__.__name__}.run() 실행됨")

        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # lib/Crawling/
        temp_dir = os.path.join(base_dir, "Datas")

        while True:
            is_crawling, interval = self.is_crawling_time()
            if is_crawling:
                print(f"{self.__class__.__name__}: 현재 크롤링 가능 시간입니다. 크롤링을 시작합니다.")

                result = self.crawl()

                if result:
                    if isinstance(result, dict):
                        result = [result]

                    for idx, result_item in enumerate(result):
                        try:
                            tag = result_item.get("tag", "unknown")

                            if isinstance(result_item.get("df"), pd.DataFrame):
                                result_item["df"] = result_item["df"].reset_index(drop=True).replace({np.nan: None}).to_dict(orient="records")

                            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"{temp_dir}/crawled_result_{timestamp}_{tag}_{idx}_{uuid4().hex[:8]}.json"

                            with open(filename, "w", encoding="utf-8") as f:
                                json.dump(result_item, f, ensure_ascii=False, indent=2, default=str)

                            print(f"{self.__class__.__name__}: 크롤링 결과 저장 완료: {filename}")

                        except Exception as e:
                            # print(f"[ERROR] 크롤링 데이터 저장 중 예외 발생! 태그: {tag}, 인덱스: {idx}")
                            # print("▶ 예외 메시지:", str(e))
                            print("▶ Traceback:")
                            self.save_traceback_to_file(tag, idx, e)

                else:
                    print("[WARNING] 크롤링 결과 없음! `crawl()`에서 반환된 데이터가 없습니다.")

            else:
                now = datetime.datetime.now()
                print(f"[{now}] {self.__class__.__name__}: 현재 크롤링 시간이 아닙니다. 대기 중...")

            sleep_time = 60 * (interval if interval else 10)
            minutes, seconds = divmod(sleep_time, 60)
            print(f"{self.__class__.__name__}: {minutes}분 {seconds}초 동안 대기...")
            time.sleep(sleep_time)

    def save_traceback_to_file(self, tag: str, idx: int, e: Exception):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(base_dir, "Logs")
        os.makedirs(log_dir, exist_ok=True)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"traceback_{tag}_{idx}_{timestamp}.log"
        filepath = os.path.join(log_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"[{timestamp}] 예외 발생 (tag: {tag}, index: {idx})\n")
            f.write(f"Error: {str(e)}\n\n")
            f.write(traceback.format_exc())  # ⬅ 핵심

        print(f"예외 트레이스백 로그 저장됨: {filepath}")

    @abstractmethod
    def crawl(self):
        """ 크롤링 실행 메서드 (각 크롤러에서 구현) """
        pass
