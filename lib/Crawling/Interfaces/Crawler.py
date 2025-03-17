from abc import ABC, abstractmethod
import time
import datetime
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
        return True, 1 # 테스트용 임시

    def run(self):
        """ 스케줄 확인 후 크롤링 실행 """
        print(f"DEBUG: {self.__class__.__name__}.run() 실행됨")  # 🔍 디버깅용

        while True:
            is_crawling, interval = self.is_crawling_time()
            if is_crawling:
                print(f"{self.__class__.__name__}: 현재 크롤링 가능 시간입니다. 크롤링을 시작합니다.")
                self.crawl()
            else:
                now = datetime.datetime.now()
                print(f"[{now}] {self.__class__.__name__}: 현재 크롤링 시간이 아닙니다. 대기 중...")

            # interval이 설정되어 있으면 해당 값으로 대기, 없으면 기본 10분 대기
            sleep_time = 60 * (interval if interval else 10)
            minutes = sleep_time // 60  # 몫: 분
            seconds = sleep_time % 60   # 나머지: 초

            print(f"{self.__class__.__name__}: {minutes}분 {seconds}초 동안 대기...")
            time.sleep(sleep_time)

    @abstractmethod
    def crawl(self):
        """ 크롤링 실행 메서드 (각 크롤러에서 구현) """
        pass
