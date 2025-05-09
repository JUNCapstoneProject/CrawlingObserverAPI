import time
import datetime
from zoneinfo import ZoneInfo
from lib.Crawling.config.LoadConfig import load_config
from lib.Config.config import Config


class Scheduler:
    """크롤링 작업의 스케줄을 관리하는 클래스"""

    # 클래스 변수: TTL 캐시
    _ttl_cache = {}

    def __init__(self, name: str):
        """Scheduler 초기화

        Args:
            name (str): 크롤러 이름
        """
        self.name = name
        self.schedule_config = load_config("schedule_config.json")  # 스케줄 설정 로드
        self.schedule_type, self.schedule = (
            self._load_schedule()
        )  # 스케줄 타입 및 설정 로드
        self.eastern = ZoneInfo("America/New_York")  # 미국 동부 시간대
        self.is_test = Config.get("is_test.toggle", False)  # 테스트 모드 여부
        self.test_interval = Config.get("is_test.interval", 10)  # 테스트 모드 간격 (분)

    def _now_et(self) -> datetime.datetime:
        """현재 시간을 동부 시간대로 반환"""
        return datetime.datetime.now(tz=self.eastern)

    def _set_ttl(self, minutes: int):
        """TTL 설정

        Args:
            minutes (int): TTL 만료 시간 (분)
        """
        Scheduler._ttl_cache[self.name] = time.time() + minutes * 60

    def _is_ttl_valid(self) -> bool:
        """TTL 유효성 검사

        Returns:
            bool: TTL이 유효하면 True, 그렇지 않으면 False
        """
        expires_at = Scheduler._ttl_cache.get(self.name)
        return expires_at is not None and time.time() < expires_at

    def _load_schedule(self):
        """스케줄 설정 로드

        Returns:
            tuple: 스케줄 타입과 스케줄 설정
        """
        for schedule_type in ["weekly", "monthly", "quarterly"]:
            if self.name in self.schedule_config.get(schedule_type, {}):
                return schedule_type, self.schedule_config[schedule_type][self.name]

        # 스케줄이 없을 경우 기본 스케줄 설정
        default_schedule = {
            "Monday": (9, 17, 60),  # 오전 9시 ~ 오후 5시, 1시간 간격
            "Tuesday": (9, 17, 60),
            "Wednesday": (9, 17, 60),
            "Thursday": (9, 17, 60),
            "Friday": (9, 17, 60),
        }
        return "weekly", default_schedule

    def is_crawling_time(self) -> bool:
        """크롤링 실행 여부 확인

        Returns:
            bool: 크롤링 실행 가능하면 True, 그렇지 않으면 False
        """
        # 테스트 모드 처리
        if self.is_test:
            if not self._is_ttl_valid():
                self._set_ttl(self.test_interval)  # 테스트 모드 TTL 설정
                return True
            return False

        # TTL이 유효하면 크롤링 실행하지 않음
        if self._is_ttl_valid():
            return False

        now = self._now_et()  # 현재 시간 (동부 시간대)

        # 주간 스케줄 처리
        if self.schedule_type == "weekly":
            weekday = now.strftime("%A")  # 현재 요일
            if weekday not in self.schedule:
                return False

            start_hour, end_hour, interval = self.schedule[weekday]
            if not (
                start_hour <= now.hour < end_hour
            ):  # 현재 시간이 스케줄 범위 내인지 확인
                return False

            elapsed_min = now.hour * 60 + now.minute
            start_min = start_hour * 60
            if (elapsed_min - start_min) % interval == 0:  # 간격에 따라 실행 여부 결정
                self._set_ttl(interval)
                return True
            return False

        # 월간 스케줄 처리
        elif self.schedule_type == "monthly":
            if now.day == self.schedule["day"] and now.hour == self.schedule["hour"]:
                self._set_ttl(1440)  # 하루 TTL 설정
                return True
            return False

        # 분기별 스케줄 처리
        elif self.schedule_type == "quarterly":
            if (
                now.month in self.schedule["months"]
                and now.day == self.schedule["day"]
                and now.hour == self.schedule["hour"]
            ):
                self._set_ttl(43200)  # 한 달 TTL 설정
                return True
            return False

        return False
