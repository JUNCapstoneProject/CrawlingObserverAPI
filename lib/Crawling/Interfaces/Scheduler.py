import time
import datetime
from zoneinfo import ZoneInfo
from lib.Crawling.config.LoadConfig import load_config
from lib.Config.config import Config


class Scheduler:
    _ttl_cache = {}

    def __init__(self, name: str):
        self.name = name
        self.schedule_config = load_config("schedule_config.json")
        self.schedule_type, self.schedule = self._load_schedule()
        self.eastern = ZoneInfo("America/New_York")
        self.is_test = Config.get("is_test", False)

    def _now_et(self) -> datetime.datetime:
        return datetime.datetime.now(tz=self.eastern)

    def _set_ttl(self, minutes: int):
        Scheduler._ttl_cache[self.name] = time.time() + minutes * 60

    def _is_ttl_valid(self):
        expires_at = Scheduler._ttl_cache.get(self.name)
        return expires_at is not None and time.time() < expires_at

    def _load_schedule(self):
        for schedule_type in ["weekly", "monthly", "quarterly"]:
            if self.name in self.schedule_config.get(schedule_type, {}):
                return schedule_type, self.schedule_config[schedule_type][self.name]
        raise ValueError(f"[Scheduler] '{self.name}' 스케줄이 정의되어 있지 않습니다.")

    def is_crawling_time(self):
        if self.is_test:
            if not self._is_ttl_valid():
                self._set_ttl(5)  # 테스트 모드: 5분 간격 TTL
                return True
            return False

        if self._is_ttl_valid():
            return False

        now = self._now_et()

        if self.schedule_type == "weekly":
            weekday = now.strftime("%A")
            if weekday not in self.schedule:
                return False

            start_hour, end_hour, interval = self.schedule[weekday]
            if not (start_hour <= now.hour < end_hour):
                return False

            elapsed_min = now.hour * 60 + now.minute
            start_min = start_hour * 60
            if (elapsed_min - start_min) % interval == 0:
                self._set_ttl(interval)
                return True
            return False

        elif self.schedule_type == "monthly":
            if now.day == self.schedule["day"] and now.hour == self.schedule["hour"]:
                self._set_ttl(1440)
                return True
            return False

        elif self.schedule_type == "quarterly":
            if (
                now.month in self.schedule["months"]
                and now.day == self.schedule["day"]
                and now.hour == self.schedule["hour"]
            ):
                self._set_ttl(43200)
                return True
            return False

        return False
