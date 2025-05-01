from abc import ABC, abstractmethod
import time
import datetime
import os
import json
import pandas as pd
import numpy as np
from uuid import uuid4

from lib.Crawling.config.LoadConfig import load_config
from lib.Config.config import Config
from lib.Logger.logger import Logger


class CrawlerInterface(ABC):
    """모든 크롤러의 최상위 인터페이스 (공통 스케줄 포함)"""

    def __init__(self, name):
        self.name = name
        self.schedule = self.load_schedule(self.name)
        self.save_method = Config.get("save_method", {})
        self.logger = Logger(self.__class__.__name__)

    def load_schedule(self, name):
        schedule_config = load_config("schedule_config.json")
        return schedule_config.get(name, {})

    def is_crawling_time(self):
        return True, 10  # 테스트용

    def run(self):
        while True:
            is_crawling, interval = self.is_crawling_time()
            if is_crawling:
                self.logger.log("START", "크롤링 시작")

                result = self.crawl()

                if result:
                    for result_item in result:
                        df = result_item.get("df")

                        if isinstance(df, pd.DataFrame):
                            if "posted_at" in df.columns:
                                df["posted_at"] = pd.to_datetime(df["posted_at"])
                            result_item["df"] = (
                                df.reset_index(drop=True)
                                .replace({np.nan: None})
                                .to_dict(orient="records")
                            )
                        elif isinstance(df, list):
                            for row in df:
                                if "posted_at" in row:
                                    row["posted_at"] = pd.to_datetime(row["posted_at"])

                    if self.save_method.get("save_to_file", False):
                        self.save_to_file(result)

                    if self.save_method.get("save_to_DB", True):
                        self.save_to_db(result)
                else:
                    self.logger.log("WARN", "크롤링 결과 없음")
            else:
                now = datetime.datetime.now()
                self.logger.log("WAIT", f"[{now}] 현재 크롤링 시간이 아님. 대기 중...")

            sleep_time = 60 * (interval if interval else 10)
            minutes, seconds = divmod(sleep_time, 60)
            self.logger.log("WAIT", f"{minutes}분 {seconds}초 대기")
            time.sleep(sleep_time)

    def save_to_file(self, result):
        base_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        temp_dir = os.path.join(base_dir, "Datas")

        tag = result[0].get("tag", "unknown") if result else "unknown"
        if tag == "income_statement":
            tag = "financials"
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{temp_dir}/crawled_result_{timestamp}_{tag}_{uuid4().hex[:8]}.json"

        os.makedirs(temp_dir, exist_ok=True)

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)

        self.logger.log("FILE", f"결과 저장: {filename}")

    def save_to_db(self, result):
        from ...Distributor.secretary.Secretary import Secretary
        from ...Distributor.secretary.session import SessionLocal
        from sqlalchemy.exc import SQLAlchemyError

        db = SessionLocal()
        secretary = Secretary(db)

        try:
            secretary.distribute(result)
            self.logger.log("DB", "DB 저장 완료")
        except SQLAlchemyError as e:
            self.logger.log_sqlalchemy_error(e)
        finally:
            db.close()

    @abstractmethod
    def crawl(self):
        pass
