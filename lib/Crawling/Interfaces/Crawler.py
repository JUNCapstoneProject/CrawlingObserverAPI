from abc import ABC, abstractmethod
import time
import datetime
import os
import json
import pandas as pd
import numpy as np
from uuid import uuid4

from lib.Crawling.Interfaces.Scheduler import Scheduler
from lib.Config.config import Config
from lib.Logger.logger import Logger


class CrawlerInterface(ABC):
    CHECK_INTERVAL_SEC = 5

    def __init__(self, name: str):
        self.name = name
        self.scheduler = Scheduler(name)
        self.save_method = Config.get("save_method", {})
        self.logger = Logger(self.__class__.__name__)

    def run(self):
        if not self.scheduler.is_test:
            self._execute_crawl()  # 반드시 1회 실행

        while True:
            time.sleep(self.CHECK_INTERVAL_SEC)
            if self.scheduler.is_crawling_time():
                self._execute_crawl()

    def _execute_crawl(self):
        self.logger.log("START", f"[{self.name}] 크롤링 시작")
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
            self.logger.log("WARN", f"[{self.name}] 크롤링 결과 없음")

    def save_to_file(self, result):
        base_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        temp_dir = os.path.join(base_dir, "Datas")
        os.makedirs(temp_dir, exist_ok=True)

        tag = result[0].get("tag", "unknown") if result else "unknown"
        if tag == "income_statement":
            tag = "financials"

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{temp_dir}/{timestamp}_{tag}__{uuid4().hex[:8]}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)

        self.logger.log("FILE", f"[{self.name}] 결과 저장: {filename}")

    def save_to_db(self, result):
        from ...Distributor.secretary.Secretary import Secretary
        from ...Distributor.secretary.session import SessionLocal
        from sqlalchemy.exc import SQLAlchemyError

        db = SessionLocal()
        secretary = Secretary(db)

        try:
            secretary.distribute(result)
            self.logger.log("DB", f"[{self.name}] DB 저장 완료")
        except SQLAlchemyError as e:
            self.logger.log_sqlalchemy_error(e)
        finally:
            db.close()

    @abstractmethod
    def crawl(self):
        """크롤링 로직은 서브클래스에서 구현"""
        pass
