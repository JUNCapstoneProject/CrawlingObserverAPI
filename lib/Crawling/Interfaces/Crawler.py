# 표준 라이브러리
from abc import ABC, abstractmethod  # 추상 클래스 정의
import time  # 시간 관련 함수
import datetime  # 날짜 및 시간 처리
import os  # 파일 및 디렉토리 경로 처리
import json  # JSON 데이터 처리
from uuid import uuid4  # 고유 ID 생성

# 외부 라이브러리
import pandas as pd  # 데이터프레임 처리
import numpy as np  # 수치 데이터 처리

# 내부 모듈
from lib.Crawling.Interfaces.Scheduler import Scheduler  # 스케줄러 클래스
from lib.Config.config import Config  # 설정 관리 클래스
from lib.Logger.logger import Logger  # 로깅 클래스


class CrawlerInterface(ABC):
    """크롤러 인터페이스 클래스"""

    # 클래스 변수
    CHECK_INTERVAL_SEC = 5  # 크롤링 주기 (초)
    SAVE_METHOD = Config.get("save_method", {})  # 저장 방식 설정

    def __init__(self, name: str):
        """크롤러 초기화"""
        self.name = name
        self.scheduler = Scheduler(name)
        self.logger = Logger(self.__class__.__name__)

    def run(self):
        """크롤러 실행 루프"""
        if not self.scheduler.is_test:
            self._execute_crawl()  # 반드시 1회 실행

        while True:
            time.sleep(self.CHECK_INTERVAL_SEC)
            if self.scheduler.is_crawling_time():
                self._execute_crawl()

    def _execute_crawl(self):
        """크롤링 실행 및 결과 처리"""
        self.logger.log("START", f"[{self.name}] 크롤링 시작")
        result = self.crawl()
        self.logger.log_summary()

        if result:
            self._process_result(result)
        else:
            self.logger.log("WARN", f"[{self.name}] 크롤링 결과 없음")

    def _process_result(self, result):
        """크롤링 결과 처리"""
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

        if self.SAVE_METHOD.get("save_to_file", False):
            self.save_to_file(result)

        if self.SAVE_METHOD.get("save_to_DB", True):
            self.save_to_db(result)

    def save_to_file(self, result):
        """크롤링 결과를 파일로 저장"""
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

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            self.logger.log("FILE", f"[{self.name}] 결과 저장: {filename}")
        except Exception as e:
            self.logger.log("ERROR", f"[{self.name}] 파일 저장 중 예외 발생: {e}")

    def save_to_db(self, result):
        """크롤링 결과를 데이터베이스에 저장"""
        from ...Distributor.secretary.Secretary import Secretary
        from sqlalchemy.exc import SQLAlchemyError

        secretary = Secretary()  # DB 세션은 Secretary 내부에서 관리

        try:
            secretary.distribute(result)
            self.logger.log("DB", f"[{self.name}] DB 저장 완료")
        except SQLAlchemyError as e:
            self.logger.log(
                "ERROR", f"[{self.name}] DB 저장 중 SQLAlchemy 예외 발생: {e}"
            )
            raise RuntimeError(f"DB 저장 중 SQLAlchemy 예외 발생: {e}") from e

    @abstractmethod
    def crawl(self):
        """크롤링 로직은 서브클래스에서 구현"""
        pass
