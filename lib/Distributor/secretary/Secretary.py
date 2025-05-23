import orjson
import hashlib
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, timedelta, timezone

from lib.Distributor.secretary.session import SessionLocal
from lib.Logger.logger import Logger
from lib.Distributor.secretary.models.core import CrawlingLog, FailLog
from lib.Distributor.secretary.handlers import (
    store_news,
    store_macro,
    store_reports,
    store_stock,
    store_income_statement,
    store_balance_sheet,
    store_cash_flow,
)

KST = timezone(timedelta(hours=9))


class Secretary:

    def __init__(self):
        self.db = SessionLocal()
        self.handlers = {}
        self.logger = Logger("Secretary")  # 통합 로그 클래스 적용
        self._auto_register()

    def _auto_register(self):
        self.register("news", store_news)
        self.register("macro", store_macro)
        self.register("reports", store_reports)
        self.register("stock", store_stock)
        self.register("income_statement", store_income_statement)
        self.register("balance_sheet", store_balance_sheet)
        self.register("cash_flow", store_cash_flow)

    def register(self, tag: str, handler_fn):
        self.handlers[tag] = handler_fn

    def _generate_hash_id(self, tag: str, df: list[dict]) -> str:
        def convert(obj):
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            if isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert(i) for i in obj]
            return obj

        cleaned_df = convert(df)

        raw_bytes = orjson.dumps(
            {"tag": tag, "df": cleaned_df},
            option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS,
        )
        return hashlib.sha256(raw_bytes).hexdigest()

    def distribute(self, result: dict | list[dict]):
        if isinstance(result, list):
            for r in result:
                try:
                    self._distribute_single(r)
                except Exception as e:
                    self.logger.log(
                        "ERROR",
                        f"다음 데이터 처리 중 예외 발생 → {type(e).__name__}: {e}",
                    )
                    continue
        else:
            try:
                self._distribute_single(result)
            except Exception as e:
                self.logger.log(
                    "ERROR", f"단일 데이터 처리 실패 → {type(e).__name__}: {e}"
                )

    def _distribute_single(self, result: dict):
        log = result.get("log", {})
        tag = result.get("tag")

        if not tag or tag not in self.handlers:
            self.logger.log("WARN", f"등록되지 않은 tag: {tag}")
            return

        df = result.get("df")
        if isinstance(df, pd.DataFrame):
            if df.empty:
                self.logger.log("WARN", f"{tag}: 빈 DataFrame")
                return
            df = df.dropna(how="all").to_dict(orient="records")

        if "fail_log" in result:
            fail_df = [
                {
                    "err_message": result["fail_log"].get("err_message"),
                    "timestamp": datetime.now().isoformat(),
                }
            ]
            crawling_id = self._generate_hash_id(tag="fail_log", df=fail_df)
        else:
            crawling_id = self._generate_hash_id(tag, df)

        try:
            crawling_log = CrawlingLog(
                crawling_id=crawling_id,
                crawling_type=log.get("crawling_type"),
                status_code=log.get("status_code"),
                target_url=log.get("target_url"),
                try_time=datetime.now(KST),
            )
            self.db.add(crawling_log)
            self.db.flush()

        except IntegrityError:
            self.db.rollback()
            return
        except SQLAlchemyError as e:
            self.db.rollback()
            raise

        try:
            if "fail_log" in result:
                self.db.add(
                    FailLog(
                        crawling_id=crawling_id,
                        err_message=result["fail_log"].get("err_message"),
                    )
                )
                self.db.commit()
                return

            self.handlers[tag](self.db, crawling_id, df)
            self.db.commit()

        except SQLAlchemyError as e:
            self.db.rollback()
            raise

        except Exception as e:
            self.db.rollback()
            raise
