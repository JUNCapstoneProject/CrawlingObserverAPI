import json
import hashlib
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST)

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


class Secretary:
    def __init__(self, db_session):
        self.db = db_session
        self.handlers = {}
        self._auto_register()

    def _auto_register(self):
        # from lib.Distributor.secretary.handlers import (
        #     store_news, store_macro, store_reports, store_stock,
        #     store_income_statement, store_balance_sheet, store_cash_flow
        # )
        self.register("news", store_news)
        self.register("macro", store_macro)
        self.register("reports", store_reports)
        self.register("stock", store_stock)
        self.register("income_statement", store_income_statement)
        self.register("balance_sheet", store_balance_sheet)
        self.register("cash_flow", store_cash_flow)

    def register(self, tag: str, handler_fn):
        self.handlers[tag] = handler_fn

    def distribute(self, result: dict | list[dict]):
        if isinstance(result, list):
            for r in result:
                try:
                    self._distribute_single(r)
                except Exception as e:
                    print(f"[SKIP] 에러 발생, 다음 데이터로 넘어갑니다: {e}")
                    continue
        else:
            try:
                self._distribute_single(result)
            except Exception as e:
                print(f"[SKIP] 에러 발생, 단일 데이터 처리 실패: {e}")

    def _generate_hash_id(self, tag: str, df: list[dict]) -> str:
        # import hashlib, json
        # import pandas as pd

        def convert(obj):
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            if isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert(i) for i in obj]
            return obj

        cleaned_df = convert(df)

        raw_string = json.dumps(
            {"tag": tag, "df": cleaned_df}, sort_keys=True, ensure_ascii=False
        )

        return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()

    def _distribute_single(self, result: dict):
        # from lib.Distributor.secretary.models.core import CrawlingLog, FailLog

        # import pandas as pd
        # import uuid
        # from sqlalchemy.exc import SQLAlchemyError

        try:
            log = result.get("log", {})
            tag = result.get("tag")
            if not tag or tag not in self.handlers:
                raise ValueError(f"[ERROR] 등록되지 않은 tag: {tag}")

            df = result.get("df")
            if isinstance(df, pd.DataFrame):
                df = df.dropna(how="all")
                df = df.to_dict(orient="records")

            # ✅ 실패 로그인 경우 uuid 사용
            if "fail_log" in result:
                fail_df = [
                    {
                        "err_message": result["fail_log"].get("err_message"),
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                ]
                crawling_id = self._generate_hash_id(tag="fail_log", df=fail_df)
            else:
                crawling_id = self._generate_hash_id(tag, df)

            # 중복된 crawling_id 존재 여부 체크 (optional, 안전장치)
            exists = (
                self.db.query(CrawlingLog).filter_by(crawling_id=crawling_id).first()
            )
            if exists:
                # print(f"[SKIP] 이미 처리된 데이터: crawling_id={crawling_id}")
                return

            crawling_log = CrawlingLog(
                crawling_id=crawling_id,
                crawling_type=log.get("crawling_type"),
                status_code=log.get("status_code"),
                target_url=log.get("target_url"),
                try_time=now_kst,
            )
            self.db.add(crawling_log)
            self.db.flush()
            self.db.refresh(crawling_log)

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
            print(f"[ROLLBACK] DB 오류 발생: {e}")
            raise

        except Exception as e:
            self.db.rollback()
            print(f"[ROLLBACK] 일반 오류 발생: {repr(e)}")
            print(f"[ROLLBACK] 일반 오류 발생: {type(e).__name__}: {e}")
            # print(f"[ROLLBACK] 일반 오류 발생: {e}")
            raise
