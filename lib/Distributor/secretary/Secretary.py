class Secretary:
    def __init__(self, db_session):
        self.db = db_session
        self.handlers = {}
        self._auto_register()

    def _auto_register(self):
        from .handlers import (
            store_news, store_macro, store_reports, store_stock,
            store_income_statement, store_balance_sheet, store_cash_flow
        )
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
                self._distribute_single(r)
        else:
            self._distribute_single(result)

    def _distribute_single(self, result: dict):
        from .models.core import CrawlingLog, FailLog
        import pandas as pd
        from sqlalchemy.exc import SQLAlchemyError

        try:
            from uuid import uuid4
            log = result.get("log", {})
            crawling_log = CrawlingLog(
                crawling_id=str(uuid4()),
                crawling_type=log.get("crawling_type"),
                status_code=log.get("status_code"),
                target_url=log.get("target_url")
            )
            self.db.add(crawling_log)
            self.db.flush()
            self.db.refresh(crawling_log)
            crawling_id = crawling_log.crawling_id

            if "fail_log" in result:
                self.db.add(FailLog(
                    crawling_id=crawling_id,
                    err_message=result["fail_log"].get("err_message")
                ))
                self.db.commit()
                return

            tag = result.get("tag")
            if not tag or tag not in self.handlers:
                raise ValueError(f"[ERROR] 등록되지 않은 tag: {tag}")

            df = result.get("df")
            if isinstance(df, pd.DataFrame):
                df = df.dropna(how="all")
                df = df.to_dict(orient="records")

            self.handlers[tag](self.db, crawling_id, df)
            self.db.commit()

        except SQLAlchemyError as e:
            self.db.rollback()
            print(f"[ROLLBACK] DB 오류 발생: {e}")
            raise

        except Exception as e:
            self.db.rollback()
            print(f"[ROLLBACK] 일반 오류 발생: {e}")
            raise
