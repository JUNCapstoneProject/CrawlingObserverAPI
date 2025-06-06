import orjson
import hashlib
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, timedelta, timezone

from lib.Distributor.secretary.models.news import News
from lib.Distributor.secretary.models.reports import Report
from lib.Distributor.secretary.session import SessionLocal
from lib.Logger.logger import get_logger
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
        self.logger = get_logger("Secretary")  # 통합 로그 클래스 적용
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
                    self.logger.error(
                        f"데이터 처리 중 예외 발생 → {type(e).__name__}: {e}",
                    )
                    continue
        else:
            try:
                self._distribute_single(result)
            except Exception as e:
                self.logger.error(f"데이터 처리 중 예외 발생 → {type(e).__name__}: {e}")

    def _distribute_single(self, result: dict):
        log = result.get("log", {})
        tag = result.get("tag")

        if not tag or tag not in self.handlers:
            self.logger.warning(f"등록되지 않은 tag: {tag}")
            return

        df = result.get("df")
        if isinstance(df, pd.DataFrame):
            if df.empty:
                self.logger.warning(f"{tag}: 빈 DataFrame")
                return
            df = df.dropna(how="all").to_dict(orient="records")

        # ✅ 필터링: CrawlingLog 기록 전에 수행
        if tag in {"news", "reports"}:
            valid_ticker_map = get_valid_ticker_map(self.db)
            filtered_df = []

            for row in df:
                title = row.get("title")
                if not title:
                    continue

                model = News if tag == "news" else Report
                if self.db.execute(select(model).where(model.title == title)).first():
                    continue

                tags = row.get("tag", "")
                if not tags:
                    continue

                chosen_tag = extract_valid_tag(tags, valid_ticker_map)
                if not chosen_tag:
                    continue

                row["_chosen_tag"] = chosen_tag  # 이후 핸들러에서 사용 가능
                filtered_df.append(row)

            df = filtered_df
            if not df:
                return

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


from sqlalchemy import select, func
from lib.Distributor.secretary.models.stock import Stock_Daily
from lib.Distributor.secretary.models.company import Company


def get_valid_ticker_map(db) -> dict[str, int]:
    """
    Stock_Daily에서 company_id별로 가장 market_cap이 큰 ticker만 선택
    :return: {ticker: market_cap}
    """
    subquery = (
        select(
            Stock_Daily.company_id,
            Company.ticker,
            Stock_Daily.market_cap,
            func.row_number()
            .over(
                partition_by=Stock_Daily.company_id,
                order_by=Stock_Daily.market_cap.desc(),
            )
            .label("rank"),
        )
        .join(Company, Stock_Daily.company_id == Company.company_id)
        .subquery()
    )

    rows = db.execute(
        select(subquery.c.ticker, subquery.c.market_cap).where(subquery.c.rank == 1)
    ).fetchall()

    return {row.ticker: row.market_cap for row in rows}


def extract_valid_tag(tags: str, valid_ticker_map: dict[str, int]) -> str | None:
    """
    태그 문자열에서 유효한 ticker 중 market_cap이 가장 큰 것 선택
    :param tags: 콤마 구분 문자열
    :param valid_ticker_map: {ticker: market_cap}
    :return: 유효한 ticker 하나 or None
    """
    candidates = [
        (t.strip(), valid_ticker_map[t.strip()])
        for t in tags.split(",")
        if t.strip() in valid_ticker_map
    ]

    if not candidates:
        return None

    return max(candidates, key=lambda x: x[1])[0]
