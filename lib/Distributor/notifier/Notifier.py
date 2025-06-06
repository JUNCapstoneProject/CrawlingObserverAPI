from sqlalchemy import text, update
import copy, time, random
from datetime import datetime, timedelta

from lib.Logger.logger import get_logger
from lib.Distributor.socket.Client import SocketClient
from lib.Distributor.secretary.session import get_session
from lib.Config.config import Config  # 설정 관리 클래스
from lib.Distributor.secretary.models.financials import FinancialStatement
from lib.Distributor.secretary.models.news import News


class NotifierBase:
    def __init__(self, name="NotifierBase"):
        self.client = SocketClient()
        self.logger = get_logger(name)
        self.interval_sec = 180
        self.socket_condition = Config.get("socket_condition", True)

    def run_all(self):
        from lib.Distributor.notifier.Article_notifier import ArticleNotifier
        from lib.Distributor.notifier.Financial_notifier import FinancialNotifier

        self.logger.info(f"실행 시작 (interval: {self.interval_sec}s)")
        try:
            while True:
                notifiers = [ArticleNotifier(), FinancialNotifier()]

                for notifier in notifiers:
                    try:
                        notifier.run()
                    except Exception as e:
                        notifier.logger.error(
                            f"{notifier.__class__.__name__} 실패: {e}"
                        )
                    finally:
                        notifier.logger.log_summary()

                self.logger.info(f"{self.interval_sec}sec 이후 재실행")
                time.sleep(self.interval_sec)

        except KeyboardInterrupt:
            self.logger.info("중단됨 (KeyboardInterrupt)")
        except Exception as e:
            self.logger.error(f"동작 중 오류 발생: {e}")
        finally:
            self.logger.log_summary()

    def _run_common(self, view_name: str, base_message: dict, source: str):
        rows = self._fetch_unanalyzed_rows(view_name)
        if not rows:
            self.logger.info(f"처리할 {source} 없음")
            return

        for row in rows:
            try:
                requests_message = copy.deepcopy(base_message)
                item = self._build_item(row)
                requests_message["body"]["item"] = item

                if not item:
                    self.logger.debug(f"no item in: {row.get('ticker')}")
                    self.update_analysis_log_time(
                        row.get("crawling_id"), row.get("ticker")
                    )
                    continue

                if self.socket_condition:
                    result = self.client.request_tcp(requests_message)

                    status_code = result.get("status_code")
                    message = result.get("message")

                    if status_code != 200:
                        if status_code == 400:
                            msg = "데이터 입력 오류 (400)"
                        elif status_code == 500:
                            msg = "시스템 오류 (500)"
                        else:
                            msg = f"알 수 없는 상태 코드({status_code})"
                        self.logger.error(f"{msg} → {message}: {row['ticker']}")
                        continue

                    self.update_analysis_log_time(
                        row.get("crawling_id"), row.get("ticker")
                    )

                    raw_result = result.get("item", {}).get("result")
                    if raw_result is not None:
                        try:
                            index = int(float(raw_result))
                            self._update_analysis(row["crawling_id"], index, source)
                        except (ValueError, TypeError):
                            self.logger.warning(f"분석 인덱스 변환 실패 → {raw_result}")
                    else:
                        self.logger.warning(f"분석 결과 없음 → {row['crawling_id']}")
                    time.sleep(random.uniform(0.1, 0.4))

            except Exception as e:
                self.logger.error(
                    f"예외 발생 → {e}: {row.get('ticker')}, {row.get('crawling_id')}"
                )
                time.sleep(1.0)

    def _fetch_unanalyzed_rows(self, view_name: str, days=1) -> list[dict]:
        try:
            threshold = datetime.now() - timedelta(days)

            with get_session() as session:
                # 1. 뷰에서 후보군 가져오기
                result = session.execute(
                    text(
                        f"""
                        SELECT * FROM {view_name}
                        WHERE ai_analysis IS NULL
                    """
                    )
                )
                candidates = [dict(r._mapping) for r in result]
                valid_rows = []

                for row in candidates:
                    crawling_id = row.get("crawling_id")
                    ticker = row.get("ticker")

                    # 2. analysis_log 조회
                    check = session.execute(
                        text(
                            """
                            SELECT try_time FROM analysis_log
                            WHERE crawling_id = :cid AND ticker = :ticker
                        """
                        ),
                        {"cid": crawling_id, "ticker": ticker},
                    ).fetchone()

                    if not check:
                        # 없음 → INSERT try_time = NULL
                        session.execute(
                            text(
                                """
                                INSERT INTO analysis_log (crawling_id, ticker, try_time)
                                VALUES (:cid, :ticker, NULL)
                            """
                            ),
                            {"cid": crawling_id, "ticker": ticker},
                        )
                        session.flush()
                        valid_rows.append(row)

                    else:
                        try_time = check[0]
                        if try_time is None or try_time < threshold:
                            # NULL 또는 오래된 경우 → 그냥 valid로 추가 (UPDATE 없음)
                            valid_rows.append(row)
                        # 최근 시도된 항목은 무시

                session.commit()
                self.logger.debug(f"분석 대상 데이터 - {len(valid_rows)}개")

                return valid_rows

        except Exception as e:
            self.logger.error(f"Failed to fetch unanalyzed rows: {e}")
            return []

    def update_analysis_log_time(
        self, crawling_id: str, ticker: str, now: datetime | None = None
    ) -> bool:
        """
        crawling_id와 ticker에 해당하는 항목의 try_time을 현재 시각으로 갱신한다.
        존재하지 않으면 False 반환 (insert는 수행하지 않음).
        """
        if now is None:
            now = datetime.now()

        try:
            with get_session() as session:
                result = session.execute(
                    text(
                        """
                        SELECT try_time FROM analysis_log
                        WHERE crawling_id = :cid AND ticker = :ticker
                        """
                    ),
                    {"cid": crawling_id, "ticker": ticker},
                ).fetchone()

                if result:
                    try_time = result[0]
                    session.execute(
                        text(
                            """
                            UPDATE analysis_log
                            SET try_time = :now
                            WHERE crawling_id = :cid AND ticker = :ticker
                            """
                        ),
                        {"now": now, "cid": crawling_id, "ticker": ticker},
                    )
                    session.commit()
                    return True

                return False

        except Exception as e:
            # 여기에 적절한 로깅 또는 예외처리 필요
            self.logger.error(f"날짜 업데이트 중 오류: {e}")
            return False

    def _update_analysis(self, crawling_id: str, analysis: int, source: str) -> None:
        model_map = {
            "news": News,
            "financial": FinancialStatement,
        }

        model = model_map.get(source)
        if not model:
            self.logger.error(f"Unknown source: {source}")
            return

        try:
            with get_session() as session:
                stmt = (
                    update(model)
                    .where(model.crawling_id == crawling_id)
                    .values(ai_analysis=analysis)
                )
                result = session.execute(stmt)

                if result.rowcount > 0:
                    session.commit()
                    self.logger.debug(f"Updated {source} → {crawling_id}: {analysis}")
                else:
                    self.logger.warning(f"{crawling_id} not matched in {source}")

        except Exception as e:
            self.logger.error(f"{source} - {crawling_id}: {type(e).__name__}: {e}")
