from sqlalchemy import text
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.Logger.logger import get_logger
from lib.Distributor.socket.Client import SocketClient
from lib.Distributor.secretary.session import get_session
from lib.Config.config import Config  # 설정 관리 클래스


class NotifierBase:
    def __init__(self, name="NotifierBase"):
        self.client = SocketClient()
        self.logger = get_logger(name)
        self.interval_sec = Config.get("notifier_interval", 180)
        self.socket_condition = Config.get("socket_condition", True)

    def run_all(self):
        from lib.Distributor.notifier.Article_notifier import ArticleNotifier
        from lib.Distributor.notifier.Financial_notifier import FinancialNotifier

        self.logger.info(f"실행 시작 (interval: {self.interval_sec}s)")
        try:
            while True:
                notifiers = [ArticleNotifier(), FinancialNotifier()]

                # 스레드 병렬 실행
                with ThreadPoolExecutor(max_workers=len(notifiers)) as executor:
                    futures = {executor.submit(n.run): n for n in notifiers}

                    for future in as_completed(futures):
                        notifier = futures[future]
                        try:
                            future.result()
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

    def _fetch_unanalyzed_rows(self, view_name: str, days=5) -> list[dict]:
        try:
            now = datetime.now()
            threshold = now - timedelta(days)

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
                        # 없음 → INSERT + valid
                        session.execute(
                            text(
                                """
                                INSERT INTO analysis_log (crawling_id, ticker, try_time)
                                VALUES (:cid, :ticker, :now)
                            """
                            ),
                            {"cid": crawling_id, "ticker": ticker, "now": now},
                        )
                        session.flush()
                        valid_rows.append(row)

                    else:
                        try_time = check[0]
                        if try_time is None or try_time < threshold:
                            # NULL 또는 오래됨 → UPDATE + valid
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
                            session.flush()
                            valid_rows.append(row)
                        # 그 외 (최근이면 무시)

                session.commit()
                self.logger.debug(f"분석 대상 데이터 - {len(valid_rows)}개")

                valid_rows = valid_rows[:3]

                return valid_rows

        except Exception as e:
            self.logger.error(f"Failed to fetch unanalyzed rows: {e}")
            return []
