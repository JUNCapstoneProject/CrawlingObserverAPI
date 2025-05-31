from sqlalchemy import text
import time
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.Logger.logger import get_logger
from lib.Distributor.socket.Client import SocketClient
from lib.Distributor.secretary.session import get_session
from lib.Config.config import Config  # 설정 관리 클래스


class NotifierBase:
    def __init__(self, name="NotifierBase"):
        self.client = SocketClient()
        self.logger = get_logger(name)
        self.interval_sec = Config.get("notifier_interval", 60)
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

                self.logger.info(f"{self.interval_sec} 이후 재실행")
                time.sleep(self.interval_sec)

        except KeyboardInterrupt:
            self.logger.info("중단됨 (KeyboardInterrupt)")
        except Exception as e:
            self.logger.error(f"동작 중 오류 발생: {e}")
        finally:
            self.logger.log_summary()

    def _fetch_unanalyzed_rows(self, view_name: str) -> list[dict]:
        try:
            with get_session() as session:
                result = session.execute(
                    text(f"SELECT * FROM {view_name} WHERE ai_analysis IS NULL")
                )
                return [dict(r._mapping) for r in result]
        except Exception as e:
            self.logger.error(f"Failed to query {view_name}: {e}")
            return []

    @abstractmethod
    def _update_analysis():
        pass
