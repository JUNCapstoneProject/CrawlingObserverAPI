from sqlalchemy import text
import time
from abc import abstractmethod

from lib.Logger.logger import Logger
from lib.Distributor.socket.Client import SocketClient
from lib.Distributor.secretary.session import get_session
from lib.Config.config import Config  # 설정 관리 클래스


class NotifierBase:
    def __init__(self, name="NotifierBase"):
        self.client = SocketClient()
        self.logger = Logger(name)
        self.interval_sec = Config.get("notifier_interval", 120)
        self.socket_condition = Config.get("socket_condition", True)

    def run_all(self):
        from lib.Distributor.notifier.Article_notifier import ArticleNotifier
        from lib.Distributor.notifier.Financial_notifier import FinancialNotifier

        self.logger.log(
            "START", f"[Notifier] 실행 시작 (interval: {self.interval_sec}s)"
        )
        try:
            while True:
                ArticleNotifier().run()
                FinancialNotifier().run()
                self.logger.log(
                    "WAIT", f"[Notifier] {self.interval_sec}초 대기 후 재실행"
                )
                time.sleep(self.interval_sec)
        except KeyboardInterrupt:
            self.logger.log("INFO", "[Notifier] 중단됨 (KeyboardInterrupt)")
        except Exception as e:
            self.logger.log("ERROR", f"[Notifier] run_all 실패: {e}")
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
            self.logger.log("WARN", f"[Fetch] Failed to query {view_name}: {e}")
            return []

    @abstractmethod
    def _update_analysis():
        pass
