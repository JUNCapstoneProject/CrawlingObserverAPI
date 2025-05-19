from sqlalchemy import text, update
import time

from lib.Logger.logger import Logger
from lib.Distributor.socket.Client import SocketClient
from lib.Distributor.secretary.session import get_session
from lib.Distributor.secretary.models.news import News
from lib.Distributor.secretary.models.reports import Report
from lib.Distributor.secretary.models.financials import FinancialStatement
from lib.Config.config import Config  # 설정 관리 클래스


class NotifierBase:
    def __init__(self, name="NotifierBase"):
        self.client = SocketClient()
        self.logger = Logger(name)
        self.interval_sec = Config.get("notifier_interval", 600)

    def run_all(self):
        from lib.Distributor.notifier.Article_notifier import ArticleNotifier
        from lib.Distributor.notifier.Financial_notifier import FinancialNotifier

        self.logger.log(
            "START", f"[Notifier] 주기적 실행 시작 (interval: {self.interval_sec}s)"
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

    def _update_analysis(
        self, crawling_id: str, analysis: str, tables: list[str]
    ) -> None:
        model_map = {
            "news": News,
            "reports": Report,
            "financials": FinancialStatement,
        }
        try:
            with get_session() as session:
                updated = False
                for tbl in tables:
                    model = model_map[tbl]
                    stmt = (
                        update(model)
                        .where(model.crawling_id == crawling_id)
                        .values(ai_analysis=analysis)
                    )
                    result = session.execute(stmt)
                    if result.rowcount > 0:
                        updated = True
                if updated:
                    session.commit()
                    self.logger.log(
                        "DEBUG",
                        f"[Update] crawling_id {crawling_id} updated in {tables}",
                    )
                else:
                    self.logger.log(
                        "WARN",
                        f"[Update] crawling_id {crawling_id} not matched in {tables}",
                    )
        except Exception as e:
            self.logger.log("ERROR", f"[Update] crawling_id {crawling_id}: {e}")
