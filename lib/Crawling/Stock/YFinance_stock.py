import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from tqdm import tqdm

from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.session import get_session
from lib.Exceptions.exceptions import *


from typing import List, Optional


def get_symbols_from_db(
    interval: str, limit: Optional[int] = None
) -> Optional[List[str]]:
    INTERVAL_SPLIT_CONFIG = {
        "1m": {"offset": 0, "limit": 100},
        "5m": {"offset": 100, "limit": 900},
        "15m": {"offset": 1000, "limit": None},
    }

    # ✅ 예외 처리: limit이 주어졌는데 1m이 아닌 경우 None 반환
    if limit is not None and interval != "1m":
        return None

    # ✅ 쿼리 실행
    with get_session() as session:
        query = session.query(Company.ticker).order_by(Company.company_id.asc())

        # ▶️ limit이 명시된 경우 (1m만 허용)
        if limit is not None:
            query = query.limit(limit)

        # ▶️ 인터벌 기반 offset/limit 처리
        else:
            config = INTERVAL_SPLIT_CONFIG.get(interval)
            if not config:
                raise ValueError(f"지원하지 않는 interval: {interval}")

            if config["offset"]:
                query = query.offset(config["offset"])
            if config["limit"]:
                query = query.limit(config["limit"])

        # ✅ 결과 반환
        results = query.all()
        return [ticker[0] for ticker in results]


class YFinanceStockCrawler(CrawlerInterface):

    def __init__(self, name, interval="1m", verbose=False):
        super().__init__(name)
        self.batch_size = 20  # 한 스레드에 넘길 배치 크기
        self.max_workers = 5  # 동시 실행 스레드 수
        self.symbols = get_symbols_from_db(interval, limit=5)
        self.tag = "stock"
        self.interval = interval
        self.verbose = verbose

    def crawl(self):
        results = []

        if not self.symbols:
            return None  # 빈 리스트 대신 None 반환

        total_symbols = len(self.symbols)

        # 배치로 나누기
        batches = [
            self.symbols[i : i + self.batch_size]
            for i in range(0, total_symbols, self.batch_size)
        ]

        progress_bar = tqdm(
            total=len(batches),
            desc=f"[{self.name}][{self.interval}] 전체 진행률",
            disable=not self.verbose,
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._crawl_batch, batch): batch for batch in batches
            }

            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    results.extend(batch_results)
                except BatchProcessingException as e:
                    for symbol in futures[future]:
                        results.append(
                            {
                                "tag": self.tag,
                                "log": {
                                    "crawling_type": self.tag,
                                    "status_code": e.status_code,
                                },
                                "fail_log": {"err_message": str(e)},
                            }
                        )
                except Exception as e:
                    for symbol in futures[future]:
                        results.append(
                            {
                                "tag": self.tag,
                                "log": {"crawling_type": self.tag, "status_code": 500},
                                "fail_log": {
                                    "err_message": f"배치 처리 중 알 수 없는 오류: {str(e)}"
                                },
                            }
                        )

                progress_bar.update(1)

        progress_bar.close()
        return results

    def _crawl_batch(self, batch):
        batch_results = []

        try:
            tickers = yf.Tickers(" ".join(batch))
        except Exception as e:
            raise BatchProcessingException(
                f"yf.Tickers 호출 실패: {str(e)}", source=batch
            )

        for symbol in batch:
            try:
                stock = tickers.tickers.get(symbol)
                if not stock:
                    raise DataNotFoundException(
                        "해당 종목을 찾을 수 없음", source=symbol
                    )

                df = stock.history(period="1d", interval=self.interval, prepost=True)[
                    ["Open", "High", "Low", "Close", "Volume"]
                ]
                if df.empty:
                    raise DataNotFoundException(
                        "Empty DataFrame (거래 데이터 없음)", source=symbol
                    )

                df = df.tail(1).reset_index()
                df.rename(columns={"Datetime": "posted_at"}, inplace=True)
                df["Symbol"] = symbol

                batch_results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 200},
                        "df": df,
                    }
                )

            except CrawlerException as e:
                batch_results.append(
                    {
                        "tag": self.tag,
                        "log": {
                            "crawling_type": self.tag,
                            "status_code": e.status_code,
                        },
                        "fail_log": {"err_message": str(e)},
                    }
                )
            except Exception as e:
                batch_results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 500},
                        "fail_log": {
                            "err_message": f"{symbol} 처리 중 알 수 없는 오류: {str(e)}"
                        },
                    }
                )

            time.sleep(random.uniform(0.1, 0.4))

        return batch_results
