"""
lib/Crawling/Stock/YFinanceStockCrawler.py
-----------------------------------------
* 하루 1회: 전일 Adj Close + MarketCap 계산·캐싱
* 분봉(15m) 데이터만 수집 후 캐시된 adj_close / market_cap 주입
"""

from __future__ import annotations

import datetime
import json
import time
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import threading, random
from curl_cffi import requests as curl_requests

session = curl_requests.Session(impersonate="chrome")

import logging
import pandas as pd
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.Crawling.Interfaces.Crawler import CrawlerInterface
from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.session import get_session
from lib.Exceptions.exceptions import (
    BatchProcessingException,
    CrawlerException,
    DataNotFoundException,
)
from lib.Config.config import Config
from lib.Logger.logger import Logger
from lib.Crawling.utils.yfhandler import _YFForwardHandler

# ──────────────────────────────────────────────────────────────────────────
# 공통 유틸
# ──────────────────────────────────────────────────────────────────────────
# def _get_us_market_date(now: datetime.datetime | None = None) -> str:
#     """UTC 22:00(=NY 17:00) 이전이면 어제를, 이후면 오늘 날짜를 반환 (yyyy-mm-dd)"""
#     now = now or datetime.datetime.utcnow()
#     cutoff = datetime.time(22, 0)
#     date = (
#         now.date() - datetime.timedelta(days=1) if now.time() < cutoff else now.date()
#     )
#     return date.isoformat()


def get_symbols_from_db(limit: int | None = None) -> List[str]:
    with get_session() as session:
        q = session.query(Company.ticker).order_by(Company.company_id.asc())
        if limit:
            q = q.limit(limit)
        return [t[0] for t in q.all()]


# ───────── Share 캐시 파일 경로 ─────────
_SHARES_FILE = Path(__file__).with_name("shares_cache.json")


# shares_cache.json ⇒ {"_meta":{"last_reset":"2025-05"},"data":{"AAPL":...}}
def _load_shares_file() -> tuple[dict[str, str], dict[str, int]]:
    if _SHARES_FILE.exists():
        try:
            raw = json.loads(_SHARES_FILE.read_text())
            if "_meta" in raw and "data" in raw:
                return raw["_meta"], raw["data"]
            # 구버전(메타 없음) 호환
            return {}, raw
        except Exception:
            pass
    return {}, {}


def _dump_shares_file(meta: dict[str, str], cache: dict[str, int]) -> None:
    try:
        _SHARES_FILE.write_text(
            json.dumps({"_meta": meta, "data": cache}, separators=(",", ":"))
        )
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────
# 메인 크롤러
# ──────────────────────────────────────────────────────────────────────────
class YFinanceStockCrawler(CrawlerInterface):
    """15 분봉 OHLCV + 전일 Adj Close + MarketCap"""

    # 클래스 레벨 캐시
    _ticker_cik_map: dict[str, str] = {}  # {TICKER: CIK(10)}
    _cached_shares: dict[str, int] = {}  # {TICKER: shares}
    _cached_price_cap: dict[str, tuple[float, float, str]] = {}
    #                        ↳ (adj_close, market_cap, adj_date)

    def __init__(self, name: str):
        super().__init__(name)
        self.batch_size = 30
        self.max_workers = 10
        self.symbols = get_symbols_from_db(Config.get("symbol_size.total", 6000))
        self.tag = "stock"
        self.logger = Logger(self.__class__.__name__)

        # 🔹 yfinance 로그 → 우리 Logger 로 리다이렉트
        yf_logger = logging.getLogger("yfinance")
        yf_logger.handlers.clear()  # 기본 스트림핸들러 제거
        yf_logger.setLevel(logging.INFO)  # 원하는 최소 레벨
        yf_logger.addHandler(_YFForwardHandler(self.logger))
        yf_logger.propagate = False  # 루트 로거로 전파 방지

        self._load_ticker_cik_map()
        self._shares_meta, self._cached_shares = _load_shares_file()

    # ─────────────────── Ticker-CIK 매핑 ───────────────────

    def _load_ticker_cik_map(self):
        """
        cik2ticker.json → {TICKER: CIK(10자리)} 로 변환
        - 리스트 형식  : [{"ticker": "AAPL", "cik_str": "320193"}, ...]
        - 딕셔너리 형식: {"AAPL": "320193"}  or  {"AAPL": {"cik_str": "320193", ...}}
        """
        if self._ticker_cik_map:
            return

        path = Path(__file__).with_name("cik2ticker.json")
        try:
            with path.open(encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):  # 📄 ① 리스트
                for obj in data:
                    ticker = (obj.get("ticker") or obj.get("symbol", "")).upper()
                    cik_raw = str(obj.get("cik_str") or obj.get("CIK") or "").lstrip(
                        "0"
                    )
                    if ticker and cik_raw:
                        self._ticker_cik_map[ticker] = f"{int(cik_raw):010d}"

            elif isinstance(data, dict):  # 📄 ② 딕셔너리
                for ticker, val in data.items():
                    ticker = ticker.upper()
                    if isinstance(val, dict):  # {"AAPL": {"cik_str": "..."}}
                        cik_raw = str(
                            val.get("cik_str") or val.get("CIK") or val.get("cik") or ""
                        ).lstrip("0")
                    else:  # {"AAPL": "320193"}
                        cik_raw = str(val).lstrip("0")

                    if cik_raw:
                        self._ticker_cik_map[ticker] = f"{int(cik_raw):010d}"

            self.logger.log(
                "DEBUG", f"CIK 매핑 로드 완료: {len(self._ticker_cik_map):,} tickers"
            )

        except Exception as e:
            self.logger.log("ERROR", f"CIK 매핑 로드 실패: {e}")

    # ───────────────── 발행주식수 캐시 ─────────────────

    def _get_shares_outstanding(self, symbol: str) -> Optional[int]:
        """
        • 캐시 → 다중 콘셉트(US-GAAP, DEI, IFRS) 순회 → Yahoo quote fallback
        • 성공 시 _cached_shares[symbol] = shares
        """
        cached = self._cached_shares.get(symbol)
        if cached:
            return cached

        cik = self._ticker_cik_map.get(symbol.upper())
        if not cik:
            return None

        # 1️⃣ SEC XBRL 콘셉트 후보
        concept_paths = [
            ("us-gaap", "CommonStockSharesOutstanding"),
            ("dei", "EntityCommonStockSharesOutstanding"),
            ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"),
            ("ifrs-full", "SharesOutstanding"),  # ─ IFRS filers
            ("ifrs-full", "OrdinarySharesNumber"),  # ─ 일부 ADR
        ]

        headers = {"User-Agent": "StockCrawler/1.0 (contact: you@example.com)"}
        for taxonomy, concept in concept_paths:
            url = (
                f"https://data.sec.gov/api/xbrl/companyconcept/"
                f"CIK{cik}/{taxonomy}/{concept}.json"
            )
            try:
                r = requests.get(url, headers=headers, timeout=8)
                if r.status_code == 404:
                    continue  # 다음 후보 콘셉트 시도
                r.raise_for_status()
                data = r.json()
                units = data.get("units", {})
                # 'shares' or 'num' 등 키가 다양 → 모든 list 합치기
                all_units = []
                for v in units.values():
                    all_units.extend(v)
                if not all_units:
                    continue

                latest = max(all_units, key=lambda x: x.get("end", ""))
                shares = int(latest.get("val", 0))
                if shares > 0:
                    # self.logger.log(
                    #     "DEBUG", f"{symbol} sharesOutstanding 성공"
                    # )  # 테스트용 =============================================
                    self._cached_shares[symbol] = shares
                    return shares
            except Exception as e:
                # 로깅은 DEBUG 수준으로만
                self.logger.log("DEBUG", f"{symbol} concept '{concept}' 실패: {e}")

        # 2️⃣ Fallback: Yahoo Finance quote?fields=sharesOutstanding
        try:
            tkr = yf.Ticker(symbol)

            # ✴️ fast_info 가 가장 빠름 (0.05s 미만)
            shares = None
            if hasattr(tkr, "fast_info"):
                shares = tkr.fast_info.get("sharesOutstanding")

            # fast_info 에 없으면 .get_info() → .info 순
            if not shares:
                try:
                    shares = tkr.get_info()["sharesOutstanding"]  # yfinance ≥0.2
                except AttributeError:
                    shares = tkr.info.get("sharesOutstanding")  # yfinance ≤0.1

            if shares:
                # self.logger.log(
                #     "DEBUG", f"{symbol} yfinance sharesOutstanding 성공"
                # )  # 테스트용 =============================================
                shares = int(shares)
                self._cached_shares[symbol] = shares
                return shares

        except Exception as e:
            self.logger.log("DEBUG", f"{symbol} yfinance sharesOutstanding 실패: {e}")

        # 3️⃣ 최종 실패
        self.logger.log("WARN", f"{symbol}: 발행주식수 확인 실패 (SEC+Yahoo 모두 실패)")
        return None

    def _bulk_fetch_shares(
        self, symbols: list[str], max_workers: int = 8
    ) -> dict[str, int]:
        """
        SEC XBRL을 병렬 호출 → {ticker: shares}
        • 초당 10건 이하 (token-bucket)
        • 각 티커의 시도/성공/실패를 DEBUG 로그로 출력
        """
        symbols = [s for s in symbols if s not in self._cached_shares]
        if not symbols:
            # self.logger.log("INFO", "[Shares] 모두 파일 캐시")
            return {}

        session = requests.Session()
        headers = {"User-Agent": "StockCrawler/1.0 (contact: you@example.com)"}

        # ───────────────── token-bucket ─────────────────
        tokens = threading.Semaphore(10)

        def refill():
            while True:
                time.sleep(1)
                for _ in range(10 - tokens._value):
                    tokens.release()

        threading.Thread(target=refill, daemon=True).start()

        concept_paths = [
            ("us-gaap", "CommonStockSharesOutstanding"),
            ("dei", "EntityCommonStockSharesOutstanding"),
            ("ifrs-full", "SharesOutstanding"),
            ("ifrs-full", "OrdinarySharesNumber"),
        ]

        def fetch_single(tkr: str) -> Optional[int]:
            cik = self._ticker_cik_map.get(tkr)
            if not cik:
                return None

            for tax, con in concept_paths:
                url = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/{tax}/{con}.json"
                tokens.acquire()
                try:
                    r = session.get(url, headers=headers, timeout=6)
                    if r.status_code == 404:
                        continue
                    r.raise_for_status()

                    units = r.json().get("units", {})
                    flat = [u for v in units.values() for u in v]
                    if not flat:
                        continue

                    latest = max(flat, key=lambda x: x.get("end", ""))
                    val = int(latest.get("val", 0))
                    if val:
                        # self.logger.log(
                        #     "DEBUG", f"{tkr} shares ✅ SEC {tax}/{con}"
                        # )  # 테스트용 =============================================
                        return val
                except Exception as e:
                    continue

            return None

        shares_map: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut = {ex.submit(fetch_single, s): s for s in symbols}
            for f in as_completed(fut):
                sym = fut[f]
                val = f.result()
                if val:
                    shares_map[sym] = val
                    self._cached_shares[sym] = val  # 메모리 캐시 동시 갱신

        # self.logger.log(
        #     "INFO",
        #     f"[Shares] 병렬 fetch 완료 ▶ 성공 {len(shares_map):,} / 요청 {len(symbols):,}",
        # )
        return shares_map

    def _ensure_price_cap_cache(self):
        """Adj Close & MarketCap 캐시 갱신 (curl + chart API)"""

        today = datetime.date.today()
        this_month = today.strftime("%Y-%m")

        if today.day == 2 and self._shares_meta.get("last_reset") != this_month:
            self.logger.log("INFO", "[Shares] 월간 리셋 실행")
            self._cached_shares.clear()
            self._shares_meta["last_reset"] = this_month

        if not self.symbols:
            return

        # 1) sentinel 심볼로 adj 날짜 파악
        sentinel = self.symbols[0]
        try:
            sentinel_data = self._download_adjclose_chart([sentinel])
            if sentinel not in sentinel_data:
                self.logger.log("WARN", f"sentinel {sentinel} 데이터 없음")
                return
            _, adj_date = sentinel_data[sentinel]
        except Exception as e:
            self.logger.log("ERROR", f"sentinel 다운로드 실패: {e}")
            return

        # 2) stale 티커 선별
        stale = [
            s
            for s in self.symbols
            if s not in self._cached_price_cap
            or self._cached_price_cap[s][2] != adj_date
        ]
        if not stale:
            self.logger.log("INFO", f"[MarketCap] 캐시 최신 (adj_date={adj_date})")
            return

        # 3) adj close 다운로드 (chart API)
        self.logger.log("DEBUG", "Adj Close 다운로드 시작")
        adjclose_map = self._download_adjclose_chart(stale)
        if not adjclose_map:
            self.logger.log("ERROR", "Adj Close 다운로드 실패")
            return
        self.logger.log("DEBUG", "Adj Close 다운로드 완료")

        # 4) SEC 병렬 호출 → 발행주식수
        shares_map = self._bulk_fetch_shares(stale)

        # 5) 병합·캐싱
        updated = 0
        for sym in stale:
            try:
                if sym not in adjclose_map:
                    continue
                adj_close, adj_date = adjclose_map[sym]

                shares = shares_map.get(sym) or self._get_shares_outstanding(sym)
                if not shares:
                    continue

                cap = adj_close * shares
                self._cached_price_cap[sym] = (adj_close, cap, adj_date)
                updated += 1
            except Exception as e:
                self.logger.log("DEBUG", f"{sym} 캐싱 실패: {e}")

        _dump_shares_file(self._shares_meta, self._cached_shares)
        self.logger.log(
            "DEBUG",
            f"[MarketCap] 캐시 갱신 완료 {updated:,}/{len(stale):,} (adj_date={adj_date})",
        )

    def _download_adjclose_chart(
        self,
        symbols: List[str],
        days: int = 7,
        max_workers: int = 10,
        batch_size: int = 20,
    ) -> Dict[str, Tuple[float, str]]:
        """yfinance Ticker().history()로 adj close + 날짜 병렬 추출 (10단위 배치 디버그 로그)"""
        result = {}

        def fetch_single(sym: str):
            for _ in range(3):  # 최대 3회 재시도
                try:
                    time.sleep(random.uniform(0.1, 0.3))
                    ticker = yf.Ticker(sym)
                    df = ticker.history(
                        period=f"{days}d",
                        interval="1d",
                        auto_adjust=False,
                    )

                    if df.empty or "Adj Close" not in df.columns:
                        continue

                    last_row = df.dropna(subset=["Adj Close"]).iloc[-1]
                    adj_close = float(last_row["Adj Close"])
                    date = last_row.name.date()
                    return sym, (adj_close, str(date))

                except Exception as e:
                    self.logger.log("WARN", f"{sym} yfinance history 중 예외 발생: {e}")
            return None

        for i in range(0, len(symbols), batch_size):
            batch_num = i // batch_size + 1
            if batch_num % 10 == 0:
                self.logger.log(
                    "DEBUG", f"{batch_num}번째 배치 실행 중 (index {i}~{i+batch_size})"
                )

            batch = symbols[i : i + batch_size]

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(fetch_single, sym): sym for sym in batch}
                for future in as_completed(futures):
                    result_item = future.result()
                    if result_item:
                        sym, value = result_item
                        result[sym] = value

        return result

    # ────────────────────────── public ▶ crawl ──────────────────────────
    def crawl(self):
        # ① 전일 Adj Close & MarketCap 캐시 확보
        self._ensure_price_cap_cache()

        if not self.symbols:
            return None

        batches = [
            self.symbols[i : i + self.batch_size]
            for i in range(0, len(self.symbols), self.batch_size)
        ]
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self._crawl_batch, batch, i): batch
                for i, batch in enumerate(batches)
            }

            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    results.extend(future.result())
                except BatchProcessingException as e:
                    for sym in batch:
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
                    for sym in batch:
                        results.append(
                            {
                                "tag": self.tag,
                                "log": {"crawling_type": self.tag, "status_code": 500},
                                "fail_log": {"err_message": f"batch 오류: {e}"},
                            }
                        )

        return results

    # ────────────────────── 분봉 데이터 수집 ──────────────────────
    def _crawl_batch(self, batch: List[str], batch_id: int):
        batch_results = []

        if batch_id % 10 == 0:
            self.logger.log("DEBUG", f"[배치 진행] [{batch_id}]번째 배치 시작")

        for symbol in batch:
            try:
                stock = yf.Ticker(symbol, session=session)

                # ▶ adj_close / market_cap 캐시
                cache_item = self._cached_price_cap.get(symbol)
                if not cache_item:
                    self.logger.log("DEBUG", f"[{symbol}]가격/시총 캐시 누락")
                    raise DataNotFoundException("가격/시총 캐시 누락", source=symbol)
                adj_close, market_cap, _ = cache_item

                # ✅ df 가공
                df_min = self._process_minute_data(stock, symbol, adj_close, market_cap)

                batch_results.append(
                    {
                        "tag": self.tag,
                        "log": {"crawling_type": self.tag, "status_code": 200},
                        "df": df_min,
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

    # ────────────────────── 분봉 → 1행 변환 ──────────────────────
    def _process_minute_data(
        self,
        stock,
        symbol: str,
        adj_close: Optional[float],
        market_cap: Optional[float],
    ) -> pd.DataFrame:

        df_min = stock.history(period="1d", interval="1m", prepost=True)[
            ["Open", "High", "Low", "Close", "Volume"]
        ]

        if df_min.empty:
            raise DataNotFoundException(
                "Empty DataFrame (거래 데이터 없음)", source=symbol
            )

        df_min = df_min.tail(1).reset_index()
        df_min.rename(columns={"Datetime": "posted_at"}, inplace=True)
        df_min["Symbol"] = symbol
        df_min["Adj Close"] = round(adj_close, 2)
        df_min["MarketCap"] = int(market_cap)

        return df_min
