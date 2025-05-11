from __future__ import annotations

# ─── Built-in Modules ─────────────────────────────────────────────────────
import datetime
import json
import time
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import threading
import random

# ─── Third-party Modules ─────────────────────────────────────────────────
import logging
import pandas as pd
import requests
import yfinance as yf
from curl_cffi import requests as curl_requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Project-specific Modules ────────────────────────────────────────────
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

# ─── Global Variables ────────────────────────────────────────────────────
session = curl_requests.Session(impersonate="chrome")
_SHARES_FILE = Path(__file__).with_name("shares_cache.json")


# ─── Main Crawler Class ──────────────────────────────────────────────────


class YFinanceStockCrawler(CrawlerInterface):
    """Crawler for fetching stock data using Yahoo Finance."""

    _ticker_cik_map: dict[str, str] = {}
    _cached_shares: dict[str, int] = {}
    _cached_price_cap: dict[str, tuple[float, float, str]] = {}

    # ─── Initialization ──────────────────────────────────────────────────

    def __init__(self, name: str):
        """Initializes the crawler."""
        super().__init__(name)
        self.batch_size = 30
        self.max_workers = 10
        self.symbols = get_symbols_from_db(Config.get("symbol_size.total", 6000))
        self.tag = "stock"
        self.logger = Logger(self.__class__.__name__)

        # Configure yfinance logger
        yf_logger = logging.getLogger("yfinance")
        yf_logger.handlers.clear()
        yf_logger.setLevel(logging.INFO)
        yf_logger.addHandler(_YFForwardHandler(self.logger))
        yf_logger.propagate = False

        self._load_ticker_cik_map()
        self._shares_meta, self._cached_shares = _load_shares_file()

    def _load_ticker_cik_map(self):
        """Loads the Ticker-CIK mapping from a JSON file."""
        if self._ticker_cik_map:
            return

        path = Path(__file__).with_name("cik2ticker.json")
        try:
            with path.open(encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                for obj in data:
                    ticker = (obj.get("ticker") or obj.get("symbol", "")).upper()
                    cik_raw = str(obj.get("cik_str") or obj.get("CIK") or "").lstrip(
                        "0"
                    )
                    if ticker and cik_raw:
                        self._ticker_cik_map[ticker] = f"{int(cik_raw):010d}"

            elif isinstance(data, dict):
                for ticker, val in data.items():
                    ticker = ticker.upper()
                    if isinstance(val, dict):
                        cik_raw = str(
                            val.get("cik_str") or val.get("CIK") or val.get("cik") or ""
                        ).lstrip("0")
                    else:
                        cik_raw = str(val).lstrip("0")

                    if cik_raw:
                        self._ticker_cik_map[ticker] = f"{int(cik_raw):010d}"

        except Exception as e:
            self.logger.log("ERROR", f"Failed to load Ticker-CIK mapping: {e}")

    # ─── Crawling ─────────────────────────────────────────────────────────

    def crawl(self):
        """Main method to execute the crawling process."""
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
                                "fail_log": {"err_message": f"Batch error: {e}"},
                            }
                        )

        return results

    def _crawl_batch(self, batch: List[str], batch_id: int):
        """Processes a batch of symbols."""
        batch_results = []

        for symbol in batch:
            try:
                stock = yf.Ticker(symbol, session=session)

                cache_item = self._cached_price_cap.get(symbol)
                if not cache_item:
                    raise DataNotFoundException(
                        "Price/MarketCap cache missing", source=symbol
                    )
                adj_close, market_cap, _ = cache_item

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
                            "err_message": f"Unknown error while processing {symbol}: {str(e)}"
                        },
                    }
                )

            time.sleep(random.uniform(0.1, 0.4))

        return batch_results

    def _process_minute_data(
        self,
        stock,
        symbol: str,
        adj_close: Optional[float],
        market_cap: Optional[float],
    ) -> pd.DataFrame:
        """Processes minute-level stock data."""
        df_min = stock.history(period="1d", interval="1m", prepost=True)[
            ["Open", "High", "Low", "Close", "Volume"]
        ]

        if df_min.empty:
            raise DataNotFoundException(
                "Empty DataFrame (No trading data)", source=symbol
            )

        df_min = df_min.tail(1).reset_index()
        df_min.rename(columns={"Datetime": "posted_at"}, inplace=True)
        df_min["Symbol"] = symbol
        df_min["Adj Close"] = round(adj_close, 2)
        df_min["MarketCap"] = int(market_cap)

        # Calculate change based on Close and Adj Close
        df_min["Change"] = 0.0
        if adj_close and not df_min["Close"].isna().all():
            try:
                df_min["Change"] = round(
                    (df_min["Close"].iloc[0] / adj_close - 1) * 100, 2
                )
            except ZeroDivisionError:
                df_min["Change"] = 0.0

        return df_min

    # ─── Cache Management ────────────────────────────────────────────────

    def _ensure_price_cap_cache(self):
        """Ensures the price and market cap cache is up-to-date."""
        today = datetime.date.today()
        this_month = today.strftime("%Y-%m")

        if today.day == 2 and self._shares_meta.get("last_reset") != this_month:
            self.logger.log("INFO", "[Shares] Monthly reset executed")
            self._cached_shares.clear()
            self._shares_meta["last_reset"] = this_month

        if not self.symbols:
            return

        sentinel = self.symbols[0]
        try:
            sentinel_data = self._download_adjclose_chart([sentinel])
            if sentinel not in sentinel_data:
                self.logger.log("WARN", f"Sentinel {sentinel} data missing")
                return
            _, adj_date = sentinel_data[sentinel]
        except Exception as e:
            self.logger.log("ERROR", f"Failed to download sentinel data: {e}")
            return

        stale = [
            s
            for s in self.symbols
            if s not in self._cached_price_cap
            or self._cached_price_cap[s][2] != adj_date
        ]
        if not stale:
            self.logger.log(
                "INFO", f"[MarketCap] Cache is up-to-date (adj_date={adj_date})"
            )
            return

        adjclose_map = self._download_adjclose_chart(stale)
        if not adjclose_map:
            self.logger.log("ERROR", "Failed to download Adj Close data")
            return

        shares_map = self._bulk_fetch_shares(stale)

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
                self.logger.log("DEBUG", f"Failed to cache {sym}: {e}")

        _dump_shares_file(self._shares_meta, self._cached_shares)
        self.logger.log(
            "DEBUG",
            f"[MarketCap] Cache updated {updated:,}/{len(stale):,} (adj_date={adj_date})",
        )

    def _get_shares_outstanding(self, symbol: str) -> Optional[int]:
        """Fetches the number of outstanding shares for a given symbol."""
        cached = self._cached_shares.get(symbol)
        if cached:
            return cached

        cik = self._ticker_cik_map.get(symbol.upper())
        if not cik:
            return None

        concept_paths = [
            ("us-gaap", "CommonStockSharesOutstanding"),
            ("dei", "EntityCommonStockSharesOutstanding"),
            ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"),
            ("ifrs-full", "SharesOutstanding"),
            ("ifrs-full", "OrdinarySharesNumber"),
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
                    continue
                r.raise_for_status()
                data = r.json()
                units = data.get("units", {})
                all_units = []
                for v in units.values():
                    all_units.extend(v)
                if not all_units:
                    continue

                latest = max(all_units, key=lambda x: x.get("end", ""))
                shares = int(latest.get("val", 0))
                if shares > 0:
                    self._cached_shares[symbol] = shares
                    return shares
            except Exception as e:
                self.logger.log(
                    "DEBUG", f"Failed to fetch concept '{concept}' for {symbol}: {e}"
                )

        try:
            tkr = yf.Ticker(symbol)
            shares = None
            if hasattr(tkr, "fast_info"):
                shares = tkr.fast_info.get("sharesOutstanding")
            if not shares:
                try:
                    shares = tkr.get_info()["sharesOutstanding"]
                except AttributeError:
                    shares = tkr.info.get("sharesOutstanding")
            if shares:
                shares = int(shares)
                self._cached_shares[symbol] = shares
                return shares
        except Exception as e:
            self.logger.log(
                "DEBUG",
                f"Failed to fetch sharesOutstanding from Yahoo Finance for {symbol}: {e}",
            )

        self.logger.log(
            "WARN", f"Failed to fetch outstanding shares for {symbol} (SEC+Yahoo)"
        )
        return None

    def _bulk_fetch_shares(
        self, symbols: list[str], max_workers: int = 8
    ) -> dict[str, int]:
        """Fetches outstanding shares for multiple symbols in bulk."""
        symbols = [s for s in symbols if s not in self._cached_shares]
        if not symbols:
            return {}

        session = requests.Session()
        headers = {"User-Agent": "StockCrawler/1.0 (contact: you@example.com)"}

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
                        return val
                except Exception:
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
                    self._cached_shares[sym] = val

        return shares_map

    # ─── Data Download ────────────────────────────────────────────────────

    def _download_adjclose_chart(
        self,
        symbols: List[str],
        days: int = 7,
        max_workers: int = 10,
        batch_size: int = 20,
    ) -> Dict[str, Tuple[float, str]]:
        """Downloads adjusted close chart data for given symbols."""
        result = {}

        def fetch_single(sym: str):
            for _ in range(3):
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
                    self.logger.log(
                        "WARN",
                        f"Exception occurred while fetching history for {sym}: {e}",
                    )
            return None

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(fetch_single, sym): sym for sym in batch}
                for future in as_completed(futures):
                    result_item = future.result()
                    if result_item:
                        sym, value = result_item
                        result[sym] = value

        return result


# ─── Utility Functions ───────────────────────────────────────────────────


def get_symbols_from_db(limit: int | None = None) -> List[str]:
    """Fetches a list of symbols from the database."""
    with get_session() as session:
        q = session.query(Company.ticker).order_by(Company.company_id.asc())
        if limit:
            q = q.limit(limit)
        return [t[0] for t in q.all()]


def _load_shares_file() -> tuple[dict[str, str], dict[str, int]]:
    """Loads the shares cache file."""
    if _SHARES_FILE.exists():
        try:
            raw = json.loads(_SHARES_FILE.read_text())
            if "_meta" in raw and "data" in raw:
                return raw["_meta"], raw["data"]
            return {}, raw
        except Exception:
            pass
    return {}, {}


def _dump_shares_file(meta: dict[str, str], cache: dict[str, int]) -> None:
    """Dumps data into the shares cache file."""
    try:
        _SHARES_FILE.write_text(
            json.dumps({"_meta": meta, "data": cache}, separators=(",", ":"))
        )
    except Exception:
        pass
