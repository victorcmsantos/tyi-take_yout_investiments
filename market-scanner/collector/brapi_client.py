"""BRAPI client wrappers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
from loguru import logger

from config.settings import AppSettings


class BRAPIClient:
    """Thin wrapper around BRAPI for normalized batch downloads."""

    PRICE_COLUMNS = ["open", "high", "low", "close", "volume"]

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.base_url = settings.brapi_base_url.rstrip("/")

    def download_batch(
        self,
        tickers: list[str],
        period: str,
        interval: str,
    ) -> dict[str, pd.DataFrame]:
        """Download and normalize OHLCV data for a batch of tickers."""

        if not tickers:
            return {}

        logger.debug(
            "Downloading prices from BRAPI",
            tickers=len(tickers),
            period=period,
            interval=interval,
        )

        normalized: dict[str, pd.DataFrame] = {}
        chunk_size = max(1, self.settings.brapi_max_tickers_per_request)
        for start in range(0, len(tickers), chunk_size):
            batch = tickers[start : start + chunk_size]
            normalized.update(self._download_sub_batch(batch, period=period, interval=interval))
        return normalized

    def _download_sub_batch(
        self,
        tickers: list[str],
        period: str,
        interval: str,
    ) -> dict[str, pd.DataFrame]:
        results = self._request_quote_results(tickers, period=period, interval=interval)
        if not results:
            return {}

        requested_symbol_map = {self._to_brapi_symbol(symbol): symbol for symbol in tickers}
        normalized: dict[str, pd.DataFrame] = {}
        for result in results:
            raw_symbol = str(result.get("symbol", "")).strip().upper()
            if not raw_symbol:
                continue
            source_symbol = requested_symbol_map.get(raw_symbol, f"{raw_symbol}.SA")
            frame = self._normalize_rows(result.get("historicalDataPrice"))
            frame = self._merge_live_quote_into_frame(frame, result)
            if not frame.empty:
                normalized[source_symbol] = frame
        return normalized

    def _merge_live_quote_into_frame(
        self,
        frame: pd.DataFrame,
        quote_result: dict[str, object],
    ) -> pd.DataFrame:
        """Overlay live quote data into the latest daily row.

        BRAPI historicalDataPrice can lag during market hours. We keep one row per day
        (03:00 UTC convention) and update it with regularMarketPrice when available.
        """

        price = self._to_float(quote_result.get("regularMarketPrice"))
        if price is None or price <= 0:
            return frame

        quote_ts = self._quote_day_timestamp(quote_result.get("regularMarketTime"))
        if quote_ts is None:
            quote_ts = self._quote_day_timestamp(datetime.now(timezone.utc).timestamp())
        if quote_ts is None:
            return frame

        open_value = self._to_float(quote_result.get("regularMarketOpen")) or price
        high_value = self._to_float(quote_result.get("regularMarketDayHigh")) or price
        low_value = self._to_float(quote_result.get("regularMarketDayLow")) or price
        volume_value = self._to_float(quote_result.get("regularMarketVolume")) or 0.0

        row_payload = {
            "open": float(open_value),
            "high": float(max(high_value, price)),
            "low": float(min(low_value, price)),
            "close": float(price),
            "volume": float(max(volume_value, 0.0)),
        }

        if frame.empty:
            merged = pd.DataFrame([row_payload], index=pd.DatetimeIndex([quote_ts]))
            return merged[self.PRICE_COLUMNS].sort_index()

        merged = frame.copy()
        merged.loc[quote_ts, self.PRICE_COLUMNS] = [
            row_payload["open"],
            row_payload["high"],
            row_payload["low"],
            row_payload["close"],
            row_payload["volume"],
        ]
        merged = merged[self.PRICE_COLUMNS].sort_index()
        return merged

    def fetch_live_quotes(self, tickers: list[str]) -> dict[str, dict[str, object]]:
        """Fetch latest quote snapshots for the requested symbols."""

        if not tickers:
            return {}

        snapshots: dict[str, dict[str, object]] = {}
        chunk_size = max(1, self.settings.brapi_max_tickers_per_request)
        for start in range(0, len(tickers), chunk_size):
            batch = tickers[start : start + chunk_size]
            results = self._request_quote_results(batch, period=None, interval=None)
            if not results:
                continue
            requested_symbol_map = {self._to_brapi_symbol(symbol): symbol for symbol in batch}
            for result in results:
                raw_symbol = str(result.get("symbol", "")).strip().upper()
                if not raw_symbol:
                    continue
                source_symbol = requested_symbol_map.get(raw_symbol, f"{raw_symbol}.SA")
                price_raw = result.get("regularMarketPrice")
                try:
                    price = float(price_raw)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                market_time = result.get("regularMarketTime")
                snapshots[source_symbol] = {
                    "price": price,
                    "timestamp": str(market_time or "").strip() or None,
                }
        return snapshots

    def _request_quote_results(
        self,
        tickers: list[str],
        period: str | None,
        interval: str | None,
    ) -> list[dict[str, object]]:
        request_symbols = [self._to_brapi_symbol(symbol) for symbol in tickers]
        query: dict[str, str] = {}
        if period:
            query["range"] = period
        if interval:
            query["interval"] = interval
        if self.settings.brapi_token:
            query["token"] = self.settings.brapi_token
        url = f"{self.base_url}/quote/{','.join(request_symbols)}?{urlencode(query)}"

        headers = {"User-Agent": "market-scanner/1.0"}
        if self.settings.brapi_token:
            headers["Authorization"] = f"Bearer {self.settings.brapi_token}"

        request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=self.settings.brapi_timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            logger.error(
                "BRAPI request failed",
                status=exc.code,
                url=url,
                body=body[:500],
            )
            raise RuntimeError(f"BRAPI HTTP error {exc.code}") from exc
        except URLError as exc:
            logger.error("BRAPI request failed", url=url, error=str(exc))
            raise RuntimeError("BRAPI request failed") from exc

        results = payload.get("results", [])
        if not isinstance(results, list) or not results:
            return []
        return [item for item in results if isinstance(item, dict)]

    def _to_brapi_symbol(self, symbol: str) -> str:
        return symbol.strip().upper().removesuffix(".SA")

    def _to_float(self, value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _quote_day_timestamp(self, market_time: object) -> datetime | None:
        if market_time is None:
            return None
        seconds: float | None = None
        try:
            seconds = float(market_time)
        except (TypeError, ValueError):
            seconds = None
        if seconds is None or seconds <= 0:
            return None
        instant = datetime.fromtimestamp(seconds, tz=timezone.utc)
        # Keep one daily candle key compatible with BRAPI historical data convention.
        return datetime(instant.year, instant.month, instant.day, 3, 0, 0)

    def _normalize_rows(self, rows: object) -> pd.DataFrame:
        if not isinstance(rows, list) or not rows:
            return pd.DataFrame(columns=self.PRICE_COLUMNS)

        frame = pd.DataFrame(rows)
        required_columns = {"date", "open", "high", "low", "close", "volume"}
        if not required_columns.issubset(frame.columns):
            missing = sorted(required_columns - set(frame.columns))
            logger.warning("BRAPI response missing price columns", missing=missing)
            return pd.DataFrame(columns=self.PRICE_COLUMNS)

        frame = frame[["date", "open", "high", "low", "close", "volume"]].copy()
        frame["timestamp"] = pd.to_datetime(
            frame["date"],
            unit="s",
            errors="coerce",
            utc=True,
        ).dt.tz_localize(None)
        frame = frame.dropna(subset=["timestamp", "close"]).set_index("timestamp")

        for column in ["open", "high", "low", "close", "volume"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["volume"] = frame["volume"].fillna(0.0)
        frame = frame.dropna(subset=["open", "high", "low", "close"])
        frame = frame.sort_index()
        return frame[self.PRICE_COLUMNS]
