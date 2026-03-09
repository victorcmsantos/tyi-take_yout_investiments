"""Ticker universe loading for B3 listed shares."""

from __future__ import annotations

import base64
import io
import json
import re
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

from loguru import logger

from config.settings import AppSettings


@dataclass(slots=True)
class B3Issuer:
    """Canonical B3 issuer base code."""

    issuing_company: str
    trading_name: str
    type_code: str
    status: str


@dataclass(slots=True)
class B3ListedTicker:
    """Official listed share recovered from B3 sources."""

    ticker: str
    yahoo_symbol: str
    issuer_code: str
    issuer_name: str
    trading_name: str
    specification: str
    isin: str
    source: str = "b3_official_cotahist"
    is_active: bool = True
    yahoo_supported: bool = True
    discovered_at: datetime | None = None
    last_verified_at: datetime | None = None


class B3TickerProvider:
    """Discover the official B3 stock universe from COTAHIST."""

    CACHE_SCHEMA = "official_cotahist_v1"
    LISTED_COMPANIES_URL = (
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/"
    )
    COTAHIST_BASE_URL = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/"
    COTAHIST_REFERER = (
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
        "market-data/historico/mercado-a-vista/cotacoes-historicas/"
    )
    STOCK_SPEC_PREFIXES = ("ON", "PN", "UNT")
    STOCK_SPEC_EXCLUSIONS = ("REC", "DIR", "SUBS", "DIREITO")

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings

    def load_tickers(self) -> list[str]:
        """Load the B3 ticker universe using official B3 sources and cache."""

        return [record.yahoo_symbol for record in self.load_catalog()]

    def load_catalog(self) -> list[B3ListedTicker]:
        """Load the complete official ticker catalog with metadata."""

        if not self.settings.auto_discover_b3_tickers:
            now = datetime.utcnow()
            return [
                B3ListedTicker(
                    ticker=symbol.removesuffix(".SA"),
                    yahoo_symbol=symbol,
                    issuer_code=symbol[:4],
                    issuer_name=symbol[:4],
                    trading_name=symbol[:4],
                    specification="MANUAL",
                    isin="",
                    source="manual",
                    discovered_at=now,
                    last_verified_at=now,
                )
                for symbol in sorted(set(self.settings.manual_tickers))
            ]

        cached = self._load_cache()
        if cached:
            manual_symbols = set(self.settings.manual_tickers)
            cached_symbols = {record.yahoo_symbol for record in cached}
            now = datetime.utcnow()
            cached.extend(
                [
                    B3ListedTicker(
                        ticker=symbol.removesuffix(".SA"),
                        yahoo_symbol=symbol,
                        issuer_code=symbol[:4],
                        issuer_name=symbol[:4],
                        trading_name=symbol[:4],
                        specification="MANUAL",
                        isin="",
                        source="manual",
                        discovered_at=now,
                        last_verified_at=now,
                    )
                    for symbol in sorted(manual_symbols - cached_symbols)
                ]
            )
            return sorted(cached, key=lambda record: record.yahoo_symbol)

        discovered = self._discover_tickers()
        if discovered:
            self._save_cache(discovered)
        return sorted(discovered, key=lambda record: record.yahoo_symbol)

    def _discover_tickers(self) -> list[B3ListedTicker]:
        issuer_map = {
            issuer.issuing_company: issuer for issuer in self._fetch_active_equity_issuers()
        }
        cotahist_symbols = self._fetch_official_cotahist_symbols()
        now = datetime.utcnow()

        if issuer_map:
            cotahist_symbols = [
                symbol for symbol in cotahist_symbols if symbol.issuer_code in issuer_map
            ]

        discovered = [
            B3ListedTicker(
                ticker=symbol.ticker,
                yahoo_symbol=f"{symbol.ticker}.SA",
                issuer_code=symbol.issuer_code,
                issuer_name=symbol.issuer_name,
                trading_name=issuer_map.get(symbol.issuer_code, B3Issuer("", "", "", "")).trading_name
                or symbol.issuer_name,
                specification=symbol.specification,
                isin=symbol.isin,
                discovered_at=now,
                last_verified_at=now,
            )
            for symbol in cotahist_symbols
        ]
        logger.info(
            "Loaded official B3 stock universe",
            tickers=len(discovered),
            active_issuers=len(issuer_map),
        )
        return sorted(discovered, key=lambda record: record.yahoo_symbol)

    def _fetch_active_equity_issuers(self) -> list[B3Issuer]:
        issuers: list[B3Issuer] = []
        page_number = 1
        while True:
            payload = {
                "language": "pt-br",
                "pageNumber": page_number,
                "pageSize": self.settings.b3_page_size,
                "company": "",
            }
            encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
            request = Request(
                f"{self.LISTED_COMPANIES_URL}GetInitialCompanies/{encoded}",
                headers={"User-Agent": "market-scanner/1.0"},
            )
            with urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode("utf-8"))

            results = data.get("results", [])
            if not results:
                break

            for row in results:
                issuer = row.get("issuingCompany", "").upper().strip()
                if (
                    row.get("type") == "1"
                    and row.get("status") == "A"
                    and re.fullmatch(r"[A-Z]{4}", issuer)
                ):
                    issuers.append(
                        B3Issuer(
                            issuing_company=issuer,
                            trading_name=row.get("tradingName", ""),
                            type_code=row.get("type", ""),
                            status=row.get("status", ""),
                        )
                    )

            page = data.get("page", {})
            total_pages = int(page.get("totalPages", page_number))
            if page_number >= total_pages:
                break
            page_number += 1

        deduped = list({issuer.issuing_company: issuer for issuer in issuers}.values())
        logger.info("Fetched active B3 equity issuers", issuers=len(deduped))
        return deduped

    def _fetch_official_cotahist_symbols(self) -> list[B3ListedTicker]:
        symbols: dict[str, B3ListedTicker] = {}
        current_year = datetime.utcnow().year
        for year in (current_year, current_year - 1):
            try:
                for record in self._fetch_cotahist_symbols_for_year(year):
                    symbols.setdefault(record.ticker, record)
            except Exception as exc:
                logger.warning("Failed to load COTAHIST year", year=year, error=str(exc))

        if not symbols:
            raise RuntimeError("Unable to load stock symbols from official B3 COTAHIST files")
        return sorted(symbols.values(), key=lambda record: record.ticker)

    def _fetch_cotahist_symbols_for_year(self, year: int) -> list[B3ListedTicker]:
        request = Request(
            f"{self.COTAHIST_BASE_URL}COTAHIST_A{year}.ZIP",
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": self.COTAHIST_REFERER,
                "Accept": "application/zip,application/octet-stream,*/*",
            },
        )
        with urlopen(request, timeout=90) as response:
            payload = response.read()

        archive = zipfile.ZipFile(io.BytesIO(payload))
        filename = archive.namelist()[0]
        lines = archive.read(filename).decode("latin-1").splitlines()

        symbols = {
            symbol.ticker: symbol
            for symbol in (
                self._extract_stock_symbol(line) for line in lines if line.startswith("01")
            )
            if symbol is not None
        }
        logger.info("Parsed official COTAHIST stock symbols", year=year, tickers=len(symbols))
        return list(symbols.values())

    def _extract_stock_symbol(self, line: str) -> B3ListedTicker | None:
        bdi_code = line[10:12]
        market_type = line[24:27]
        ticker = line[12:24].strip().upper()
        issuer_code = ticker[:4]
        issuer_name = line[27:39].strip().upper()
        spec = line[39:49].strip().upper()
        isin = line[230:242].strip().upper()

        if bdi_code != "02":
            return None
        if market_type != "010":
            return None
        if not any(spec.startswith(prefix) for prefix in self.STOCK_SPEC_PREFIXES):
            return None
        if any(exclusion in spec for exclusion in self.STOCK_SPEC_EXCLUSIONS):
            return None
        if not re.fullmatch(r"[A-Z]{4}\d{1,2}[A-Z]?", ticker):
            return None
        return B3ListedTicker(
            ticker=ticker,
            yahoo_symbol=f"{ticker}.SA",
            issuer_code=issuer_code,
            issuer_name=issuer_name,
            trading_name=issuer_name,
            specification=spec,
            isin=isin,
        )

    def _load_cache(self) -> list[B3ListedTicker]:
        path = self.settings.ticker_cache_file
        if not path.exists():
            return []
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
            if cached.get("schema") != self.CACHE_SCHEMA:
                return []
            generated_at = datetime.fromisoformat(cached["generated_at"])
            if datetime.utcnow() - generated_at > timedelta(hours=self.settings.ticker_cache_ttl_hours):
                return []
            records = cached.get("records", [])
            if not records and cached.get("symbols"):
                records = [
                    {
                        "ticker": symbol.removesuffix(".SA"),
                        "yahoo_symbol": symbol,
                        "issuer_code": symbol[:4],
                        "issuer_name": symbol[:4],
                        "trading_name": symbol[:4],
                        "specification": "",
                        "isin": "",
                        "source": "legacy_cache",
                        "is_active": True,
                        "yahoo_supported": True,
                        "discovered_at": cached["generated_at"],
                        "last_verified_at": cached["generated_at"],
                    }
                    for symbol in cached["symbols"]
                ]
            if records:
                logger.info("Loaded cached B3 ticker universe", tickers=len(records))
            return [self._record_from_cache(record) for record in records]
        except Exception as exc:
            logger.warning("Failed to load ticker cache", error=str(exc), path=str(path))
            return []

    def _save_cache(self, symbols: list[B3ListedTicker]) -> None:
        payload = {
            "schema": self.CACHE_SCHEMA,
            "generated_at": datetime.utcnow().isoformat(),
            "symbols": [record.yahoo_symbol for record in symbols],
            "records": [self._record_to_cache(record) for record in symbols],
        }
        path: Path = self.settings.ticker_cache_file
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _record_to_cache(self, record: B3ListedTicker) -> dict[str, object]:
        payload = asdict(record)
        for key in ("discovered_at", "last_verified_at"):
            if payload[key] is not None:
                payload[key] = payload[key].isoformat()
        return payload

    def _record_from_cache(self, payload: dict[str, object]) -> B3ListedTicker:
        discovered_at = payload.get("discovered_at")
        last_verified_at = payload.get("last_verified_at")
        return B3ListedTicker(
            ticker=str(payload["ticker"]),
            yahoo_symbol=str(payload["yahoo_symbol"]),
            issuer_code=str(payload.get("issuer_code", "")),
            issuer_name=str(payload.get("issuer_name", "")),
            trading_name=str(payload.get("trading_name", "")),
            specification=str(payload.get("specification", "")),
            isin=str(payload.get("isin", "")),
            source=str(payload.get("source", "b3_official_cotahist")),
            is_active=bool(payload.get("is_active", True)),
            yahoo_supported=bool(payload.get("yahoo_supported", True)),
            discovered_at=None if not discovered_at else datetime.fromisoformat(str(discovered_at)),
            last_verified_at=None
            if not last_verified_at
            else datetime.fromisoformat(str(last_verified_at)),
        )
