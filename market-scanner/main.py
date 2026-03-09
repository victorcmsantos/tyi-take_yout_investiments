"""Runtime entrypoint for API and daemon modes."""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace

import uvicorn
from loguru import logger

from api.api_server import create_app
from config.settings import AppSettings, get_settings
from database.db import create_session_factory
from scheduler.daemon import MarketScannerDaemon


def configure_logging(settings: AppSettings) -> None:
    """Initialize structured application logging."""

    logger.remove()
    logger.add(
        sys.stdout,
        level=settings.log_level,
        serialize=False,
        backtrace=False,
        diagnose=False,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function}:{line} | {message}",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quantitative B3 market scanner")
    parser.add_argument("--mode", choices=["api", "daemon", "all"], default="all")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings)

    if args.mode == "daemon":
        session_factory = create_session_factory(settings)
        daemon = MarketScannerDaemon(settings, session_factory)
        daemon.run_forever()
        return

    if args.mode == "api":
        settings = replace(settings, start_scheduler_with_api=False)
    elif args.mode == "all":
        settings = replace(settings, start_scheduler_with_api=True)

    app = create_app(settings)
    uvicorn.run(
        app,
        host=args.host or settings.api_host,
        port=args.port or settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
