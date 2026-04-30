#!/usr/bin/env python3
"""
Build a DuckDB database for CMC Top 60 Binance H4 data.

Default dataset:
- Period: 2022-01-01 to 2026-04-01 close
- Universe: CoinMarketCap Top 60, daily snapshots
- Binance data: spot + futures OHLCV + futures funding rates
- Interval: 4h for spot/futures OHLCV

After download, run:
    uv run python scripts/validate_data_quality.py \
      crypto_top60_h4_2022_01_2026_04.db \
      --tables spot futures funding_rates crypto_universe \
      --universe-frequency daily \
      --universe-start-date 2022-01-01 \
      --universe-end-date 2026-03-31 \
      --universe-top-n 60
"""

from __future__ import annotations

import argparse
from pathlib import Path

from crypto_data import (
    DEFAULT_UNIVERSE_EXCLUDE_TAGS,
    DataType,
    Interval,
    create_binance_database,
    setup_colored_logging,
)
from crypto_data.quality import QualityConfig, audit_database, format_findings, has_errors

DEFAULT_DB_PATH = "crypto_top60_h4_2022_01_2026_04.db"
START_DATE = "2022-01-01"
# The pipeline stores kline timestamps as close times. For H4 klines, using
# 2026-03-31 includes the final 2026-04-01 00:00 close without downloading
# April 2026 archives. Funding rates are monthly, so this covers the last
# complete funding-rate archive before 2026-04-01.
END_DATE = "2026-03-31"
TOP_N = 60
DAILY_QUOTA = 1_000_000

DEFAULT_EXCLUDE_SYMBOLS = [
    "LUNA",
    "FTT",
]

QUALITY_TABLES = ["spot", "futures", "funding_rates"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download CMC Top 60 Binance spot/futures H4 data and funding rates "
            "from 2022-01-01 to the 2026-04-01 close."
        )
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Output DuckDB path. Default: {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--universe-frequency",
        choices=["monthly", "weekly", "daily"],
        default="daily",
        help=("CMC snapshot frequency used to build the symbol universe. Default: daily."),
    )
    parser.add_argument(
        "--daily-quota",
        type=int,
        default=DAILY_QUOTA,
        help=(
            f"CoinMarketCap daily request quota for the internal limiter. Default: {DAILY_QUOTA}."
        ),
    )
    parser.add_argument(
        "--refresh-universe",
        action="store_true",
        help="Re-download universe dates even if they already exist in the database.",
    )
    parser.add_argument(
        "--run-quality-audit",
        action="store_true",
        help="Run the local quality audit for spot/futures/funding rates after download completes.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)
    setup_colored_logging()

    create_binance_database(
        db_path=str(db_path),
        start_date=START_DATE,
        end_date=END_DATE,
        top_n=TOP_N,
        interval=Interval.HOUR_4,
        data_types=[
            DataType.SPOT,
            DataType.FUTURES,
            DataType.FUNDING_RATES,
        ],
        exclude_tags=DEFAULT_UNIVERSE_EXCLUDE_TAGS,
        exclude_symbols=DEFAULT_EXCLUDE_SYMBOLS,
        universe_frequency=args.universe_frequency,
        skip_existing_universe=not args.refresh_universe,
        daily_quota=args.daily_quota,
    )

    if args.run_quality_audit:
        findings = audit_database(
            db_path,
            tables=[*QUALITY_TABLES, "crypto_universe"],
            config=QualityConfig(
                universe_frequency="daily",
                universe_start_date=START_DATE,
                universe_end_date=END_DATE,
                universe_top_n=TOP_N,
            ),
        )
        print()
        print(format_findings(findings, db_path=db_path))
        return 1 if has_errors(findings) else 0

    print()
    print("Download complete.")
    print("Run quality audit with:")
    print(
        f"  uv run python scripts/validate_data_quality.py {db_path} "
        "--tables spot futures funding_rates crypto_universe "
        f"--universe-frequency daily --universe-start-date {START_DATE} "
        f"--universe-end-date {END_DATE} --universe-top-n {TOP_N}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
