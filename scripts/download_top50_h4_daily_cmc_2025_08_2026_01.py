#!/usr/bin/env python3
"""
Build a DuckDB database using daily CMC Top 50 snapshots.

Default dataset:
- Period: 2025-08-01 to 2026-01-01 close
- Universe: CoinMarketCap Top 50, daily snapshots
- Binance data: spot + futures OHLCV
- Interval: 4h
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
from crypto_data.quality import audit_database, format_findings, has_errors

DEFAULT_DB_PATH = "crypto_top50_h4_daily_cmc_2025_08_2026_01.db"
START_DATE = "2025-08-01"
# The pipeline downloads whole monthly Binance periods. For H4 klines, using
# 2025-12-31 includes the final 2026-01-01 00:00 close without downloading the
# full January 2026 archive.
END_DATE = "2025-12-31"
TOP_N = 50
DAILY_QUOTA = 1_000_000

DEFAULT_EXCLUDE_SYMBOLS = [
    "LUNA",
    "FTT",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download CMC Top 50 daily-universe Binance spot/futures H4 data "
            "from 2025-08-01 to the 2026-01-01 close."
        )
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Output DuckDB path. Default: {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--refresh-universe",
        action="store_true",
        help="Re-download universe dates even if they already exist in the database.",
    )
    parser.add_argument(
        "--run-quality-audit",
        action="store_true",
        help="Run the local quality audit for spot/futures after download completes.",
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
        data_types=[DataType.SPOT, DataType.FUTURES],
        exclude_tags=DEFAULT_UNIVERSE_EXCLUDE_TAGS,
        exclude_symbols=DEFAULT_EXCLUDE_SYMBOLS,
        universe_frequency="daily",
        skip_existing_universe=not args.refresh_universe,
        daily_quota=DAILY_QUOTA,
    )

    if args.run_quality_audit:
        findings = audit_database(db_path, tables=["spot", "futures"])
        print()
        print(format_findings(findings, db_path=db_path))
        return 1 if has_errors(findings) else 0

    print()
    print("Download complete.")
    print("Run quality audit with:")
    print(f"  uv run python scripts/validate_data_quality.py {db_path} --tables spot futures")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
