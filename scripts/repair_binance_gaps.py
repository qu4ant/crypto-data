#!/usr/bin/env python
"""CLI wrapper for Binance REST gap repair."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from crypto_data import repair_binance_gaps


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair Binance gaps via public REST API")
    parser.add_argument("db_path", help="Path to DuckDB database")
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=["spot", "futures", "funding_rates"],
        default=None,
        help="Tables to repair (default: spot futures funding_rates)",
    )
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=None,
        help="Symbol filter; may be repeated",
    )
    parser.add_argument(
        "--interval",
        dest="intervals",
        action="append",
        default=None,
        help="Kline interval filter; may be repeated",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=5,
        help="Maximum concurrent REST requests (default: 5)",
    )
    parser.add_argument("--json", dest="json_path", help="Optional report JSON output path")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable INFO logging")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    report = repair_binance_gaps(
        db_path=args.db_path,
        tables=args.tables,
        symbols=args.symbols,
        intervals=args.intervals,
        max_concurrent=args.max_concurrent,
    )
    print(report.summary())

    if args.json_path:
        json_path = Path(args.json_path)
        json_path.write_text(json.dumps(report.to_jsonable(), indent=2), encoding="utf-8")

    return 1 if report.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
