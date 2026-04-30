#!/usr/bin/env python3
"""Run post-import data quality checks against a crypto-data DuckDB database."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from crypto_data.quality import (
    SUPPORTED_TABLES,
    QualityConfig,
    audit_database,
    findings_to_jsonable,
    format_findings,
    has_errors,
    has_warnings,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate continuity, gaps, invalid values, duplicates, and outliers in DuckDB."
    )
    parser.add_argument("db_path", help="Path to the DuckDB database file.")
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=SUPPORTED_TABLES,
        help="Subset of tables to audit. Defaults to all supported tables.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        help="Symbol filter. May be repeated or comma-separated.",
    )
    parser.add_argument(
        "--interval",
        action="append",
        help="Kline interval filter for spot/futures. May be repeated or comma-separated.",
    )
    parser.add_argument(
        "--json",
        dest="json_path",
        help="Optional path to write the detailed report as JSON.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return exit code 2 when warnings exist but no errors exist.",
    )
    parser.add_argument(
        "--price-return-sigma",
        type=float,
        default=QualityConfig.price_return_sigma,
        help=(
            "Sigma threshold for price log-return outlier warnings. "
            f"Default: {QualityConfig.price_return_sigma}."
        ),
    )
    parser.add_argument(
        "--volume-iqr-multiplier",
        type=float,
        default=QualityConfig.volume_iqr_multiplier,
        help=(
            "IQR multiplier for volume outlier warnings. "
            f"Default: {QualityConfig.volume_iqr_multiplier}."
        ),
    )
    parser.add_argument(
        "--min-outlier-observations",
        type=int,
        default=QualityConfig.min_outlier_observations,
        help=(
            "Minimum observations per symbol/interval before outlier checks run. "
            f"Default: {QualityConfig.min_outlier_observations}."
        ),
    )
    parser.add_argument(
        "--universe-frequency",
        choices=["daily", "weekly", "monthly"],
        help=(
            "Expected crypto_universe snapshot frequency. Enables missing snapshot "
            "checks when provided."
        ),
    )
    parser.add_argument(
        "--universe-start-date",
        help="Expected crypto_universe start date, YYYY-MM-DD. Defaults to table min date.",
    )
    parser.add_argument(
        "--universe-end-date",
        help="Expected crypto_universe end date, YYYY-MM-DD. Defaults to table max date.",
    )
    parser.add_argument(
        "--universe-top-n",
        type=int,
        help="Expected CoinMarketCap Top N request size for rank coverage checks.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path)

    config = QualityConfig(
        price_return_sigma=args.price_return_sigma,
        volume_iqr_multiplier=args.volume_iqr_multiplier,
        min_outlier_observations=args.min_outlier_observations,
        universe_frequency=args.universe_frequency,
        universe_start_date=args.universe_start_date,
        universe_end_date=args.universe_end_date,
        universe_top_n=args.universe_top_n,
    )

    findings = audit_database(
        db_path,
        tables=args.tables,
        symbols=args.symbol,
        intervals=args.interval,
        config=config,
    )

    print(format_findings(findings, db_path=db_path))

    if args.json_path:
        output_path = Path(args.json_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(findings_to_jsonable(findings), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    if has_errors(findings):
        return 1
    if args.fail_on_warning and has_warnings(findings):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
