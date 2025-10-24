#!/usr/bin/env python3
"""
Fetch CoinMarketCap Universe Rankings to Parquet

Standalone script for fetching cryptocurrency universe rankings from CoinMarketCap
and saving to Parquet files. Completely decoupled from Binance and DuckDB.

This script:
- Fetches top N cryptocurrencies by market cap
- Filters based on tags (stablecoins, wrapped tokens, etc.)
- Saves to Parquet file with automatic deduplication
- Supports single snapshot or monthly range
- Skips existing snapshots automatically
- Handles rate limits and server errors gracefully

Examples:
    # Fetch single snapshot
    python fetch_universe.py --date 2024-01-01 --top-n 100

    # Fetch monthly range
    python fetch_universe.py --start 2022-01-01 --end 2024-12-31 --top-n 100

    # Custom output path
    python fetch_universe.py --date 2024-01-01 --top-n 50 --output data/universe.parquet

    # Custom config file
    python fetch_universe.py --date 2024-01-01 --top-n 100 --config my_config.yaml
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add src to path
script_dir = Path(__file__).parent
src_dir = script_dir.parent / "src"
sys.path.insert(0, str(src_dir))

from crypto_data.universe import (
    load_config,
    fetch_snapshot,
    save_to_parquet,
    snapshot_exists,
    get_universe_stats
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def generate_month_list(start_date: str, end_date: str) -> list:
    """Generate list of YYYY-MM-DD strings (1st of each month) between dates."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start.replace(day=1)
    end_month = end.replace(day=1)

    while current <= end_month:
        months.append(current.strftime("%Y-%m-%d"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch CoinMarketCap universe rankings to Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch single snapshot (historical date)
  python fetch_universe.py --date 2024-01-01 --top-n 100

  # Fetch monthly snapshots for a date range
  python fetch_universe.py \\
      --start 2022-01-01 \\
      --end 2024-12-31 \\
      --top-n 100

  # Custom output path
  python fetch_universe.py \\
      --date 2024-01-01 \\
      --top-n 50 \\
      --output data/universe.parquet

  # Custom config file (for tag filtering)
  python fetch_universe.py \\
      --date 2024-01-01 \\
      --top-n 100 \\
      --config my_config.yaml

  # Backfill historical data (FREE with CoinMarketCap!)
  python fetch_universe.py \\
      --start 2020-01-01 \\
      --end 2024-12-31 \\
      --top-n 100
        """
    )

    parser.add_argument('--output', default='crypto_universe.parquet',
                        help='Output Parquet file path (default: crypto_universe.parquet)')
    parser.add_argument('--date', help='Single snapshot date (YYYY-MM-DD)')
    parser.add_argument('--start', help='Start date for monthly snapshots (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date for monthly snapshots (YYYY-MM-DD)')
    parser.add_argument('--top-n', type=int, required=True,
                        help='Number of top coins to fetch (e.g., 50, 100, 250)')
    parser.add_argument('--config', help='Path to config YAML file (optional)')
    parser.add_argument('--skip-existing', action='store_true', default=True,
                        help='Skip snapshots that already exist (default: True)')

    args = parser.parse_args()

    # Validate arguments
    if args.date and (args.start or args.end):
        logger.error("Cannot specify both --date and --start/--end")
        sys.exit(1)

    if not args.date and not (args.start and args.end):
        logger.error("Must specify either --date OR both --start and --end")
        sys.exit(1)

    if args.top_n <= 0:
        logger.error("--top-n must be positive")
        sys.exit(1)

    # Determine snapshot dates
    if args.date:
        snapshot_dates = [args.date]
    else:
        snapshot_dates = generate_month_list(args.start, args.end)

    # Print header
    logger.info("=" * 70)
    logger.info("COINMARKETCAP UNIVERSE → PARQUET")
    logger.info("=" * 70)
    logger.info(f"Output: {args.output}")
    logger.info(f"Top N: {args.top_n}")
    logger.info(f"Snapshots: {len(snapshot_dates)}")
    if args.config:
        logger.info(f"Config: {args.config}")
    logger.info("")

    # Load configuration (excluded tags)
    excluded_tags = load_config(args.config)
    if excluded_tags:
        logger.info(f"Excluding {len(excluded_tags)} tag categories")
        logger.info(f"  Tags: {', '.join(excluded_tags[:5])}{'...' if len(excluded_tags) > 5 else ''}")
        logger.info("")

    # Estimate time
    if len(snapshot_dates) > 1:
        estimated_minutes = len(snapshot_dates) * 0.5
        logger.info(f"Estimated time: ~{estimated_minutes:.0f} minutes")
        logger.info("")

    # Statistics
    stats = {
        'fetched': 0,
        'skipped': 0,
        'failed': 0
    }

    # Fetch each snapshot
    for idx, date in enumerate(snapshot_dates):
        progress = f"[{idx + 1}/{len(snapshot_dates)}]"

        logger.info(f"{progress} Processing snapshot for {date}")

        # Skip if already exists
        if args.skip_existing and snapshot_exists(args.output, date):
            logger.info(f"{progress}   ↳ Skipped (already exists)")
            stats['skipped'] += 1
            continue

        try:
            # Fetch snapshot from CoinMarketCap
            df = fetch_snapshot(
                date=date,
                top_n=args.top_n,
                excluded_tags=excluded_tags
            )

            # Save to Parquet
            save_to_parquet(df, args.output)

            stats['fetched'] += 1
            logger.info(f"{progress}   ↳ Success ({len(df)} coins)")

        except Exception as e:
            logger.error(f"{progress}   ↳ Failed: {e}")
            stats['failed'] += 1
            # Continue to next snapshot instead of exiting

        logger.info("")

    # Get final statistics
    parquet_stats = get_universe_stats(args.output)

    # Summary
    logger.info("=" * 70)
    logger.info("Universe Fetch Summary:")
    logger.info(f"  Fetched: {stats['fetched']}")
    logger.info(f"  Skipped: {stats['skipped']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info("")
    logger.info("Parquet File Statistics:")
    logger.info(f"  Total records: {parquet_stats['total_records']:,}")
    logger.info(f"  Snapshots: {parquet_stats['num_snapshots']}")
    logger.info(f"  Date range: {parquet_stats['date_range'][0]} to {parquet_stats['date_range'][1]}")
    logger.info(f"  Unique symbols: {parquet_stats['unique_symbols']}")
    logger.info(f"  File size: {parquet_stats['file_size_mb']} MB")
    logger.info("=" * 70)

    if stats['failed'] > 0:
        logger.warning("Note: Some snapshots failed. Re-run to retry failed snapshots.")

    logger.info("")
    logger.info(f"✓ Universe data ready: {args.output}")
    logger.info("")
    logger.info("You can now use this universe with populate_binance.py or in your analysis:")
    logger.info("")
    logger.info("  # Python usage")
    logger.info("  import pandas as pd")
    logger.info(f"  df = pd.read_parquet('{args.output}')")
    logger.info("  top_50 = df[df['rank'] <= 50]")
    logger.info("")
    logger.info("  # DuckDB usage (in populate_binance.py)")
    logger.info("  SELECT * FROM read_parquet('crypto_universe.parquet')")
    logger.info("  WHERE rank <= 50 AND date = '2024-01-01'")
    logger.info("")


if __name__ == "__main__":
    main()
