#!/usr/bin/env python3
"""
Update CoinMarketCap Universe Rankings

Dedicated script for fetching and storing cryptocurrency universe rankings
from CoinMarketCap API into a dedicated universe database.

This script manages universe data independently from Binance OHLCV data.
"""

import sys
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime

# Add src to path
script_dir = Path(__file__).parent
src_dir = script_dir.parent / "src"
sys.path.insert(0, str(src_dir))

from crypto_data import CryptoDatabase, ingest_universe

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def _generate_month_list(start_date: str, end_date: str) -> list:
    """Generate list of YYYY-MM strings between start and end dates."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start.replace(day=1)
    end_month = end.replace(day=1)

    while current <= end_month:
        months.append(current.strftime("%Y-%m"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def _universe_snapshot_exists(db_path: str, date: str) -> bool:
    """Check if universe snapshot exists for given date."""
    import duckdb
    try:
        conn = duckdb.connect(db_path)
        result = conn.execute(
            "SELECT COUNT(*) FROM crypto_universe WHERE date = ?",
            [date]
        ).fetchone()
        conn.close()
        return result[0] > 0
    except Exception:
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Update CoinMarketCap universe rankings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch single snapshot (historical date)
  python update_universe.py --date 2022-01-01 --top-n 100

  # Fetch monthly snapshots for a date range
  python update_universe.py \\
      --start 2022-01-01 \\
      --end 2024-12-31 \\
      --top-n 100

  # Custom database path
  python update_universe.py \\
      --db my_universe.db \\
      --date 2023-06-01 \\
      --top-n 50

  # Backfill historical data (FREE with CoinMarketCap!)
  python update_universe.py \\
      --start 2021-01-01 \\
      --end 2024-12-31 \\
      --top-n 100
        """
    )

    parser.add_argument('--db', default='crypto_universe.db',
                        help='Universe database path (default: crypto_universe.db)')
    parser.add_argument('--date', help='Single snapshot date (YYYY-MM-DD)')
    parser.add_argument('--start', help='Start date for monthly snapshots (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date for monthly snapshots (YYYY-MM-DD)')
    parser.add_argument('--top-n', type=int, required=True,
                        help='Number of top coins to fetch (e.g., 50, 100, 250)')

    args = parser.parse_args()

    # Validate arguments
    if args.date and (args.start or args.end):
        logger.error("Cannot specify both --date and --start/--end")
        sys.exit(1)

    if not args.date and not (args.start and args.end):
        logger.error("Must specify either --date OR both --start and --end")
        sys.exit(1)

    # Determine snapshot dates
    if args.date:
        snapshot_dates = [args.date]
    else:
        months = _generate_month_list(args.start, args.end)
        snapshot_dates = [f"{month}-01" for month in months]

    logger.info("=" * 70)
    logger.info("COINMARKETCAP UNIVERSE UPDATE")
    logger.info("=" * 70)
    logger.info(f"Database: {args.db}")
    logger.info(f"Top N: {args.top_n}")
    logger.info(f"Snapshots: {len(snapshot_dates)}")
    logger.info("")

    # Initialize database
    logger.info("Initializing universe database...")
    db = CryptoDatabase(args.db)
    db.close()  # Close connection before ingestion (ingest_universe opens its own connection)
    logger.info("✓ Database initialized")
    logger.info("")

    # Estimate time
    if len(snapshot_dates) > 1:
        estimated_minutes = len(snapshot_dates) * 0.5  # Simplified version is fast
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

        logger.info(f"{progress} Processing universe snapshot for {date}")

        # Skip if already exists
        if _universe_snapshot_exists(args.db, date):
            logger.info(f"{progress}   ↳ Skipped (already exists)")
            stats['skipped'] += 1
            continue

        try:
            ingest_universe(
                db_path=args.db,
                date=date,
                top_n=args.top_n,
                rate_limit_delay=0.0  # No delay - rely on 429 retry logic
            )
            stats['fetched'] += 1
            logger.info(f"{progress}   ↳ Success")

        except Exception as e:
            logger.error(f"{progress}   ↳ Failed: {e}")
            stats['failed'] += 1
            # Continue to next snapshot instead of exiting

    # Summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("Universe Update Summary:")
    logger.info(f"  Fetched: {stats['fetched']}")
    logger.info(f"  Skipped: {stats['skipped']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info("=" * 70)

    if stats['failed'] > 0:
        logger.warning("Note: Some snapshots failed. Re-run to retry failed snapshots.")

    logger.info("")
    logger.info(f"✓ Universe database ready: {args.db}")
    logger.info("")
    logger.info("You can now use this universe with populate_binance.py:")
    logger.info(f"  python scripts/populate_binance.py \\")
    logger.info(f"      --db crypto_5m.db \\")
    logger.info(f"      --interval 5m \\")
    logger.info(f"      --top-n {args.top_n} \\")
    logger.info(f"      --universe-db {args.db} \\")
    logger.info(f"      --data-types spot futures \\")
    logger.info(f"      --start 2024-01-01 \\")
    logger.info(f"      --end 2024-12-31")
    logger.info("")


if __name__ == "__main__":
    main()
