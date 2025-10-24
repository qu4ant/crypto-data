#!/usr/bin/env python3
"""
Populate DuckDB Database with Binance OHLCV Data

Script for downloading and importing Binance market data to DuckDB.
Reads symbols from universe database or manual specification.

For universe rankings, use update_universe.py first.
"""

import sys
import argparse
import logging
from pathlib import Path
import yaml
import duckdb

# Add src to path
script_dir = Path(__file__).parent
src_dir = script_dir.parent / "src"
sys.path.insert(0, str(src_dir))

from crypto_data import CryptoDatabase, ingest_binance

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)


def _get_symbols_from_universe(universe_db_path: str, start_date: str, end_date: str, top_n: int) -> list:
    """
    Extract unique symbols from universe snapshots (UNION strategy).

    Returns all symbols that appeared in top N at ANY point during the period.

    Parameters
    ----------
    universe_db_path : str
        Path to universe database
    start_date : str
        Start date (YYYY-MM-DD)
    end_date : str
        End date (YYYY-MM-DD)
    top_n : int
        Rank threshold (e.g., 50 for top 50)

    Returns
    -------
    list
        List of symbols with USDT suffix (e.g., ['BTCUSDT', 'ETHUSDT', ...])
    """
    try:
        conn = duckdb.connect(universe_db_path)

        # Get unique symbols from universe within date range and rank threshold
        result = conn.execute("""
            SELECT DISTINCT symbol
            FROM coingecko_universe
            WHERE date >= ?
                AND date <= ?
                AND rank <= ?
            ORDER BY symbol
        """, [start_date, end_date, top_n]).fetchall()

        conn.close()

        # Convert to list and add USDT suffix
        symbols = [f"{row[0]}USDT" for row in result]

        logger.info(f"Extracted {len(symbols)} unique symbols from universe (top {top_n} across period)")
        logger.debug(f"Symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

        return symbols

    except Exception as e:
        logger.error(f"Failed to extract symbols from universe: {e}")
        logger.error(f"Make sure {universe_db_path} exists and contains universe data.")
        logger.error(f"Run 'python scripts/update_universe.py' first to populate universe.")
        return []


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Populate DuckDB database with Binance OHLCV data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Universe-based symbol selection (RECOMMENDED)
  python populate_binance.py \\
      --db crypto_5m.db \\
      --interval 5m \\
      --top-n 50 \\
      --data-types spot futures \\
      --start 2024-01-01 \\
      --end 2024-12-31 \\
      --universe-db crypto_universe.db

  # Manual symbol specification
  python populate_binance.py \\
      --db crypto_5m.db \\
      --interval 5m \\
      --symbols BTCUSDT ETHUSDT SOLUSDT \\
      --data-types spot futures \\
      --start 2024-01-01 \\
      --end 2024-12-31

  # Using configuration file
  python populate_binance.py --config config/binance_config.yaml
        """
    )

    parser.add_argument('--config', '-c', help='Configuration file (YAML)')
    parser.add_argument('--db', help='Database path (default: crypto_5m.db)', default='crypto_5m.db')

    # Binance options
    parser.add_argument('--symbols', nargs='+', help='Symbols to download (manual mode)')
    parser.add_argument('--data-types', nargs='+',
                        choices=['spot', 'futures'],
                        help='Data types to download')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    parser.add_argument('--interval', default='5m', help='Kline interval (default: 5m)')
    parser.add_argument('--no-skip', action='store_true',
                        help='Re-download existing data')

    # Universe options
    parser.add_argument('--top-n', type=int,
                        help='Number of top coins from universe (requires --universe-db)')
    parser.add_argument('--universe-db', default='crypto_universe.db',
                        help='Universe database path (default: crypto_universe.db)')

    args = parser.parse_args()

    # Load config if specified
    config = {}
    if args.config:
        config = load_config(args.config)

    # Get database path
    db_path = args.db or config.get('database', 'crypto_5m.db')

    logger.info("=" * 70)
    logger.info("BINANCE DATA - DATABASE POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info("")

    # Initialize database
    logger.info("Initializing database...")
    db = CryptoDatabase(db_path)
    logger.info("✓ Database initialized")
    logger.info("")

    # Get parameters
    manual_symbols = args.symbols or config.get('symbols', [])
    top_n = args.top_n or config.get('top_n')
    universe_db_path = args.universe_db or config.get('universe_db', 'crypto_universe.db')
    data_types = args.data_types or config.get('data_types', [])
    start_date = args.start or config.get('start_date')
    end_date = args.end or config.get('end_date')
    interval = args.interval or config.get('interval', '5m')

    # Validate symbol source
    if manual_symbols and top_n is not None:
        logger.error("✗ Cannot specify both --symbols and --top-n")
        logger.error("   Use --top-n to auto-select from universe, OR --symbols for manual list")
        sys.exit(1)

    # Determine symbols to use
    symbols_to_use = None
    symbol_source = None

    if top_n is not None:
        # Universe-based symbol selection
        symbol_source = "universe"
        logger.info("📋 UNIVERSE-BASED SYMBOL SELECTION")
        logger.info("-" * 70)
        logger.info(f"Universe database: {universe_db_path}")
        logger.info(f"Rank threshold: top {top_n}")
        logger.info("")

        # Extract symbols from universe
        if not start_date or not end_date:
            logger.error("✗ --start and --end dates required for universe-based selection")
            sys.exit(1)

        symbols_to_use = _get_symbols_from_universe(universe_db_path, start_date, end_date, top_n)

        if not symbols_to_use:
            logger.error("✗ No symbols found in universe. Cannot proceed.")
            logger.error(f"   Make sure {universe_db_path} contains data for {start_date} to {end_date}")
            logger.error("   Run 'python scripts/update_universe.py' first.")
            sys.exit(1)

        logger.info(f"✓ Will download Binance data for {len(symbols_to_use)} symbols")
        logger.info("")

    elif manual_symbols:
        # Manual symbol specification
        symbol_source = "manual"
        symbols_to_use = manual_symbols
        logger.info("📋 MANUAL SYMBOL SPECIFICATION")
        logger.info("-" * 70)
        logger.info(f"Using {len(manual_symbols)} manually specified symbols")
        logger.info("")
    else:
        logger.error("✗ Must specify either --symbols OR --top-n")
        sys.exit(1)

    # Ingest Binance data
    if symbols_to_use and data_types and start_date and end_date:
        logger.info("📊 BINANCE DATA INGESTION")
        logger.info("-" * 70)

        try:
            ingest_binance(
                db_path=db_path,
                symbols=symbols_to_use,
                data_types=data_types,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                skip_existing=not args.no_skip
            )
            logger.info("✓ Binance data ingestion complete")
        except Exception as e:
            logger.error(f"✗ Binance ingestion failed: {e}")
            sys.exit(1)

        logger.info("")
    else:
        logger.error("✗ Missing required parameters (symbols, data_types, start, end)")
        sys.exit(1)

    # Show database stats
    logger.info("=" * 70)
    logger.info("DATABASE STATISTICS")
    logger.info("=" * 70)

    stats = db.get_table_stats()

    for table, table_stats in stats.items():
        if table != '_metadata' and table != 'coingecko_universe':  # Skip metadata and universe tables
            logger.info(f"\n{table}:")
            for key, value in table_stats.items():
                logger.info(f"  {key}: {value}")

    logger.info("")
    logger.info("=" * 70)
    logger.info("✓ POPULATION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Database ready: {db_path}")
    logger.info("You can now query it with DuckDB:")
    logger.info("")
    logger.info("  import duckdb")
    logger.info(f"  conn = duckdb.connect('{db_path}')")
    logger.info("  df = conn.execute('SELECT * FROM binance_spot WHERE ...').df()")
    logger.info("")


if __name__ == "__main__":
    main()
