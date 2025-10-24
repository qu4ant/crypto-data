#!/usr/bin/env python3
"""
Inspect DuckDB Database

View statistics and contents of the crypto data database.
"""

import sys
import argparse
from pathlib import Path

# Add src to path
script_dir = Path(__file__).parent
src_dir = script_dir.parent / "src"
sys.path.insert(0, str(src_dir))

from crypto_data import CryptoDatabase


def format_size(bytes_size):
    """Format bytes to human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def print_table_stats(table_name, stats):
    """Print statistics for a table."""
    print(f"\n{'='*70}")
    print(f"TABLE: {table_name}")
    print(f"{'='*70}")

    if stats['records'] == 0:
        print("  (empty)")
        return

    print(f"  Records:       {stats['records']:,}")

    if 'symbols' in stats:
        print(f"  Symbols:       {stats['symbols']}")

    if 'snapshots' in stats:
        print(f"  Snapshots:     {stats['snapshots']}")

    if 'min_date' in stats and stats['min_date']:
        print(f"  Date range:    {stats['min_date']} to {stats['max_date']}")


def show_sample_data(db, table_name, limit=5):
    """Show sample data from a table."""
    print(f"\n  Sample data (first {limit} rows):")
    print(f"  {'-'*66}")

    result = db.execute(f"SELECT * FROM {table_name} LIMIT {limit}").df()

    if len(result) > 0:
        # Show in a compact format
        print(result.to_string(index=False, max_rows=limit))
    else:
        print("  (no data)")


def show_symbol_list(db, table_name):
    """Show list of symbols in a table."""
    result = db.execute(f"""
        SELECT DISTINCT symbol
        FROM {table_name}
        ORDER BY symbol
    """).fetchall()

    if result:
        symbols = [row[0] for row in result]
        print(f"\n  Symbols ({len(symbols)}): {', '.join(symbols)}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Inspect crypto data database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic inspection
  python inspect_database.py crypto_data.db

  # Show sample data
  python inspect_database.py crypto_data.db --samples

  # Show symbol lists
  python inspect_database.py crypto_data.db --symbols

  # Show everything
  python inspect_database.py crypto_data.db --samples --symbols
        """
    )

    parser.add_argument('database', help='Path to database file')
    parser.add_argument('--samples', action='store_true', help='Show sample data')
    parser.add_argument('--symbols', action='store_true', help='Show symbol lists')
    parser.add_argument('--limit', type=int, default=5, help='Number of sample rows (default: 5)')

    args = parser.parse_args()

    # Check if database exists
    db_path = Path(args.database)
    if not db_path.exists():
        print(f"Error: Database not found: {db_path}")
        sys.exit(1)

    # Get file size
    file_size = db_path.stat().st_size

    print("\n" + "="*70)
    print("CRYPTO DATA DATABASE INSPECTION")
    print("="*70)
    print(f"Database: {db_path}")
    print(f"Size:     {format_size(file_size)}")

    # Connect to database
    db = CryptoDatabase(str(db_path))

    # Get statistics
    stats = db.get_table_stats()

    # Print stats for each table
    for table_name in ['binance_spot', 'binance_futures', 'coingecko_universe']:
        if table_name in stats:
            print_table_stats(table_name, stats[table_name])

            # Show symbol list if requested
            if args.symbols and stats[table_name]['records'] > 0:
                show_symbol_list(db, table_name)

            # Show sample data if requested
            if args.samples and stats[table_name]['records'] > 0:
                show_sample_data(db, table_name, args.limit)

    # Summary
    total_records = sum(s['records'] for s in stats.values())

    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Total records: {total_records:,}")
    print(f"Database size: {format_size(file_size)}")

    if total_records > 0:
        print(f"Bytes/record:  {format_size(file_size / total_records)}")

    print("\nTo query this database, use DuckDB:")
    print(f"  import duckdb")
    print(f"  conn = duckdb.connect('{db_path}')")
    print(f"  df = conn.execute('SELECT * FROM binance_spot WHERE ...').df()")
    print()

    db.close()


if __name__ == "__main__":
    main()
