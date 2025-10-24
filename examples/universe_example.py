#!/usr/bin/env python3
"""
Example: Using CoinMarketCap Universe Module

Demonstrates how to use the crypto_data.universe module programmatically
instead of using the CLI script.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.universe import (
    load_config,
    fetch_snapshot,
    save_to_parquet,
    load_universe,
    get_universe_stats,
    snapshot_exists
)

# ============================================================================
# Example 1: Fetch and save single snapshot
# ============================================================================

print("=" * 70)
print("Example 1: Fetch single snapshot")
print("=" * 70)

# Load config (excluded tags)
excluded_tags = load_config()
print(f"Loaded {len(excluded_tags)} excluded tags")

# Fetch snapshot for 2024-01-01
df = fetch_snapshot(
    date='2024-01-01',
    top_n=100,
    excluded_tags=excluded_tags
)

print(f"Fetched {len(df)} coins")
print(f"\nFirst 5 coins:")
print(df.head())

# Save to Parquet
save_to_parquet(df, 'universe_example.parquet')
print("\n✓ Saved to universe_example.parquet")

# ============================================================================
# Example 2: Append another snapshot
# ============================================================================

print("\n" + "=" * 70)
print("Example 2: Append another snapshot")
print("=" * 70)

# Check if snapshot exists
exists = snapshot_exists('universe_example.parquet', '2024-02-01')
print(f"Snapshot for 2024-02-01 exists: {exists}")

# Fetch and append
df2 = fetch_snapshot(
    date='2024-02-01',
    top_n=100,
    excluded_tags=excluded_tags
)

save_to_parquet(df2, 'universe_example.parquet')
print("✓ Appended 2024-02-01 snapshot")

# ============================================================================
# Example 3: Load and analyze
# ============================================================================

print("\n" + "=" * 70)
print("Example 3: Load and analyze universe")
print("=" * 70)

# Load full universe
df_universe = load_universe('universe_example.parquet')

print(f"Total records: {len(df_universe)}")
print(f"Unique dates: {df_universe['date'].nunique()}")
print(f"Unique symbols: {df_universe['symbol'].nunique()}")

# Get top 10 coins by average rank
top_10 = df_universe.groupby('symbol').agg({
    'rank': 'mean',
    'market_cap': 'mean'
}).sort_values('rank').head(10)

print("\nTop 10 coins by average rank:")
print(top_10)

# Get symbols that were in top 50 at any point
top_50_symbols = df_universe[df_universe['rank'] <= 50]['symbol'].unique()
print(f"\nSymbols in top 50 at any point: {len(top_50_symbols)}")
print(f"Examples: {sorted(top_50_symbols)[:10]}")

# ============================================================================
# Example 4: Get statistics
# ============================================================================

print("\n" + "=" * 70)
print("Example 4: File statistics")
print("=" * 70)

stats = get_universe_stats('universe_example.parquet')
print(f"Total records: {stats['total_records']:,}")
print(f"Snapshots: {stats['num_snapshots']}")
print(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
print(f"Unique symbols: {stats['unique_symbols']}")
print(f"File size: {stats['file_size_mb']} MB")

# ============================================================================
# Example 5: Filter by categories
# ============================================================================

print("\n" + "=" * 70)
print("Example 5: Filter by categories")
print("=" * 70)

# Get DeFi coins
defi_coins = df_universe[
    df_universe['categories'].str.contains('defi', case=False, na=False)
]['symbol'].unique()

print(f"DeFi coins: {len(defi_coins)}")
print(f"Examples: {sorted(defi_coins)[:10]}")

# Get Layer-1 blockchains
layer1_coins = df_universe[
    df_universe['categories'].str.contains('layer-1|platform', case=False, na=False)
]['symbol'].unique()

print(f"\nLayer-1 blockchains: {len(layer1_coins)}")
print(f"Examples: {sorted(layer1_coins)[:10]}")

print("\n" + "=" * 70)
print("✓ Examples complete!")
print("=" * 70)
print("\nTo clean up:")
print("  rm universe_example.parquet")
