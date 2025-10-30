from crypto_data import sync, setup_colored_logging

# Universe filtering constants
# Exclude these CoinMarketCap tags (categories) from the universe
DEFAULT_EXCLUDE_TAGS = [
    'stablecoin',           # USD-pegged tokens (USDT, USDC, etc.)
    'wrapped-tokens',       # Wrapped assets (WBTC, WETH, etc.)
    'real-world-assets',    # Tokenized real-world assets
    'privacy',              # Privacy coins (Monero, Zcash, etc.)
    'tokenized-gold'        # Gold-backed tokens
]

# Exclude these specific symbols (tickers) from the universe
DEFAULT_EXCLUDE_SYMBOLS = [
    'LUNA',  # Terra collapse (May 2022)
    'FTT',   # FTX Token collapse (Nov 2022)
]

setup_colored_logging()

def main():
    # Test universe filtering with TOP 10 coins
    # This will automatically:
    # 1. Fetch monthly universe snapshots from CoinMarketCap
    # 2. Filter out excluded tags (stablecoins, wrapped tokens, etc.)
    # 3. Filter out excluded symbols (LUNA, FTT, UST)
    # 4. Extract symbols from universe using UNION strategy
    # 5. Download Binance spot + futures data for filtered symbols
    sync(
        db_path='crypto_data.db',
        start_date='2022-01-01',
        end_date='2025-09-01',
        top_n=25,  # Top X coins by market cap
        interval='4h',  # interval : 1m, 5m, 15m, 30m, 1h, 4h, 1d
        data_types=['spot', 'futures'],
        exclude_tags=DEFAULT_EXCLUDE_TAGS,
        exclude_symbols=DEFAULT_EXCLUDE_SYMBOLS
    )

if __name__ == "__main__":
    main()