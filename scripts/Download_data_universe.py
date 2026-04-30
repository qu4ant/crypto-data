from crypto_data import (
    DEFAULT_UNIVERSE_EXCLUDE_TAGS,
    DataType,
    Interval,
    create_binance_database,
    setup_colored_logging,
)

# Universe filtering constants
# Exclude these CoinMarketCap tags (categories) from the universe
DEFAULT_EXCLUDE_TAGS = DEFAULT_UNIVERSE_EXCLUDE_TAGS

# Exclude these specific symbols (tickers) from the universe
DEFAULT_EXCLUDE_SYMBOLS = [
    "LUNA",  # Terra collapse (May 2022)
    "FTT",  # FTX Token collapse (Nov 2022)
]

# Setup logging (returns log file path)
log_file = setup_colored_logging()


def main():
    # Test universe filtering with TOP 10 coins
    # This will automatically:
    # 1. Fetch daily universe snapshots from CoinMarketCap
    # 2. Filter out excluded tags (stablecoins, wrapped tokens, etc.)
    # 3. Filter out excluded symbols (LUNA, FTT, UST)
    # 4. Extract symbols from universe using UNION dataset
    # 5. Download Binance spot + futures data for filtered symbols
    create_binance_database(
        db_path="crypto_data-new.db",
        start_date="2022-01-01",
        end_date="2026-04-01",
        top_n=50,  # Top X coins by market cap
        interval=Interval.HOUR_4,  # interval : Interval.MIN_1, MIN_5, MIN_15, MIN_30, HOUR_1, HOUR_4, DAY_1
        data_types=[DataType.SPOT, DataType.FUTURES, DataType.FUNDING_RATES],
        exclude_tags=DEFAULT_EXCLUDE_TAGS,
        exclude_symbols=DEFAULT_EXCLUDE_SYMBOLS,
        universe_frequency="daily",
        skip_existing_universe=True,
    )


if __name__ == "__main__":
    main()
