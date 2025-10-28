from crypto_data import sync, setup_colored_logging
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
        top_n=10,  # Top 5 coins to test filtering
        interval='15m',
        data_types=['spot', 'futures']
    )

if __name__ == "__main__":
    main()