# Binance Historical Data Downloader

A Python script to download historical market data from Binance Data Vision, including Spot Klines, USD-M Futures Klines, Premium Index Klines, and Funding Rates.

## Features

- Download multiple data types: Spot, Futures, Premium Index, and Funding Rate data
- Support for multiple symbols and custom date ranges
- Automatic data availability checking before download
- Organized file storage by symbol and data type
- Skip existing files to avoid re-downloading
- Retry mechanism for failed downloads
- Progress logging and summary statistics
- Configuration file support

## Installation

1. Clone or download this repository
2. Install required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Using Configuration File

Create or modify `config.yaml` with your desired parameters:

```yaml
symbols:
  - BTCUSDT
  - ETHUSDT
  - BNBUSDT

interval: 5m
start_date: 2024-01-01
end_date: 2024-03-31
destination: ./binance_data_files
```

Then run:

```bash
python binance_data_downloader.py -c config.yaml
```

### Using Command Line Arguments

```bash
python binance_data_downloader.py -s BTCUSDT ETHUSDT -i 5m -start 2024-01-01 -end 2024-03-31 -d ./data
```

### Using Symbols File

Create a text file with one symbol per line, then:

```bash
python binance_data_downloader.py -f symbols.txt -i 5m -start 2024-01-01 -end 2024-03-31 -d ./data
```

## Command Line Options

- `-c, --config`: Path to configuration file (YAML)
- `-s, --symbols`: List of symbols to download (space-separated)
- `-f, --symbols-file`: File containing symbols (one per line)
- `-i, --interval`: Kline interval (default: 5m)
- `-start, --start-date`: Start date in YYYY-MM-DD format
- `-end, --end-date`: End date in YYYY-MM-DD format
- `-d, --destination`: Destination directory (default: ./binance_data)
- `--no-skip`: Re-download existing files

## Supported Intervals

- Minutes: 1m, 3m, 5m, 15m, 30m
- Hours: 1h, 2h, 4h, 6h, 8h, 12h
- Days: 1d, 3d
- Weeks: 1w
- Months: 1M

## Data Organization

Downloaded files are organized in the following structure:

```
destination_folder/
├── BTCUSDT/
│   ├── spot/
│   │   ├── BTCUSDT-5m-2024-01.zip
│   │   └── BTCUSDT-5m-2024-02.zip
│   ├── futures/
│   │   └── BTCUSDT-5m-2024-01.zip
│   ├── premium_index/
│   │   └── BTCUSDT-5m-2024-01.zip
│   └── funding_rate/
│       └── BTCUSDT-fundingRate-2024-01.zip
├── ETHUSDT/
│   └── ...
```

## Data Types

1. **Spot Klines**: Candlestick data for spot markets
2. **USD-M Futures Klines**: Candlestick data for USD-Margined perpetual futures
3. **Premium Index Klines**: Premium index and mark price data for futures
4. **Funding Rate**: 8-hour funding intervals for perpetual futures

## Notes

- The script automatically checks data availability before downloading
- Funding rate data is fixed at 8-hour intervals
- Files are kept in compressed .zip format as provided by Binance
- The script will skip months where data doesn't exist for a symbol
- Network errors are handled with automatic retry (up to 3 attempts)

## Examples

### Download recent data for major pairs

```bash
python binance_data_downloader.py -s BTCUSDT ETHUSDT BNBUSDT -i 1h -start 2024-01-01 -end 2024-03-31 -d ./market_data
```

### Download full year data using config

```yaml
# config_full_year.yaml
symbols:
  - BTCUSDT
  - ETHUSDT
interval: 1d
start_date: 2023-01-01
end_date: 2023-12-31
destination: ./binance_2023_data
```

```bash
python binance_data_downloader.py -c config_full_year.yaml
```

## Troubleshooting

- **404 Errors**: The symbol may not have been listed during the requested period
- **Connection Errors**: The script will automatically retry up to 3 times
- **Missing Data**: Some symbols may not have futures or funding data if they're spot-only

## Data Source

All data is downloaded from [Binance Data Vision](https://data.binance.vision/), Binance's official public data repository.