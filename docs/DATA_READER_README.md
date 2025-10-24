# Binance Data Reader

A Python library to read and convert downloaded Binance historical data into pandas DataFrames with proper datetime indexing.

## Features

✅ **Separate function per data type** - Clean, modular API
✅ **Automatic file handling** - Reads both CSV and ZIP files
✅ **Datetime indexing** - Uses `close_time` as default index
✅ **Proper data types** - Numeric conversion with error handling
✅ **Date range support** - Load single months or date ranges
✅ **Quality checks** - Built-in data validation
✅ **Convenience functions** - Quick access without class instantiation

## Quick Start

```python
from binance_data_reader import BinanceDataReader

# Initialize reader
reader = BinanceDataReader("./binance_data")

# Load different data types
spot_df = reader.read_spot_klines('BTCUSDT', '2024-01', index='close_time')
futures_df = reader.read_futures_klines('BTCUSDT', '2024-01', index='close_time')
funding_df = reader.read_funding_rate('BTCUSDT', '2024-01', index='calc_time')
premium_df = reader.read_premium_index('BTCUSDT', '2024-01', index='close_time')
```

## Data Types Supported

### 1. **Spot Klines** (`read_spot_klines`)
- **Columns**: open_time, open, high, low, close, volume, close_time, quote_volume, trades_count, taker_buy_base_volume, taker_buy_quote_volume, ignore
- **Index**: close_time (default) or open_time
- **Format**: 12 columns, no header, timestamps in milliseconds

### 2. **Futures Klines** (`read_futures_klines`)
- **Columns**: Same as spot klines
- **Index**: close_time (default) or open_time
- **Format**: 12 columns, no header, timestamps in milliseconds

### 3. **Premium Index** (`read_premium_index`)
- **Columns**: Same as klines format
- **Index**: close_time (default) or open_time
- **Format**: 12 columns, no header, timestamps in milliseconds

### 4. **Funding Rate** (`read_funding_rate`)
- **Columns**: calc_time, funding_interval_hours, last_funding_rate
- **Index**: calc_time (default)
- **Format**: 3 columns with header, 8-hour intervals

## Usage Examples

### Basic Usage
```python
# Load single month
df = reader.read_spot_klines('BTCUSDT', '2024-01')

# Load date range
df = reader.read_spot_klines('BTCUSDT', '2024-01-01:2024-03-31')
df = reader.read_spot_klines('BTCUSDT', '2024-01:2024-03')  # Monthly range

# Use different index
df = reader.read_spot_klines('BTCUSDT', '2024-01', index='open_time')
df = reader.read_spot_klines('BTCUSDT', '2024-01', index=None)  # No index
```

### Convenience Functions
```python
from binance_data_reader import load_spot_klines, load_funding_rate

# Quick loading without instantiating class
spot_df = load_spot_klines('BTCUSDT', '2024-01', data_dir="./binance_data")
funding_df = load_funding_rate('BTCUSDT', '2024-01', data_dir="./binance_data")
```

### Data Discovery
```python
# Find available symbols and data types
symbols = reader.list_available_symbols()
data_types = reader.list_available_data_types('BTCUSDT')
date_range = reader.get_date_range('BTCUSDT', 'spot')
```

### Analysis Examples
```python
# Daily OHLC data
daily = spot_df.resample('1D').agg({
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum'
})

# Price statistics
price_stats = spot_df['close'].describe()

# Funding rate analysis
avg_funding = funding_df['last_funding_rate'].mean()

# Spot vs Futures comparison
spot_prices = reader.read_spot_klines('BTCUSDT', '2024-01')['close']
futures_prices = reader.read_futures_klines('BTCUSDT', '2024-01')['close']
basis = futures_prices - spot_prices
```

## File Structure Expected

```
data_directory/
├── BTCUSDT/
│   ├── spot/
│   │   ├── BTCUSDT-5m-2024-01.zip
│   │   └── BTCUSDT-5m-2024-02.zip
│   ├── futures/
│   │   ├── BTCUSDT-5m-2024-01.zip
│   │   └── BTCUSDT-5m-2024-02.zip
│   ├── premium_index/
│   │   ├── BTCUSDT-5m-2024-01.zip
│   │   └── BTCUSDT-5m-2024-02.zip
│   └── funding_rate/
│       ├── BTCUSDT-fundingRate-2024-01.zip
│       └── BTCUSDT-fundingRate-2024-02.zip
└── ETHUSDT/
    └── ...
```

## DataFrame Output

### Kline DataFrames
```
                                  open_time          open  ...  ignore
close_time                                                 ...
2024-01-01 00:04:59.999 2024-01-01 00:00:00  42397.23000  ...       0
2024-01-01 00:09:59.999 2024-01-01 00:05:00  42409.96000  ...       0
```

### Funding Rate DataFrames
```
                         funding_interval_hours  last_funding_rate
calc_time
2024-01-01 00:00:00.006                       8             0.0001
2024-01-01 08:00:00.009                       8             0.0001
```

## Error Handling

- **Missing files**: Logs warnings, continues with available data
- **Corrupt data**: Uses `errors='coerce'` for graceful handling
- **Invalid dates**: Clear error messages for format issues
- **Empty datasets**: Raises informative FileNotFoundError

## Requirements

```
pandas>=1.5.0
numpy>=1.20.0
```

## Complete Example Script

Run `python example_usage.py` to see comprehensive usage examples including:
- Loading different data types
- Data quality checks
- Spot vs futures analysis
- Date range operations
- Chart generation

This reader provides a clean, type-specific interface to work with Binance historical data in pandas for quantitative analysis and algorithmic trading research.