# Crypto Data

![Tests](https://github.com/USERNAME/crypto-data/actions/workflows/tests.yml/badge.svg)
[![Python Version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue)](https://www.python.org/downloads/)

DuckDB ingestion pipeline for cryptocurrency market data.

## Features

- Downloads OHLCV data from Binance Data Vision
- Downloads universe rankings from CoinMarketCap
- Populates DuckDB database with cleaned data
- Async parallel downloads (5-10x faster)

## Installation

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## Usage

```python
from crypto_data import sync, setup_colored_logging

setup_colored_logging()

# Complete workflow: universe + binance
sync(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,
    interval='5m',
    data_types=['spot', 'futures']
)
```

## Testing

```bash
pytest tests/ -v
```

With coverage:
```bash
pytest tests/ --cov=src/crypto_data --cov-report=html
```

## Documentation

See `CLAUDE.md` for detailed developer documentation.

## License

MIT
