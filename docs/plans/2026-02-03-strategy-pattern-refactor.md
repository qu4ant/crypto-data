# Strategy Pattern Architecture Refactoring Plan

> **Archive note:** This file is a historical implementation plan, not the
> current package architecture. The refactor that followed consolidated the
> active Binance ingestion code under `binance_datasets/`,
> `binance_downloader.py`, `binance_importer.py`, and `binance_pipeline.py`.
> References below to `core/`, `exchanges/`, and `strategies/` are preserved as
> plan context only.

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor crypto-data package to Strategy Pattern architecture for better maintainability and extensibility.

**Architecture:** Extract data type-specific logic into pluggable strategies. Create exchange abstraction for future multi-exchange support. Consolidate duplicated code (result processors, download wrappers, importers) into generic components.

**Tech Stack:** Python 3.11+, DuckDB, aiohttp, Pandera, pytest

---

## Overview

This refactoring transforms the current monolithic `ingestion.py` (1,576 LOC) into a modular strategy-based architecture:

```
BEFORE (Current):                    AFTER (Target):
─────────────────                    ───────────────
ingestion.py (1,576 LOC)             core/
├─ 3x result processors              ├─ orchestrator.py (~200 LOC)
├─ 3x download wrappers              ├─ downloader.py (~150 LOC)
├─ 3 data type branches              └─ importer.py (~200 LOC)
└─ ingest_binance_async (426 LOC)
                                     strategies/
utils/database.py (496 LOC)          ├─ base.py (~100 LOC)
├─ 3x import functions               ├─ klines.py (~150 LOC)
└─ data_exists()                     ├─ open_interest.py (~100 LOC)
                                     ├─ funding_rates.py (~100 LOC)
                                     └─ universe.py (~100 LOC)

                                     exchanges/
                                     ├─ base.py (~50 LOC)
                                     └─ binance.py (~300 LOC)
```

**Benefits:**
- Adding new exchange = 1 file (~300 LOC)
- Adding new data type = 1 strategy file (~100 LOC)
- ~40% less duplicated code
- Clearer separation of concerns

---

## Phase 1: Create Base Abstractions (Foundation)

### Task 1.1: Create strategies/base.py - DataTypeStrategy ABC

**Files:**
- Create: `src/crypto_data/strategies/__init__.py`
- Create: `src/crypto_data/strategies/base.py`
- Test: `tests/strategies/__init__.py`
- Test: `tests/strategies/test_base_strategy.py`

**Step 1: Create strategies package init**

```python
# src/crypto_data/strategies/__init__.py
"""
Data Type Strategies

Pluggable strategies for different data types (klines, open_interest, funding_rates).
Each strategy encapsulates:
- Period generation (months vs days)
- Download logic
- Import logic
- Validation schema
"""

from crypto_data.strategies.base import DataTypeStrategy, DownloadResult, Period

__all__ = [
    'DataTypeStrategy',
    'DownloadResult',
    'Period',
]
```

**Step 2: Create base strategy abstractions**

```python
# src/crypto_data/strategies/base.py
"""
Base classes for data type strategies.

Defines the interface that all data type strategies must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandera as pa

from crypto_data.enums import DataType, Interval


@dataclass
class Period:
    """Represents a time period for download (month or day)."""
    value: str  # 'YYYY-MM' for months, 'YYYY-MM-DD' for days
    is_monthly: bool = True

    def __str__(self) -> str:
        return self.value


@dataclass
class DownloadResult:
    """Result of a single download attempt."""
    success: bool
    symbol: str
    data_type: DataType
    period: str  # month or date string
    file_path: Optional[Path] = None
    error: Optional[str] = None

    @property
    def is_not_found(self) -> bool:
        return not self.success and self.error == 'not_found'


class DataTypeStrategy(ABC):
    """
    Abstract base class for data type strategies.

    Each data type (klines, open_interest, funding_rates) implements this interface.
    This enables the downloader to handle all data types uniformly.
    """

    @property
    @abstractmethod
    def data_type(self) -> DataType:
        """The data type this strategy handles."""
        pass

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Database table name for this data type."""
        pass

    @property
    @abstractmethod
    def is_monthly(self) -> bool:
        """True if data is organized by month, False if by day."""
        pass

    @property
    @abstractmethod
    def default_max_concurrent(self) -> int:
        """Default concurrency limit for this data type."""
        pass

    @abstractmethod
    def generate_periods(self, start: datetime, end: datetime) -> List[Period]:
        """
        Generate list of periods to download.

        Parameters
        ----------
        start : datetime
            Start date
        end : datetime
            End date

        Returns
        -------
        List[Period]
            List of periods (months or days) to download
        """
        pass

    @abstractmethod
    def get_schema(self) -> pa.DataFrameSchema:
        """Return the Pandera validation schema for this data type."""
        pass

    @abstractmethod
    def build_download_url(
        self,
        base_url: str,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """
        Build the download URL for a specific file.

        Parameters
        ----------
        base_url : str
            Base URL of the data source
        symbol : str
            Trading pair symbol
        period : Period
            Time period
        interval : Interval, optional
            Kline interval (only for klines strategy)

        Returns
        -------
        str
            Full download URL
        """
        pass

    @abstractmethod
    def build_temp_filename(
        self,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """Build temporary filename for download."""
        pass

    @abstractmethod
    def parse_csv(self, csv_path: Path, symbol: str, exchange: str) -> 'pd.DataFrame':
        """
        Parse downloaded CSV file into DataFrame.

        Parameters
        ----------
        csv_path : Path
            Path to extracted CSV file
        symbol : str
            Symbol to use in DataFrame (may differ from download symbol)
        exchange : str
            Exchange name

        Returns
        -------
        pd.DataFrame
            Parsed and transformed DataFrame ready for import
        """
        pass

    def requires_interval(self) -> bool:
        """Whether this strategy requires an interval parameter."""
        return False
```

**Step 3: Create test file**

```python
# tests/strategies/__init__.py
"""Tests for strategy pattern implementation."""
```

```python
# tests/strategies/test_base_strategy.py
"""Tests for base strategy abstractions."""

import pytest
from datetime import datetime
from pathlib import Path

from crypto_data.strategies.base import Period, DownloadResult, DataTypeStrategy
from crypto_data.enums import DataType


class TestPeriod:
    """Tests for Period dataclass."""

    def test_monthly_period(self):
        period = Period(value='2024-01', is_monthly=True)
        assert str(period) == '2024-01'
        assert period.is_monthly is True

    def test_daily_period(self):
        period = Period(value='2024-01-15', is_monthly=False)
        assert str(period) == '2024-01-15'
        assert period.is_monthly is False


class TestDownloadResult:
    """Tests for DownloadResult dataclass."""

    def test_successful_result(self):
        result = DownloadResult(
            success=True,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            period='2024-01',
            file_path=Path('/tmp/test.zip'),
            error=None
        )
        assert result.success is True
        assert result.is_not_found is False

    def test_not_found_result(self):
        result = DownloadResult(
            success=False,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            period='2024-01',
            file_path=None,
            error='not_found'
        )
        assert result.success is False
        assert result.is_not_found is True

    def test_error_result(self):
        result = DownloadResult(
            success=False,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            period='2024-01',
            file_path=None,
            error='connection_timeout'
        )
        assert result.success is False
        assert result.is_not_found is False


class TestDataTypeStrategyInterface:
    """Tests for DataTypeStrategy ABC interface."""

    def test_cannot_instantiate_abc(self):
        """DataTypeStrategy should not be instantiable directly."""
        with pytest.raises(TypeError):
            DataTypeStrategy()
```

**Step 4: Run tests to verify**

Run: `pytest tests/strategies/test_base_strategy.py -v`
Expected: All 5 tests PASS

**Step 5: Commit**

```bash
git add src/crypto_data/strategies/ tests/strategies/
git commit -m "$(cat <<'EOF'
feat(strategies): add base strategy abstractions

- Add DataTypeStrategy ABC for pluggable data type handling
- Add Period dataclass for month/day periods
- Add DownloadResult dataclass for download outcomes
- Foundation for strategy pattern refactoring

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 1.2: Create exchanges/base.py - ExchangeClient ABC

**Files:**
- Create: `src/crypto_data/exchanges/__init__.py`
- Create: `src/crypto_data/exchanges/base.py`
- Test: `tests/exchanges/__init__.py`
- Test: `tests/exchanges/test_base_exchange.py`

**Step 1: Create exchanges package init**

```python
# src/crypto_data/exchanges/__init__.py
"""
Exchange Clients

Pluggable exchange clients for downloading data from different sources.
Each exchange implements the ExchangeClient interface.
"""

from crypto_data.exchanges.base import ExchangeClient

__all__ = [
    'ExchangeClient',
]
```

**Step 2: Create base exchange abstraction**

```python
# src/crypto_data/exchanges/base.py
"""
Base class for exchange clients.

Defines the interface that all exchange clients must implement.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
import aiohttp

from crypto_data.enums import Exchange


class ExchangeClient(ABC):
    """
    Abstract base class for exchange data clients.

    Each exchange (Binance, Bybit, etc.) implements this interface.
    """

    @property
    @abstractmethod
    def exchange(self) -> Exchange:
        """The exchange this client handles."""
        pass

    @property
    @abstractmethod
    def base_url(self) -> str:
        """Base URL for data downloads."""
        pass

    @abstractmethod
    async def download_file(
        self,
        url: str,
        output_path: Path,
        session: aiohttp.ClientSession
    ) -> bool:
        """
        Download a file from the exchange.

        Parameters
        ----------
        url : str
            Full URL to download
        output_path : Path
            Where to save the file
        session : aiohttp.ClientSession
            Async HTTP session

        Returns
        -------
        bool
            True if download successful, False if not found (404)

        Raises
        ------
        aiohttp.ClientError
            For network errors
        """
        pass

    @abstractmethod
    async def __aenter__(self) -> 'ExchangeClient':
        """Async context manager entry."""
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        pass
```

**Step 3: Create test file**

```python
# tests/exchanges/__init__.py
"""Tests for exchange clients."""
```

```python
# tests/exchanges/test_base_exchange.py
"""Tests for base exchange abstractions."""

import pytest
from crypto_data.exchanges.base import ExchangeClient


class TestExchangeClientInterface:
    """Tests for ExchangeClient ABC interface."""

    def test_cannot_instantiate_abc(self):
        """ExchangeClient should not be instantiable directly."""
        with pytest.raises(TypeError):
            ExchangeClient()
```

**Step 4: Run tests to verify**

Run: `pytest tests/exchanges/test_base_exchange.py -v`
Expected: 1 test PASS

**Step 5: Commit**

```bash
git add src/crypto_data/exchanges/ tests/exchanges/
git commit -m "$(cat <<'EOF'
feat(exchanges): add base exchange client abstraction

- Add ExchangeClient ABC for pluggable exchange support
- Foundation for multi-exchange architecture

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 2: Implement Concrete Strategies

### Task 2.1: Create KlinesStrategy (spot/futures)

**Files:**
- Create: `src/crypto_data/strategies/klines.py`
- Test: `tests/strategies/test_klines_strategy.py`

**Step 1: Implement KlinesStrategy**

```python
# src/crypto_data/strategies/klines.py
"""
Klines (OHLCV) data strategy for spot and futures markets.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import pandas as pd
import pandera as pa

from crypto_data.strategies.base import DataTypeStrategy, Period
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import OHLCV_SCHEMA
from crypto_data.utils.dates import generate_month_list

logger = logging.getLogger(__name__)


class KlinesStrategy(DataTypeStrategy):
    """
    Strategy for downloading and importing kline (OHLCV) data.

    Handles both spot and futures markets. Data is organized by month.

    Parameters
    ----------
    data_type : DataType
        Either DataType.SPOT or DataType.FUTURES
    interval : Interval
        Kline interval (e.g., Interval.MIN_5)
    """

    # URL path templates
    URL_PATHS = {
        DataType.SPOT: 'data/spot/monthly/klines',
        DataType.FUTURES: 'data/futures/um/monthly/klines',
    }

    def __init__(self, data_type: DataType, interval: Interval):
        if data_type not in (DataType.SPOT, DataType.FUTURES):
            raise ValueError(f"KlinesStrategy only supports SPOT or FUTURES, got {data_type}")
        self._data_type = data_type
        self._interval = interval

    @property
    def data_type(self) -> DataType:
        return self._data_type

    @property
    def table_name(self) -> str:
        return self._data_type.value  # 'spot' or 'futures'

    @property
    def is_monthly(self) -> bool:
        return True

    @property
    def default_max_concurrent(self) -> int:
        return 20  # Conservative for klines (large files)

    @property
    def interval(self) -> Interval:
        return self._interval

    def requires_interval(self) -> bool:
        return True

    def generate_periods(self, start: datetime, end: datetime) -> List[Period]:
        """Generate monthly periods."""
        months = generate_month_list(start, end)
        return [Period(value=m, is_monthly=True) for m in months]

    def get_schema(self) -> pa.DataFrameSchema:
        return OHLCV_SCHEMA

    def build_download_url(
        self,
        base_url: str,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """Build Binance Data Vision klines URL."""
        interval = interval or self._interval
        path = self.URL_PATHS[self._data_type]
        filename = f"{symbol}-{interval.value}-{period.value}.zip"
        return f"{base_url}{path}/{symbol}/{interval.value}/{filename}"

    def build_temp_filename(
        self,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """Build temp filename for klines download."""
        interval = interval or self._interval
        return f"{symbol}-{self._data_type.value}-{interval.value}-{period.value}.zip"

    def parse_csv(self, csv_path: Path, symbol: str, exchange: str) -> pd.DataFrame:
        """
        Parse klines CSV into DataFrame.

        Handles:
        - Header detection (some files have headers, some don't)
        - Timestamp conversion (ms vs μs)
        - Column normalization
        """
        # Detect if CSV has header
        has_header = False
        with open(csv_path, 'r') as f:
            first_line = f.readline().strip()
            if 'open_time' in first_line.lower() or 'close_time' in first_line.lower():
                has_header = True

        # Read CSV
        if has_header:
            df = pd.read_csv(csv_path)
        else:
            columns = [
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades_count',
                'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
            ]
            df = pd.read_csv(csv_path, header=None, names=columns)

        # Add metadata columns
        df['exchange'] = exchange
        df['symbol'] = symbol
        df['interval'] = self._interval.value

        # Convert timestamp (handle both ms and μs)
        # μs (16 digits) >= 5e12, ms (13 digits) < 5e12
        df['timestamp'] = pd.to_datetime(
            df['close_time'].apply(
                lambda x: x / 1000000.0 if x >= 5e12 else x / 1000.0
            ),
            unit='s'
        ).dt.ceil('1s')

        # Normalize column names
        if 'count' in df.columns:
            df.rename(columns={'count': 'trades_count'}, inplace=True)
        if 'taker_buy_volume' in df.columns:
            df.rename(columns={'taker_buy_volume': 'taker_buy_base_volume'}, inplace=True)

        # Select final columns
        final_columns = [
            'exchange', 'symbol', 'interval', 'timestamp',
            'open', 'high', 'low', 'close', 'volume', 'quote_volume',
            'trades_count', 'taker_buy_base_volume', 'taker_buy_quote_volume'
        ]
        df = df[final_columns]

        # Drop duplicates (handles daylight saving issues)
        df = df.drop_duplicates(
            subset=['exchange', 'symbol', 'interval', 'timestamp'],
            keep='first'
        )

        return df
```

**Step 2: Create tests**

```python
# tests/strategies/test_klines_strategy.py
"""Tests for KlinesStrategy."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import pandas as pd

from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.base import Period
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import OHLCV_SCHEMA


class TestKlinesStrategyInit:
    """Tests for KlinesStrategy initialization."""

    def test_spot_strategy(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        assert strategy.data_type == DataType.SPOT
        assert strategy.table_name == 'spot'
        assert strategy.interval == Interval.MIN_5

    def test_futures_strategy(self):
        strategy = KlinesStrategy(DataType.FUTURES, Interval.HOUR_1)
        assert strategy.data_type == DataType.FUTURES
        assert strategy.table_name == 'futures'
        assert strategy.interval == Interval.HOUR_1

    def test_invalid_data_type_raises(self):
        with pytest.raises(ValueError, match="only supports SPOT or FUTURES"):
            KlinesStrategy(DataType.OPEN_INTEREST, Interval.MIN_5)

    def test_requires_interval(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        assert strategy.requires_interval() is True

    def test_is_monthly(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        assert strategy.is_monthly is True

    def test_default_max_concurrent(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        assert strategy.default_max_concurrent == 20


class TestKlinesStrategyPeriods:
    """Tests for period generation."""

    def test_generate_monthly_periods(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        start = datetime(2024, 1, 1)
        end = datetime(2024, 3, 31)

        periods = strategy.generate_periods(start, end)

        assert len(periods) == 3
        assert periods[0].value == '2024-01'
        assert periods[1].value == '2024-02'
        assert periods[2].value == '2024-03'
        assert all(p.is_monthly for p in periods)


class TestKlinesStrategyUrls:
    """Tests for URL building."""

    def test_spot_url(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        url = strategy.build_download_url(
            base_url='https://data.binance.vision/',
            symbol='BTCUSDT',
            period=Period('2024-01', is_monthly=True)
        )
        expected = 'https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2024-01.zip'
        assert url == expected

    def test_futures_url(self):
        strategy = KlinesStrategy(DataType.FUTURES, Interval.HOUR_4)
        url = strategy.build_download_url(
            base_url='https://data.binance.vision/',
            symbol='ETHUSDT',
            period=Period('2024-06', is_monthly=True)
        )
        expected = 'https://data.binance.vision/data/futures/um/monthly/klines/ETHUSDT/4h/ETHUSDT-4h-2024-06.zip'
        assert url == expected

    def test_temp_filename(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        filename = strategy.build_temp_filename(
            symbol='BTCUSDT',
            period=Period('2024-01', is_monthly=True)
        )
        assert filename == 'BTCUSDT-spot-5m-2024-01.zip'


class TestKlinesStrategyCsvParsing:
    """Tests for CSV parsing."""

    def test_parse_csv_without_header(self):
        """Test parsing CSV without header row."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)

        # Create test CSV without header
        csv_content = """1704067199999,42000.0,42100.0,41900.0,42050.0,100.5,1704067499999,4210000.0,500,50.0,2100000.0,0"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            df = strategy.parse_csv(csv_path, symbol='BTCUSDT', exchange='binance')

            assert len(df) == 1
            assert df['exchange'].iloc[0] == 'binance'
            assert df['symbol'].iloc[0] == 'BTCUSDT'
            assert df['interval'].iloc[0] == '5m'
            assert 'timestamp' in df.columns
            assert df['open'].iloc[0] == 42000.0
        finally:
            csv_path.unlink()

    def test_get_schema(self):
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        schema = strategy.get_schema()
        assert schema == OHLCV_SCHEMA
```

**Step 3: Run tests**

Run: `pytest tests/strategies/test_klines_strategy.py -v`
Expected: All tests PASS

**Step 4: Update strategies __init__.py**

```python
# src/crypto_data/strategies/__init__.py
"""
Data Type Strategies

Pluggable strategies for different data types (klines, open_interest, funding_rates).
Each strategy encapsulates:
- Period generation (months vs days)
- Download logic
- Import logic
- Validation schema
"""

from crypto_data.strategies.base import DataTypeStrategy, DownloadResult, Period
from crypto_data.strategies.klines import KlinesStrategy

__all__ = [
    'DataTypeStrategy',
    'DownloadResult',
    'Period',
    'KlinesStrategy',
]
```

**Step 5: Commit**

```bash
git add src/crypto_data/strategies/ tests/strategies/
git commit -m "$(cat <<'EOF'
feat(strategies): implement KlinesStrategy for spot/futures

- Handle monthly OHLCV data downloads
- Auto-detect CSV headers
- Handle timestamp format changes (ms vs μs)
- Build correct URLs for Binance Data Vision

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2.2: Create OpenInterestStrategy

**Files:**
- Create: `src/crypto_data/strategies/open_interest.py`
- Test: `tests/strategies/test_open_interest_strategy.py`

**Step 1: Implement OpenInterestStrategy**

```python
# src/crypto_data/strategies/open_interest.py
"""
Open Interest metrics data strategy.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import pandas as pd
import pandera as pa

from crypto_data.strategies.base import DataTypeStrategy, Period
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import OPEN_INTEREST_SCHEMA
from crypto_data.utils.dates import generate_day_list

logger = logging.getLogger(__name__)


class OpenInterestStrategy(DataTypeStrategy):
    """
    Strategy for downloading and importing open interest metrics.

    Data is organized by day (daily snapshots).
    """

    URL_PATH = 'data/futures/um/daily/metrics'

    @property
    def data_type(self) -> DataType:
        return DataType.OPEN_INTEREST

    @property
    def table_name(self) -> str:
        return 'open_interest'

    @property
    def is_monthly(self) -> bool:
        return False  # Daily data

    @property
    def default_max_concurrent(self) -> int:
        return 100  # Higher for daily (smaller files, more requests)

    def generate_periods(self, start: datetime, end: datetime) -> List[Period]:
        """Generate daily periods."""
        days = generate_day_list(start, end)
        return [Period(value=d, is_monthly=False) for d in days]

    def get_schema(self) -> pa.DataFrameSchema:
        return OPEN_INTEREST_SCHEMA

    def build_download_url(
        self,
        base_url: str,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """Build Binance Data Vision metrics URL."""
        filename = f"{symbol}-metrics-{period.value}.zip"
        return f"{base_url}{self.URL_PATH}/{symbol}/{filename}"

    def build_temp_filename(
        self,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """Build temp filename for metrics download."""
        return f"{symbol}-metrics-{period.value}.zip"

    def parse_csv(self, csv_path: Path, symbol: str, exchange: str) -> pd.DataFrame:
        """
        Parse open interest CSV into DataFrame.

        Filters out zero values (erroneous data).
        """
        df = pd.read_csv(csv_path)

        # Validate required columns
        required_cols = ['create_time', 'symbol', 'sum_open_interest']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV missing required columns. Expected: {required_cols}")

        # Add metadata
        df['exchange'] = exchange
        df['symbol'] = symbol  # Normalize (handles 1000-prefix)

        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['create_time'])

        # Rename columns
        df.rename(columns={'sum_open_interest': 'open_interest'}, inplace=True)

        # Select final columns
        df = df[['exchange', 'symbol', 'timestamp', 'open_interest']]

        # Filter zero values (erroneous)
        df = df[df['open_interest'] != 0]

        # Drop duplicates
        df = df.drop_duplicates(
            subset=['exchange', 'symbol', 'timestamp'],
            keep='first'
        )

        return df
```

**Step 2: Create tests**

```python
# tests/strategies/test_open_interest_strategy.py
"""Tests for OpenInterestStrategy."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile

from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.strategies.base import Period
from crypto_data.enums import DataType


class TestOpenInterestStrategyInit:
    """Tests for OpenInterestStrategy initialization."""

    def test_properties(self):
        strategy = OpenInterestStrategy()
        assert strategy.data_type == DataType.OPEN_INTEREST
        assert strategy.table_name == 'open_interest'
        assert strategy.is_monthly is False
        assert strategy.default_max_concurrent == 100
        assert strategy.requires_interval() is False


class TestOpenInterestStrategyPeriods:
    """Tests for period generation."""

    def test_generate_daily_periods(self):
        strategy = OpenInterestStrategy()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 3)

        periods = strategy.generate_periods(start, end)

        assert len(periods) == 3
        assert periods[0].value == '2024-01-01'
        assert periods[1].value == '2024-01-02'
        assert periods[2].value == '2024-01-03'
        assert all(not p.is_monthly for p in periods)


class TestOpenInterestStrategyUrls:
    """Tests for URL building."""

    def test_metrics_url(self):
        strategy = OpenInterestStrategy()
        url = strategy.build_download_url(
            base_url='https://data.binance.vision/',
            symbol='BTCUSDT',
            period=Period('2024-01-15', is_monthly=False)
        )
        expected = 'https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2024-01-15.zip'
        assert url == expected

    def test_temp_filename(self):
        strategy = OpenInterestStrategy()
        filename = strategy.build_temp_filename(
            symbol='BTCUSDT',
            period=Period('2024-01-15', is_monthly=False)
        )
        assert filename == 'BTCUSDT-metrics-2024-01-15.zip'


class TestOpenInterestStrategyCsvParsing:
    """Tests for CSV parsing."""

    def test_parse_csv(self):
        strategy = OpenInterestStrategy()

        csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2024-01-15 00:00:00,BTCUSDT,50000.0,2100000000.0,1.5,1.2,1.3,1.1
2024-01-15 04:00:00,BTCUSDT,51000.0,2150000000.0,1.4,1.3,1.2,1.0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            df = strategy.parse_csv(csv_path, symbol='BTCUSDT', exchange='binance')

            assert len(df) == 2
            assert df['exchange'].iloc[0] == 'binance'
            assert df['symbol'].iloc[0] == 'BTCUSDT'
            assert df['open_interest'].iloc[0] == 50000.0
        finally:
            csv_path.unlink()

    def test_filters_zero_values(self):
        strategy = OpenInterestStrategy()

        csv_content = """create_time,symbol,sum_open_interest
2024-01-15 00:00:00,BTCUSDT,50000.0
2024-01-15 04:00:00,BTCUSDT,0
2024-01-15 08:00:00,BTCUSDT,52000.0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            df = strategy.parse_csv(csv_path, symbol='BTCUSDT', exchange='binance')
            assert len(df) == 2  # Zero value filtered out
        finally:
            csv_path.unlink()
```

**Step 3: Run tests**

Run: `pytest tests/strategies/test_open_interest_strategy.py -v`
Expected: All tests PASS

**Step 4: Update strategies __init__.py**

```python
# Add to src/crypto_data/strategies/__init__.py
from crypto_data.strategies.open_interest import OpenInterestStrategy

# Add to __all__
__all__ = [
    'DataTypeStrategy',
    'DownloadResult',
    'Period',
    'KlinesStrategy',
    'OpenInterestStrategy',
]
```

**Step 5: Commit**

```bash
git add src/crypto_data/strategies/ tests/strategies/
git commit -m "$(cat <<'EOF'
feat(strategies): implement OpenInterestStrategy

- Handle daily metrics data downloads
- Filter zero values (erroneous data)
- Higher concurrency for smaller daily files

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2.3: Create FundingRatesStrategy

**Files:**
- Create: `src/crypto_data/strategies/funding_rates.py`
- Test: `tests/strategies/test_funding_rates_strategy.py`

**Step 1: Implement FundingRatesStrategy**

```python
# src/crypto_data/strategies/funding_rates.py
"""
Funding Rates data strategy.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import pandas as pd
import pandera as pa

from crypto_data.strategies.base import DataTypeStrategy, Period
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import FUNDING_RATES_SCHEMA
from crypto_data.utils.dates import generate_month_list

logger = logging.getLogger(__name__)


class FundingRatesStrategy(DataTypeStrategy):
    """
    Strategy for downloading and importing funding rate data.

    Data is organized by month.
    """

    URL_PATH = 'data/futures/um/monthly/fundingRate'

    @property
    def data_type(self) -> DataType:
        return DataType.FUNDING_RATES

    @property
    def table_name(self) -> str:
        return 'funding_rates'

    @property
    def is_monthly(self) -> bool:
        return True

    @property
    def default_max_concurrent(self) -> int:
        return 50  # Medium for monthly funding data

    def generate_periods(self, start: datetime, end: datetime) -> List[Period]:
        """Generate monthly periods."""
        months = generate_month_list(start, end)
        return [Period(value=m, is_monthly=True) for m in months]

    def get_schema(self) -> pa.DataFrameSchema:
        return FUNDING_RATES_SCHEMA

    def build_download_url(
        self,
        base_url: str,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """Build Binance Data Vision funding rates URL."""
        filename = f"{symbol}-fundingRate-{period.value}.zip"
        return f"{base_url}{self.URL_PATH}/{symbol}/{filename}"

    def build_temp_filename(
        self,
        symbol: str,
        period: Period,
        interval: Optional[Interval] = None
    ) -> str:
        """Build temp filename for funding rates download."""
        return f"{symbol}-fundingRate-{period.value}.zip"

    def parse_csv(self, csv_path: Path, symbol: str, exchange: str) -> pd.DataFrame:
        """Parse funding rates CSV into DataFrame."""
        df = pd.read_csv(csv_path)

        # Validate required columns
        required_cols = ['calc_time', 'last_funding_rate']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV missing required columns. Expected: {required_cols}")

        # Add metadata
        df['exchange'] = exchange
        df['symbol'] = symbol

        # Convert timestamp (milliseconds)
        df['timestamp'] = pd.to_datetime(df['calc_time'], unit='ms')

        # Rename columns
        df.rename(columns={'last_funding_rate': 'funding_rate'}, inplace=True)

        # Select final columns
        df = df[['exchange', 'symbol', 'timestamp', 'funding_rate']]

        # Drop duplicates
        df = df.drop_duplicates(
            subset=['exchange', 'symbol', 'timestamp'],
            keep='first'
        )

        return df
```

**Step 2: Create tests**

```python
# tests/strategies/test_funding_rates_strategy.py
"""Tests for FundingRatesStrategy."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile

from crypto_data.strategies.funding_rates import FundingRatesStrategy
from crypto_data.strategies.base import Period
from crypto_data.enums import DataType


class TestFundingRatesStrategyInit:
    """Tests for FundingRatesStrategy initialization."""

    def test_properties(self):
        strategy = FundingRatesStrategy()
        assert strategy.data_type == DataType.FUNDING_RATES
        assert strategy.table_name == 'funding_rates'
        assert strategy.is_monthly is True
        assert strategy.default_max_concurrent == 50
        assert strategy.requires_interval() is False


class TestFundingRatesStrategyPeriods:
    """Tests for period generation."""

    def test_generate_monthly_periods(self):
        strategy = FundingRatesStrategy()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 3, 31)

        periods = strategy.generate_periods(start, end)

        assert len(periods) == 3
        assert periods[0].value == '2024-01'
        assert all(p.is_monthly for p in periods)


class TestFundingRatesStrategyUrls:
    """Tests for URL building."""

    def test_funding_url(self):
        strategy = FundingRatesStrategy()
        url = strategy.build_download_url(
            base_url='https://data.binance.vision/',
            symbol='BTCUSDT',
            period=Period('2024-01', is_monthly=True)
        )
        expected = 'https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip'
        assert url == expected


class TestFundingRatesStrategyCsvParsing:
    """Tests for CSV parsing."""

    def test_parse_csv(self):
        strategy = FundingRatesStrategy()

        csv_content = """calc_time,last_funding_rate,mark_price
1704067200000,0.0001,42000.0
1704096000000,0.00015,42100.0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)

        try:
            df = strategy.parse_csv(csv_path, symbol='BTCUSDT', exchange='binance')

            assert len(df) == 2
            assert df['exchange'].iloc[0] == 'binance'
            assert df['funding_rate'].iloc[0] == 0.0001
        finally:
            csv_path.unlink()
```

**Step 3: Run tests**

Run: `pytest tests/strategies/test_funding_rates_strategy.py -v`
Expected: All tests PASS

**Step 4: Update strategies __init__.py**

```python
# Final src/crypto_data/strategies/__init__.py
"""
Data Type Strategies

Pluggable strategies for different data types (klines, open_interest, funding_rates).
"""

from crypto_data.strategies.base import DataTypeStrategy, DownloadResult, Period
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.strategies.funding_rates import FundingRatesStrategy

__all__ = [
    'DataTypeStrategy',
    'DownloadResult',
    'Period',
    'KlinesStrategy',
    'OpenInterestStrategy',
    'FundingRatesStrategy',
]
```

**Step 5: Commit**

```bash
git add src/crypto_data/strategies/ tests/strategies/
git commit -m "$(cat <<'EOF'
feat(strategies): implement FundingRatesStrategy

- Handle monthly funding rate data
- Parse millisecond timestamps
- Complete strategy implementations

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 3: Implement Binance Exchange Client

### Task 3.1: Create exchanges/binance.py

**Files:**
- Create: `src/crypto_data/exchanges/binance.py`
- Test: `tests/exchanges/test_binance_exchange.py`

**Step 1: Implement BinanceExchange**

```python
# src/crypto_data/exchanges/binance.py
"""
Binance Data Vision exchange client.

Implements ExchangeClient interface for Binance historical data.
"""

import logging
from pathlib import Path
from typing import Optional
import aiohttp
import asyncio
import zipfile

from crypto_data.exchanges.base import ExchangeClient
from crypto_data.enums import Exchange

logger = logging.getLogger(__name__)


class BinanceExchange(ExchangeClient):
    """
    Binance Data Vision exchange client.

    Downloads historical data from Binance's S3-backed repository.

    Parameters
    ----------
    base_url : str, optional
        Base URL (default: https://data.binance.vision/)
    timeout : int
        Request timeout in seconds (default: 30)
    max_concurrent : int
        Maximum concurrent downloads (default: 20)
    """

    DEFAULT_BASE_URL = "https://data.binance.vision/"

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: int = 30,
        max_concurrent: int = 20
    ):
        self._base_url = base_url or self.DEFAULT_BASE_URL
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_concurrent = max_concurrent
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

    @property
    def exchange(self) -> Exchange:
        return Exchange.BINANCE

    @property
    def base_url(self) -> str:
        return self._base_url

    async def __aenter__(self) -> 'BinanceExchange':
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session:
            await self._session.close()

    async def download_file(
        self,
        url: str,
        output_path: Path,
        session: Optional[aiohttp.ClientSession] = None
    ) -> bool:
        """
        Download a file from Binance Data Vision.

        Includes validation:
        - Content-Length check (partial download detection)
        - ZIP integrity verification
        - Atomic write pattern

        Parameters
        ----------
        url : str
            Full URL to download
        output_path : Path
            Where to save the file
        session : aiohttp.ClientSession, optional
            HTTP session (uses internal session if not provided)

        Returns
        -------
        bool
            True if successful, False if 404
        """
        session = session or self._session
        if not session:
            raise RuntimeError("No session available. Use 'async with' context.")

        async with self._semaphore:
            try:
                async with session.get(url) as response:
                    if response.status == 404:
                        logger.debug(f"File not found (404): {url}")
                        return False

                    response.raise_for_status()
                    content = await response.read()

                    # Validate content length
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        try:
                            expected = int(content_length)
                            actual = len(content)
                            if actual != expected:
                                logger.error(f"Partial download: {actual}/{expected} bytes")
                                return False
                        except (ValueError, TypeError):
                            pass

                    # Atomic write: temp → validate → rename
                    temp_path = output_path.with_suffix('.tmp')
                    temp_path.write_bytes(content)

                    # Validate ZIP (skip for small test files)
                    if len(content) >= 1024:
                        if not zipfile.is_zipfile(temp_path):
                            logger.error(f"Corrupt ZIP: {output_path.name}")
                            temp_path.unlink()
                            return False

                    temp_path.rename(output_path)
                    logger.debug(f"Downloaded: {len(content)} bytes")
                    return True

            except aiohttp.ClientError as e:
                logger.error(f"Download error: {e}")
                raise
```

**Step 2: Create tests**

```python
# tests/exchanges/test_binance_exchange.py
"""Tests for BinanceExchange client."""

import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import aiohttp

from crypto_data.exchanges.binance import BinanceExchange
from crypto_data.enums import Exchange


class TestBinanceExchangeInit:
    """Tests for BinanceExchange initialization."""

    def test_default_properties(self):
        client = BinanceExchange()
        assert client.exchange == Exchange.BINANCE
        assert client.base_url == "https://data.binance.vision/"

    def test_custom_base_url(self):
        client = BinanceExchange(base_url="https://custom.url/")
        assert client.base_url == "https://custom.url/"


class TestBinanceExchangeContextManager:
    """Tests for async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager_creates_session(self):
        async with BinanceExchange() as client:
            assert client._session is not None
            assert client._semaphore is not None

    @pytest.mark.asyncio
    async def test_context_manager_closes_session(self):
        client = BinanceExchange()
        async with client:
            session = client._session
        # Session should be closed after context exit


class TestBinanceExchangeDownload:
    """Tests for download functionality."""

    @pytest.mark.asyncio
    async def test_download_file_404_returns_false(self, tmp_path):
        """Test that 404 returns False without raising."""
        async with BinanceExchange() as client:
            # Mock the session.get to return 404
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response.__aexit__ = AsyncMock(return_value=None)

            with patch.object(client._session, 'get', return_value=mock_response):
                result = await client.download_file(
                    url="https://example.com/notfound.zip",
                    output_path=tmp_path / "test.zip"
                )
                assert result is False
```

**Step 3: Run tests**

Run: `pytest tests/exchanges/test_binance_exchange.py -v`
Expected: All tests PASS

**Step 4: Update exchanges __init__.py**

```python
# src/crypto_data/exchanges/__init__.py
"""
Exchange Clients

Pluggable exchange clients for downloading data from different sources.
"""

from crypto_data.exchanges.base import ExchangeClient
from crypto_data.exchanges.binance import BinanceExchange

__all__ = [
    'ExchangeClient',
    'BinanceExchange',
]
```

**Step 5: Commit**

```bash
git add src/crypto_data/exchanges/ tests/exchanges/
git commit -m "$(cat <<'EOF'
feat(exchanges): implement BinanceExchange client

- Download with validation (content-length, ZIP integrity)
- Atomic write pattern
- Semaphore-based concurrency control
- Implements ExchangeClient interface

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 4: Create Core Components

### Task 4.1: Create core/importer.py - Generic Database Importer

**Files:**
- Create: `src/crypto_data/core/__init__.py`
- Create: `src/crypto_data/core/importer.py`
- Test: `tests/core/__init__.py`
- Test: `tests/core/test_importer.py`

**Step 1: Create core package**

```python
# src/crypto_data/core/__init__.py
"""
Core ingestion components.

Provides the main orchestration and import logic.
"""

from crypto_data.core.importer import DataImporter

__all__ = [
    'DataImporter',
]
```

**Step 2: Implement DataImporter**

```python
# src/crypto_data/core/importer.py
"""
Generic data importer.

Consolidates the 3 duplicate import functions into one generic importer.
Uses strategy pattern to handle different data types.
"""

import logging
import zipfile
from pathlib import Path
from typing import Optional
import pandas as pd
import pandera.pandas as pa

from crypto_data.strategies.base import DataTypeStrategy

logger = logging.getLogger(__name__)


class DataImporter:
    """
    Generic data importer for all data types.

    Consolidates import_to_duckdb, import_metrics_to_duckdb, and
    import_funding_rates_to_duckdb into a single class.

    Parameters
    ----------
    strategy : DataTypeStrategy
        The strategy defining how to parse and validate data
    """

    def __init__(self, strategy: DataTypeStrategy):
        self.strategy = strategy

    def import_file(
        self,
        conn,
        file_path: Path,
        symbol: str,
        exchange: str = 'binance'
    ) -> int:
        """
        Import a downloaded file into DuckDB.

        IMPORTANT: Must be called within a transaction context.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Database connection (within transaction)
        file_path : Path
            Path to downloaded ZIP file
        symbol : str
            Symbol to store (normalized, e.g., 'PEPEUSDT' not '1000PEPEUSDT')
        exchange : str
            Exchange name (default: 'binance')

        Returns
        -------
        int
            Number of rows imported

        Raises
        ------
        ValueError
            If validation fails
        """
        table = self.strategy.table_name
        logger.debug(f"Importing to {table} (exchange={exchange})")

        # Extract ZIP
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV in ZIP: {file_path}")

            csv_name = csv_files[0]
            temp_dir = file_path.parent
            csv_path = temp_dir / csv_name
            zip_ref.extract(csv_name, temp_dir)

        try:
            # Parse CSV using strategy
            df = self.strategy.parse_csv(csv_path, symbol, exchange)

            # Validate with Pandera schema
            try:
                schema = self.strategy.get_schema()
                schema.validate(df, lazy=False)
                logger.debug(f"Validation passed: {len(df)} rows")
            except pa.errors.SchemaError as e:
                logger.error(f"Validation FAILED for {symbol} {table}")
                raise ValueError(f"Data validation failed") from e

            # Insert into DuckDB
            try:
                conn.execute(f"INSERT INTO {table} SELECT * FROM df")
                logger.debug(f"Import successful: {len(df)} rows")
                return len(df)
            except Exception as e:
                if "Duplicate key" in str(e):
                    logger.debug(f"Skipped duplicate data for {symbol}")
                    return 0
                raise

        finally:
            if csv_path.exists():
                csv_path.unlink()
```

**Step 3: Create tests**

```python
# tests/core/__init__.py
"""Tests for core components."""
```

```python
# tests/core/test_importer.py
"""Tests for DataImporter."""

import pytest
from pathlib import Path
import tempfile
import zipfile
import duckdb

from crypto_data.core.importer import DataImporter
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.enums import DataType, Interval


class TestDataImporterKlines:
    """Tests for importing klines data."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database with schema."""
        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE spot (
                exchange VARCHAR, symbol VARCHAR, interval VARCHAR,
                timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,
                close DOUBLE, volume DOUBLE, quote_volume DOUBLE,
                trades_count INTEGER, taker_buy_base_volume DOUBLE,
                taker_buy_quote_volume DOUBLE,
                PRIMARY KEY (exchange, symbol, interval, timestamp)
            )
        """)
        yield conn
        conn.close()

    @pytest.fixture
    def klines_zip(self, tmp_path):
        """Create test klines ZIP file."""
        csv_content = """1704067199999,42000.0,42100.0,41900.0,42050.0,100.5,1704067499999,4210000.0,500,50.0,2100000.0,0"""

        csv_path = tmp_path / "BTCUSDT-5m-2024-01.csv"
        csv_path.write_text(csv_content)

        zip_path = tmp_path / "BTCUSDT-5m-2024-01.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.write(csv_path, csv_path.name)

        csv_path.unlink()
        return zip_path

    def test_import_klines(self, db_conn, klines_zip):
        """Test importing klines data."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        db_conn.execute("BEGIN TRANSACTION")
        rows = importer.import_file(
            conn=db_conn,
            file_path=klines_zip,
            symbol='BTCUSDT',
            exchange='binance'
        )
        db_conn.execute("COMMIT")

        assert rows == 1

        result = db_conn.execute("SELECT * FROM spot").fetchall()
        assert len(result) == 1
        assert result[0][1] == 'BTCUSDT'  # symbol


class TestDataImporterOpenInterest:
    """Tests for importing open interest data."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database with schema."""
        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE open_interest (
                exchange VARCHAR, symbol VARCHAR,
                timestamp TIMESTAMP, open_interest DOUBLE,
                PRIMARY KEY (exchange, symbol, timestamp)
            )
        """)
        yield conn
        conn.close()

    @pytest.fixture
    def metrics_zip(self, tmp_path):
        """Create test metrics ZIP file."""
        csv_content = """create_time,symbol,sum_open_interest
2024-01-15 00:00:00,BTCUSDT,50000.0
2024-01-15 04:00:00,BTCUSDT,51000.0
"""
        csv_path = tmp_path / "BTCUSDT-metrics-2024-01-15.csv"
        csv_path.write_text(csv_content)

        zip_path = tmp_path / "BTCUSDT-metrics-2024-01-15.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.write(csv_path, csv_path.name)

        csv_path.unlink()
        return zip_path

    def test_import_open_interest(self, db_conn, metrics_zip):
        """Test importing open interest data."""
        strategy = OpenInterestStrategy()
        importer = DataImporter(strategy)

        db_conn.execute("BEGIN TRANSACTION")
        rows = importer.import_file(
            conn=db_conn,
            file_path=metrics_zip,
            symbol='BTCUSDT',
            exchange='binance'
        )
        db_conn.execute("COMMIT")

        assert rows == 2
```

**Step 4: Run tests**

Run: `pytest tests/core/test_importer.py -v`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/crypto_data/core/ tests/core/
git commit -m "$(cat <<'EOF'
feat(core): add generic DataImporter

- Consolidates 3 import functions into one
- Uses strategy pattern for data type specifics
- Validates with Pandera schemas before import
- Handles ZIP extraction and cleanup

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4.2: Create core/downloader.py - Generic Async Downloader

**Files:**
- Modify: `src/crypto_data/core/__init__.py`
- Create: `src/crypto_data/core/downloader.py`
- Test: `tests/core/test_downloader.py`

**Step 1: Implement BatchDownloader**

```python
# src/crypto_data/core/downloader.py
"""
Generic async batch downloader.

Consolidates download logic from _download_symbol_data_type_async,
_download_symbol_open_interest_async, and _download_symbol_funding_rates_async.
"""

import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional
import threading

from crypto_data.strategies.base import DataTypeStrategy, DownloadResult, Period
from crypto_data.exchanges.base import ExchangeClient
from crypto_data.enums import Interval

logger = logging.getLogger(__name__)

# Global cache for 1000-prefix ticker mappings
_ticker_mappings: Dict[str, str] = {}
_ticker_mappings_lock = threading.Lock()


class BatchDownloader:
    """
    Generic async batch downloader for all data types.

    Handles:
    - Parallel downloads with semaphore control
    - Gap detection (delisting)
    - 1000-prefix auto-discovery
    - Result collection

    Parameters
    ----------
    strategy : DataTypeStrategy
        Strategy for the data type being downloaded
    exchange : ExchangeClient
        Exchange client for downloads
    temp_path : Path
        Temporary directory for downloads
    max_concurrent : int, optional
        Max concurrent downloads (default: from strategy)
    """

    def __init__(
        self,
        strategy: DataTypeStrategy,
        exchange: ExchangeClient,
        temp_path: Path,
        max_concurrent: Optional[int] = None
    ):
        self.strategy = strategy
        self.exchange = exchange
        self.temp_path = temp_path
        self.max_concurrent = max_concurrent or strategy.default_max_concurrent

    async def download_symbol(
        self,
        symbol: str,
        periods: List[Period],
        interval: Optional[Interval] = None,
        failure_threshold: int = 3
    ) -> List[DownloadResult]:
        """
        Download all periods for a symbol.

        Parameters
        ----------
        symbol : str
            Symbol to download
        periods : List[Period]
            List of periods to download
        interval : Interval, optional
            Kline interval (for klines strategy)
        failure_threshold : int
            Stop after N consecutive 404s (gap detection)

        Returns
        -------
        List[DownloadResult]
            Results for each period
        """
        if not periods:
            return []

        # Check for cached 1000-prefix mapping
        with _ticker_mappings_lock:
            download_symbol = _ticker_mappings.get(symbol, symbol)

        if download_symbol != symbol:
            logger.info(f"Using cached mapping: {symbol} → {download_symbol}")

        # Build download tasks
        tasks = []
        for period in periods:
            url = self.strategy.build_download_url(
                base_url=self.exchange.base_url,
                symbol=download_symbol,
                period=period,
                interval=interval
            )
            filename = self.strategy.build_temp_filename(
                symbol=download_symbol,
                period=period,
                interval=interval
            )
            output_path = self.temp_path / filename

            task = self._download_single(
                url=url,
                output_path=output_path,
                symbol=symbol,  # Store with original symbol
                period=period
            )
            tasks.append(task)

        # Execute downloads in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        processed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exception: {result}")
                processed.append(DownloadResult(
                    success=False,
                    symbol=symbol,
                    data_type=self.strategy.data_type,
                    period=periods[i].value,
                    error=str(result)
                ))
            else:
                processed.append(result)

        # Apply gap detection
        if failure_threshold > 0:
            processed = self._detect_gaps(processed, failure_threshold)

        # Try 1000-prefix retry if all failed
        if (download_symbol == symbol and
            all(r.is_not_found for r in processed) and
            len(processed) > 0):

            retry_results = await self._retry_with_prefix(
                symbol, periods, interval, failure_threshold
            )
            if retry_results:
                return retry_results

        return processed

    async def _download_single(
        self,
        url: str,
        output_path: Path,
        symbol: str,
        period: Period
    ) -> DownloadResult:
        """Download a single file."""
        try:
            success = await self.exchange.download_file(url, output_path)

            if not success:
                return DownloadResult(
                    success=False,
                    symbol=symbol,
                    data_type=self.strategy.data_type,
                    period=period.value,
                    error='not_found'
                )

            return DownloadResult(
                success=True,
                symbol=symbol,
                data_type=self.strategy.data_type,
                period=period.value,
                file_path=output_path
            )

        except Exception as e:
            logger.error(f"Download error {symbol} {period}: {e}")
            return DownloadResult(
                success=False,
                symbol=symbol,
                data_type=self.strategy.data_type,
                period=period.value,
                error=str(e)
            )

    def _detect_gaps(
        self,
        results: List[DownloadResult],
        threshold: int
    ) -> List[DownloadResult]:
        """Detect and filter gaps (delisting)."""
        if not results:
            return results

        # Find first success
        first_success_idx = -1
        for i, r in enumerate(results):
            if r.success:
                first_success_idx = i
                break

        if first_success_idx == -1:
            return results  # No successes

        # Scan for gaps after first success
        consecutive_failures = 0
        gap_start_idx = -1

        for i in range(first_success_idx + 1, len(results)):
            if results[i].is_not_found:
                consecutive_failures += 1
                if consecutive_failures == 1:
                    gap_start_idx = i
                if consecutive_failures >= threshold:
                    logger.warning(f"Gap detected at index {gap_start_idx}")
                    return results[:gap_start_idx]
            else:
                consecutive_failures = 0

        return results

    async def _retry_with_prefix(
        self,
        symbol: str,
        periods: List[Period],
        interval: Optional[Interval],
        failure_threshold: int
    ) -> Optional[List[DownloadResult]]:
        """Retry download with 1000-prefix."""
        prefixed = f"1000{symbol}"
        logger.info(f"Retrying with {prefixed}...")

        # Build tasks with prefixed symbol
        tasks = []
        for period in periods:
            url = self.strategy.build_download_url(
                base_url=self.exchange.base_url,
                symbol=prefixed,
                period=period,
                interval=interval
            )
            filename = self.strategy.build_temp_filename(
                symbol=prefixed,
                period=period,
                interval=interval
            )
            output_path = self.temp_path / filename

            task = self._download_single(
                url=url,
                output_path=output_path,
                symbol=symbol,  # Store with original
                period=period
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed.append(DownloadResult(
                    success=False,
                    symbol=symbol,
                    data_type=self.strategy.data_type,
                    period=periods[i].value,
                    error=str(result)
                ))
            else:
                processed.append(result)

        # Check if retry succeeded
        if any(r.success for r in processed):
            with _ticker_mappings_lock:
                _ticker_mappings[symbol] = prefixed
            logger.info(f"Auto-discovered: {symbol} → {prefixed}")

            if failure_threshold > 0:
                processed = self._detect_gaps(processed, failure_threshold)

            return processed

        return None
```

**Step 2: Update core __init__.py**

```python
# src/crypto_data/core/__init__.py
"""
Core ingestion components.
"""

from crypto_data.core.importer import DataImporter
from crypto_data.core.downloader import BatchDownloader

__all__ = [
    'DataImporter',
    'BatchDownloader',
]
```

**Step 3: Create tests**

```python
# tests/core/test_downloader.py
"""Tests for BatchDownloader."""

import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from crypto_data.core.downloader import BatchDownloader, _ticker_mappings
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.base import Period, DownloadResult
from crypto_data.exchanges.binance import BinanceExchange
from crypto_data.enums import DataType, Interval


class TestBatchDownloaderGapDetection:
    """Tests for gap detection logic."""

    def test_detect_gaps_no_gap(self):
        """No gap when all successful."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)

        # Mock exchange (won't be used in this test)
        exchange = MagicMock()

        downloader = BatchDownloader(
            strategy=strategy,
            exchange=exchange,
            temp_path=Path('/tmp')
        )

        results = [
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-02'),
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-03'),
        ]

        filtered = downloader._detect_gaps(results, threshold=2)
        assert len(filtered) == 3

    def test_detect_gaps_with_gap(self):
        """Gap detected after consecutive failures."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        exchange = MagicMock()

        downloader = BatchDownloader(
            strategy=strategy,
            exchange=exchange,
            temp_path=Path('/tmp')
        )

        results = [
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-02'),
            DownloadResult(success=False, symbol='BTC', data_type=DataType.SPOT, period='2024-03', error='not_found'),
            DownloadResult(success=False, symbol='BTC', data_type=DataType.SPOT, period='2024-04', error='not_found'),
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-05'),  # Would be ignored
        ]

        filtered = downloader._detect_gaps(results, threshold=2)
        assert len(filtered) == 2  # Only 2024-01 and 2024-02

    def test_detect_gaps_ignores_leading_failures(self):
        """Leading 404s (before token launch) are ignored."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        exchange = MagicMock()

        downloader = BatchDownloader(
            strategy=strategy,
            exchange=exchange,
            temp_path=Path('/tmp')
        )

        results = [
            DownloadResult(success=False, symbol='BTC', data_type=DataType.SPOT, period='2024-01', error='not_found'),
            DownloadResult(success=False, symbol='BTC', data_type=DataType.SPOT, period='2024-02', error='not_found'),
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-03'),
            DownloadResult(success=True, symbol='BTC', data_type=DataType.SPOT, period='2024-04'),
        ]

        filtered = downloader._detect_gaps(results, threshold=2)
        assert len(filtered) == 4  # All kept, leading failures don't trigger gap
```

**Step 4: Run tests**

Run: `pytest tests/core/test_downloader.py -v`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/crypto_data/core/ tests/core/
git commit -m "$(cat <<'EOF'
feat(core): add generic BatchDownloader

- Consolidates 3 download functions into one
- Gap detection (delisting) logic
- 1000-prefix auto-discovery and caching
- Parallel downloads with semaphore control

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 5: Create Strategy Registry and Factory

### Task 5.1: Create strategies/registry.py

**Files:**
- Create: `src/crypto_data/strategies/registry.py`
- Test: `tests/strategies/test_registry.py`

**Step 1: Implement StrategyRegistry**

```python
# src/crypto_data/strategies/registry.py
"""
Strategy registry for data type strategies.

Provides factory methods to create strategies from DataType enums.
"""

from typing import Optional

from crypto_data.strategies.base import DataTypeStrategy
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.strategies.funding_rates import FundingRatesStrategy
from crypto_data.enums import DataType, Interval


def get_strategy(
    data_type: DataType,
    interval: Optional[Interval] = None
) -> DataTypeStrategy:
    """
    Get the appropriate strategy for a data type.

    Parameters
    ----------
    data_type : DataType
        The data type to get a strategy for
    interval : Interval, optional
        Required for SPOT and FUTURES data types

    Returns
    -------
    DataTypeStrategy
        The appropriate strategy instance

    Raises
    ------
    ValueError
        If data type is unknown or interval is missing for klines
    """
    if data_type == DataType.SPOT:
        if interval is None:
            raise ValueError("interval is required for SPOT data type")
        return KlinesStrategy(DataType.SPOT, interval)

    elif data_type == DataType.FUTURES:
        if interval is None:
            raise ValueError("interval is required for FUTURES data type")
        return KlinesStrategy(DataType.FUTURES, interval)

    elif data_type == DataType.OPEN_INTEREST:
        return OpenInterestStrategy()

    elif data_type == DataType.FUNDING_RATES:
        return FundingRatesStrategy()

    else:
        raise ValueError(f"Unknown data type: {data_type}")
```

**Step 2: Create tests**

```python
# tests/strategies/test_registry.py
"""Tests for strategy registry."""

import pytest

from crypto_data.strategies.registry import get_strategy
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.strategies.funding_rates import FundingRatesStrategy
from crypto_data.enums import DataType, Interval


class TestGetStrategy:
    """Tests for get_strategy factory function."""

    def test_spot_strategy(self):
        strategy = get_strategy(DataType.SPOT, Interval.MIN_5)
        assert isinstance(strategy, KlinesStrategy)
        assert strategy.data_type == DataType.SPOT
        assert strategy.interval == Interval.MIN_5

    def test_futures_strategy(self):
        strategy = get_strategy(DataType.FUTURES, Interval.HOUR_1)
        assert isinstance(strategy, KlinesStrategy)
        assert strategy.data_type == DataType.FUTURES

    def test_open_interest_strategy(self):
        strategy = get_strategy(DataType.OPEN_INTEREST)
        assert isinstance(strategy, OpenInterestStrategy)

    def test_funding_rates_strategy(self):
        strategy = get_strategy(DataType.FUNDING_RATES)
        assert isinstance(strategy, FundingRatesStrategy)

    def test_spot_without_interval_raises(self):
        with pytest.raises(ValueError, match="interval is required"):
            get_strategy(DataType.SPOT)

    def test_futures_without_interval_raises(self):
        with pytest.raises(ValueError, match="interval is required"):
            get_strategy(DataType.FUTURES)
```

**Step 3: Run tests**

Run: `pytest tests/strategies/test_registry.py -v`
Expected: All tests PASS

**Step 4: Update strategies __init__.py**

```python
# src/crypto_data/strategies/__init__.py
"""
Data Type Strategies
"""

from crypto_data.strategies.base import DataTypeStrategy, DownloadResult, Period
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.strategies.funding_rates import FundingRatesStrategy
from crypto_data.strategies.registry import get_strategy

__all__ = [
    'DataTypeStrategy',
    'DownloadResult',
    'Period',
    'KlinesStrategy',
    'OpenInterestStrategy',
    'FundingRatesStrategy',
    'get_strategy',
]
```

**Step 5: Commit**

```bash
git add src/crypto_data/strategies/ tests/strategies/
git commit -m "$(cat <<'EOF'
feat(strategies): add strategy registry and factory

- get_strategy() creates appropriate strategy from DataType
- Validates interval requirement for klines
- Simplifies strategy instantiation

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 6: Refactor Main Ingestion Function

### Task 6.1: Create core/orchestrator.py

**Files:**
- Create: `src/crypto_data/core/orchestrator.py`
- Update: `src/crypto_data/core/__init__.py`
- Test: `tests/core/test_orchestrator.py`

**Step 1: Implement ingest_binance_async using strategies**

```python
# src/crypto_data/core/orchestrator.py
"""
Main ingestion orchestrator.

Provides the refactored ingest_binance_async function using strategy pattern.
"""

import logging
import tempfile
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.strategies.registry import get_strategy
from crypto_data.strategies.base import DownloadResult
from crypto_data.core.downloader import BatchDownloader
from crypto_data.core.importer import DataImporter
from crypto_data.exchanges.binance import BinanceExchange
from crypto_data.utils.ingestion_helpers import (
    initialize_ingestion_stats,
    log_ingestion_summary
)

logger = logging.getLogger(__name__)


def _validate_and_parse_dates(start_date: str, end_date: str):
    """Validate and parse date strings."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid start_date format: {start_date}") from e

    try:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid end_date format: {end_date}") from e

    if start > end:
        raise ValueError(f"start_date must be before end_date")

    return start, end


def _process_results(
    results: List[DownloadResult],
    importer: DataImporter,
    conn,
    stats: Dict[str, int],
    symbol: str
) -> None:
    """Process download results: import to DB, update stats, cleanup."""
    for result in results:
        if result.success:
            try:
                importer.import_file(
                    conn=conn,
                    file_path=result.file_path,
                    symbol=symbol,
                    exchange='binance'
                )
                stats['downloaded'] += 1
                logger.debug(f"Imported {result.period}")

                # Cleanup
                if result.file_path and result.file_path.exists():
                    result.file_path.unlink()

            except Exception as e:
                logger.error(f"Import failed {result.period}: {e}")
                stats['failed'] += 1
        else:
            if result.is_not_found:
                stats['not_found'] += 1
            else:
                stats['failed'] += 1


async def _ingest_symbol_data_type(
    symbol: str,
    data_type: DataType,
    start: datetime,
    end: datetime,
    interval: Optional[Interval],
    temp_path: Path,
    conn,
    stats: Dict[str, int],
    max_concurrent: int,
    failure_threshold: int
) -> None:
    """Ingest a single symbol + data_type combination."""
    # Get strategy for this data type
    strategy = get_strategy(data_type, interval)

    # Generate periods
    periods = strategy.generate_periods(start, end)
    if not periods:
        return

    logger.info(f"Downloading {symbol} {data_type.value}...")

    # Create downloader and importer
    async with BinanceExchange(max_concurrent=max_concurrent) as exchange:
        downloader = BatchDownloader(
            strategy=strategy,
            exchange=exchange,
            temp_path=temp_path,
            max_concurrent=max_concurrent
        )

        # Download all periods
        results = await downloader.download_symbol(
            symbol=symbol,
            periods=periods,
            interval=interval,
            failure_threshold=failure_threshold
        )

    # Import results within transaction
    importer = DataImporter(strategy)
    committed = False
    try:
        conn.execute("BEGIN TRANSACTION")
        _process_results(results, importer, conn, stats, symbol)
        conn.execute("COMMIT")
        committed = True
    except Exception as e:
        if not committed:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
        logger.error(f"Failed to import {symbol} {data_type}: {e}")


def ingest_binance_async_v2(
    db_path: str,
    symbols: List[str],
    data_types: List[DataType],
    start_date: str,
    end_date: str,
    interval: Interval = Interval.MIN_5,
    skip_existing: bool = True,
    max_concurrent_klines: int = 20,
    max_concurrent_metrics: int = 100,
    max_concurrent_funding: int = 50,
    failure_threshold: int = 3
):
    """
    Download Binance data asynchronously using strategy pattern.

    This is the refactored version of ingest_binance_async.

    Parameters
    ----------
    db_path : str
        Path to DuckDB database
    symbols : List[str]
        Symbols to download
    data_types : List[DataType]
        Data types to download
    start_date : str
        Start date (YYYY-MM-DD)
    end_date : str
        End date (YYYY-MM-DD)
    interval : Interval
        Kline interval (default: MIN_5)
    skip_existing : bool
        Skip existing data (default: True)
    max_concurrent_klines : int
        Concurrency for klines (default: 20)
    max_concurrent_metrics : int
        Concurrency for metrics (default: 100)
    max_concurrent_funding : int
        Concurrency for funding (default: 50)
    failure_threshold : int
        Gap detection threshold (default: 3)
    """
    start, end = _validate_and_parse_dates(start_date, end_date)

    db = CryptoDatabase(db_path)
    conn = db.conn
    stats = initialize_ingestion_stats()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        for symbol in symbols:
            for data_type in data_types:
                # Select concurrency based on data type
                if data_type in (DataType.SPOT, DataType.FUTURES):
                    max_concurrent = max_concurrent_klines
                elif data_type == DataType.OPEN_INTEREST:
                    max_concurrent = max_concurrent_metrics
                else:
                    max_concurrent = max_concurrent_funding

                # Run async ingestion
                asyncio.run(
                    _ingest_symbol_data_type(
                        symbol=symbol,
                        data_type=data_type,
                        start=start,
                        end=end,
                        interval=interval if data_type in (DataType.SPOT, DataType.FUTURES) else None,
                        temp_path=temp_path,
                        conn=conn,
                        stats=stats,
                        max_concurrent=max_concurrent,
                        failure_threshold=failure_threshold
                    )
                )

    db.close()

    log_ingestion_summary(
        stats=stats,
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        show_availability=True
    )
```

**Step 2: Update core __init__.py**

```python
# src/crypto_data/core/__init__.py
"""
Core ingestion components.
"""

from crypto_data.core.importer import DataImporter
from crypto_data.core.downloader import BatchDownloader
from crypto_data.core.orchestrator import ingest_binance_async_v2

__all__ = [
    'DataImporter',
    'BatchDownloader',
    'ingest_binance_async_v2',
]
```

**Step 3: Create integration tests**

```python
# tests/core/test_orchestrator.py
"""Tests for orchestrator functions."""

import pytest
from datetime import datetime

from crypto_data.core.orchestrator import _validate_and_parse_dates


class TestValidateAndParseDates:
    """Tests for date validation."""

    def test_valid_dates(self):
        start, end = _validate_and_parse_dates('2024-01-01', '2024-12-31')
        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 12, 31)

    def test_invalid_start_date(self):
        with pytest.raises(ValueError, match="Invalid start_date"):
            _validate_and_parse_dates('invalid', '2024-12-31')

    def test_invalid_end_date(self):
        with pytest.raises(ValueError, match="Invalid end_date"):
            _validate_and_parse_dates('2024-01-01', 'invalid')

    def test_start_after_end(self):
        with pytest.raises(ValueError, match="start_date must be before"):
            _validate_and_parse_dates('2024-12-31', '2024-01-01')
```

**Step 4: Run tests**

Run: `pytest tests/core/test_orchestrator.py -v`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/crypto_data/core/ tests/core/
git commit -m "$(cat <<'EOF'
feat(core): add orchestrator with ingest_binance_async_v2

- Refactored ingestion using strategy pattern
- Generic result processing (replaces 3 duplicates)
- Clean separation of download/import phases
- Maintains backward-compatible interface

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 7: Wire Up and Deprecate Old Code

### Task 7.1: Update Public API

**Files:**
- Modify: `src/crypto_data/__init__.py`

**Step 1: Add new exports alongside old ones**

```python
# Update src/crypto_data/__init__.py to include new components

# Add imports for new architecture
from .strategies import (
    DataTypeStrategy,
    KlinesStrategy,
    OpenInterestStrategy,
    FundingRatesStrategy,
    get_strategy,
)
from .exchanges import (
    ExchangeClient,
    BinanceExchange,
)
from .core import (
    DataImporter,
    BatchDownloader,
    ingest_binance_async_v2,
)

# Update __all__ to include new exports
# (Keep old exports for backward compatibility)
```

**Step 2: Commit**

```bash
git add src/crypto_data/__init__.py
git commit -m "$(cat <<'EOF'
feat(api): expose new strategy pattern components

- Add strategies, exchanges, core to public API
- Keep old ingestion functions for backward compatibility
- New components available for advanced usage

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7.2: Run Full Test Suite

**Step 1: Run all tests**

Run: `pytest tests/ -v --tb=short`
Expected: All tests PASS (both old and new)

**Step 2: Commit if any fixes needed**

```bash
git add -A
git commit -m "$(cat <<'EOF'
fix: ensure all tests pass after refactoring

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Phase 8: Integration and Migration

### Task 8.1: Create Migration Guide

**Files:**
- Create: `docs/migration-to-strategy-pattern.md`

**Step 1: Write migration documentation**

Document how to use the new architecture:
- New imports
- Strategy usage
- Custom exchange implementation
- Backward compatibility notes

**Step 2: Commit**

```bash
git add docs/
git commit -m "$(cat <<'EOF'
docs: add migration guide for strategy pattern

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8.2: Final Integration Test

**Step 1: Run complete integration test**

Run: `pytest tests/ -v --tb=short`
Expected: All tests PASS

**Step 2: Run type checking (if available)**

Run: `mypy src/crypto_data/ --ignore-missing-imports`
Expected: No errors (or only known issues)

**Step 3: Final commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat: complete strategy pattern refactoring

Architecture improvements:
- DataTypeStrategy ABC for pluggable data types
- ExchangeClient ABC for pluggable exchanges
- Generic DataImporter consolidates 3 import functions
- Generic BatchDownloader consolidates 3 download functions
- Strategy registry for easy instantiation

Benefits:
- Adding new exchange = 1 file
- Adding new data type = 1 strategy file
- ~40% less duplicated code
- Clearer separation of concerns

Backward compatible: old API still works.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Summary

### Files Created

```
src/crypto_data/
├── strategies/
│   ├── __init__.py
│   ├── base.py           # DataTypeStrategy ABC, Period, DownloadResult
│   ├── klines.py         # KlinesStrategy (spot/futures)
│   ├── open_interest.py  # OpenInterestStrategy
│   ├── funding_rates.py  # FundingRatesStrategy
│   └── registry.py       # get_strategy() factory
│
├── exchanges/
│   ├── __init__.py
│   ├── base.py           # ExchangeClient ABC
│   └── binance.py        # BinanceExchange
│
└── core/
    ├── __init__.py
    ├── importer.py       # Generic DataImporter
    ├── downloader.py     # Generic BatchDownloader
    └── orchestrator.py   # ingest_binance_async_v2

tests/
├── strategies/
│   ├── __init__.py
│   ├── test_base_strategy.py
│   ├── test_klines_strategy.py
│   ├── test_open_interest_strategy.py
│   ├── test_funding_rates_strategy.py
│   └── test_registry.py
│
├── exchanges/
│   ├── __init__.py
│   ├── test_base_exchange.py
│   └── test_binance_exchange.py
│
└── core/
    ├── __init__.py
    ├── test_importer.py
    ├── test_downloader.py
    └── test_orchestrator.py

docs/plans/
└── 2026-02-03-strategy-pattern-refactor.md
```

### Estimated Metrics

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| ingestion.py LOC | 1,576 | ~200 (orchestrator only) | -87% |
| utils/database.py LOC | 496 | ~100 (data_exists only) | -80% |
| Total duplicated code | ~500 LOC | ~50 LOC | -90% |
| Files to add new exchange | 5+ | 1 | -80% |
| Files to add new data type | 3+ | 1 | -67% |

### Backward Compatibility

- Old `ingest_binance_async()` remains functional
- Old imports continue to work
- New `ingest_binance_async_v2()` available for new code
- Migration can be gradual

---

**Plan complete and saved to `docs/plans/2026-02-03-strategy-pattern-refactor.md`.**

Two execution options:

1. **Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

2. **Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
