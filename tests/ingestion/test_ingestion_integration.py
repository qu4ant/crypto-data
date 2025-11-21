"""
Integration tests for ingestion.py module.

Tests the main ingestion functions with proper mocking for CI/CD compatibility:
- ingest_universe() - CoinMarketCap ingestion with transactions
- ingest_binance_async() - Async Binance ingestion
- _download_single_month() - Single file async download
- _download_symbol_data_type_async() - Parallel downloads with gap detection
- Transaction safety and 1000-prefix auto-discovery

All tests are GitHub Actions compatible:
- No real network calls (mocked aiohttp/requests)
- Temporary in-memory databases
- No sleep() or long timeouts
- Fully parallelizable
"""

import pytest
from crypto_data.enums import DataType, Interval
import tempfile
import asyncio
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch, AsyncMock, call
import pandas as pd

from crypto_data.ingestion import (
    ingest_universe,
    ingest_binance_async,
    _download_single_month,
    _download_symbol_data_type_async
)
from crypto_data.database import CryptoDatabase


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')
        db = CryptoDatabase(db_path)
        yield db_path
        db.close()


@pytest.fixture
def mock_cmc_client():
    """Mock CoinMarketCapClient for testing."""
    with patch('crypto_data.ingestion.CoinMarketCapClient') as mock_client:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.__aexit__.return_value = None
        mock_client.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_binance_client():
    """Mock BinanceDataVisionClientAsync for testing."""
    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync') as mock_client:
        yield mock_client


# =============================================================================
# Section 1: ingest_universe() tests
# =============================================================================

def test_ingest_universe_fetches_and_stores_snapshot(temp_db, mock_cmc_client):
    """Test that universe snapshot is fetched from CMC and stored in database."""
    # Mock CMC API response
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[
        {
            'symbol': 'BTC',
            'cmcRank': 1,
            'tags': ['mineable'],
            'quotes': [{'marketCap': 500000000000}]
        },
        {
            'symbol': 'ETH',
            'cmcRank': 2,
            'tags': ['smart-contracts'],
            'quotes': [{'marketCap': 200000000000}]
        }
    ])

    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Verify data was stored
    db = CryptoDatabase(temp_db)
    result = db.execute("SELECT * FROM crypto_universe ORDER BY rank").fetchall()
    db.close()

    assert len(result) == 2
    assert result[0][1] == 'BTC'  # symbol
    assert result[0][2] == 1      # rank
    assert result[1][1] == 'ETH'
    assert result[1][2] == 2


def test_ingest_universe_filters_excluded_tags(temp_db, mock_cmc_client):
    """Test that coins with excluded tags are filtered out."""
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[
        {
            'symbol': 'BTC',
            'cmcRank': 1,
            'tags': ['mineable'],
            'quotes': [{'marketCap': 500000000000}]
        },
        {
            'symbol': 'USDT',
            'cmcRank': 3,
            'tags': ['stablecoin'],  # Should be filtered
            'quotes': [{'marketCap': 80000000000}]
        }
    ])

    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=['stablecoin'], exclude_symbols=[]))

    # Verify USDT was filtered
    db = CryptoDatabase(temp_db)
    result = db.execute("SELECT symbol FROM crypto_universe").fetchall()
    db.close()

    symbols = [row[0] for row in result]
    assert 'BTC' in symbols
    assert 'USDT' not in symbols


def test_ingest_universe_filters_excluded_symbols(temp_db, mock_cmc_client):
    """Test that blacklisted symbols are filtered out."""
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[
        {
            'symbol': 'BTC',
            'cmcRank': 1,
            'tags': [],
            'quotes': [{'marketCap': 500000000000}]
        },
        {
            'symbol': 'FTT',
            'cmcRank': 25,
            'tags': [],
            'quotes': [{'marketCap': 1000000000}]
        }
    ])

    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=['FTT', 'LUNA']))

    # Verify FTT was filtered
    db = CryptoDatabase(temp_db)
    result = db.execute("SELECT symbol FROM crypto_universe").fetchall()
    db.close()

    symbols = [row[0] for row in result]
    assert 'BTC' in symbols
    assert 'FTT' not in symbols


def test_ingest_universe_atomic_transaction_commits(temp_db, mock_cmc_client):
    """Test that transaction commits on success."""
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[
        {
            'symbol': 'BTC',
            'cmcRank': 1,
            'tags': [],
            'quotes': [{'marketCap': 500000000000}]
        }
    ])

    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Verify data persisted (transaction committed)
    db = CryptoDatabase(temp_db)
    count = db.execute("SELECT COUNT(*) FROM crypto_universe").fetchone()[0]
    db.close()

    assert count == 1


def test_ingest_universe_transaction_rolls_back_on_error(temp_db, mock_cmc_client):
    """Test that failed API calls don't insert data (batch version logs and continues)."""
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[
        {
            'symbol': 'BTC',
            'cmcRank': 1,
            'tags': [],
            'quotes': [{'marketCap': 500000000000}]
        }
    ])

    # First insert succeeds
    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Verify data was inserted
    db = CryptoDatabase(temp_db)
    count = db.execute("SELECT COUNT(*) FROM crypto_universe").fetchone()[0]
    db.close()
    assert count == 1

    # Simulate error during fetch (before transaction)
    # Batch version logs error and continues (doesn't raise)
    mock_cmc_client.get_historical_listings = AsyncMock(side_effect=Exception("API error"))

    # Should not raise - batch version continues on error
    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Original data should still be there (no new data inserted due to error)
    db = CryptoDatabase(temp_db)
    count = db.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()[0]
    db.close()

    assert count == 1  # Still just the original data


def test_ingest_universe_replaces_existing_date(temp_db, mock_cmc_client):
    """Test that existing data for a date is replaced (DELETE + INSERT)."""
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[
        {
            'symbol': 'BTC',
            'cmcRank': 1,
            'tags': [],
            'quotes': [{'marketCap': 500000000000}]
        }
    ])

    # First insert
    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Update mock to return different data
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[
        {
            'symbol': 'ETH',
            'cmcRank': 1,
            'tags': [],
            'quotes': [{'marketCap': 200000000000}]
        }
    ])

    # Second insert (same date)
    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Verify only ETH exists (BTC was deleted)
    db = CryptoDatabase(temp_db)
    result = db.execute("SELECT symbol FROM crypto_universe WHERE date = '2024-01-01'").fetchall()
    db.close()

    symbols = [row[0] for row in result]
    assert symbols == ['ETH']
    assert 'BTC' not in symbols


def test_ingest_universe_handles_api_failure(temp_db, mock_cmc_client):
    """Test that API failures are handled gracefully (batch version logs and continues)."""
    mock_cmc_client.get_historical_listings = AsyncMock(side_effect=Exception("API error"))

    # Batch version doesn't raise - it logs error and continues
    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Verify no partial data was stored
    db = CryptoDatabase(temp_db)
    count = db.execute("SELECT COUNT(*) FROM crypto_universe").fetchone()[0]
    db.close()

    assert count == 0


def test_ingest_universe_handles_empty_response(temp_db, mock_cmc_client):
    """Test that empty API responses are handled."""
    mock_cmc_client.get_historical_listings = AsyncMock(return_value=[])

    asyncio.run(ingest_universe(temp_db, months=['2024-01'], top_n=100, exclude_tags=[], exclude_symbols=[]))

    # Verify no data stored
    db = CryptoDatabase(temp_db)
    count = db.execute("SELECT COUNT(*) FROM crypto_universe").fetchone()[0]
    db.close()

    assert count == 0


# =============================================================================
# Section 2: Async download tests
# =============================================================================

@pytest.mark.asyncio
async def test_download_single_month_success():
    """Test successful single month download."""
    mock_client = AsyncMock()
    mock_client.download_klines = AsyncMock(return_value=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        result = await _download_single_month(
            client=mock_client,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            month='2024-01',
            interval=Interval.MIN_5,
            temp_path=temp_path,
            progress_info={'total': 1}
        )

    assert result['success'] is True
    assert result['symbol'] == 'BTCUSDT'
    assert result['data_type'] == 'spot'
    assert result['month'] == '2024-01'
    assert result['error'] is None


@pytest.mark.asyncio
async def test_download_single_month_404_returns_not_found():
    """Test that 404 returns not_found error."""
    mock_client = AsyncMock()
    mock_client.download_klines = AsyncMock(return_value=False)  # 404

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        result = await _download_single_month(
            client=mock_client,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            month='2024-01',
            interval=Interval.MIN_5,
            temp_path=temp_path,
            progress_info={'total': 1}
        )

    assert result['success'] is False
    assert result['error'] == 'not_found'


@pytest.mark.asyncio
async def test_download_single_month_network_error():
    """Test that network errors are captured."""
    mock_client = AsyncMock()
    mock_client.download_klines = AsyncMock(side_effect=Exception("Network timeout"))

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        result = await _download_single_month(
            client=mock_client,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            month='2024-01',
            interval=Interval.MIN_5,
            temp_path=temp_path,
            progress_info={'total': 1}
        )

    assert result['success'] is False
    assert 'Network timeout' in result['error']


@pytest.mark.asyncio
async def test_download_symbol_parallel_downloads(temp_db):
    """Test that multiple months are downloaded in parallel."""
    mock_client_instance = AsyncMock()
    mock_client_instance.download_klines = AsyncMock(return_value=True)
    mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
    mock_client_instance.__aexit__ = AsyncMock(return_value=None)

    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync', return_value=mock_client_instance), \
         patch('crypto_data.ingestion.data_exists', return_value=False), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02', '2024-03'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=False,
            stats=stats,
            max_concurrent=20,
            failure_threshold=3
        )

        db.close()

    assert len(results) == 3
    assert all(r['success'] for r in results)


@pytest.mark.asyncio
async def test_download_symbol_skips_existing_data(temp_db):
    """Test that existing data is skipped."""
    with patch('crypto_data.ingestion.data_exists', return_value=True), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=True,
            stats=stats,
            max_concurrent=20,
            failure_threshold=3
        )

        db.close()

    assert len(results) == 0  # All skipped
    assert stats['skipped'] == 2


@pytest.mark.asyncio
async def test_download_symbol_respects_max_concurrent(temp_db):
    """Test that max_concurrent limit is respected."""
    mock_client_instance = AsyncMock()
    mock_client_instance.download_klines = AsyncMock(return_value=True)
    mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
    mock_client_instance.__aexit__ = AsyncMock(return_value=None)

    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync') as mock_client, \
         patch('crypto_data.ingestion.data_exists', return_value=False), \
         tempfile.TemporaryDirectory() as tmpdir:

        mock_client.return_value = mock_client_instance
        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        await _download_symbol_data_type_async(
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=False,
            stats=stats,
            max_concurrent=5,
            failure_threshold=3
        )

        db.close()

    # Verify client was created with max_concurrent
    mock_client.assert_called_with(max_concurrent=5)


@pytest.mark.asyncio
async def test_download_symbol_handles_mixed_results(temp_db):
    """Test handling of mixed success/failure results."""
    call_count = [0]

    async def mock_download(*args, **kwargs):
        call_count[0] += 1
        return call_count[0] % 2 == 1  # Alternate success/failure

    mock_client_instance = AsyncMock()
    mock_client_instance.download_klines = mock_download
    mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
    mock_client_instance.__aexit__ = AsyncMock(return_value=None)

    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync', return_value=mock_client_instance), \
         patch('crypto_data.ingestion.data_exists', return_value=False), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02', '2024-03', '2024-04'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=False,
            stats=stats,
            max_concurrent=20,
            failure_threshold=10  # High threshold to not trigger gap detection
        )

        db.close()

    successes = sum(1 for r in results if r['success'])
    failures = sum(1 for r in results if not r['success'])

    assert successes == 2
    assert failures == 2


@pytest.mark.asyncio
async def test_download_symbol_empty_results_when_all_exist(temp_db):
    """Test that empty results are returned when all data exists."""
    with patch('crypto_data.ingestion.data_exists', return_value=True), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            months=['2024-01'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=True,
            stats=stats,
            max_concurrent=20,
            failure_threshold=3
        )

        db.close()

    assert results == []


# =============================================================================
# Section 3: Gap detection tests
# =============================================================================

@pytest.mark.asyncio
async def test_gap_detection_stops_at_threshold(temp_db):
    """Test that gap detection stops downloading after N consecutive 404s."""
    call_count = [0]

    async def mock_download(*args, **kwargs):
        call_count[0] += 1
        # First 2 succeed, then 3 consecutive failures
        return call_count[0] <= 2

    mock_client_instance = AsyncMock()
    mock_client_instance.download_klines = mock_download
    mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
    mock_client_instance.__aexit__ = AsyncMock(return_value=None)

    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync', return_value=mock_client_instance), \
         patch('crypto_data.ingestion.data_exists', return_value=False), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='FTTUSDT',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=False,
            stats=stats,
            max_concurrent=20,
            failure_threshold=3  # Stop after 3 consecutive failures
        )

        db.close()

    # Should only have first 2 successes (rest filtered by gap detection)
    assert len(results) == 2
    assert all(r['success'] for r in results)


@pytest.mark.asyncio
async def test_gap_detection_ignores_leading_failures(temp_db):
    """Test that leading 404s (before launch) don't trigger gap detection."""
    call_count = [0]

    async def mock_download(*args, **kwargs):
        call_count[0] += 1
        # First 3 fail (before launch), then successes
        return call_count[0] > 3

    mock_client_instance = AsyncMock()
    mock_client_instance.download_klines = mock_download
    mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
    mock_client_instance.__aexit__ = AsyncMock(return_value=None)

    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync', return_value=mock_client_instance), \
         patch('crypto_data.ingestion.data_exists', return_value=False), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='NEWTOKEN',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02', '2024-03', '2024-04', '2024-05'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=False,
            stats=stats,
            max_concurrent=20,
            failure_threshold=3
        )

        db.close()

    # Should have all results (leading failures don't trigger gap detection)
    assert len(results) == 5
    successes = sum(1 for r in results if r['success'])
    assert successes == 2  # Last 2 months


@pytest.mark.asyncio
async def test_gap_detection_resets_on_success(temp_db):
    """Test that consecutive failure counter resets on success."""
    call_count = [0]

    async def mock_download(*args, **kwargs):
        call_count[0] += 1
        # Pattern: success, 2 fails, success, 2 fails
        # Should NOT trigger gap (threshold=3)
        return call_count[0] in [1, 4]

    mock_client_instance = AsyncMock()
    mock_client_instance.download_klines = mock_download
    mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
    mock_client_instance.__aexit__ = AsyncMock(return_value=None)

    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync', return_value=mock_client_instance), \
         patch('crypto_data.ingestion.data_exists', return_value=False), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02', '2024-03', '2024-04', '2024-05'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=False,
            stats=stats,
            max_concurrent=20,
            failure_threshold=3
        )

        db.close()

    # Should have all 5 results (no gap detected - counter reset on success)
    assert len(results) == 5


@pytest.mark.asyncio
async def test_gap_detection_disabled_when_threshold_zero(temp_db):
    """Test that gap detection is disabled when threshold=0."""
    call_count = [0]

    async def mock_download(*args, **kwargs):
        call_count[0] += 1
        # Only first succeeds, rest fail
        return call_count[0] == 1

    mock_client_instance = AsyncMock()
    mock_client_instance.download_klines = mock_download
    mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
    mock_client_instance.__aexit__ = AsyncMock(return_value=None)

    with patch('crypto_data.ingestion.BinanceDataVisionClientAsync', return_value=mock_client_instance), \
         patch('crypto_data.ingestion.data_exists', return_value=False), \
         tempfile.TemporaryDirectory() as tmpdir:

        temp_path = Path(tmpdir)
        db = CryptoDatabase(temp_db)
        stats = {'skipped': 0}

        results = await _download_symbol_data_type_async(
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            months=['2024-01', '2024-02', '2024-03', '2024-04'],
            interval=Interval.MIN_5,
            temp_path=temp_path,
            conn=db.conn,
            skip_existing=False,
            stats=stats,
            max_concurrent=20,
            failure_threshold=0  # Disabled
        )

        db.close()

    # Should have all 4 results (gap detection disabled)
    assert len(results) == 4


# =============================================================================
# Section 4: 1000-prefix auto-discovery tests
# =============================================================================

def test_1000prefix_retry_on_all_404s(temp_db):
    """Test that 1000-prefix retry is triggered when all futures downloads 404."""
    from crypto_data.ingestion import _ticker_mappings

    # Clear cache to ensure clean test state
    _ticker_mappings.clear()

    with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
         patch('crypto_data.ingestion.process_download_results'), \
         patch('crypto_data.ingestion.initialize_ingestion_stats') as mock_stats, \
         patch('crypto_data.ingestion.log_ingestion_summary'), \
         patch('crypto_data.ingestion.generate_month_list', return_value=['2024-01']), \
         patch('crypto_data.ingestion.CryptoDatabase') as mock_db:

        mock_stats.return_value = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'not_found': 0}
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        # First call returns all 404s, second call (retry) succeeds
        mock_run.side_effect = [
            [{'success': False, 'error': 'not_found', 'symbol': 'PEPEUSDT', 'data_type': DataType.FUTURES.value, 'month': '2024-01', 'file_path': None}],
            [{'success': True, 'error': None, 'symbol': '1000PEPEUSDT', 'data_type': DataType.FUTURES.value, 'month': '2024-01', 'file_path': Path('/tmp/test.zip')}]
        ]

        ingest_binance_async(
            db_path=temp_db,
            symbols=['PEPEUSDT'],
            data_types=[DataType.FUTURES],
            start_date='2024-01-01',
            end_date='2024-01-31',
            interval=Interval.MIN_5
        )

    # Verify two calls: original + retry
    assert mock_run.call_count == 2


def test_1000prefix_cache_persists_across_symbols(temp_db):
    """Test that 1000-prefix mapping is cached and reused."""
    from crypto_data.ingestion import _ticker_mappings

    # Clear cache
    _ticker_mappings.clear()

    with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
         patch('crypto_data.ingestion.process_download_results'), \
         patch('crypto_data.ingestion.initialize_ingestion_stats') as mock_stats, \
         patch('crypto_data.ingestion.log_ingestion_summary'), \
         patch('crypto_data.ingestion.generate_month_list', return_value=['2024-01']), \
         patch('crypto_data.ingestion.CryptoDatabase') as mock_db:

        mock_stats.return_value = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'not_found': 0}
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        # First symbol: all 404s, retry succeeds
        mock_run.side_effect = [
            [{'success': False, 'error': 'not_found', 'symbol': 'PEPEUSDT', 'data_type': DataType.FUTURES.value, 'month': '2024-01', 'file_path': None}],
            [{'success': True, 'error': None, 'symbol': '1000PEPEUSDT', 'data_type': DataType.FUTURES.value, 'month': '2024-01', 'file_path': Path('/tmp/test.zip')}],
            # Second symbol: uses cached mapping (only 1 call)
            [{'success': True, 'error': None, 'symbol': '1000PEPEUSDT', 'data_type': DataType.FUTURES.value, 'month': '2024-01', 'file_path': Path('/tmp/test2.zip')}]
        ]

        ingest_binance_async(
            db_path=temp_db,
            symbols=['PEPEUSDT', 'PEPEUSDT'],  # Same symbol twice
            data_types=[DataType.FUTURES],
            start_date='2024-01-01',
            end_date='2024-01-31',
            interval=Interval.MIN_5
        )

    # Verify cache was used: first symbol = 2 calls, second symbol = 1 call (cached)
    assert mock_run.call_count == 3
    assert _ticker_mappings.get('PEPEUSDT') == '1000PEPEUSDT'


def test_1000prefix_skips_retry_if_cached(temp_db):
    """Test that retry is skipped if mapping is already cached."""
    from crypto_data.ingestion import _ticker_mappings

    # Pre-populate cache
    _ticker_mappings['PEPEUSDT'] = '1000PEPEUSDT'

    with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
         patch('crypto_data.ingestion.process_download_results'), \
         patch('crypto_data.ingestion.initialize_ingestion_stats') as mock_stats, \
         patch('crypto_data.ingestion.log_ingestion_summary'), \
         patch('crypto_data.ingestion.generate_month_list', return_value=['2024-01']), \
         patch('crypto_data.ingestion.CryptoDatabase') as mock_db:

        mock_stats.return_value = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'not_found': 0}
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        # Only one call (uses cached mapping)
        mock_run.return_value = [{'success': True, 'error': None, 'symbol': '1000PEPEUSDT', 'data_type': DataType.FUTURES.value, 'month': '2024-01', 'file_path': Path('/tmp/test.zip')}]

        ingest_binance_async(
            db_path=temp_db,
            symbols=['PEPEUSDT'],
            data_types=[DataType.FUTURES],
            start_date='2024-01-01',
            end_date='2024-01-31',
            interval=Interval.MIN_5
        )

    # Verify only one call (no retry)
    assert mock_run.call_count == 1


# =============================================================================
# Section 5: Transaction safety tests
# =============================================================================

def test_binance_import_commits_on_success(temp_db):
    """Test that transaction commits when import succeeds."""
    with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
         patch('crypto_data.ingestion.process_download_results'), \
         patch('crypto_data.ingestion.initialize_ingestion_stats') as mock_stats, \
         patch('crypto_data.ingestion.log_ingestion_summary'), \
         patch('crypto_data.ingestion.generate_month_list', return_value=['2024-01']), \
         patch('crypto_data.ingestion.CryptoDatabase') as mock_db:

        mock_stats.return_value = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'not_found': 0}
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        mock_run.return_value = [{'success': True, 'error': None, 'symbol': 'BTCUSDT', 'data_type': DataType.SPOT.value, 'month': '2024-01', 'file_path': Path('/tmp/test.zip')}]

        ingest_binance_async(
            db_path=temp_db,
            symbols=['BTCUSDT'],
            data_types=[DataType.SPOT],
            start_date='2024-01-01',
            end_date='2024-01-31',
            interval=Interval.MIN_5
        )

    # Verify BEGIN and COMMIT were called
    conn_execute_calls = [str(call) for call in mock_db_instance.conn.execute.call_args_list]
    assert any('BEGIN TRANSACTION' in str(call) for call in conn_execute_calls)
    assert any('COMMIT' in str(call) for call in conn_execute_calls)


def test_binance_import_rolls_back_on_error(temp_db):
    """Test that transaction rolls back when import fails."""
    with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
         patch('crypto_data.ingestion.process_download_results') as mock_process, \
         patch('crypto_data.ingestion.initialize_ingestion_stats') as mock_stats, \
         patch('crypto_data.ingestion.log_ingestion_summary'), \
         patch('crypto_data.ingestion.generate_month_list', return_value=['2024-01']), \
         patch('crypto_data.ingestion.CryptoDatabase') as mock_db:

        mock_stats.return_value = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'not_found': 0}
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        mock_run.return_value = [{'success': True, 'error': None, 'symbol': 'BTCUSDT', 'data_type': DataType.SPOT.value, 'month': '2024-01', 'file_path': Path('/tmp/test.zip')}]

        # Simulate import error
        mock_process.side_effect = Exception("Import failed")

        ingest_binance_async(
            db_path=temp_db,
            symbols=['BTCUSDT'],
            data_types=[DataType.SPOT],
            start_date='2024-01-01',
            end_date='2024-01-31',
            interval=Interval.MIN_5
        )

    # Verify ROLLBACK was called
    conn_execute_calls = [str(call) for call in mock_db_instance.conn.execute.call_args_list]
    assert any('ROLLBACK' in str(call) for call in conn_execute_calls)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
