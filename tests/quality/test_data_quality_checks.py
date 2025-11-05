"""
Tests for data quality checking script

Tests each quality check with synthetic bad data to ensure violations are detected.
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

# Add scripts directory to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

from check_data_quality import DataQualityChecker


@pytest.fixture
def temp_db():
    """Create a temporary database with schema"""
    # Create a temporary directory and let DuckDB create the database file
    import tempfile
    import os
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.db")

    conn = duckdb.connect(db_path)

    # Create spot table
    conn.execute("""
        CREATE TABLE spot (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            interval VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trades_count INTEGER,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE,
            PRIMARY KEY (exchange, symbol, interval, timestamp)
        )
    """)

    # Create futures table (same schema)
    conn.execute("""
        CREATE TABLE futures (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            interval VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trades_count INTEGER,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE,
            PRIMARY KEY (exchange, symbol, interval, timestamp)
        )
    """)

    # Create open_interest table
    conn.execute("""
        CREATE TABLE open_interest (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open_interest DOUBLE,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    """)

    # Create funding_rates table
    conn.execute("""
        CREATE TABLE funding_rates (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            funding_rate DOUBLE,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    """)

    # Create crypto_universe table
    conn.execute("""
        CREATE TABLE crypto_universe (
            date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            rank INTEGER NOT NULL,
            market_cap DOUBLE,
            categories VARCHAR,
            PRIMARY KEY (date, symbol)
        )
    """)

    conn.close()

    yield db_path

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)


def test_ohlc_relationships_valid(temp_db):
    """Test that valid OHLC data passes all checks"""
    conn = duckdb.connect(temp_db)

    # Insert valid data
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 50100.0, 49900.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0),
        ('binance', 'ETHUSDT', '5m', '2024-01-01 00:00:00', 3000.0, 3010.0, 2990.0, 3005.0, 200.0, 600000.0, 500, 100.0, 300000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        checker.check_ohlcv_table("spot")

        # All OHLC relationship checks should pass
        results = {r["name"]: r for r in checker.results}
        assert results["OHLC: high >= low"]["violation_count"] == 0
        assert results["OHLC: high >= open"]["violation_count"] == 0
        assert results["OHLC: high >= close"]["violation_count"] == 0
        assert results["OHLC: low <= open"]["violation_count"] == 0
        assert results["OHLC: low <= close"]["violation_count"] == 0


def test_ohlc_high_less_than_low(temp_db):
    """Test detection of high < low violation"""
    conn = duckdb.connect(temp_db)

    # Insert invalid data: high < low
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 49900.0, 50100.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: high >= low",
            "SELECT COUNT(*) FROM spot WHERE high < low"
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_ohlc_high_less_than_open(temp_db):
    """Test detection of high < open violation"""
    conn = duckdb.connect(temp_db)

    # Insert invalid data: high < open
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 49900.0, 49800.0, 49850.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: high >= open",
            "SELECT COUNT(*) FROM spot WHERE high < open"
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_null_ohlcv_values(temp_db):
    """Test detection of NULL values in OHLCV fields"""
    conn = duckdb.connect(temp_db)

    # Insert data with NULL open price
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', NULL, 50100.0, 49900.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    # Insert data with NULL volume
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'ETHUSDT', '5m', '2024-01-01 00:00:00', 3000.0, 3010.0, 2990.0, 3005.0, NULL, 600000.0, 500, 100.0, 300000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Null OHLCV",
            """
            SELECT COUNT(*) FROM spot
            WHERE open IS NULL
               OR high IS NULL
               OR low IS NULL
               OR close IS NULL
               OR volume IS NULL
               OR quote_volume IS NULL
            """
        )

        assert result["violation_count"] == 2
        assert result["status"] == "error"


def test_zero_negative_prices(temp_db):
    """Test detection of zero or negative prices"""
    conn = duckdb.connect(temp_db)

    # Insert data with zero price
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 0.0, 50100.0, 49900.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    # Insert data with negative price
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'ETHUSDT', '5m', '2024-01-01 00:00:00', 3000.0, -3010.0, 2990.0, 3005.0, 200.0, 600000.0, 500, 100.0, 300000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Zero/negative prices",
            """
            SELECT COUNT(*) FROM spot
            WHERE open <= 0
               OR high <= 0
               OR low <= 0
               OR close <= 0
            """
        )

        assert result["violation_count"] == 2
        assert result["status"] == "error"


def test_negative_volumes(temp_db):
    """Test detection of negative volumes"""
    conn = duckdb.connect(temp_db)

    # Insert data with negative volume
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 50100.0, 49900.0, 50050.0, -100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Negative volumes",
            "SELECT COUNT(*) FROM spot WHERE volume < 0 OR quote_volume < 0"
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_open_interest_null_values(temp_db):
    """Test detection of NULL values in open interest"""
    conn = duckdb.connect(temp_db)

    # Insert data with NULL open_interest
    conn.execute("""
        INSERT INTO open_interest VALUES
        ('binance', 'BTCUSDT', '2024-01-01 00:00:00', NULL)
    """)

    # Insert valid data
    conn.execute("""
        INSERT INTO open_interest VALUES
        ('binance', 'ETHUSDT', '2024-01-01 00:00:00', 50000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Null open interest",
            """
            SELECT COUNT(*) FROM open_interest
            WHERE open_interest IS NULL
            """
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_open_interest_zero_values(temp_db):
    """Test detection of zero values in open interest (warning)"""
    conn = duckdb.connect(temp_db)

    # Insert data with zero values
    conn.execute("""
        INSERT INTO open_interest VALUES
        ('binance', 'BTCUSDT', '2024-01-01 00:00:00', 0.0),
        ('binance', 'ETHUSDT', '2024-01-01 00:00:00', 50000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Zero open interest",
            """
            SELECT COUNT(*) FROM open_interest
            WHERE open_interest = 0
            """,
            severity="warning"
        )

        assert result["violation_count"] == 1
        assert result["status"] == "warning"


def test_open_interest_negative_values(temp_db):
    """Test detection of negative open interest values"""
    conn = duckdb.connect(temp_db)

    # Insert data with negative values
    conn.execute("""
        INSERT INTO open_interest VALUES
        ('binance', 'BTCUSDT', '2024-01-01 00:00:00', -100.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Negative open interest",
            """
            SELECT COUNT(*) FROM open_interest
            WHERE open_interest < 0
            """
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_funding_rates_null_values(temp_db):
    """Test detection of NULL funding rates"""
    conn = duckdb.connect(temp_db)

    # Insert data with NULL funding_rate
    conn.execute("""
        INSERT INTO funding_rates VALUES
        ('binance', 'BTCUSDT', '2024-01-01 00:00:00', NULL)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Null funding rates",
            "SELECT COUNT(*) FROM funding_rates WHERE funding_rate IS NULL"
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_funding_rates_extreme_values(temp_db):
    """Test detection of extreme funding rates (warning)"""
    conn = duckdb.connect(temp_db)

    # Insert data with extreme funding rates (>1%)
    conn.execute("""
        INSERT INTO funding_rates VALUES
        ('binance', 'BTCUSDT', '2024-01-01 00:00:00', 0.015),
        ('binance', 'ETHUSDT', '2024-01-01 00:00:00', -0.020)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Extreme funding rates",
            "SELECT COUNT(*) FROM funding_rates WHERE ABS(funding_rate) > 0.01",
            severity="warning"
        )

        assert result["violation_count"] == 2
        assert result["status"] == "warning"


def test_funding_rates_negative_allowed(temp_db):
    """Test that negative funding rates are allowed (not flagged as error)"""
    conn = duckdb.connect(temp_db)

    # Insert valid negative funding rates (within ±1%)
    conn.execute("""
        INSERT INTO funding_rates VALUES
        ('binance', 'BTCUSDT', '2024-01-01 00:00:00', -0.0001),
        ('binance', 'ETHUSDT', '2024-01-01 00:00:00', 0.0002)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        # Check that NULL check passes
        result1 = checker.run_check(
            "Test: Null funding rates",
            "SELECT COUNT(*) FROM funding_rates WHERE funding_rate IS NULL"
        )
        assert result1["violation_count"] == 0

        # Check that extreme value check passes (within ±1%)
        result2 = checker.run_check(
            "Test: Extreme funding rates",
            "SELECT COUNT(*) FROM funding_rates WHERE ABS(funding_rate) > 0.01",
            severity="warning"
        )
        assert result2["violation_count"] == 0


def test_crypto_universe_null_values(temp_db):
    """Test detection of NULL values in crypto universe

    Note: rank is NOT NULL in schema, but market_cap can be NULL
    """
    conn = duckdb.connect(temp_db)

    # Try to insert data with NULL rank - should fail due to constraint
    try:
        conn.execute("""
            INSERT INTO crypto_universe VALUES
            ('2024-01-01', 'BTC', NULL, 1000000000.0, 'currency')
        """)
        assert False, "Should have raised ConstraintException for NULL rank"
    except Exception:
        pass  # Expected - rank has NOT NULL constraint

    # Insert data with NULL market_cap (allowed by schema)
    conn.execute("""
        INSERT INTO crypto_universe VALUES
        ('2024-01-01', 'ETH', 2, NULL, 'smart-contracts')
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Null rank or market_cap",
            """
            SELECT COUNT(*) FROM crypto_universe
            WHERE rank IS NULL OR market_cap IS NULL
            """
        )

        # Should find 1 violation (NULL market_cap)
        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_crypto_universe_negative_market_cap(temp_db):
    """Test detection of negative market cap"""
    conn = duckdb.connect(temp_db)

    # Insert data with negative market_cap
    conn.execute("""
        INSERT INTO crypto_universe VALUES
        ('2024-01-01', 'BTC', 1, -1000000000.0, 'currency')
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Negative market cap",
            "SELECT COUNT(*) FROM crypto_universe WHERE market_cap < 0"
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_crypto_universe_rank_gaps(temp_db):
    """Test detection of rank gaps"""
    conn = duckdb.connect(temp_db)

    # Insert data with rank gap (1, 2, 4 - missing 3)
    conn.execute("""
        INSERT INTO crypto_universe VALUES
        ('2024-01-01', 'BTC', 1, 1000000000.0, 'currency'),
        ('2024-01-01', 'ETH', 2, 500000000.0, 'smart-contracts'),
        ('2024-01-01', 'BNB', 4, 100000000.0, 'exchange')
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Rank gaps",
            """
            WITH date_ranks AS (
                SELECT
                    date,
                    MAX(rank) as max_rank,
                    COUNT(DISTINCT rank) as unique_ranks
                FROM crypto_universe
                GROUP BY date
            )
            SELECT COUNT(*) FROM date_ranks
            WHERE unique_ranks != max_rank
            """,
            severity="warning"
        )

        assert result["violation_count"] == 1
        assert result["status"] == "warning"


def test_crypto_universe_duplicate_ranks(temp_db):
    """Test detection of duplicate ranks on same date"""
    conn = duckdb.connect(temp_db)

    # Insert data with duplicate rank on same date
    conn.execute("""
        INSERT INTO crypto_universe VALUES
        ('2024-01-01', 'BTC', 1, 1000000000.0, 'currency'),
        ('2024-01-01', 'ETH', 1, 500000000.0, 'smart-contracts')
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        result = checker.run_check(
            "Test: Duplicate ranks",
            """
            SELECT COUNT(*) FROM (
                SELECT date, rank, COUNT(*) as cnt
                FROM crypto_universe
                GROUP BY date, rank
                HAVING COUNT(*) > 1
            )
            """
        )

        assert result["violation_count"] == 1
        assert result["status"] == "error"


def test_duplicate_primary_keys_spot(temp_db):
    """Test detection of duplicate primary keys in spot table"""
    conn = duckdb.connect(temp_db)

    # Try to insert duplicate - should be prevented by primary key constraint
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 50100.0, 49900.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    # This should fail due to primary key constraint
    with pytest.raises(Exception):
        conn.execute("""
            INSERT INTO spot VALUES
            ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50010.0, 50110.0, 49910.0, 50060.0, 110.0, 5500000.0, 1100, 55.0, 2750000.0)
        """)

    conn.close()


def test_empty_database(temp_db):
    """Test that checks work on empty database without errors"""
    with DataQualityChecker(temp_db) as checker:
        # Should not raise errors
        checker.check_ohlcv_table("spot")
        checker.check_ohlcv_table("futures")
        checker.check_open_interest_table()
        checker.check_funding_rates_table()
        checker.check_crypto_universe_table()

        # All checks should pass (0 violations in empty database)
        for result in checker.results:
            if result["violation_count"] is not None:
                assert result["violation_count"] == 0


def test_table_info(temp_db):
    """Test that table info is correctly retrieved"""
    conn = duckdb.connect(temp_db)

    # Insert some data
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 50100.0, 49900.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0),
        ('binance', 'ETHUSDT', '5m', '2024-01-01 00:00:00', 3000.0, 3010.0, 2990.0, 3005.0, 200.0, 600000.0, 500, 100.0, 300000.0),
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:05:00', 50050.0, 50150.0, 49950.0, 50100.0, 110.0, 5500000.0, 1100, 55.0, 2750000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        info = checker.get_table_info("spot")

        assert info is not None
        row_count, symbol_count = info
        assert row_count == 3
        assert symbol_count == 2  # BTCUSDT and ETHUSDT


def test_verbose_mode(temp_db):
    """Test that verbose mode includes example violations"""
    conn = duckdb.connect(temp_db)

    # Insert data with violations
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 49900.0, 50100.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    conn.close()

    # Run with verbose=True
    with DataQualityChecker(temp_db, verbose=True) as checker:
        result = checker.run_check(
            "Test: high >= low",
            "SELECT symbol, timestamp, high, low FROM spot WHERE high < low"
        )

        # In verbose mode with multi-column query, examples should be included
        assert result["violation_count"] == 1
        assert result["examples"] is not None
        assert len(result["examples"]) == 1


def test_summary_report(temp_db, capsys):
    """Test that summary report is correctly generated"""
    conn = duckdb.connect(temp_db)

    # Insert valid data
    conn.execute("""
        INSERT INTO spot VALUES
        ('binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00', 50000.0, 50100.0, 49900.0, 50050.0, 100.0, 5000000.0, 1000, 50.0, 2500000.0)
    """)

    # Insert invalid data
    conn.execute("""
        INSERT INTO futures VALUES
        ('binance', 'ETHUSDT', '5m', '2024-01-01 00:00:00', 3000.0, 2990.0, 3010.0, 3005.0, 200.0, 600000.0, 500, 100.0, 300000.0)
    """)

    conn.close()

    with DataQualityChecker(temp_db) as checker:
        # Run checks on spot (should pass)
        checker.run_check(
            "Test spot",
            "SELECT COUNT(*) FROM spot WHERE high < low"
        )

        # Run checks on futures (should fail)
        checker.run_check(
            "Test futures",
            "SELECT COUNT(*) FROM futures WHERE high < low"
        )

        # Print summary
        success = checker.print_summary()

        assert not success  # Should return False due to failure
        assert len(checker.results) == 2
        assert checker.results[0]["violation_count"] == 0
        assert checker.results[1]["violation_count"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
