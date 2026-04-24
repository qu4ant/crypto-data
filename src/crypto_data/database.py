"""
DuckDB Database Manager for Crypto Data

Manages schema creation and database connections for cryptocurrency data storage.
This is a pure ingestion/storage layer - querying is done externally via SQL.
"""

import logging
from pathlib import Path
import duckdb

logger = logging.getLogger(__name__)


class CryptoDatabase:
    """
    Manages DuckDB database schema and connections.

    This class handles:
    - Database file creation and connection
    - Table schema initialization
    - Basic database operations

    It does NOT provide query conveniences - users write SQL directly.

    Example
    -------
    >>> db = CryptoDatabase('crypto_data.db')
    >>> # Database is ready, tables created
    >>> # Now use ingestion functions to populate it
    """

    def __init__(self, db_path: str):
        """
        Initialize database connection and create schema.

        Parameters
        ----------
        db_path : str
            Path to DuckDB database file (will be created if doesn't exist)
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect to database
        self.conn = duckdb.connect(str(self.db_path))

        logger.debug(f"Connected to database: {self.db_path}")

        # Create tables if they don't exist
        self._create_tables()

        logger.debug("Database initialized successfully")

    def _create_tables(self):
        """Create all required tables and indexes."""
        self._create_spot()
        self._create_futures()
        self._create_open_interest()
        self._create_funding_rates()
        self._create_crypto_universe()

        logger.debug("All tables and indexes created")

    def _create_spot(self):
        """Create spot table for spot OHLCV data (Binance)."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS spot (
                exchange VARCHAR NOT NULL CHECK (exchange = 'binance'),
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

        # Create index for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spot_exchange_symbol_interval_time
            ON spot(exchange, symbol, interval, timestamp)
        """)

        logger.debug("Created spot table")

    def _create_futures(self):
        """Create futures table for futures OHLCV data (Binance)."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS futures (
                exchange VARCHAR NOT NULL CHECK (exchange = 'binance'),
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

        # Create index for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_futures_exchange_symbol_interval_time
            ON futures(exchange, symbol, interval, timestamp)
        """)

        logger.debug("Created futures table")

    def _create_open_interest(self):
        """Create open_interest table for futures open interest metrics (Binance)."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS open_interest (
                exchange VARCHAR NOT NULL CHECK (exchange = 'binance'),
                symbol VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open_interest DOUBLE,
                PRIMARY KEY (exchange, symbol, timestamp)
            )
        """)

        # Create index for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_interest_exchange_symbol_time
            ON open_interest(exchange, symbol, timestamp)
        """)

        logger.debug("Created open_interest table")

    def _create_funding_rates(self):
        """Create funding_rates table for futures funding rate data (Binance)."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS funding_rates (
                exchange VARCHAR NOT NULL CHECK (exchange = 'binance'),
                symbol VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                funding_rate DOUBLE,
                PRIMARY KEY (exchange, symbol, timestamp)
            )
        """)

        # Create index for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_funding_rates_exchange_symbol_time
            ON funding_rates(exchange, symbol, timestamp)
        """)

        logger.debug("Created funding_rates table")

    def _create_crypto_universe(self):
        """Create crypto_universe table for CoinMarketCap rankings."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS crypto_universe (
                date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                rank INTEGER NOT NULL,
                market_cap DOUBLE,
                categories VARCHAR,
                PRIMARY KEY (date, symbol)
            )
        """)

        # Create index for common queries (backtesting, symbol selection)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_universe_date_rank
            ON crypto_universe(date, rank)
        """)

        logger.debug("Created crypto_universe table")

    def execute(self, sql: str):
        """
        Execute SQL query and return result.

        Parameters
        ----------
        sql : str
            SQL query to execute

        Returns
        -------
        DuckDBPyRelation
            Query result
        """
        return self.conn.execute(sql)

    def close(self):
        """Close database connection."""
        self.conn.close()
        logger.debug("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
