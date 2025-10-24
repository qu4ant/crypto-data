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

        logger.info(f"Connected to database: {self.db_path}")

        # Create tables if they don't exist
        self._create_tables()

        logger.info("Database initialized successfully")

    def _create_tables(self):
        """Create all required tables and indexes."""
        self._create_metadata()
        self._create_binance_spot()
        self._create_binance_futures()
        self._create_crypto_universe()

        logger.info("All tables and indexes created")

    def _create_metadata(self):
        """Create metadata table for storing database configuration."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)

        logger.debug("Created _metadata table")

    def _create_binance_spot(self):
        """Create binance_spot table for spot OHLCV data."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS binance_spot (
                symbol VARCHAR NOT NULL,
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
                PRIMARY KEY (symbol, timestamp)
            )
        """)

        # Create index for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spot_symbol_time
            ON binance_spot(symbol, timestamp)
        """)

        logger.debug("Created binance_spot table")

    def _create_binance_futures(self):
        """Create binance_futures table for futures OHLCV data."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS binance_futures (
                symbol VARCHAR NOT NULL,
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
                PRIMARY KEY (symbol, timestamp)
            )
        """)

        # Create index for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_futures_symbol_time
            ON binance_futures(symbol, timestamp)
        """)

        logger.debug("Created binance_futures table")

    def _create_crypto_universe(self):
        """Create crypto_universe table for cryptocurrency rankings."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS crypto_universe (
                date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                rank INTEGER,
                market_cap DOUBLE,
                categories VARCHAR,
                exchanges VARCHAR,
                has_perpetual BOOLEAN,
                PRIMARY KEY (date, symbol)
            )
        """)

        # Create index for date queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_universe_date
            ON crypto_universe(date)
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

    def set_metadata(self, key: str, value: str):
        """
        Store metadata key-value pair.

        Parameters
        ----------
        key : str
            Metadata key
        value : str
            Metadata value
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO _metadata (key, value)
            VALUES (?, ?)
        """, [key, value])

        logger.debug(f"Stored metadata: {key} = {value}")

    def get_metadata(self, key: str):
        """
        Retrieve metadata value by key.

        Parameters
        ----------
        key : str
            Metadata key

        Returns
        -------
        str or None
            Metadata value, or None if key doesn't exist
        """
        result = self.conn.execute("""
            SELECT value FROM _metadata WHERE key = ?
        """, [key]).fetchone()

        return result[0] if result else None

    def get_interval(self):
        """
        Get stored interval from metadata.

        Returns
        -------
        str or None
            Interval string (e.g., '5m', '1h'), or None if not set
        """
        return self.get_metadata('interval')

    def get_table_stats(self):
        """
        Get statistics for all tables.

        Returns
        -------
        dict
            Dictionary with table statistics
        """
        stats = {}

        # Binance spot stats
        result = self.conn.execute("""
            SELECT
                COUNT(*) as record_count,
                COUNT(DISTINCT symbol) as symbol_count,
                MIN(timestamp) as min_date,
                MAX(timestamp) as max_date
            FROM binance_spot
        """).fetchone()

        if result:
            stats['binance_spot'] = {
                'records': result[0],
                'symbols': result[1],
                'min_date': result[2],
                'max_date': result[3]
            }

        # Binance futures stats
        result = self.conn.execute("""
            SELECT
                COUNT(*) as record_count,
                COUNT(DISTINCT symbol) as symbol_count,
                MIN(timestamp) as min_date,
                MAX(timestamp) as max_date
            FROM binance_futures
        """).fetchone()

        if result:
            stats['binance_futures'] = {
                'records': result[0],
                'symbols': result[1],
                'min_date': result[2],
                'max_date': result[3]
            }

        # Crypto universe stats
        result = self.conn.execute("""
            SELECT
                COUNT(*) as record_count,
                COUNT(DISTINCT symbol) as symbol_count,
                COUNT(DISTINCT date) as snapshot_count,
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM crypto_universe
        """).fetchone()

        if result:
            stats['crypto_universe'] = {
                'records': result[0],
                'symbols': result[1],
                'snapshots': result[2],
                'min_date': result[3],
                'max_date': result[4]
            }

        return stats

    def close(self):
        """Close database connection."""
        self.conn.close()
        logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
