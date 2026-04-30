"""
Binance Futures Data Validation Tests

Verify that data in `futures` table matches Binance Futures REST API.
"""

import pytest

from tests.validation.conftest import ValidationCollector, ValidationMismatch
from tests.validation.utils.binance_api import BinanceKline
from tests.validation.utils.sampling import (
    OHLCVSample,
    sample_ohlcv,
    sample_ohlcv_ratio_by_symbol,
)

# Tolerances
PRICE_TOLERANCE = 1e-8  # Exact match for prices (8 decimal places)
VOLUME_TOLERANCE = 1e-4  # 0.01% for volumes
TRADES_COUNT_TOLERANCE = 0  # Exact match for trade counts


def compare_futures_kline(
    db_kline: OHLCVSample,
    api_kline: BinanceKline,
    collector: ValidationCollector,
) -> None:
    """Compare DB futures kline with API kline, collect mismatches"""

    def check_field(field: str, db_val, api_val, tolerance: float, is_relative: bool = False):
        if db_val is None or api_val is None:
            collector.record_skip(
                f"Missing value for {db_kline.symbol} @ {db_kline.timestamp} ({field})"
            )
            return

        if is_relative and api_val != 0:
            diff = abs(db_val - api_val) / abs(api_val)
        else:
            diff = abs(db_val - api_val)

        if diff > tolerance:
            collector.add_mismatch(
                ValidationMismatch(
                    table="futures",
                    symbol=db_kline.symbol,
                    timestamp=db_kline.timestamp,
                    field=field,
                    db_value=db_val,
                    api_value=api_val,
                    tolerance=tolerance,
                    diff=diff,
                )
            )

    # OHLC - exact match
    check_field("open", db_kline.open, api_kline.open, PRICE_TOLERANCE)
    check_field("high", db_kline.high, api_kline.high, PRICE_TOLERANCE)
    check_field("low", db_kline.low, api_kline.low, PRICE_TOLERANCE)
    check_field("close", db_kline.close, api_kline.close, PRICE_TOLERANCE)

    # Volume - relative tolerance
    vol_tol = abs(api_kline.volume * VOLUME_TOLERANCE) if api_kline.volume > 0 else VOLUME_TOLERANCE
    check_field("volume", db_kline.volume, api_kline.volume, vol_tol)

    quote_vol_tol = (
        abs(api_kline.quote_volume * VOLUME_TOLERANCE)
        if api_kline.quote_volume > 0
        else VOLUME_TOLERANCE
    )
    check_field("quote_volume", db_kline.quote_volume, api_kline.quote_volume, quote_vol_tol)

    # Trades count - exact match
    check_field(
        "trades_count", db_kline.trades_count, api_kline.trades_count, TRADES_COUNT_TOLERANCE
    )


@pytest.mark.validation
class TestFuturesValidation:
    """Futures OHLCV data validation against Binance Futures API"""

    @pytest.mark.api
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_random_sample_matches_binance_api(
        self,
        validation_db_path,
        binance_client,
        validation_sample_ratio,
        validation_max_symbols,
        validation_interval,
    ):
        """
        Validate random futures OHLCV subset against Binance API.

        Sampling approach:
        - Use a global ratio budget (20% by default) over available points
        - Split sample budget per symbol
        - Avoid covering every symbol for faster API validation
        """
        collector = ValidationCollector()

        # Sample from database
        samples = sample_ohlcv_ratio_by_symbol(
            validation_db_path,
            "futures",
            sample_ratio=validation_sample_ratio,
            max_symbols=validation_max_symbols,
            interval=validation_interval,
        )

        if not samples:
            pytest.skip("No futures data in database")

        for sample in samples:
            try:
                api_kline = await binance_client.get_futures_kline(
                    sample.symbol, sample.interval, sample.timestamp
                )

                if api_kline is None:
                    collector.record_skip(f"No API data for {sample.symbol} @ {sample.timestamp}")
                    continue

                compare_futures_kline(sample, api_kline, collector)
                collector.record_success()

            except Exception as e:
                collector.record_skip(f"API error for {sample.symbol} @ {sample.timestamp}: {e!s}")

        # Report results
        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_1000prefix_symbols_match(
        self,
        validation_db_path,
        binance_client,
    ):
        """Verify 1000-prefix symbols (PEPE, SHIB, etc.) are correctly mapped"""
        import duckdb

        collector = ValidationCollector()

        # Known 1000-prefix symbols
        prefix_symbols = ["PEPEUSDT", "SHIBUSDT", "BONKUSDT", "FLOKIUSDT", "LUNCUSDT"]

        conn = duckdb.connect(validation_db_path, read_only=True)
        try:
            # Find which of these exist in futures table
            placeholders = ", ".join(f"'{s}'" for s in prefix_symbols)
            existing = conn.execute(
                f"""
                SELECT DISTINCT symbol
                FROM futures
                WHERE symbol IN ({placeholders})
            """
            ).fetchall()
            existing_symbols = [row[0] for row in existing]
        finally:
            conn.close()

        if not existing_symbols:
            pytest.skip("No 1000-prefix symbols in futures table")

        # Sample from each existing 1000-prefix symbol
        for symbol in existing_symbols[:3]:  # Limit to 3 symbols for speed
            samples = sample_ohlcv(validation_db_path, "futures", n=2, symbol=symbol)

            for sample in samples:
                try:
                    api_kline = await binance_client.get_futures_kline(
                        sample.symbol, sample.interval, sample.timestamp
                    )

                    if api_kline is None:
                        collector.record_skip(
                            f"No API data for {sample.symbol} @ {sample.timestamp} "
                            "(may need 1000 prefix)"
                        )
                        continue

                    compare_futures_kline(sample, api_kline, collector)
                    collector.record_success()

                except Exception as e:
                    collector.record_skip(
                        f"API error for {sample.symbol} @ {sample.timestamp}: {e!s}"
                    )

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_btcusdt_futures_sample(
        self,
        validation_db_path,
        binance_client,
    ):
        """Verify random BTCUSDT futures candles"""
        collector = ValidationCollector()

        # Sample BTCUSDT specifically
        samples = sample_ohlcv(validation_db_path, "futures", n=5, symbol="BTCUSDT")

        if not samples:
            pytest.skip("No BTCUSDT futures data in database")

        for sample in samples:
            try:
                api_kline = await binance_client.get_futures_kline(
                    sample.symbol, sample.interval, sample.timestamp
                )

                if api_kline is None:
                    collector.record_skip(f"No API data for BTCUSDT futures @ {sample.timestamp}")
                    continue

                compare_futures_kline(sample, api_kline, collector)
                collector.record_success()

            except Exception as e:
                collector.record_skip(f"API error for BTCUSDT futures @ {sample.timestamp}: {e!s}")

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_multiple_intervals(
        self,
        validation_db_path,
        binance_client,
    ):
        """Verify different interval data matches API"""
        import duckdb

        collector = ValidationCollector()

        conn = duckdb.connect(validation_db_path, read_only=True)
        try:
            # Get available intervals
            intervals = conn.execute("SELECT DISTINCT interval FROM futures LIMIT 5").fetchall()
            intervals = [row[0] for row in intervals]
        finally:
            conn.close()

        if not intervals:
            pytest.skip("No futures data in database")

        # Sample from each interval
        for interval in intervals[:3]:  # Limit to 3 intervals
            samples = sample_ohlcv(validation_db_path, "futures", n=2, interval=interval)

            for sample in samples:
                try:
                    api_kline = await binance_client.get_futures_kline(
                        sample.symbol, sample.interval, sample.timestamp
                    )

                    if api_kline is None:
                        collector.record_skip(
                            f"No API data for {sample.symbol} ({sample.interval}) "
                            f"@ {sample.timestamp}"
                        )
                        continue

                    compare_futures_kline(sample, api_kline, collector)
                    collector.record_success()

                except Exception as e:
                    collector.record_skip(
                        f"API error for {sample.symbol} ({sample.interval}) "
                        f"@ {sample.timestamp}: {e!s}"
                    )

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()
