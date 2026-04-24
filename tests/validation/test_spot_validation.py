"""
Binance Spot Data Validation Tests

Verify that data in `spot` table matches Binance REST API for historical klines.
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


def compare_spot_kline(
    db_kline: OHLCVSample,
    api_kline: BinanceKline,
    collector: ValidationCollector,
) -> None:
    """Compare DB kline with API kline, collect mismatches"""

    def check_field(
        field: str, db_val, api_val, tolerance: float, is_relative: bool = False
    ):
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
                    table="spot",
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
    check_field("trades_count", db_kline.trades_count, api_kline.trades_count, TRADES_COUNT_TOLERANCE)


@pytest.mark.validation
class TestSpotValidation:
    """Spot OHLCV data validation against Binance API"""

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
        Validate random OHLCV subset against Binance API.

        Sampling approach:
        - Use a global ratio budget (20% by default) over available points
        - Split sample budget per symbol
        - Avoid covering every symbol for faster API validation
        """
        collector = ValidationCollector()

        # Sample from database
        samples = sample_ohlcv_ratio_by_symbol(
            validation_db_path,
            "spot",
            sample_ratio=validation_sample_ratio,
            max_symbols=validation_max_symbols,
            interval=validation_interval,
        )

        if not samples:
            pytest.skip("No spot data in database")

        for sample in samples:
            try:
                api_kline = await binance_client.get_spot_kline(
                    sample.symbol, sample.interval, sample.timestamp
                )

                if api_kline is None:
                    collector.record_skip(
                        f"No API data for {sample.symbol} @ {sample.timestamp}"
                    )
                    continue

                compare_spot_kline(sample, api_kline, collector)
                collector.record_success()

            except Exception as e:
                collector.record_skip(
                    f"API error for {sample.symbol} @ {sample.timestamp}: {str(e)}"
                )

        # Report results
        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_btcusdt_sample(
        self,
        validation_db_path,
        binance_client,
    ):
        """Verify random BTCUSDT candles (most liquid, most reliable)"""
        collector = ValidationCollector()

        # Sample BTCUSDT specifically
        samples = sample_ohlcv(validation_db_path, "spot", n=5, symbol="BTCUSDT")

        if not samples:
            pytest.skip("No BTCUSDT spot data in database")

        for sample in samples:
            try:
                api_kline = await binance_client.get_spot_kline(
                    sample.symbol, sample.interval, sample.timestamp
                )

                if api_kline is None:
                    collector.record_skip(f"No API data for BTCUSDT @ {sample.timestamp}")
                    continue

                compare_spot_kline(sample, api_kline, collector)
                collector.record_success()

            except Exception as e:
                collector.record_skip(f"API error for BTCUSDT @ {sample.timestamp}: {str(e)}")

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_ohlc_relationships_match(
        self,
        validation_db_path,
        binance_client,
    ):
        """Verify OHLC relationships (high >= low, etc.) match API exactly"""
        collector = ValidationCollector()

        samples = sample_ohlcv(validation_db_path, "spot", n=10)

        if not samples:
            pytest.skip("No spot data in database")

        for sample in samples:
            try:
                api_kline = await binance_client.get_spot_kline(
                    sample.symbol, sample.interval, sample.timestamp
                )

                if api_kline is None:
                    collector.record_skip(
                        f"No API data for {sample.symbol} @ {sample.timestamp}"
                    )
                    continue

                # Check OHLC relationships match exactly
                db_high_is_max = sample.high >= max(sample.open, sample.close, sample.low)
                api_high_is_max = api_kline.high >= max(
                    api_kline.open, api_kline.close, api_kline.low
                )

                db_low_is_min = sample.low <= min(sample.open, sample.close, sample.high)
                api_low_is_min = api_kline.low <= min(
                    api_kline.open, api_kline.close, api_kline.high
                )

                if db_high_is_max != api_high_is_max:
                    collector.add_mismatch(
                        ValidationMismatch(
                            table="spot",
                            symbol=sample.symbol,
                            timestamp=sample.timestamp,
                            field="high_is_max",
                            db_value=db_high_is_max,
                            api_value=api_high_is_max,
                            tolerance=0,
                            diff=1,
                        )
                    )

                if db_low_is_min != api_low_is_min:
                    collector.add_mismatch(
                        ValidationMismatch(
                            table="spot",
                            symbol=sample.symbol,
                            timestamp=sample.timestamp,
                            field="low_is_min",
                            db_value=db_low_is_min,
                            api_value=api_low_is_min,
                            tolerance=0,
                            diff=1,
                        )
                    )

                collector.record_success()

            except Exception as e:
                collector.record_skip(
                    f"API error for {sample.symbol} @ {sample.timestamp}: {str(e)}"
                )

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_volume_consistency(
        self,
        validation_db_path,
        binance_client,
    ):
        """Verify volume and quote_volume match API"""
        collector = ValidationCollector()

        samples = sample_ohlcv(validation_db_path, "spot", n=10)

        if not samples:
            pytest.skip("No spot data in database")

        for sample in samples:
            try:
                api_kline = await binance_client.get_spot_kline(
                    sample.symbol, sample.interval, sample.timestamp
                )

                if api_kline is None:
                    collector.record_skip(
                        f"No API data for {sample.symbol} @ {sample.timestamp}"
                    )
                    continue

                # Check volume consistency
                if api_kline.volume > 0:
                    vol_diff = abs(sample.volume - api_kline.volume) / api_kline.volume
                    if vol_diff > VOLUME_TOLERANCE:
                        collector.add_mismatch(
                            ValidationMismatch(
                                table="spot",
                                symbol=sample.symbol,
                                timestamp=sample.timestamp,
                                field="volume",
                                db_value=sample.volume,
                                api_value=api_kline.volume,
                                tolerance=VOLUME_TOLERANCE,
                                diff=vol_diff,
                            )
                        )

                if api_kline.quote_volume > 0:
                    qvol_diff = (
                        abs(sample.quote_volume - api_kline.quote_volume)
                        / api_kline.quote_volume
                    )
                    if qvol_diff > VOLUME_TOLERANCE:
                        collector.add_mismatch(
                            ValidationMismatch(
                                table="spot",
                                symbol=sample.symbol,
                                timestamp=sample.timestamp,
                                field="quote_volume",
                                db_value=sample.quote_volume,
                                api_value=api_kline.quote_volume,
                                tolerance=VOLUME_TOLERANCE,
                                diff=qvol_diff,
                            )
                        )

                collector.record_success()

            except Exception as e:
                collector.record_skip(
                    f"API error for {sample.symbol} @ {sample.timestamp}: {str(e)}"
                )

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()
