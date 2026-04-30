"""
CoinMarketCap Universe Data Validation Tests

Verify that data in `crypto_universe` table matches CoinMarketCap API responses.
"""

import pytest

from tests.validation.conftest import ValidationCollector, ValidationMismatch
from tests.validation.utils.sampling import (
    UniverseSample,
    group_samples_by_date,
    sample_universe,
)

# Tolerances
MARKET_CAP_TOLERANCE = 0.01  # 1% tolerance for market cap rounding
RANK_TOLERANCE = 0  # Exact match for ranks


def parse_cmc_response(api_response: list) -> dict:
    """
    Parse CoinMarketCap API response into lookup by symbol.

    The CMC API returns coins with:
    - 'symbol': coin symbol (e.g., 'BTC')
    - 'cmcRank': ranking by market cap
    - 'quotes': list with market data, first element has 'marketCap'
    """
    api_by_symbol = {}
    for item in api_response:
        symbol = item.get("symbol")
        if symbol:
            # Extract market cap from quotes
            quotes = item.get("quotes", [])
            market_cap = None
            if quotes and len(quotes) > 0:
                market_cap = quotes[0].get("marketCap")

            candidate = {
                "rank": item.get("cmcRank"),
                "market_cap": market_cap,
            }

            existing = api_by_symbol.get(symbol)
            if existing is None:
                api_by_symbol[symbol] = candidate
                continue

            # CMC can return duplicate symbols (e.g., SOL for Solana and Wrapped Solana).
            # Keep the canonical listing by preferring the lowest rank (best market-cap rank).
            existing_rank = existing.get("rank")
            candidate_rank = candidate.get("rank")
            if candidate_rank is not None and (
                existing_rank is None or candidate_rank < existing_rank
            ):
                api_by_symbol[symbol] = candidate
            elif candidate_rank == existing_rank:
                existing_mcap = existing.get("market_cap")
                candidate_mcap = candidate.get("market_cap")
                if candidate_mcap is not None and (
                    existing_mcap is None or candidate_mcap > existing_mcap
                ):
                    api_by_symbol[symbol] = candidate
    return api_by_symbol


def compare_universe_sample(
    db_sample: UniverseSample,
    api_data: dict,
    collector: ValidationCollector,
) -> None:
    """Compare DB universe sample with CMC API data, collect mismatches"""

    def check_field(field: str, db_val, api_val, tolerance: float, is_relative: bool = False):
        if api_val is None:
            collector.record_skip(f"API returned None for {field}")
            return

        if is_relative and api_val != 0:
            diff = abs(db_val - api_val) / abs(api_val)
        else:
            diff = abs(db_val - api_val)

        if diff > tolerance:
            collector.add_mismatch(
                ValidationMismatch(
                    table="crypto_universe",
                    symbol=db_sample.symbol,
                    timestamp=db_sample.date,
                    field=field,
                    db_value=db_val,
                    api_value=api_val,
                    tolerance=tolerance,
                    diff=diff,
                )
            )

    check_field("rank", db_sample.rank, api_data.get("rank"), RANK_TOLERANCE)
    check_field(
        "market_cap",
        db_sample.market_cap,
        api_data.get("market_cap"),
        MARKET_CAP_TOLERANCE,
        is_relative=True,
    )


@pytest.mark.validation
class TestUniverseValidation:
    """Universe data validation against CoinMarketCap API"""

    @pytest.mark.api
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_random_sample_matches_cmc_api(
        self,
        validation_db_path,
        cmc_client,
        validation_sample_size,
    ):
        """Sample N random (date, symbol) pairs and verify against CMC API"""
        collector = ValidationCollector()

        # Sample from database
        samples = sample_universe(validation_db_path, n=validation_sample_size)

        if not samples:
            pytest.skip("No universe data in database")

        # Group by date to minimize API calls
        grouped = group_samples_by_date(samples)

        for date, date_samples in grouped.items():
            try:
                # Fetch CMC data for this date
                date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
                api_response = await cmc_client.get_historical_listings(
                    date=date_str,
                    limit=500,
                )

                if not api_response:
                    for sample in date_samples:
                        collector.record_skip(f"No API data for {date}")
                    continue

                # Build lookup by symbol
                api_by_symbol = parse_cmc_response(api_response)

                # Compare each sample
                for sample in date_samples:
                    if sample.symbol in api_by_symbol:
                        compare_universe_sample(sample, api_by_symbol[sample.symbol], collector)
                        collector.record_success()
                    else:
                        collector.record_skip(
                            f"Symbol {sample.symbol} not in API response for {date}"
                        )

            except Exception as e:
                for sample in date_samples:
                    collector.record_skip(f"API error for {date}: {e!s}")

        # Report results
        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_top_10_coins_match(self, validation_db_path, cmc_client):
        """Verify top 10 coins for a random date in the database"""
        import duckdb

        collector = ValidationCollector()

        conn = duckdb.connect(validation_db_path, read_only=True)
        try:
            # Get a random date from the database
            result = conn.execute(
                """
                SELECT date FROM crypto_universe
                GROUP BY date
                ORDER BY RANDOM()
                LIMIT 1
            """
            ).fetchone()

            if not result:
                pytest.skip("No universe data in database")

            test_date = result[0]

            # Get top 10 from database
            db_top10 = conn.execute(
                f"""
                SELECT symbol, rank, market_cap
                FROM crypto_universe
                WHERE date = '{test_date}'
                  AND rank <= 10
                ORDER BY rank
            """
            ).fetchall()

        finally:
            conn.close()

        if not db_top10:
            pytest.skip(f"No top 10 data for {test_date}")

        # Fetch from API
        try:
            date_str = (
                test_date.strftime("%Y-%m-%d") if hasattr(test_date, "strftime") else str(test_date)
            )
            api_response = await cmc_client.get_historical_listings(
                date=date_str,
                limit=100,  # Get more to account for filtering
            )
        except Exception as e:
            pytest.skip(f"API error: {e}")

        if not api_response:
            pytest.skip("No API response")

        # Build lookup
        api_by_symbol = parse_cmc_response(api_response)

        # Compare
        for symbol, rank, market_cap in db_top10:
            sample = UniverseSample(date=test_date, symbol=symbol, rank=rank, market_cap=market_cap)
            if symbol in api_by_symbol:
                compare_universe_sample(sample, api_by_symbol[symbol], collector)
                collector.record_success()
            else:
                collector.record_skip(f"Symbol {symbol} not in API top 10")

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()

    @pytest.mark.api
    @pytest.mark.asyncio
    async def test_market_cap_ordering(self, validation_db_path):
        """Verify rank ordering matches market cap ordering in database"""
        import duckdb

        collector = ValidationCollector()

        conn = duckdb.connect(validation_db_path, read_only=True)
        try:
            # Get a random date
            result = conn.execute(
                """
                SELECT date FROM crypto_universe
                GROUP BY date
                ORDER BY RANDOM()
                LIMIT 1
            """
            ).fetchone()

            if not result:
                pytest.skip("No universe data in database")

            test_date = result[0]

            # Check that rank ordering matches market cap ordering
            # (higher market cap should have lower rank number)
            violations = conn.execute(
                f"""
                WITH ranked AS (
                    SELECT
                        symbol,
                        rank,
                        market_cap,
                        LAG(market_cap) OVER (ORDER BY rank) as prev_market_cap
                    FROM crypto_universe
                    WHERE date = '{test_date}'
                    ORDER BY rank
                )
                SELECT symbol, rank, market_cap, prev_market_cap
                FROM ranked
                WHERE prev_market_cap IS NOT NULL
                  AND market_cap > prev_market_cap
            """
            ).fetchall()

        finally:
            conn.close()

        for symbol, rank, market_cap, prev_market_cap in violations:
            collector.add_mismatch(
                ValidationMismatch(
                    table="crypto_universe",
                    symbol=symbol,
                    timestamp=test_date,
                    field="rank_ordering",
                    db_value=f"rank={rank}, mcap={market_cap:.0f}",
                    api_value=f"prev_mcap={prev_market_cap:.0f}",
                    tolerance=0,
                    diff=market_cap - prev_market_cap,
                )
            )

        if not violations:
            collector.record_success()

        print(f"\n{collector.get_summary()}")
        collector.assert_no_mismatches()
