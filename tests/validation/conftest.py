"""
Shared fixtures for validation tests.
"""

import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List

import pytest
from dateutil.relativedelta import relativedelta

from crypto_data.universe_filters import DEFAULT_UNIVERSE_EXCLUDE_TAGS

# Default auto-build dataset for validation tests (fast but representative)
DEFAULT_VALIDATION_MONTH_LOOKBACK = 12
DEFAULT_VALIDATION_TOP_N = 30
DEFAULT_VALIDATION_INTERVAL = "4h"
DEFAULT_VALIDATION_UNIVERSE_FREQUENCY = "monthly"
DEFAULT_VALIDATION_SAMPLE_RATIO = 0.2
DEFAULT_VALIDATION_MAX_SYMBOLS = 8

# Default universe filters (same intent as user script)
DEFAULT_VALIDATION_EXCLUDE_TAGS = DEFAULT_UNIVERSE_EXCLUDE_TAGS
DEFAULT_VALIDATION_EXCLUDE_SYMBOLS: List[str] = []


# Validation tolerances
VALIDATION_CONFIG = {
    "price_tolerance": 1e-8,  # For OHLC prices (8 decimal places)
    "volume_tolerance": 1e-4,  # 0.01% for volumes
    "market_cap_tolerance": 0.01,  # 1% for market cap (CMC rounding)
    "rank_tolerance": 0,  # Exact match for ranks
    "trades_count_tolerance": 0,  # Exact match for trade counts
}


@dataclass
class ValidationMismatch:
    """Single validation mismatch"""

    table: str  # 'spot', 'futures', 'crypto_universe'
    symbol: str
    timestamp: datetime
    field: str  # 'open', 'high', 'rank', 'market_cap', etc.
    db_value: Any
    api_value: Any
    tolerance: float
    diff: float  # Actual difference


@dataclass
class ValidationCollector:
    """Collects mismatches during test run"""

    mismatches: List[ValidationMismatch] = field(default_factory=list)
    validated_count: int = 0
    skipped_count: int = 0
    skip_reasons: List[str] = field(default_factory=list)

    def add_mismatch(self, mismatch: ValidationMismatch) -> None:
        self.mismatches.append(mismatch)

    def record_success(self) -> None:
        self.validated_count += 1

    def record_skip(self, reason: str) -> None:
        self.skipped_count += 1
        self.skip_reasons.append(reason)

    def assert_no_mismatches(self) -> None:
        """Call at end of test - fails with detailed report if mismatches"""
        if self.mismatches:
            report = self._format_report()
            pytest.fail(f"\n{report}")

    def get_summary(self) -> str:
        """Get summary without failing"""
        return self._format_report()

    def _format_report(self) -> str:
        """Format all mismatches as readable report"""
        lines = [
            "=== VALIDATION REPORT ===",
            f"Validated: {self.validated_count}",
            f"Skipped: {self.skipped_count}",
            f"Mismatches: {len(self.mismatches)}",
            "",
        ]

        if self.skip_reasons:
            lines.append("--- SKIP REASONS (first 5) ---")
            for reason in self.skip_reasons[:5]:
                lines.append(f"  - {reason}")
            if len(self.skip_reasons) > 5:
                lines.append(f"  ... and {len(self.skip_reasons) - 5} more")
            lines.append("")

        if self.mismatches:
            lines.append("--- MISMATCHES ---")
            for m in self.mismatches:
                lines.append(
                    f"[{m.table}] {m.symbol} @ {m.timestamp} | "
                    f"{m.field}: DB={m.db_value} vs API={m.api_value} "
                    f"(diff={m.diff:.8f}, tolerance={m.tolerance})"
                )

        return "\n".join(lines)


def pytest_addoption(parser):
    """Add custom CLI options for validation tests"""
    parser.addoption(
        "--validation-sample-size",
        action="store",
        default=10,
        type=int,
        help="Number of random samples per validation test",
    )
    parser.addoption(
        "--validation-db-path",
        action="store",
        default=None,
        help=(
            "Path to database file for validation tests. If omitted (and "
            "CRYPTO_DATA_DB_PATH is unset), tests auto-build a temporary DB."
        ),
    )
    parser.addoption(
        "--validation-start-date",
        action="store",
        default=None,
        help=(
            "Auto-build DB start date (YYYY-MM-DD). If omitted with end date, "
            "a random closed month from the last 12 months is selected."
        ),
    )
    parser.addoption(
        "--validation-end-date",
        action="store",
        default=None,
        help=(
            "Auto-build DB end date (YYYY-MM-DD). If omitted with start date, "
            "a random closed month from the last 12 months is selected."
        ),
    )
    parser.addoption(
        "--validation-top-n",
        action="store",
        default=DEFAULT_VALIDATION_TOP_N,
        type=int,
        help=f"Auto-build DB universe top_n (default: {DEFAULT_VALIDATION_TOP_N})",
    )
    parser.addoption(
        "--validation-interval",
        action="store",
        default=DEFAULT_VALIDATION_INTERVAL,
        help=f"Auto-build DB OHLCV interval (default: {DEFAULT_VALIDATION_INTERVAL})",
    )
    parser.addoption(
        "--validation-universe-frequency",
        action="store",
        default=DEFAULT_VALIDATION_UNIVERSE_FREQUENCY,
        choices=["daily", "weekly", "monthly"],
        help=(
            "Auto-build DB universe snapshot frequency "
            f"(default: {DEFAULT_VALIDATION_UNIVERSE_FREQUENCY})"
        ),
    )
    parser.addoption(
        "--validation-month-lookback",
        action="store",
        default=DEFAULT_VALIDATION_MONTH_LOOKBACK,
        type=int,
        help=(
            "When start/end are omitted, pick random month from this many "
            f"closed months back (default: {DEFAULT_VALIDATION_MONTH_LOOKBACK})"
        ),
    )
    parser.addoption(
        "--validation-month-seed",
        action="store",
        default=None,
        type=int,
        help=(
            "Optional RNG seed for random month selection "
            "(default: unset = non-deterministic)"
        ),
    )
    parser.addoption(
        "--validation-sample-ratio",
        action="store",
        default=DEFAULT_VALIDATION_SAMPLE_RATIO,
        type=float,
        help=(
            "Ratio of available OHLCV points to validate in random-sample tests "
            f"(default: {DEFAULT_VALIDATION_SAMPLE_RATIO})"
        ),
    )
    parser.addoption(
        "--validation-max-symbols",
        action="store",
        default=DEFAULT_VALIDATION_MAX_SYMBOLS,
        type=int,
        help=(
            "Maximum number of symbols used in ratio-based random validation "
            f"(default: {DEFAULT_VALIDATION_MAX_SYMBOLS})"
        ),
    )


@pytest.fixture
def validation_sample_size(request):
    """Get sample size from CLI or use default"""
    return request.config.getoption("--validation-sample-size")


@pytest.fixture
def validation_sample_ratio(request):
    """Get OHLCV random-sample ratio (0 < ratio <= 1)."""
    ratio = request.config.getoption("--validation-sample-ratio")
    if ratio <= 0 or ratio > 1:
        raise pytest.UsageError(
            f"Invalid --validation-sample-ratio '{ratio}'. Expected 0 < ratio <= 1."
        )
    return ratio


@pytest.fixture
def validation_max_symbols(request):
    """Get max symbols used for ratio-based random validation."""
    value = request.config.getoption("--validation-max-symbols")
    if value <= 0:
        raise pytest.UsageError(
            f"Invalid --validation-max-symbols '{value}'. Expected integer > 0."
        )
    return value


@pytest.fixture
def validation_interval(request):
    """Get interval string used by temporary validation DB build."""
    return request.config.getoption("--validation-interval")


def _resolve_validation_interval(interval_value: str):
    """Resolve CLI interval string to Interval enum with friendly error."""
    from crypto_data import Interval

    try:
        return Interval(interval_value)
    except ValueError as exc:
        valid_values = ", ".join(interval.value for interval in Interval)
        raise pytest.UsageError(
            f"Invalid --validation-interval '{interval_value}'. "
            f"Expected one of: {valid_values}"
        ) from exc


def _pick_random_closed_month_range(
    lookback_months: int,
    seed: int | None = None,
) -> tuple[str, str]:
    """
    Pick a random closed calendar month in the last `lookback_months` months.

    Returns (start_date, end_date) as YYYY-MM-DD.
    """
    if lookback_months <= 0:
        raise pytest.UsageError(
            f"Invalid --validation-month-lookback '{lookback_months}'. Expected integer > 0."
        )

    rng = random.Random(seed)
    now_utc = datetime.now(timezone.utc).date()
    current_month_start = now_utc.replace(day=1)

    # Candidate months are fully closed months before current month.
    candidates = [current_month_start - relativedelta(months=i) for i in range(1, lookback_months + 1)]
    month_start = rng.choice(candidates)
    month_end = (month_start + relativedelta(months=1)) - relativedelta(days=1)
    return month_start.isoformat(), month_end.isoformat()


def _build_temporary_validation_db(db_path: Path, request) -> None:
    """Create a temporary DB by downloading a compact validation dataset."""
    from crypto_data import DataType, create_binance_database

    start_date = request.config.getoption("--validation-start-date")
    end_date = request.config.getoption("--validation-end-date")
    top_n = request.config.getoption("--validation-top-n")
    interval_value = request.config.getoption("--validation-interval")
    universe_frequency = request.config.getoption("--validation-universe-frequency")
    month_lookback = request.config.getoption("--validation-month-lookback")
    month_seed = request.config.getoption("--validation-month-seed")
    interval = _resolve_validation_interval(interval_value)

    if bool(start_date) ^ bool(end_date):
        raise pytest.UsageError(
            "Provide both --validation-start-date and --validation-end-date, or omit both."
        )
    if not start_date and not end_date:
        start_date, end_date = _pick_random_closed_month_range(
            lookback_months=month_lookback,
            seed=month_seed,
        )

    create_binance_database(
        db_path=str(db_path),
        start_date=start_date,
        end_date=end_date,
        top_n=top_n,
        interval=interval,
        data_types=[DataType.SPOT, DataType.FUTURES],
        exclude_tags=DEFAULT_VALIDATION_EXCLUDE_TAGS,
        exclude_symbols=DEFAULT_VALIDATION_EXCLUDE_SYMBOLS,
        universe_frequency=universe_frequency,
        skip_existing_universe=True,
        daily_quota=200,
    )


@pytest.fixture(scope="session")
def validation_db_path(request, tmp_path_factory):
    """
    Resolve DB path for validation tests.

    Priority:
    1) --validation-db-path
    2) CRYPTO_DATA_DB_PATH env var
    3) Auto-build temporary DB (default behavior)
    """
    explicit_path = request.config.getoption("--validation-db-path")
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise pytest.UsageError(f"Database not found: {path}")
        return str(path)

    env_path = os.environ.get("CRYPTO_DATA_DB_PATH")
    if env_path:
        path = Path(env_path)
        if not path.exists():
            raise pytest.UsageError(f"Database not found via CRYPTO_DATA_DB_PATH: {path}")
        return str(path)

    temp_dir = tmp_path_factory.mktemp("validation-db")
    db_path = temp_dir / "validation_auto.db"
    _build_temporary_validation_db(db_path, request)
    return str(db_path)


@pytest.fixture
async def binance_client():
    """Binance API client for validation"""
    from tests.validation.utils.binance_api import BinanceValidationClient

    async with BinanceValidationClient() as client:
        yield client


@pytest.fixture
async def cmc_client():
    """CoinMarketCap API client for validation"""
    from crypto_data.clients.coinmarketcap import CoinMarketCapClient

    async with CoinMarketCapClient() as client:
        yield client


@pytest.fixture
def validation_collector():
    """Fresh validation collector for each test"""
    return ValidationCollector()


@pytest.fixture
def validation_config():
    """Validation tolerance configuration"""
    return VALIDATION_CONFIG
