"""
Shared fixtures for validation tests.
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, List

import pytest


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
        help="Path to database file for validation tests",
    )


@pytest.fixture
def validation_sample_size(request):
    """Get sample size from CLI or use default"""
    return request.config.getoption("--validation-sample-size")


@pytest.fixture
def validation_db_path(request):
    """Get database path from CLI or environment"""
    db_path = request.config.getoption("--validation-db-path")
    if db_path is None:
        db_path = os.environ.get("CRYPTO_DATA_DB_PATH", "crypto_data.db")

    if not Path(db_path).exists():
        pytest.skip(f"Database not found: {db_path}")

    return db_path


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
