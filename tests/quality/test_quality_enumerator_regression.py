"""Regression tests for shared gap enumerators used by quality audits."""

from __future__ import annotations

import pandas as pd
import pytest
from tests.repair.conftest import _insert_funding, _insert_klines

from crypto_data.database import CryptoDatabase
from crypto_data.quality import audit_connection


@pytest.fixture
def repair_db(tmp_path):
    db = CryptoDatabase(str(tmp_path / "quality.db"))
    try:
        yield db
    finally:
        db.close()


def test_kline_gap_finding_shape(repair_db):
    pre = pd.date_range("2024-01-01T00:00", periods=5, freq="4h")
    post = pd.date_range("2024-01-02T16:00", periods=5, freq="4h")
    _insert_klines(repair_db.conn, "futures", "SOLUSDT", "4h", list(pre) + list(post))

    findings = audit_connection(repair_db.conn, tables=["futures"])

    gap_findings = [finding for finding in findings if finding.check_name == "time_gaps"]
    assert len(gap_findings) == 1
    finding = gap_findings[0]
    assert finding.severity == "ERROR"
    assert finding.symbol == "SOLUSDT"
    assert finding.interval == "4h"
    assert finding.count == 1
    assert finding.metadata["expected_seconds"] == 14400
    assert finding.metadata["max_delta_seconds"] == 24 * 3600


def test_funding_gap_finding_shape(repair_db):
    pre = pd.date_range("2024-01-01T00:00", periods=4, freq="8h")
    post = pd.date_range("2024-01-03T08:00", periods=4, freq="8h")
    _insert_funding(repair_db.conn, "BTCUSDT", list(pre) + list(post))

    findings = audit_connection(repair_db.conn, tables=["funding_rates"])

    gap_findings = [finding for finding in findings if finding.check_name == "time_gaps"]
    assert len(gap_findings) == 1
    finding = gap_findings[0]
    assert finding.symbol == "BTCUSDT"
    assert finding.count == 1
    assert finding.metadata["expected_seconds"] == 8 * 3600
    assert finding.metadata["max_delta_seconds"] == 32 * 3600
