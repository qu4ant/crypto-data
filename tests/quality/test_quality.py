from __future__ import annotations

from datetime import datetime, timedelta

import duckdb

from crypto_data.quality import (
    QualityConfig,
    QualityFinding,
    audit_connection,
    findings_to_dataframe,
    format_findings,
)


def create_spot_table(conn):
    conn.execute(
        """
        CREATE TABLE spot (
            exchange VARCHAR,
            symbol VARCHAR,
            interval VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trades_count INTEGER,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE
        )
        """
    )


def create_funding_rates_table(conn):
    conn.execute(
        """
        CREATE TABLE funding_rates (
            exchange VARCHAR,
            symbol VARCHAR,
            timestamp TIMESTAMP,
            funding_rate DOUBLE
        )
        """
    )


def create_universe_table(conn):
    conn.execute(
        """
        CREATE TABLE crypto_universe (
            date DATE,
            symbol VARCHAR,
            rank INTEGER,
            market_cap DOUBLE,
            categories VARCHAR
        )
        """
    )


def make_spot_row(
    timestamp,
    *,
    symbol="BTCUSDT",
    interval="4h",
    open_=100.0,
    high=101.0,
    low=99.0,
    close=100.5,
    volume=100.0,
):
    return (
        "binance",
        symbol,
        interval,
        timestamp,
        open_,
        high,
        low,
        close,
        volume,
        volume * 100,
        10,
        volume / 2,
        volume * 50,
    )


def insert_spot_rows(conn, rows):
    conn.executemany(
        """
        INSERT INTO spot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def finding_names(findings):
    return {
        (finding.table, finding.symbol, finding.interval, finding.check_name)
        for finding in findings
    }


def test_clean_spot_table_has_no_findings():
    conn = duckdb.connect(":memory:")
    create_spot_table(conn)
    start = datetime(2024, 1, 1)
    insert_spot_rows(conn, [make_spot_row(start + timedelta(hours=4 * i)) for i in range(5)])

    findings = audit_connection(conn, tables=["spot"])

    assert findings == []


def test_spot_gap_is_error():
    conn = duckdb.connect(":memory:")
    create_spot_table(conn)
    start = datetime(2024, 1, 1)
    insert_spot_rows(
        conn,
        [
            make_spot_row(start),
            make_spot_row(start + timedelta(hours=4)),
            make_spot_row(start + timedelta(hours=12)),
        ],
    )

    findings = audit_connection(conn, tables=["spot"])

    assert ("spot", "BTCUSDT", "4h", "time_gaps") in finding_names(findings)
    gap = next(finding for finding in findings if finding.check_name == "time_gaps")
    assert gap.severity == "ERROR"
    assert gap.count == 1
    assert gap.metadata["expected_seconds"] == 4 * 3600
    assert gap.metadata["max_delta_seconds"] == 8 * 3600


def test_spot_invalid_ohlc_is_error():
    conn = duckdb.connect(":memory:")
    create_spot_table(conn)
    row = make_spot_row(
        datetime(2024, 1, 1),
        open_=100.0,
        high=95.0,
        low=96.0,
        close=100.0,
    )
    insert_spot_rows(conn, [row])

    findings = audit_connection(conn, tables=["spot"])

    assert ("spot", "BTCUSDT", "4h", "invalid_ohlc") in finding_names(findings)
    ohlc = next(finding for finding in findings if finding.check_name == "invalid_ohlc")
    assert ohlc.severity == "ERROR"
    assert ohlc.count == 1


def test_spot_duplicate_primary_key_is_error():
    conn = duckdb.connect(":memory:")
    create_spot_table(conn)
    row = make_spot_row(datetime(2024, 1, 1))
    insert_spot_rows(conn, [row, row])

    findings = audit_connection(conn, tables=["spot"])

    assert ("spot", "BTCUSDT", "4h", "duplicate_primary_key") in finding_names(findings)
    duplicate = next(
        finding for finding in findings if finding.check_name == "duplicate_primary_key"
    )
    assert duplicate.severity == "ERROR"
    assert duplicate.count == 1


def test_spot_volume_outlier_is_warning():
    conn = duckdb.connect(":memory:")
    create_spot_table(conn)
    start = datetime(2024, 1, 1)
    volumes = [100 + i for i in range(20)] + [10_000]
    rows = [
        make_spot_row(
            start + timedelta(hours=4 * i),
            close=100 + i * 0.01,
            volume=float(volume),
        )
        for i, volume in enumerate(volumes)
    ]
    insert_spot_rows(conn, rows)

    findings = audit_connection(conn, tables=["spot"])

    assert ("spot", "BTCUSDT", "4h", "volume_outliers") in finding_names(findings)
    outlier = next(finding for finding in findings if finding.check_name == "volume_outliers")
    assert outlier.severity == "WARN"
    assert outlier.count == 1


def test_funding_rate_gap_is_error():
    conn = duckdb.connect(":memory:")
    create_funding_rates_table(conn)
    start = datetime(2024, 1, 1)
    conn.executemany(
        "INSERT INTO funding_rates VALUES (?, ?, ?, ?)",
        [
            ("binance", "BTCUSDT", start, 0.0001),
            ("binance", "BTCUSDT", start + timedelta(hours=8), 0.0001),
            ("binance", "BTCUSDT", start + timedelta(hours=24), 0.0001),
        ],
    )

    findings = audit_connection(conn, tables=["funding_rates"])

    assert ("funding_rates", "BTCUSDT", None, "time_gaps") in finding_names(findings)
    gap = next(finding for finding in findings if finding.check_name == "time_gaps")
    assert gap.severity == "ERROR"
    assert gap.count == 1
    assert gap.metadata["expected_seconds"] == 8 * 3600
    assert gap.metadata["max_delta_seconds"] == 16 * 3600


def test_universe_missing_daily_snapshot_is_error():
    conn = duckdb.connect(":memory:")
    create_universe_table(conn)
    conn.executemany(
        "INSERT INTO crypto_universe VALUES (?, ?, ?, ?, ?)",
        [
            ("2024-01-01", "BTC", 1, 1_000_000.0, ""),
            ("2024-01-03", "BTC", 1, 1_000_000.0, ""),
        ],
    )

    findings = audit_connection(
        conn,
        tables=["crypto_universe"],
        config=QualityConfig(
            universe_frequency="daily",
            universe_start_date="2024-01-01",
            universe_end_date="2024-01-03",
        ),
    )

    assert ("crypto_universe", None, None, "missing_snapshots") in finding_names(findings)
    gap = next(finding for finding in findings if finding.check_name == "missing_snapshots")
    assert gap.severity == "ERROR"
    assert gap.count == 1
    assert gap.metadata["sample_missing_dates"] == ["2024-01-02"]


def test_universe_top_n_rank_checks():
    conn = duckdb.connect(":memory:")
    create_universe_table(conn)
    conn.executemany(
        "INSERT INTO crypto_universe VALUES (?, ?, ?, ?, ?)",
        [
            ("2024-01-01", "BTC", 1, 1_000_000.0, ""),
            ("2024-01-01", "ETH", 2, 900_000.0, ""),
            ("2024-01-02", "BTC", 1, 1_000_000.0, ""),
            ("2024-01-02", "BAD", 4, 100_000.0, ""),
        ],
    )

    findings = audit_connection(
        conn,
        tables=["crypto_universe"],
        config=QualityConfig(universe_top_n=3),
    )

    names = finding_names(findings)
    assert ("crypto_universe", None, None, "rank_above_top_n") in names
    assert ("crypto_universe", None, None, "rank_coverage_below_top_n") in names
    above = next(finding for finding in findings if finding.check_name == "rank_above_top_n")
    below = next(
        finding for finding in findings if finding.check_name == "rank_coverage_below_top_n"
    )
    assert above.severity == "ERROR"
    assert below.severity == "WARN"


def test_findings_report_schema_and_formatting():
    findings = [
        QualityFinding(
            severity="WARN",
            table="spot",
            check_name="volume_outliers",
            message="1 volume outlier detected",
            count=1,
            symbol="BTCUSDT",
            interval="4h",
        )
    ]

    df = findings_to_dataframe(findings)
    report = format_findings(findings, db_path="crypto_data.db")

    assert list(df.columns) == [
        "severity",
        "table",
        "check_name",
        "message",
        "count",
        "symbol",
        "interval",
        "first_timestamp",
        "last_timestamp",
        "metadata",
    ]
    assert "WARNINGS: 1" in report
    assert "[WARN] spot BTCUSDT 4h volume_outliers" in report
