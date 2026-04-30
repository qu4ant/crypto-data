"""
Tests for formatting utilities.

Tests format_file_size() and format_availability_bar() which are used
for display and progress reporting in the crypto-data package.
"""

from datetime import date, datetime

import pytest

from crypto_data.utils.formatting import (
    format_availability_bar,
    format_availability_bar_daily,
    format_file_size,
)

# ============================================================================
# Tests for format_file_size()
# ============================================================================


def test_format_bytes():
    """Test formatting of byte values < 1024."""
    assert format_file_size(0) == "0.00 bytes"
    assert format_file_size(512) == "512.00 bytes"
    assert format_file_size(1023) == "1023.00 bytes"


def test_format_kilobytes():
    """Test formatting of kilobyte values."""
    assert format_file_size(1024) == "1.00 KB"
    assert format_file_size(2048) == "2.00 KB"
    assert format_file_size(1536) == "1.50 KB"
    assert format_file_size(1024 * 1023) == "1023.00 KB"


def test_format_megabytes():
    """Test formatting of megabyte values."""
    assert format_file_size(1024 * 1024) == "1.00 MB"
    assert format_file_size(5 * 1024 * 1024) == "5.00 MB"
    assert format_file_size(int(2.5 * 1024 * 1024)) == "2.50 MB"


def test_format_gigabytes():
    """Test formatting of gigabyte values."""
    assert format_file_size(1024 * 1024 * 1024) == "1.00 GB"
    assert format_file_size(int(1.5 * 1024 * 1024 * 1024)) == "1.50 GB"
    # Example from docstring
    assert format_file_size(1500000000) == "1.40 GB"


def test_format_terabytes():
    """Test formatting of terabyte values."""
    assert format_file_size(1024 * 1024 * 1024 * 1024) == "1.00 TB"
    assert format_file_size(int(2.5 * 1024 * 1024 * 1024 * 1024)) == "2.50 TB"


def test_format_petabytes():
    """Test formatting of very large values (petabytes)."""
    pb = 1024 * 1024 * 1024 * 1024 * 1024
    assert format_file_size(pb) == "1.00 PB"
    assert format_file_size(int(1.5 * pb)) == "1.50 PB"


# ============================================================================
# Tests for format_availability_bar()
# ============================================================================


def test_availability_full_coverage():
    """Test bar when data covers entire requested period."""
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str="2024-01-01",
        last_date_str="2024-12-31",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert pct == 100
    assert months_cov == 12
    assert total_months == 12
    assert "█" in bar  # Should have filled blocks
    assert "░" not in bar or bar.count("░") == 0  # No empty blocks


def test_availability_partial_coverage():
    """Test bar when data covers part of requested period."""
    # Data: May 2024 - Aug 2024 (4 months)
    # Requested: Jan 2024 - Dec 2024 (12 months)
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str="2024-05-01",
        last_date_str="2024-08-31",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert total_months == 12
    assert months_cov == 4
    assert pct == 33  # 4/12 = 33%
    assert "█" in bar  # Should have some filled blocks
    assert "░" in bar  # Should have empty blocks before/after


def test_availability_no_overlap():
    """Test bar when data doesn't overlap with requested period."""
    # Data: 2025
    # Requested: 2024
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str="2025-01-01",
        last_date_str="2025-12-31",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert pct == 0
    assert months_cov == 0
    assert total_months == 12
    # Bar should be all empty
    assert bar.count("░") + bar.count("[") + bar.count("]") >= 24


def test_availability_date_format():
    """Test that date range string is formatted correctly."""
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str="2023-05-15",
        last_date_str="2025-01-20",
        requested_start="2023-01-01",
        requested_end="2025-12-31",
        bar_width=24,
    )

    # Date format should be like "Mai'23→Jan'25"
    assert "→" in date_range or "->" in date_range
    assert "'23" in date_range  # Year abbreviation
    assert "'25" in date_range  # Year abbreviation
    assert "Mai" in date_range or "Jan" in date_range  # French month names


def test_availability_percentage_calculation():
    """Test that percentage is calculated correctly."""
    # 6 months coverage out of 12 months = 50%
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str="2024-01-01",
        last_date_str="2024-06-30",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert total_months == 12
    assert months_cov == 6
    assert pct == 50  # 6/12 = 50%


def test_availability_with_datetime_objects():
    """Test that function accepts datetime objects as well as strings."""
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str=datetime(2024, 1, 1),
        last_date_str=datetime(2024, 12, 31),
        requested_start=datetime(2024, 1, 1),
        requested_end=datetime(2024, 12, 31),
        bar_width=24,
    )

    assert pct == 100
    assert months_cov == 12
    assert total_months == 12


def test_availability_with_date_objects():
    """Test that function accepts date objects as well as strings."""
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str=date(2024, 1, 1),
        last_date_str=date(2024, 12, 31),
        requested_start=date(2024, 1, 1),
        requested_end=date(2024, 12, 31),
        bar_width=24,
    )

    assert pct == 100
    assert months_cov == 12
    assert total_months == 12


def test_availability_bar_structure():
    """Test that bar has correct structure with brackets."""
    bar, _, _, _, _ = format_availability_bar(
        first_date_str="2024-01-01",
        last_date_str="2024-12-31",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=10,
    )

    assert bar.startswith("[")
    assert bar.endswith("]") or "]" in bar  # May have ANSI codes after bracket
    # Should contain visual elements (blocks or empty)
    assert "█" in bar or "░" in bar


def test_availability_single_month():
    """Test bar for single month coverage."""
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str="2024-06-01",
        last_date_str="2024-06-30",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert total_months == 12
    assert months_cov == 1
    assert pct == 8  # 1/12 ≈ 8%
    assert "█" in bar  # Should have at least one filled block


def test_availability_year_boundary():
    """Test that availability calculation works across year boundaries."""
    # Data: Nov 2023 - Feb 2024 (4 months)
    # Requested: Jan 2023 - Dec 2024 (24 months)
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str="2023-11-01",
        last_date_str="2024-02-29",
        requested_start="2023-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert total_months == 24
    assert months_cov == 4
    assert pct == 16  # 4/24 ≈ 16%


# ============================================================================
# Tests for format_availability_bar_daily()
# ============================================================================


def test_availability_daily_full_coverage():
    """Test daily bar when data covers entire requested period."""
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-01-01",
        last_date_str="2024-12-31",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert pct == 100
    assert days_cov == 366  # 2024 is a leap year
    assert total_days == 366
    assert "█" in bar  # Should have filled blocks
    assert "░" not in bar or bar.count("░") == 0  # No empty blocks


def test_availability_daily_partial_coverage():
    """Test daily bar when data covers part of requested period."""
    # Data: Jan 15 - Jan 25 (11 days)
    # Requested: Jan 1 - Jan 31 (31 days)
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-01-15",
        last_date_str="2024-01-25",
        requested_start="2024-01-01",
        requested_end="2024-01-31",
        bar_width=24,
    )

    assert total_days == 31
    assert days_cov == 11
    assert pct == 35  # 11/31 ≈ 35%
    assert "█" in bar  # Should have some filled blocks
    assert "░" in bar  # Should have empty blocks before/after


def test_availability_daily_single_day():
    """Test daily bar for single day coverage."""
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-06-15",
        last_date_str="2024-06-15",
        requested_start="2024-06-01",
        requested_end="2024-06-30",
        bar_width=24,
    )

    assert total_days == 30
    assert days_cov == 1
    assert pct == 3  # 1/30 ≈ 3%
    assert "█" in bar  # Should have at least one filled block


def test_availability_daily_no_overlap():
    """Test daily bar when data doesn't overlap with requested period."""
    # Data: 2025
    # Requested: 2024
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2025-01-01",
        last_date_str="2025-12-31",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert pct == 0
    assert days_cov == 0
    assert total_days == 366  # 2024 is leap year
    # Bar should be all empty
    assert bar.count("░") + bar.count("[") + bar.count("]") >= 24


def test_availability_daily_percentage_calculation():
    """Test that daily percentage is calculated correctly."""
    # 183 days coverage out of 366 days (half of 2024) ≈ 50%
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-01-01",
        last_date_str="2024-06-30",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    assert total_days == 366
    assert days_cov == 182  # Jan 1 to June 30
    assert pct == 49  # 182/366 ≈ 49%


def test_availability_daily_with_datetime_objects():
    """Test that daily function accepts datetime objects."""
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str=datetime(2024, 1, 1),
        last_date_str=datetime(2024, 12, 31),
        requested_start=datetime(2024, 1, 1),
        requested_end=datetime(2024, 12, 31),
        bar_width=24,
    )

    assert pct == 100
    assert days_cov == 366
    assert total_days == 366


def test_availability_daily_with_date_objects():
    """Test that daily function accepts date objects."""
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str=date(2024, 1, 1),
        last_date_str=date(2024, 12, 31),
        requested_start=date(2024, 1, 1),
        requested_end=date(2024, 12, 31),
        bar_width=24,
    )

    assert pct == 100
    assert days_cov == 366
    assert total_days == 366


def test_availability_daily_date_format():
    """Test that date range string is formatted correctly for daily bars."""
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-05-15",
        last_date_str="2024-08-20",
        requested_start="2024-01-01",
        requested_end="2024-12-31",
        bar_width=24,
    )

    # Date format should be like "Mai'24→Aoû'24"
    assert "→" in date_range or "->" in date_range
    assert "'24" in date_range  # Year abbreviation
    assert "Mai" in date_range or "Aoû" in date_range  # French month names


def test_availability_daily_same_start_and_end():
    """Test daily bar when start equals end (single day period)."""
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-06-15",
        last_date_str="2024-06-15",
        requested_start="2024-06-15",
        requested_end="2024-06-15",
        bar_width=24,
    )

    assert total_days == 1
    assert days_cov == 1
    assert pct == 100
    assert "█" in bar  # Should have filled blocks


def test_availability_daily_invalid_date_range():
    """Test daily bar with invalid date range (end before start)."""
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-06-15",
        last_date_str="2024-06-20",
        requested_start="2024-12-31",
        requested_end="2024-01-01",  # End before start
        bar_width=24,
    )

    assert pct == 0
    assert days_cov == 0
    assert "░" in bar  # Should show empty bar
    assert date_range == "Invalid"


def test_availability_daily_leap_year_coverage():
    """Test daily coverage calculation for leap year."""
    # Feb 28 to Mar 1 in leap year 2024 = 3 days (Feb 28, Feb 29, Mar 1)
    bar, pct, days_cov, total_days, date_range = format_availability_bar_daily(
        first_date_str="2024-02-28",
        last_date_str="2024-03-01",
        requested_start="2024-02-28",
        requested_end="2024-03-01",
        bar_width=24,
    )

    # Feb 28, 29 (leap day), Mar 1 = 3 days
    assert total_days == 3
    assert days_cov == 3
    assert pct == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
