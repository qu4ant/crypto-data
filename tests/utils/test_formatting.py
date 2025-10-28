"""
Tests for formatting utilities.

Tests format_file_size() and format_availability_bar() which are used
for display and progress reporting in the crypto-data package.
"""

import pytest
from datetime import datetime, date

from crypto_data.utils.formatting import format_file_size, format_availability_bar


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
        first_date_str='2024-01-01',
        last_date_str='2024-12-31',
        requested_start='2024-01-01',
        requested_end='2024-12-31',
        bar_width=24
    )

    assert pct == 100
    assert months_cov == 12
    assert total_months == 12
    assert '█' in bar  # Should have filled blocks
    assert '░' not in bar or bar.count('░') == 0  # No empty blocks


def test_availability_partial_coverage():
    """Test bar when data covers part of requested period."""
    # Data: May 2024 - Aug 2024 (4 months)
    # Requested: Jan 2024 - Dec 2024 (12 months)
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str='2024-05-01',
        last_date_str='2024-08-31',
        requested_start='2024-01-01',
        requested_end='2024-12-31',
        bar_width=24
    )

    assert total_months == 12
    assert months_cov == 4
    assert pct == 33  # 4/12 = 33%
    assert '█' in bar  # Should have some filled blocks
    assert '░' in bar  # Should have empty blocks before/after


def test_availability_no_overlap():
    """Test bar when data doesn't overlap with requested period."""
    # Data: 2025
    # Requested: 2024
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str='2025-01-01',
        last_date_str='2025-12-31',
        requested_start='2024-01-01',
        requested_end='2024-12-31',
        bar_width=24
    )

    assert pct == 0
    assert months_cov == 0
    assert total_months == 12
    # Bar should be all empty
    assert bar.count('░') + bar.count('[') + bar.count(']') >= 24


def test_availability_date_format():
    """Test that date range string is formatted correctly."""
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str='2023-05-15',
        last_date_str='2025-01-20',
        requested_start='2023-01-01',
        requested_end='2025-12-31',
        bar_width=24
    )

    # Date format should be like "Mai'23→Jan'25"
    assert '→' in date_range or '->' in date_range
    assert "'23" in date_range  # Year abbreviation
    assert "'25" in date_range  # Year abbreviation
    assert 'Mai' in date_range or 'Jan' in date_range  # French month names


def test_availability_percentage_calculation():
    """Test that percentage is calculated correctly."""
    # 6 months coverage out of 12 months = 50%
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str='2024-01-01',
        last_date_str='2024-06-30',
        requested_start='2024-01-01',
        requested_end='2024-12-31',
        bar_width=24
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
        bar_width=24
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
        bar_width=24
    )

    assert pct == 100
    assert months_cov == 12
    assert total_months == 12


def test_availability_bar_structure():
    """Test that bar has correct structure with brackets."""
    bar, _, _, _, _ = format_availability_bar(
        first_date_str='2024-01-01',
        last_date_str='2024-12-31',
        requested_start='2024-01-01',
        requested_end='2024-12-31',
        bar_width=10
    )

    assert bar.startswith('[')
    assert bar.endswith(']') or ']' in bar  # May have ANSI codes after bracket
    # Should contain visual elements (blocks or empty)
    assert '█' in bar or '░' in bar


def test_availability_single_month():
    """Test bar for single month coverage."""
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str='2024-06-01',
        last_date_str='2024-06-30',
        requested_start='2024-01-01',
        requested_end='2024-12-31',
        bar_width=24
    )

    assert total_months == 12
    assert months_cov == 1
    assert pct == 8  # 1/12 ≈ 8%
    assert '█' in bar  # Should have at least one filled block


def test_availability_year_boundary():
    """Test that availability calculation works across year boundaries."""
    # Data: Nov 2023 - Feb 2024 (4 months)
    # Requested: Jan 2023 - Dec 2024 (24 months)
    bar, pct, months_cov, total_months, date_range = format_availability_bar(
        first_date_str='2023-11-01',
        last_date_str='2024-02-29',
        requested_start='2023-01-01',
        requested_end='2024-12-31',
        bar_width=24
    )

    assert total_months == 24
    assert months_cov == 4
    assert pct == 16  # 4/24 ≈ 16%


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
