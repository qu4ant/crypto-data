"""
Tests for date manipulation utilities.

Tests the generate_month_list() function which is used for generating
monthly snapshots for universe data ingestion.
"""

from datetime import datetime

import pytest

from crypto_data.utils.dates import generate_date_list, generate_month_list


def test_single_month():
    """Test that dates within the same month return a single month."""
    start = datetime(2024, 1, 15)
    end = datetime(2024, 1, 31)

    result = generate_month_list(start, end)

    assert result == ["2024-01"]
    assert len(result) == 1


def test_multiple_months():
    """Test that dates spanning multiple months return all months."""
    start = datetime(2024, 1, 15)
    end = datetime(2024, 3, 20)

    result = generate_month_list(start, end)

    assert result == ["2024-01", "2024-02", "2024-03"]
    assert len(result) == 3


def test_year_boundary():
    """Test that month list correctly crosses year boundaries."""
    start = datetime(2023, 11, 1)
    end = datetime(2024, 2, 28)

    result = generate_month_list(start, end)

    assert result == ["2023-11", "2023-12", "2024-01", "2024-02"]
    assert len(result) == 4
    # Verify year transition
    assert "2023-12" in result
    assert "2024-01" in result


def test_same_date():
    """Test that start and end on same date returns that month."""
    date = datetime(2024, 6, 15)

    result = generate_month_list(date, date)

    assert result == ["2024-06"]
    assert len(result) == 1


def test_first_day_of_month():
    """Test that dates on first day of month work correctly."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 3, 1)

    result = generate_month_list(start, end)

    assert result == ["2024-01", "2024-02", "2024-03"]
    assert len(result) == 3


def test_last_day_of_month():
    """Test that dates on last day of month work correctly."""
    start = datetime(2024, 1, 31)
    end = datetime(2024, 3, 31)

    result = generate_month_list(start, end)

    assert result == ["2024-01", "2024-02", "2024-03"]
    assert len(result) == 3


def test_backward_dates_returns_empty():
    """Test that start date after end date returns empty list."""
    start = datetime(2024, 3, 1)
    end = datetime(2024, 1, 1)

    result = generate_month_list(start, end)

    assert result == []
    assert len(result) == 0


def test_full_year():
    """Test generating all months in a year."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)

    result = generate_month_list(start, end)

    assert len(result) == 12
    assert result[0] == "2024-01"
    assert result[-1] == "2024-12"
    # Verify all months present
    expected = [f"2024-{str(i).zfill(2)}" for i in range(1, 13)]
    assert result == expected


def test_generate_date_list_monthly_matches_month_list():
    """Monthly frequency should match month list with -01 suffix."""
    start = datetime(2024, 1, 15)
    end = datetime(2024, 3, 20)

    monthly = generate_date_list(start, end, frequency="monthly")
    expected = [f"{month}-01" for month in generate_month_list(start, end)]

    assert monthly == expected


def test_generate_date_list_daily_three_days():
    """Daily frequency should include every day in inclusive range."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 3)

    assert generate_date_list(start, end, frequency="daily") == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_generate_date_list_daily_year_boundary():
    """Daily generation should cross year boundaries correctly."""
    start = datetime(2023, 12, 31)
    end = datetime(2024, 1, 2)

    assert generate_date_list(start, end, frequency="daily") == [
        "2023-12-31",
        "2024-01-01",
        "2024-01-02",
    ]


def test_generate_date_list_weekly_aligns_on_monday():
    """Weekly frequency should align snapshots to Mondays."""
    start = datetime(2024, 1, 2)  # Tuesday
    end = datetime(2024, 1, 22)

    assert generate_date_list(start, end, frequency="weekly") == [
        "2024-01-08",
        "2024-01-15",
        "2024-01-22",
    ]


def test_generate_date_list_invalid_frequency_raises():
    """Invalid frequency should raise ValueError."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 2)

    with pytest.raises(ValueError, match="Invalid frequency"):
        generate_date_list(start, end, frequency="hourly")  # type: ignore[arg-type]


@pytest.mark.parametrize("frequency", ["daily", "weekly", "monthly"])
def test_generate_date_list_backward_returns_empty(frequency):
    """All frequencies should return empty when start > end."""
    start = datetime(2024, 2, 1)
    end = datetime(2024, 1, 1)

    assert generate_date_list(start, end, frequency=frequency) == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
