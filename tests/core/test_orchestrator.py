"""
Tests for the orchestrator module.

Focuses on unit testing helper functions.
Full integration tests would require mocking network calls.
"""

from datetime import datetime

import pytest

from crypto_data.core.orchestrator import _validate_and_parse_dates


class TestValidateAndParseDates:
    """Tests for _validate_and_parse_dates helper function."""

    def test_valid_dates(self):
        """Test parsing valid date strings."""
        start, end = _validate_and_parse_dates('2024-01-01', '2024-12-31')

        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 12, 31)

    def test_valid_dates_same_day(self):
        """Test parsing when start and end are the same day."""
        start, end = _validate_and_parse_dates('2024-06-15', '2024-06-15')

        assert start == datetime(2024, 6, 15)
        assert end == datetime(2024, 6, 15)

    def test_invalid_start_date_raises(self):
        """Test that invalid start_date format raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('01-01-2024', '2024-12-31')

        assert "Invalid start_date format" in str(exc_info.value)
        assert "01-01-2024" in str(exc_info.value)
        assert "expected YYYY-MM-DD" in str(exc_info.value)

    def test_invalid_end_date_raises(self):
        """Test that invalid end_date format raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024-01-01', '12/31/2024')

        assert "Invalid end_date format" in str(exc_info.value)
        assert "12/31/2024" in str(exc_info.value)
        assert "expected YYYY-MM-DD" in str(exc_info.value)

    def test_start_after_end_raises(self):
        """Test that start_date after end_date raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024-12-31', '2024-01-01')

        assert "cannot be after" in str(exc_info.value)
        assert "2024-12-31" in str(exc_info.value)
        assert "2024-01-01" in str(exc_info.value)

    def test_invalid_date_values_raises(self):
        """Test that invalid date values (e.g., month 13) raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024-13-01', '2024-12-31')

        assert "Invalid start_date format" in str(exc_info.value)

    def test_empty_string_raises(self):
        """Test that empty strings raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('', '2024-12-31')

        assert "Invalid start_date format" in str(exc_info.value)

    def test_none_values_raise(self):
        """Test that None values raise appropriate errors."""
        with pytest.raises((ValueError, TypeError)):
            _validate_and_parse_dates(None, '2024-12-31')

        with pytest.raises((ValueError, TypeError)):
            _validate_and_parse_dates('2024-01-01', None)
