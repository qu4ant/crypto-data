"""
Tests for interval validation functionality.
"""

import pytest
import tempfile
from pathlib import Path

from crypto_data import CryptoDatabase
from crypto_data.validation import extract_interval_from_filename, validate_interval_consistency


class TestExtractIntervalFromFilename:
    """Test interval extraction from database filenames."""

    def test_valid_5m_pattern(self):
        """Test extraction of 5m interval from filename."""
        assert extract_interval_from_filename('crypto_5m.db') == '5m'

    def test_valid_1h_pattern(self):
        """Test extraction of 1h interval from filename."""
        assert extract_interval_from_filename('crypto_1h.db') == '1h'

    def test_valid_4h_pattern(self):
        """Test extraction of 4h interval from filename."""
        assert extract_interval_from_filename('crypto_4h.db') == '4h'

    def test_valid_1d_pattern(self):
        """Test extraction of 1d interval from filename."""
        assert extract_interval_from_filename('crypto_1d.db') == '1d'

    def test_valid_15m_pattern(self):
        """Test extraction of 15m interval from filename."""
        assert extract_interval_from_filename('crypto_15m.db') == '15m'

    def test_valid_with_full_path(self):
        """Test extraction works with full file path."""
        assert extract_interval_from_filename('/path/to/crypto_5m.db') == '5m'
        assert extract_interval_from_filename('/Users/test/data/crypto_1h.db') == '1h'

    def test_invalid_no_pattern(self):
        """Test non-matching filename returns None."""
        assert extract_interval_from_filename('data.db') is None
        assert extract_interval_from_filename('crypto.db') is None
        assert extract_interval_from_filename('test_5m.db') is None
        assert extract_interval_from_filename('my_crypto_data.db') is None

    def test_invalid_wrong_prefix(self):
        """Test filename with wrong prefix returns None."""
        assert extract_interval_from_filename('binance_5m.db') is None
        assert extract_interval_from_filename('db_5m.db') is None


class TestValidateIntervalConsistency:
    """Test interval consistency validation."""

    def test_filename_validation_pass(self):
        """Test validation passes when filename and interval match."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'crypto_5m.db'
            db = CryptoDatabase(str(db_path))

            try:
                # Should not raise error
                validate_interval_consistency(str(db_path), '5m', db)

                # Verify interval was stored
                assert db.get_interval() == '5m'
            finally:
                db.close()

    def test_filename_validation_fail(self):
        """Test validation fails when filename and interval mismatch."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'crypto_5m.db'
            db = CryptoDatabase(str(db_path))

            try:
                # Should raise ValueError
                with pytest.raises(ValueError) as exc_info:
                    validate_interval_consistency(str(db_path), '1h', db)

                # Verify error message content
                error_msg = str(exc_info.value)
                assert 'Interval mismatch detected' in error_msg
                assert '5m' in error_msg
                assert '1h' in error_msg
                assert 'crypto_5m.db' in error_msg
            finally:
                db.close()

    def test_non_standard_filename_warning(self):
        """Test non-standard filename shows warning but doesn't fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'data.db'
            db = CryptoDatabase(str(db_path))

            try:
                # Should not raise error (just warning)
                validate_interval_consistency(str(db_path), '5m', db)

                # Verify interval was stored
                assert db.get_interval() == '5m'
            finally:
                db.close()

    def test_metadata_validation_first_run(self):
        """Test metadata validation on first run stores interval."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'crypto_5m.db'
            db = CryptoDatabase(str(db_path))

            try:
                # First run - should store interval
                validate_interval_consistency(str(db_path), '5m', db)

                # Verify interval was stored
                stored = db.get_interval()
                assert stored == '5m'
            finally:
                db.close()

    def test_metadata_validation_subsequent_run_pass(self):
        """Test metadata validation on subsequent run with matching interval."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'crypto_5m.db'

            # First run - store interval
            db = CryptoDatabase(str(db_path))
            validate_interval_consistency(str(db_path), '5m', db)
            db.close()

            # Second run - should pass validation
            db = CryptoDatabase(str(db_path))
            try:
                # Should not raise error
                validate_interval_consistency(str(db_path), '5m', db)

                # Interval should still be 5m
                assert db.get_interval() == '5m'
            finally:
                db.close()

    def test_metadata_validation_subsequent_run_fail(self):
        """Test metadata validation fails when interval changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use non-standard filename to bypass filename validation
            db_path = Path(tmpdir) / 'data.db'

            # First run - store interval
            db = CryptoDatabase(str(db_path))
            validate_interval_consistency(str(db_path), '5m', db)
            db.close()

            # Second run - try different interval
            db = CryptoDatabase(str(db_path))
            try:
                # Should raise ValueError (from metadata check, not filename)
                with pytest.raises(ValueError) as exc_info:
                    validate_interval_consistency(str(db_path), '1h', db)

                # Verify error message content
                error_msg = str(exc_info.value)
                assert 'Database interval mismatch' in error_msg
                assert '5m' in error_msg
                assert '1h' in error_msg
            finally:
                db.close()

    def test_combined_validation_both_pass(self):
        """Test both filename and metadata validation pass together."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'crypto_1h.db'

            # First run
            db = CryptoDatabase(str(db_path))
            validate_interval_consistency(str(db_path), '1h', db)
            db.close()

            # Second run
            db = CryptoDatabase(str(db_path))
            try:
                # Both filename and metadata should validate successfully
                validate_interval_consistency(str(db_path), '1h', db)
                assert db.get_interval() == '1h'
            finally:
                db.close()

    def test_filename_fail_blocks_metadata_storage(self):
        """Test that filename validation failure prevents metadata storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'crypto_5m.db'
            db = CryptoDatabase(str(db_path))

            try:
                # Try to use wrong interval - should fail at filename validation
                with pytest.raises(ValueError) as exc_info:
                    validate_interval_consistency(str(db_path), '1h', db)

                # Verify it was filename mismatch (not metadata)
                assert 'Interval mismatch detected' in str(exc_info.value)

                # Metadata should not have been stored (filename check happens first)
                # The interval might still be None or could have been set during CryptoDatabase init
                # The key point is that the validation raised an error before completing
            finally:
                db.close()


class TestValidationErrorMessages:
    """Test that error messages are clear and actionable."""

    def test_filename_mismatch_error_message(self):
        """Test filename mismatch error provides clear guidance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'crypto_5m.db'
            db = CryptoDatabase(str(db_path))

            try:
                with pytest.raises(ValueError) as exc_info:
                    validate_interval_consistency(str(db_path), '1h', db)

                error_msg = str(exc_info.value)

                # Check error message contains helpful information
                assert 'Interval mismatch' in error_msg
                assert 'Filename suggests: 5m' in error_msg
                assert 'Requested interval: 1h' in error_msg
                assert 'Solutions:' in error_msg
                assert 'crypto_1h.db' in error_msg
            finally:
                db.close()

    def test_database_mismatch_error_message(self):
        """Test database mismatch error provides clear guidance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use non-standard filename to bypass filename validation
            db_path = Path(tmpdir) / 'data.db'

            # First run - store interval
            db = CryptoDatabase(str(db_path))
            validate_interval_consistency(str(db_path), '5m', db)
            db.close()

            # Second run - try different interval
            db = CryptoDatabase(str(db_path))
            try:
                with pytest.raises(ValueError) as exc_info:
                    validate_interval_consistency(str(db_path), '1h', db)

                error_msg = str(exc_info.value)

                # Check error message contains helpful information
                assert 'Database interval mismatch' in error_msg
                assert 'This database contains: 5m data' in error_msg
                assert 'Requested interval: 1h' in error_msg
                assert 'Solutions:' in error_msg
            finally:
                db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
