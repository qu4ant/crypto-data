"""
Tests for logging utilities.

Tests ColoredFormatter, setup_colored_logging(), and get_logger()
which provide colored console output for better log readability.
"""

import logging
import os
import re
import tempfile
from pathlib import Path

import pytest

from crypto_data.logging_utils import ColoredFormatter, get_logger, setup_colored_logging

# ============================================================================
# Tests for ColoredFormatter
# ============================================================================


def test_colored_formatter_adds_color_codes():
    """Test that ColoredFormatter adds ANSI color codes to log messages."""
    # Create formatter with colors enabled (force use_color)
    formatter = ColoredFormatter(
        fmt="%(levelname)s - %(message)s", use_color=True, force_color=True
    )

    # Create a log record
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Format the record
    formatted = formatter.format(record)

    # Should contain ANSI escape codes
    assert "\033[" in formatted  # ANSI escape sequence
    assert "INFO" in formatted
    assert "Test message" in formatted


def test_colored_formatter_respects_use_color_false():
    """Test that ColoredFormatter respects use_color=False."""
    # Create formatter with colors disabled
    formatter = ColoredFormatter(fmt="%(levelname)s - %(message)s", use_color=False)

    # Create a log record
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Format the record
    formatted = formatter.format(record)

    # Should NOT contain ANSI escape codes
    assert "\033[" not in formatted
    assert formatted == "INFO - Test message"


def test_colored_formatter_handles_different_levels():
    """Test that ColoredFormatter applies different colors for different log levels."""
    formatter = ColoredFormatter(
        fmt="%(levelname)s - %(message)s", use_color=True, force_color=True
    )

    levels_and_messages = [
        (logging.DEBUG, "Debug message"),
        (logging.INFO, "Info message"),
        (logging.WARNING, "Warning message"),
        (logging.ERROR, "Error message"),
        (logging.CRITICAL, "Critical message"),
    ]

    formatted_messages = []
    for level, msg in levels_and_messages:
        record = logging.LogRecord(
            name="test", level=level, pathname="", lineno=0, msg=msg, args=(), exc_info=None
        )
        formatted = formatter.format(record)
        formatted_messages.append(formatted)

    # Each formatted message should be unique (different color codes)
    assert len(set(formatted_messages)) == len(formatted_messages)

    # All should contain ANSI codes
    for formatted in formatted_messages:
        assert "\033[" in formatted


def test_colored_formatter_highlights_headers():
    """Test that ColoredFormatter highlights header messages with ===."""
    formatter = ColoredFormatter(fmt="%(message)s", use_color=True, force_color=True)

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="=" * 60 + "\nHeader Message\n" + "=" * 60,
        args=(),
        exc_info=None,
    )

    formatted = formatter.format(record)

    # Should contain bold and bright blue codes
    assert "\033[1m" in formatted  # Bold
    assert "\033[94m" in formatted  # Bright blue


def test_colored_formatter_highlights_success():
    """Test that ColoredFormatter highlights success messages."""
    formatter = ColoredFormatter(fmt="%(message)s", use_color=True, force_color=True)

    success_messages = ["✓ Success!", "Success: operation completed", "Import complete"]

    for msg in success_messages:
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg=msg, args=(), exc_info=None
        )

        formatted = formatter.format(record)

        # Should contain bright green code
        assert "\033[92m" in formatted, f"Message '{msg}' should be bright green"


def test_colored_formatter_highlights_progress():
    """Test that ColoredFormatter highlights progress indicators."""
    formatter = ColoredFormatter(fmt="%(message)s", use_color=True, force_color=True)

    progress_messages = ["[1/10] Downloading files"]

    for msg in progress_messages:
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg=msg, args=(), exc_info=None
        )

        formatted = formatter.format(record)

        # Should contain bright cyan code
        assert "\033[96m" in formatted, f"Message '{msg}' should be bright cyan"


def test_colored_formatter_highlights_skipped():
    """Test that ColoredFormatter highlights skipped messages."""
    formatter = ColoredFormatter(fmt="%(message)s", use_color=True, force_color=True)

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Skipped: already exists",
        args=(),
        exc_info=None,
    )

    formatted = formatter.format(record)

    # Should contain bright black (gray) code
    assert "\033[90m" in formatted


def test_colored_formatter_preserves_levelname():
    """Test that ColoredFormatter preserves original levelname after formatting."""
    formatter = ColoredFormatter(fmt="%(levelname)s - %(message)s", use_color=True)

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    original_levelname = record.levelname
    formatter.format(record)

    # Levelname should be restored to original
    assert record.levelname == original_levelname


# ============================================================================
# Tests for setup_colored_logging()
# ============================================================================


def test_setup_colored_logging_configures_root():
    """Test that setup_colored_logging() configures root logger."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Clear existing handlers
            root = logging.getLogger()
            for handler in root.handlers[:]:
                root.removeHandler(handler)

            # Setup colored logging
            setup_colored_logging(level=logging.DEBUG)

            # Root logger should have 2 handlers (console + file)
            assert len(root.handlers) == 2

            # Root logger should have correct level
            assert root.level == logging.DEBUG

            # Both handlers should have ColoredFormatter
            for handler in root.handlers:
                assert isinstance(handler.formatter, ColoredFormatter)

        finally:
            os.chdir(original_dir)


def test_setup_colored_logging_replaces_existing_handlers():
    """Test that setup_colored_logging() replaces existing handlers."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            root = logging.getLogger()

            # Add a dummy handler
            dummy_handler = logging.StreamHandler()
            root.addHandler(dummy_handler)

            # Setup colored logging
            setup_colored_logging()

            # Should have exactly 2 handlers (console + file, old one replaced)
            assert len(root.handlers) == 2

            # Dummy handler should not be in the list
            assert dummy_handler not in root.handlers

        finally:
            os.chdir(original_dir)


def test_setup_colored_logging_accepts_custom_format():
    """Test that setup_colored_logging() accepts custom format strings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            setup_colored_logging(fmt="%(name)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S")

            root = logging.getLogger()

            # Both handlers should have the custom format
            for handler in root.handlers:
                formatter = handler.formatter

                # Formatter should be ColoredFormatter
                assert isinstance(formatter, ColoredFormatter)

                # Custom format should be applied
                assert "%(name)s" in formatter._fmt

        finally:
            os.chdir(original_dir)


# ============================================================================
# Tests for get_logger()
# ============================================================================


def test_get_logger_returns_configured_logger():
    """Test that get_logger() returns a logger with the correct name."""
    logger = get_logger("test_logger")

    assert logger.name == "test_logger"
    assert isinstance(logger, logging.Logger)


def test_get_logger_configures_root():
    """Test that get_logger() calls setup_colored_logging()."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Clear root handlers
            root = logging.getLogger()
            for handler in root.handlers[:]:
                root.removeHandler(handler)

            # Get logger
            logger = get_logger("test_logger", level=logging.WARNING)

            # Root logger should be configured with 2 handlers (console + file)
            assert len(root.handlers) == 2
            assert root.level == logging.WARNING

        finally:
            os.chdir(original_dir)


def test_get_logger_accepts_custom_level():
    """Test that get_logger() accepts custom log level."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            logger = get_logger("test_logger", level=logging.DEBUG)

            root = logging.getLogger()
            assert root.level == logging.DEBUG

        finally:
            os.chdir(original_dir)


# ============================================================================
# Integration test
# ============================================================================


def test_logging_integration(capsys):
    """Test complete logging workflow with actual output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Setup logging
            setup_colored_logging(level=logging.INFO)

            # Get logger
            logger = logging.getLogger("integration_test")

            # Log messages at different levels
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")

            # Capture output
            captured = capsys.readouterr()

            # All messages should appear in stdout
            assert "Info message" in captured.out
            assert "Warning message" in captured.out
            assert "Error message" in captured.out

        finally:
            os.chdir(original_dir)


# ============================================================================
# Tests for File Logging (New in v3.x)
# ============================================================================


def test_setup_colored_logging_creates_log_directory():
    """Test that setup_colored_logging() creates logs directory."""
    # Use a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Setup logging (should create logs/ directory)
            log_file = setup_colored_logging()

            # Logs directory should exist
            logs_dir = Path("logs")
            assert logs_dir.exists()
            assert logs_dir.is_dir()

            # Log file should exist
            assert Path(log_file).exists()

        finally:
            os.chdir(original_dir)


def test_setup_colored_logging_returns_log_file_path():
    """Test that setup_colored_logging() returns the log file path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            log_file = setup_colored_logging()

            # Should return a string
            assert isinstance(log_file, str)

            # Should be a valid path
            assert "logs" in log_file
            assert log_file.endswith(".log")

            # Should match the expected pattern: logs/crypto_data_YYYY-MM-DD_HH-MM-SS.log
            pattern = r"logs[/\\]crypto_data_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.log"
            assert re.match(pattern, log_file), (
                f"Log file path '{log_file}' doesn't match expected pattern"
            )

        finally:
            os.chdir(original_dir)


def test_setup_colored_logging_creates_timestamped_file():
    """Test that log file has timestamped name."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            log_file = setup_colored_logging()

            # Extract filename
            filename = Path(log_file).name

            # Should start with crypto_data_
            assert filename.startswith("crypto_data_")

            # Should end with .log
            assert filename.endswith(".log")

            # Should contain timestamp pattern: YYYY-MM-DD_HH-MM-SS
            # Extract timestamp part (between crypto_data_ and .log)
            timestamp = filename.replace("crypto_data_", "").replace(".log", "")

            # Validate timestamp format
            pattern = r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
            assert re.match(pattern, timestamp), (
                f"Timestamp '{timestamp}' doesn't match expected pattern"
            )

        finally:
            os.chdir(original_dir)


def test_file_logging_receives_log_messages():
    """Test that log messages are written to file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Setup logging
            log_file = setup_colored_logging(level=logging.INFO)

            # Log test messages
            logger = logging.getLogger("test_file_logging")
            logger.info("Test info message")
            logger.warning("Test warning message")
            logger.error("Test error message")

            # Force flush
            for handler in logging.getLogger().handlers:
                handler.flush()

            # Read log file
            with open(log_file, encoding="utf-8") as f:
                log_content = f.read()

            # All messages should be in the file
            assert "Test info message" in log_content
            assert "Test warning message" in log_content
            assert "Test error message" in log_content

        finally:
            os.chdir(original_dir)


def test_file_logging_contains_ansi_color_codes():
    """Test that log file contains ANSI color codes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Setup logging
            log_file = setup_colored_logging(level=logging.INFO)

            # Log a message
            logger = logging.getLogger("test_ansi")
            logger.info("Colored message")

            # Force flush
            for handler in logging.getLogger().handlers:
                handler.flush()

            # Read log file
            with open(log_file, encoding="utf-8") as f:
                log_content = f.read()

            # Should contain ANSI escape codes
            assert "\033[" in log_content, "Log file should contain ANSI color codes"

        finally:
            os.chdir(original_dir)


def test_file_logging_and_console_receive_same_messages(capsys):
    """Test that both console and file receive the same log messages."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Setup logging
            log_file = setup_colored_logging(level=logging.INFO)

            # Log test messages
            logger = logging.getLogger("test_dual_logging")
            logger.info("Dual logging test")
            logger.warning("Warning in dual mode")

            # Force flush
            for handler in logging.getLogger().handlers:
                handler.flush()

            # Capture console output
            captured = capsys.readouterr()

            # Read log file
            with open(log_file, encoding="utf-8") as f:
                log_content = f.read()

            # Both should contain the messages
            assert "Dual logging test" in captured.out
            assert "Dual logging test" in log_content
            assert "Warning in dual mode" in captured.out
            assert "Warning in dual mode" in log_content

        finally:
            os.chdir(original_dir)


def test_setup_colored_logging_logs_file_path(capsys):
    """Test that setup_colored_logging() logs the file path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Setup logging
            log_file = setup_colored_logging()

            # Capture output
            captured = capsys.readouterr()

            # Should log the file path
            assert "Logging to file:" in captured.out
            assert log_file in captured.out

        finally:
            os.chdir(original_dir)


def test_multiple_logging_setups_create_different_files():
    """Test that multiple setup calls create different timestamped files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # First setup
            log_file1 = setup_colored_logging()

            # Wait a bit to ensure different timestamp
            import time

            time.sleep(1)

            # Second setup
            log_file2 = setup_colored_logging()

            # Should be different files
            assert log_file1 != log_file2

            # Both should exist
            assert Path(log_file1).exists()
            assert Path(log_file2).exists()

        finally:
            os.chdir(original_dir)


def test_file_handler_has_correct_properties():
    """Test that file handler is configured correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_dir = os.getcwd()
        try:
            os.chdir(tmpdir)

            # Setup logging
            log_file = setup_colored_logging()

            # Get root logger
            root = logging.getLogger()

            # Should have 2 handlers (console + file)
            assert len(root.handlers) == 2

            # Find file handler
            file_handler = None
            for handler in root.handlers:
                if isinstance(handler, logging.FileHandler):
                    file_handler = handler
                    break

            assert file_handler is not None, "Should have a FileHandler"

            # File handler should use ColoredFormatter
            assert isinstance(file_handler.formatter, ColoredFormatter)

            # File handler should be writing to the correct file
            assert file_handler.baseFilename == str(Path(log_file).resolve())

        finally:
            os.chdir(original_dir)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
