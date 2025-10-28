"""
Tests for logging utilities.

Tests ColoredFormatter, setup_colored_logging(), and get_logger()
which provide colored console output for better log readability.
"""

import pytest
import logging
import sys
from io import StringIO

from crypto_data.logging_utils import ColoredFormatter, setup_colored_logging, get_logger


# ============================================================================
# Tests for ColoredFormatter
# ============================================================================

def test_colored_formatter_adds_color_codes():
    """Test that ColoredFormatter adds ANSI color codes to log messages."""
    # Create formatter with colors enabled (force use_color)
    formatter = ColoredFormatter(
        fmt='%(levelname)s - %(message)s',
        use_color=True
    )
    # Override auto-detection for testing
    formatter.use_color = True

    # Create a log record
    record = logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='',
        lineno=0,
        msg='Test message',
        args=(),
        exc_info=None
    )

    # Format the record
    formatted = formatter.format(record)

    # Should contain ANSI escape codes
    assert '\033[' in formatted  # ANSI escape sequence
    assert 'INFO' in formatted
    assert 'Test message' in formatted


def test_colored_formatter_respects_use_color_false():
    """Test that ColoredFormatter respects use_color=False."""
    # Create formatter with colors disabled
    formatter = ColoredFormatter(
        fmt='%(levelname)s - %(message)s',
        use_color=False
    )

    # Create a log record
    record = logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='',
        lineno=0,
        msg='Test message',
        args=(),
        exc_info=None
    )

    # Format the record
    formatted = formatter.format(record)

    # Should NOT contain ANSI escape codes
    assert '\033[' not in formatted
    assert formatted == 'INFO - Test message'


def test_colored_formatter_handles_different_levels():
    """Test that ColoredFormatter applies different colors for different log levels."""
    formatter = ColoredFormatter(
        fmt='%(levelname)s - %(message)s',
        use_color=True
    )
    # Override auto-detection for testing
    formatter.use_color = True

    levels_and_messages = [
        (logging.DEBUG, 'Debug message'),
        (logging.INFO, 'Info message'),
        (logging.WARNING, 'Warning message'),
        (logging.ERROR, 'Error message'),
        (logging.CRITICAL, 'Critical message'),
    ]

    formatted_messages = []
    for level, msg in levels_and_messages:
        record = logging.LogRecord(
            name='test',
            level=level,
            pathname='',
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None
        )
        formatted = formatter.format(record)
        formatted_messages.append(formatted)

    # Each formatted message should be unique (different color codes)
    assert len(set(formatted_messages)) == len(formatted_messages)

    # All should contain ANSI codes
    for formatted in formatted_messages:
        assert '\033[' in formatted


def test_colored_formatter_highlights_headers():
    """Test that ColoredFormatter highlights header messages with ===."""
    formatter = ColoredFormatter(
        fmt='%(message)s',
        use_color=True
    )
    # Override auto-detection for testing
    formatter.use_color = True

    record = logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='',
        lineno=0,
        msg='=' * 60 + '\nHeader Message\n' + '=' * 60,
        args=(),
        exc_info=None
    )

    formatted = formatter.format(record)

    # Should contain bold and bright blue codes
    assert '\033[1m' in formatted  # Bold
    assert '\033[94m' in formatted  # Bright blue


def test_colored_formatter_highlights_success():
    """Test that ColoredFormatter highlights success messages."""
    formatter = ColoredFormatter(
        fmt='%(message)s',
        use_color=True
    )
    # Override auto-detection for testing
    formatter.use_color = True

    success_messages = [
        '✓ Success!',
        'Success: operation completed',
        'Import complete'
    ]

    for msg in success_messages:
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None
        )

        formatted = formatter.format(record)

        # Should contain bright green code
        assert '\033[92m' in formatted, f"Message '{msg}' should be bright green"


def test_colored_formatter_highlights_progress():
    """Test that ColoredFormatter highlights progress indicators."""
    formatter = ColoredFormatter(
        fmt='%(message)s',
        use_color=True
    )
    # Override auto-detection for testing
    formatter.use_color = True

    progress_messages = [
        'Processing data...',
        '[1/10] Downloading files'
    ]

    for msg in progress_messages:
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None
        )

        formatted = formatter.format(record)

        # Should contain bright cyan code
        assert '\033[96m' in formatted, f"Message '{msg}' should be bright cyan"


def test_colored_formatter_highlights_skipped():
    """Test that ColoredFormatter highlights skipped messages."""
    formatter = ColoredFormatter(
        fmt='%(message)s',
        use_color=True
    )
    # Override auto-detection for testing
    formatter.use_color = True

    record = logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='',
        lineno=0,
        msg='Skipped: already exists',
        args=(),
        exc_info=None
    )

    formatted = formatter.format(record)

    # Should contain bright black (gray) code
    assert '\033[90m' in formatted


def test_colored_formatter_preserves_levelname():
    """Test that ColoredFormatter preserves original levelname after formatting."""
    formatter = ColoredFormatter(
        fmt='%(levelname)s - %(message)s',
        use_color=True
    )

    record = logging.LogRecord(
        name='test',
        level=logging.INFO,
        pathname='',
        lineno=0,
        msg='Test message',
        args=(),
        exc_info=None
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
    # Clear existing handlers
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Setup colored logging
    setup_colored_logging(level=logging.DEBUG)

    # Root logger should have handlers
    assert len(root.handlers) > 0

    # Root logger should have correct level
    assert root.level == logging.DEBUG

    # Handler should have ColoredFormatter
    handler = root.handlers[0]
    assert isinstance(handler.formatter, ColoredFormatter)


def test_setup_colored_logging_replaces_existing_handlers():
    """Test that setup_colored_logging() replaces existing handlers."""
    root = logging.getLogger()

    # Add a dummy handler
    dummy_handler = logging.StreamHandler()
    root.addHandler(dummy_handler)
    initial_count = len(root.handlers)

    # Setup colored logging
    setup_colored_logging()

    # Should have exactly 1 handler (old one replaced)
    assert len(root.handlers) == 1
    assert root.handlers[0] != dummy_handler


def test_setup_colored_logging_accepts_custom_format():
    """Test that setup_colored_logging() accepts custom format strings."""
    setup_colored_logging(
        fmt='%(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    root = logging.getLogger()
    handler = root.handlers[0]
    formatter = handler.formatter

    # Formatter should be ColoredFormatter
    assert isinstance(formatter, ColoredFormatter)

    # Custom format should be applied
    assert '%(name)s' in formatter._fmt


# ============================================================================
# Tests for get_logger()
# ============================================================================

def test_get_logger_returns_configured_logger():
    """Test that get_logger() returns a logger with the correct name."""
    logger = get_logger('test_logger')

    assert logger.name == 'test_logger'
    assert isinstance(logger, logging.Logger)


def test_get_logger_configures_root():
    """Test that get_logger() calls setup_colored_logging()."""
    # Clear root handlers
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Get logger
    logger = get_logger('test_logger', level=logging.WARNING)

    # Root logger should be configured
    assert len(root.handlers) > 0
    assert root.level == logging.WARNING


def test_get_logger_accepts_custom_level():
    """Test that get_logger() accepts custom log level."""
    logger = get_logger('test_logger', level=logging.DEBUG)

    root = logging.getLogger()
    assert root.level == logging.DEBUG


# ============================================================================
# Integration test
# ============================================================================

def test_logging_integration(capsys):
    """Test complete logging workflow with actual output."""
    # Setup logging
    setup_colored_logging(level=logging.INFO)

    # Get logger
    logger = logging.getLogger('integration_test')

    # Log messages at different levels
    logger.info('Info message')
    logger.warning('Warning message')
    logger.error('Error message')

    # Capture output
    captured = capsys.readouterr()

    # All messages should appear in stdout
    assert 'Info message' in captured.out
    assert 'Warning message' in captured.out
    assert 'Error message' in captured.out


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
