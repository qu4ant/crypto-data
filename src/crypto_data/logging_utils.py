"""
Colored Logging Utilities

Provides colored console output for better log readability.
Compatible with both Unix/Linux/macOS and Windows terminals.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import ClassVar


class ColoredFormatter(logging.Formatter):
    """
    Colored log formatter using ANSI escape codes.

    Colors:
    - DEBUG: Cyan
    - INFO: Green (or custom for headers/success)
    - WARNING: Yellow
    - ERROR: Red
    - CRITICAL: Red + Bold
    """

    # ANSI color codes
    RESET = "\033[0m"
    BOLD = "\033[1m"

    # Foreground colors (used in LEVEL_COLORS)
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"

    # Bright colors (used in format method)
    BRIGHT_BLACK = "\033[90m"  # Gray
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"

    # Level colors
    LEVEL_COLORS: ClassVar[dict[str, str]] = {
        "DEBUG": CYAN,
        "INFO": GREEN,
        "WARNING": YELLOW,
        "ERROR": RED,
        "CRITICAL": BOLD + RED,
    }

    def __init__(self, fmt=None, datefmt=None, use_color=True, force_color=False):
        """
        Initialize colored formatter.

        Parameters
        ----------
        fmt : str, optional
            Log format string
        datefmt : str, optional
            Date format string
        use_color : bool, optional
            Enable/disable colors (default: True, auto-detected)
        force_color : bool, optional
            Force colors even if TTY detection fails (default: False, useful for file logging)
        """
        super().__init__(fmt, datefmt)

        # Auto-detect if terminal supports colors, or force if requested
        if force_color:
            self.use_color = use_color
        else:
            self.use_color = use_color and self._supports_color()

    def _supports_color(self):
        """Check if terminal supports ANSI colors."""
        # Check if output is a TTY
        if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
            return False

        # Windows check
        if sys.platform == "win32":
            # Windows 10+ supports ANSI colors in CMD/PowerShell
            try:
                import ctypes

                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
                return True
            except Exception:
                return False

        # Unix/Linux/macOS
        return True

    def format(self, record):
        """Format log record with colors."""
        if not self.use_color:
            return super().format(record)

        # Save original levelname
        levelname_orig = record.levelname

        # Color the level name
        levelname_color = self.LEVEL_COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{levelname_color}{record.levelname}{self.RESET}"

        # Special handling for different message types
        message = record.getMessage()

        # Headers (lines with "===")
        if "=" * 10 in message:
            record.msg = f"{self.BOLD}{self.BRIGHT_BLUE}{message}{self.RESET}"

        # Success indicators
        elif "✓" in message or "Success" in message or "complete" in message:
            record.msg = f"{self.BRIGHT_GREEN}{message}{self.RESET}"

        # Progress indicators
        elif "Downloading" in message or message.startswith("["):
            record.msg = f"{self.BRIGHT_CYAN}{message}{self.RESET}"

        # Skip indicators
        elif "Skipped" in message or "already exists" in message:
            record.msg = f"{self.BRIGHT_BLACK}{message}{self.RESET}"

        # Filtered/Removed indicators
        elif (
            "Filtered" in message
            or "Removed" in message
            or "Excluding" in message
            or "Excluded" in message
        ):
            record.msg = f"{self.YELLOW}{message}{self.RESET}"

        # Arrow indicators (→, ↳)
        elif "→" in message or "↳" in message:
            record.msg = f"{self.BRIGHT_MAGENTA}{message}{self.RESET}"

        # Format the record
        formatted = super().format(record)

        # Restore original levelname
        record.levelname = levelname_orig

        return formatted


def setup_colored_logging(level=logging.INFO, fmt=None, datefmt=None):
    """
    Configure root logger with colored output to both console and file.

    Automatically saves logs to './logs/' directory with timestamped filenames.
    Both console and file outputs include ANSI color codes.

    Parameters
    ----------
    level : int, optional
        Logging level (default: logging.INFO)
    fmt : str, optional
        Log format string (default: '%(asctime)s - %(levelname)s - %(message)s')
    datefmt : str, optional
        Date format string (default: '%Y-%m-%d %H:%M:%S')

    Returns
    -------
    str
        Path to the log file

    Example
    -------
    >>> from crypto_data.logging_utils import setup_colored_logging
    >>> log_file = setup_colored_logging()
    >>> logger = logging.getLogger(__name__)
    >>> logger.info("This is colored!")
    """
    if fmt is None:
        fmt = "%(asctime)s - %(levelname)s - %(message)s"

    if datefmt is None:
        datefmt = "%Y-%m-%d %H:%M:%S"

    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Generate timestamped log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = logs_dir / f"crypto_data_{timestamp}.log"

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create colored formatter for console (auto-detect TTY)
    console_formatter = ColoredFormatter(fmt=fmt, datefmt=datefmt)

    # Create colored formatter for file (force colors, no TTY detection)
    file_formatter = ColoredFormatter(fmt=fmt, datefmt=datefmt, use_color=True, force_color=True)

    # Add console handler with colored formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Add file handler with colored formatter
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Log the file path (will appear in both console and file)
    log_path = str(log_file)
    root_logger.info(f"Logging to file: {log_path}")
    root_logger.info("")

    return log_path


# Convenience function for scripts
def get_logger(name, level=logging.INFO):
    """
    Get a logger with colored output.

    Parameters
    ----------
    name : str
        Logger name (usually __name__)
    level : int, optional
        Logging level (default: logging.INFO)

    Returns
    -------
    logging.Logger
        Configured logger instance

    Example
    -------
    >>> from crypto_data.logging_utils import get_logger
    >>> logger = get_logger(__name__)
    >>> logger.info("Colored log message")
    """
    setup_colored_logging(level=level)
    return logging.getLogger(name)
