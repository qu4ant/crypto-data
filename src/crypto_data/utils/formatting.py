"""
Formatting Utilities

Provides display and formatting utilities for the crypto-data package.
"""

from datetime import date, datetime


def format_file_size(size_bytes: int) -> str:
    """
    Format bytes as human-readable file size.

    Parameters
    ----------
    size_bytes : int
        File size in bytes

    Returns
    -------
    str
        Human-readable file size (e.g., "1.47 GB")

    Example
    -------
    >>> format_file_size(1500000000)
    '1.40 GB'
    """
    for unit in ["bytes", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def format_availability_bar(
    first_date_str, last_date_str, requested_start, requested_end, bar_width: int = 24
) -> tuple:
    """
    Format availability as visual timeline progress bar.

    Shows WHEN data is available (temporal positioning), not just coverage %.
    Uses blue colored blocks positioned according to actual timeline.

    Parameters
    ----------
    first_date_str : str, date, or datetime
        First date available (YYYY-MM-DD string or date object)
    last_date_str : str, date, or datetime
        Last date available (YYYY-MM-DD string or date object)
    requested_start : str, date, or datetime
        Requested start date (YYYY-MM-DD string or date object)
    requested_end : str, date, or datetime
        Requested end date (YYYY-MM-DD string or date object)
    bar_width : int
        Width of progress bar in characters (default: 24)

    Returns
    -------
    tuple
        (bar_string, percentage, months_covered, total_months, date_range_str)

    Example
    -------
    >>> bar, pct, months, total, dates = format_availability_bar(
    ...     '2023-05-01', '2025-01-31', '2022-01-01', '2025-01-31'
    ... )
    >>> # bar shows temporal positioning with blue blocks
    >>> # dates shows range like "Mai'23->Jan'25"
    """
    # ANSI color codes
    BLUE = "\033[34m"
    RESET = "\033[0m"

    # Helper to convert to datetime
    def to_datetime(d):
        if isinstance(d, datetime):
            return d
        if isinstance(d, date):
            return datetime.combine(d, datetime.min.time())
        # string
        return datetime.strptime(d, "%Y-%m-%d")

    # Parse dates
    first = to_datetime(first_date_str)
    last = to_datetime(last_date_str)
    req_start = to_datetime(requested_start)
    req_end = to_datetime(requested_end)

    # Total months in requested period
    total_months = (req_end.year - req_start.year) * 12 + (req_end.month - req_start.month) + 1

    # Months covered (clip to requested period)
    actual_start = max(first, req_start)
    actual_end = min(last, req_end)

    if actual_start <= actual_end:
        months_covered = (
            (actual_end.year - actual_start.year) * 12 + (actual_end.month - actual_start.month) + 1
        )
    else:
        months_covered = 0

    # Calculate percentage
    percentage = int((months_covered / total_months) * 100) if total_months > 0 else 0

    # Calculate temporal positions (in days for precision)
    total_days = (req_end - req_start).days
    start_offset = (actual_start - req_start).days
    end_offset = (actual_end - req_start).days

    # Map to bar positions
    if total_days > 0:
        start_pos = int((start_offset / total_days) * bar_width)
        end_pos = int((end_offset / total_days) * bar_width)
        # Ensure at least 1 block if there's data
        if end_pos == start_pos and months_covered > 0:
            end_pos = start_pos + 1
    else:
        start_pos = 0
        end_pos = bar_width if months_covered > 0 else 0

    # Clamp to bar_width
    start_pos = max(0, min(start_pos, bar_width))
    end_pos = max(0, min(end_pos, bar_width))

    # Create visual timeline bar with temporal positioning
    empty_before = "░" * start_pos
    filled = f"{BLUE}{'█' * (end_pos - start_pos)}{RESET}"
    empty_after = "░" * (bar_width - end_pos)
    bar = f"[{empty_before}{filled}{empty_after}]"

    # Format date range string (e.g., "Mai'23->Jan'25")
    def format_month(dt):
        months_fr = [
            "Jan",
            "Fév",
            "Mar",
            "Avr",
            "Mai",
            "Jun",
            "Jul",
            "Aoû",
            "Sep",
            "Oct",
            "Nov",
            "Déc",
        ]
        return f"{months_fr[dt.month - 1]}'{dt.year % 100:02d}"

    date_range_str = f"{format_month(actual_start)}→{format_month(actual_end)}"

    return bar, percentage, months_covered, total_months, date_range_str


def format_availability_bar_daily(
    first_date_str, last_date_str, requested_start, requested_end, bar_width: int = 24
) -> tuple:
    """
    Format availability as visual timeline progress bar (DAILY granularity).

    Similar to format_availability_bar() but calculates coverage based on days
    instead of months. Used for open_interest which has daily data.

    **Note**: All date ranges are INCLUSIVE on both ends. For example,
    requesting 2024-01-01 to 2024-01-31 represents 31 days (not 30).

    Parameters
    ----------
    first_date_str : str, date, or datetime
        First date available (YYYY-MM-DD string or date object)
    last_date_str : str, date, or datetime
        Last date available (YYYY-MM-DD string or date object)
    requested_start : str, date, or datetime
        Requested start date (YYYY-MM-DD string or date object) - INCLUSIVE
    requested_end : str, date, or datetime
        Requested end date (YYYY-MM-DD string or date object) - INCLUSIVE
    bar_width : int
        Width of progress bar in characters (default: 24)

    Returns
    -------
    tuple
        (bar_string, percentage, days_covered, total_days, date_range_str)
        - percentage: Coverage as integer 0-100
        - days_covered: Number of calendar days with data
        - total_days: Total calendar days in requested range
        - date_range_str: Formatted as "Jan'24→Dec'24"

    Example
    -------
    >>> bar, pct, days, total, dates = format_availability_bar_daily(
    ...     '2024-01-15', '2024-12-20', '2024-01-01', '2024-12-31'
    ... )
    >>> # bar shows temporal positioning with blue blocks
    >>> # pct and days show daily coverage (not monthly)
    """
    # ANSI color codes
    BLUE = "\033[34m"
    RESET = "\033[0m"

    # Helper to convert to datetime
    def to_datetime(d):
        if isinstance(d, datetime):
            return d
        if isinstance(d, date):
            return datetime.combine(d, datetime.min.time())
        # string
        return datetime.strptime(d, "%Y-%m-%d")

    # Parse dates
    first = to_datetime(first_date_str)
    last = to_datetime(last_date_str)
    req_start = to_datetime(requested_start)
    req_end = to_datetime(requested_end)

    # Total days in requested period
    total_days = (req_end - req_start).days + 1

    # Validate date range
    if total_days <= 0:
        # Invalid date range (end before start) - return empty bar
        bar = f"[{'░' * bar_width}]"
        return bar, 0, 0, max(1, total_days), "Invalid"

    # Days covered (clip to requested period)
    actual_start = max(first, req_start)
    actual_end = min(last, req_end)

    days_covered = (actual_end - actual_start).days + 1 if actual_start <= actual_end else 0

    # Calculate percentage
    percentage = int((days_covered / total_days) * 100) if total_days > 0 else 0

    # Calculate temporal positions (in days)
    start_offset = (actual_start - req_start).days
    end_offset = (actual_end - req_start).days

    # Map to bar positions
    if total_days > 1:
        start_pos = int((start_offset / (total_days - 1)) * bar_width)
        end_pos = int((end_offset / (total_days - 1)) * bar_width)
        # Ensure at least 1 block if there's data
        if end_pos == start_pos and days_covered > 0:
            end_pos = start_pos + 1
    else:
        start_pos = 0
        end_pos = bar_width if days_covered > 0 else 0

    # Clamp to bar_width
    start_pos = max(0, min(start_pos, bar_width))
    end_pos = max(0, min(end_pos, bar_width))

    # Create visual timeline bar with temporal positioning
    empty_before = "░" * start_pos
    filled = f"{BLUE}{'█' * (end_pos - start_pos)}{RESET}"
    empty_after = "░" * (bar_width - end_pos)
    bar = f"[{empty_before}{filled}{empty_after}]"

    # Format date range string (e.g., "Mai'23→Jan'25")
    def format_month(dt):
        months_fr = [
            "Jan",
            "Fév",
            "Mar",
            "Avr",
            "Mai",
            "Jun",
            "Jul",
            "Aoû",
            "Sep",
            "Oct",
            "Nov",
            "Déc",
        ]
        return f"{months_fr[dt.month - 1]}'{dt.year % 100:02d}"

    date_range_str = f"{format_month(actual_start)}→{format_month(actual_end)}"

    return bar, percentage, days_covered, total_days, date_range_str
