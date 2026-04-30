"""
Date Manipulation Utilities

Provides utilities for date and time manipulation in the crypto-data package.
"""

from datetime import datetime, timedelta
from typing import Literal

Frequency = Literal["daily", "weekly", "monthly"]


def parse_date_range(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    """
    Parse and validate YYYY-MM-DD start/end dates.
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid start_date format '{start_date}': expected YYYY-MM-DD") from e

    try:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid end_date format '{end_date}': expected YYYY-MM-DD") from e

    if start > end:
        raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")

    return start, end


def generate_month_list(start: datetime, end: datetime) -> list[str]:
    """
    Generate list of YYYY-MM strings between start and end dates.

    Normalizes dates to 1st of each month.

    Parameters
    ----------
    start : datetime
        Start date
    end : datetime
        End date

    Returns
    -------
    List[str]
        List of month strings in YYYY-MM format

    Example
    -------
    >>> from datetime import datetime
    >>> generate_month_list(
    ...     datetime(2024, 1, 15),
    ...     datetime(2024, 3, 20)
    ... )
    ['2024-01', '2024-02', '2024-03']
    """
    months = []
    current = start.replace(day=1)
    end_month = end.replace(day=1)

    while current <= end_month:
        months.append(current.strftime("%Y-%m"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def generate_day_list(start: datetime, end: datetime) -> list[str]:
    """
    Generate list of YYYY-MM-DD strings between start and end dates.

    Parameters
    ----------
    start : datetime
        Start date
    end : datetime
        End date

    Returns
    -------
    List[str]
        List of day strings in YYYY-MM-DD format

    Example
    -------
    >>> from datetime import datetime
    >>> generate_day_list(
    ...     datetime(2024, 1, 15),
    ...     datetime(2024, 1, 17)
    ... )
    ['2024-01-15', '2024-01-16', '2024-01-17']
    """
    days = []
    current = start

    while current <= end:
        days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return days


def generate_date_list(
    start: datetime,
    end: datetime,
    frequency: Frequency = "monthly",
) -> list[str]:
    """
    Generate list of YYYY-MM-DD strings between start and end dates.

    Parameters
    ----------
    start : datetime
        Start date
    end : datetime
        End date
    frequency : {'daily', 'weekly', 'monthly'}
        Snapshot frequency:
        - monthly: first day of each month
        - weekly: Monday snapshots
        - daily: every calendar day

    Returns
    -------
    List[str]
        List of date strings in YYYY-MM-DD format
    """
    if start > end:
        return []

    if frequency == "monthly":
        return [f"{month}-01" for month in generate_month_list(start, end)]

    if frequency == "weekly":
        # Monday = 0; advance to first Monday on/after start.
        days_until_monday = (7 - start.weekday()) % 7
        current = start + timedelta(days=days_until_monday)
        dates: list[str] = []
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=7)
        return dates

    if frequency == "daily":
        return generate_day_list(start, end)

    raise ValueError(f"Invalid frequency '{frequency}'. Expected one of: daily, weekly, monthly")
