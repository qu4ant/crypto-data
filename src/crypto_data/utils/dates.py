"""
Date Manipulation Utilities

Provides utilities for date and time manipulation in the crypto-data package.
"""

from datetime import datetime
from typing import List


def generate_month_list(start: datetime, end: datetime) -> List[str]:
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
