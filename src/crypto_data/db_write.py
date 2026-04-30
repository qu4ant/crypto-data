"""Shared DuckDB write helpers."""

from __future__ import annotations

import pandas as pd

from crypto_data.tables import get_table_spec


def insert_idempotent(conn, table: str, df: pd.DataFrame) -> int:
    """
    Insert rows while ignoring existing primary keys.

    The caller owns transaction boundaries. The return value is the number of
    rows whose primary key was absent before the insert.
    """
    if df.empty:
        return 0

    spec = get_table_spec(table)
    columns_sql = ", ".join(spec.columns)
    join_conditions = " AND ".join(
        f"existing.{column} = incoming.{column}" for column in spec.primary_key
    )
    null_probe_column = spec.primary_key[0]

    inserted_count = conn.execute(f"""
        SELECT COUNT(*)
        FROM df AS incoming
        LEFT JOIN {table} AS existing
          ON {join_conditions}
        WHERE existing.{null_probe_column} IS NULL
    """).fetchone()[0]

    conn.execute(f"INSERT OR IGNORE INTO {table} ({columns_sql}) SELECT {columns_sql} FROM df")
    return int(inserted_count)
