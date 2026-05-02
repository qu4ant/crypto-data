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
    deduped_columns_sql = ", ".join(f"deduped.{column}" for column in spec.columns)
    primary_key_sql = ", ".join(spec.primary_key)
    join_conditions = " AND ".join(
        f"existing.{column} = deduped.{column}" for column in spec.primary_key
    )
    null_probe_column = spec.primary_key[0]
    new_rows_cte = f"""
        WITH incoming AS (
            SELECT
                {columns_sql},
                ROW_NUMBER() OVER (
                    PARTITION BY {primary_key_sql}
                    ORDER BY {primary_key_sql}
                ) AS _row_number
            FROM df
        ),
        deduped AS (
            SELECT {columns_sql}
            FROM incoming
            WHERE _row_number = 1
        ),
        new_rows AS (
            SELECT {deduped_columns_sql}
            FROM deduped
            LEFT JOIN {table} AS existing
              ON {join_conditions}
            WHERE existing.{null_probe_column} IS NULL
        )
    """

    inserted_count = conn.execute(f"{new_rows_cte} SELECT COUNT(*) FROM new_rows").fetchone()[0]

    conn.execute(
        f"{new_rows_cte} INSERT INTO {table} ({columns_sql}) SELECT {columns_sql} FROM new_rows"
    )
    return int(inserted_count)
