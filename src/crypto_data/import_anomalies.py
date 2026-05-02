"""JSONL reporting helpers for non-blocking import anomalies."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ImportAnomaly:
    """An import-time issue that was handled before rows landed in DuckDB."""

    check_name: str
    count: int
    metadata: dict[str, Any] = field(default_factory=dict)


def default_import_anomaly_report_path(db_path: str | Path) -> Path:
    """Return the default sidecar path for import anomaly JSONL reports."""
    db_path = Path(str(db_path))
    stem = "memory" if str(db_path) == ":memory:" else db_path.stem
    return Path("logs") / f"{stem}_import_anomalies.jsonl"


def contextualize_import_anomalies(
    anomalies: list[ImportAnomaly],
    *,
    table: str,
    symbol: str,
    interval: str | None,
    period: str | None,
    source_file: str | None,
) -> list[dict[str, Any]]:
    """Attach stable import context to parser-level anomalies."""
    event_timestamp = datetime.utcnow().isoformat(timespec="seconds")
    return [
        {
            "table": table,
            "symbol": symbol,
            "interval": interval,
            "period": period,
            "source_file": source_file,
            "check_name": anomaly.check_name,
            "count": int(anomaly.count),
            "metadata": _jsonable(anomaly.metadata),
            "event_timestamp": event_timestamp,
        }
        for anomaly in anomalies
        if anomaly.count > 0
    ]


def append_import_anomaly_jsonl(
    path: str | Path | None,
    records: list[dict[str, Any]],
) -> None:
    """Append import anomaly records to a JSONL sidecar file."""
    if path is None or not records:
        return

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(_jsonable(record), sort_keys=True))
            f.write("\n")


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable(asdict(value))
    return value
