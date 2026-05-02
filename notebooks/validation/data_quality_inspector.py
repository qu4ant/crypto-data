from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from tempfile import gettempdir
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from bokeh.io import output_notebook, show
from bokeh.layouts import column
from bokeh.models import (
    BoxAnnotation,
    ColumnDataSource,
    CustomJS,
    DataTable,
    DateFormatter,
    Div,
    HoverTool,
    NumberFormatter,
    TableColumn,
)
from bokeh.plotting import figure

try:
    from IPython.display import HTML, Markdown, display
except ModuleNotFoundError:
    HTML = str
    Markdown = str

    def display(value: object) -> None:
        print(value)

KLINE_TABLES = {"spot", "futures"}
SUPPORTED_TABLES = {"spot", "futures", "funding_rates", "open_interest", "crypto_universe"}

INTERVAL_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1_800,
    "1h": 3_600,
    "2h": 7_200,
    "4h": 14_400,
    "6h": 21_600,
    "8h": 28_800,
    "12h": 43_200,
    "1d": 86_400,
    "3d": 259_200,
    "1w": 604_800,
}

DEFAULT_THRESHOLDS = {
    "min_outlier_observations": 20,
    "price_return_sigma": 20.0,
    "volume_iqr_multiplier": 20.0,
    "timestamp_alignment_tolerance_seconds": 0,
    "quote_volume_price_band_tolerance": 0.001,
    "price_jump_abs_return_warn": 0.50,
    "price_jump_mad_multiplier": 15.0,
    "wick_range_pct_warn": 1.0,
    "wick_side_pct_warn": 0.75,
    "stale_run_min_duration_seconds": 86_400,
    "stale_run_min_observations": 6,
    "funding_rate_abs_warn": 0.01,
    "funding_expected_seconds": 28_800,
}

CHECK_COLORS = {
    "time_gaps": "#d32f2f",
    "timestamp_alignment": "#f57c00",
    "volume_price_inconsistency": "#7b1fa2",
    "price_return_outliers": "#c2185b",
    "robust_price_jumps": "#b71c1c",
    "wick_outliers": "#fbc02d",
    "volume_outliers": "#1976d2",
    "stale_price_runs": "#616161",
    "funding_rate_extremes": "#00796b",
    "funding_rate_distribution": "#455a64",
}

CHECK_MARKERS = {
    "time_gaps": "x",
    "timestamp_alignment": "circle",
    "volume_price_inconsistency": "diamond",
    "price_return_outliers": "inverted_triangle",
    "robust_price_jumps": "triangle",
    "wick_outliers": "square",
    "volume_outliers": "circle_cross",
    "stale_price_runs": "square_pin",
    "funding_rate_extremes": "triangle",
    "funding_rate_distribution": "circle_x",
}

DARK_BACKGROUND = "#0e1117"
DARK_PANEL = "#151a21"
DARK_PANEL_ALT = "#1b222c"
DARK_BORDER = "#303846"
DARK_GRID = "#2a313d"
DARK_TEXT = "#e5e7eb"
DARK_MUTED = "#a8b3c1"
DARK_ACCENT = "#8ab4f8"

DARK_NOTEBOOK_CSS = f"""
<style>
html body,
.jp-Notebook,
.jp-Cell,
.jp-OutputArea,
.jp-OutputArea-output,
.jp-RenderedHTMLCommon,
.rendered_html {{
    background: {DARK_BACKGROUND} !important;
    color: {DARK_TEXT} !important;
}}
.jp-RenderedHTMLCommon h1,
.jp-RenderedHTMLCommon h2,
.jp-RenderedHTMLCommon h3,
.jp-RenderedHTMLCommon h4,
.rendered_html h1,
.rendered_html h2,
.rendered_html h3,
.rendered_html h4 {{
    color: {DARK_TEXT} !important;
}}
.jp-RenderedHTMLCommon a,
.rendered_html a {{
    color: {DARK_ACCENT} !important;
}}
.jp-RenderedHTMLCommon table,
.rendered_html table,
table.dataframe {{
    background: {DARK_PANEL} !important;
    color: {DARK_TEXT} !important;
    border-color: {DARK_BORDER} !important;
}}
.jp-RenderedHTMLCommon thead,
.rendered_html thead,
table.dataframe thead tr {{
    background: {DARK_PANEL_ALT} !important;
    color: {DARK_TEXT} !important;
}}
.jp-RenderedHTMLCommon tbody tr:nth-child(even),
.rendered_html tbody tr:nth-child(even),
table.dataframe tbody tr:nth-child(even) {{
    background: #111820 !important;
}}
.jp-RenderedHTMLCommon tbody tr:nth-child(odd),
.rendered_html tbody tr:nth-child(odd),
table.dataframe tbody tr:nth-child(odd) {{
    background: {DARK_PANEL} !important;
}}
.jp-RenderedHTMLCommon th,
.jp-RenderedHTMLCommon td,
.rendered_html th,
.rendered_html td,
table.dataframe th,
table.dataframe td {{
    color: {DARK_TEXT} !important;
    border-color: {DARK_BORDER} !important;
}}
.bk-root {{
    background: {DARK_BACKGROUND} !important;
    color: {DARK_TEXT} !important;
}}
.bk-tooltip,
.bk-tooltip-content,
.bk-tooltip-row-label,
.bk-tooltip-row-value {{
    background: {DARK_PANEL_ALT} !important;
    color: {DARK_TEXT} !important;
    border-color: {DARK_BORDER} !important;
}}
.bk-tooltip {{
    border: 1px solid {DARK_BORDER} !important;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45) !important;
}}
</style>
"""

DARK_DATATABLE_CSS = f"""
:host {{
    color: {DARK_TEXT};
    background: {DARK_PANEL};
}}
.bk-data-table,
.slickgrid-container,
.slick-pane,
.slick-viewport,
.slick-row,
.slick-cell {{
    background: {DARK_PANEL} !important;
    color: {DARK_TEXT} !important;
}}
.slick-header,
.slick-header-columns,
.slick-header-column {{
    background: {DARK_PANEL_ALT} !important;
    color: {DARK_TEXT} !important;
    border-color: {DARK_BORDER} !important;
}}
.slick-cell,
.slick-header-column {{
    border-color: {DARK_BORDER} !important;
}}
.slick-row.odd .slick-cell {{
    background: #111820 !important;
}}
.slick-row.even .slick-cell {{
    background: {DARK_PANEL} !important;
}}
.slick-row.active .slick-cell,
.slick-row:hover .slick-cell {{
    background: #263247 !important;
}}
.slick-cell.selected {{
    background: #30415f !important;
    color: #ffffff !important;
}}
.bk-input,
input {{
    background: {DARK_PANEL_ALT} !important;
    color: {DARK_TEXT} !important;
    border-color: {DARK_BORDER} !important;
}}
"""


def resolve_project_root(start: str | Path | None = None) -> Path:
    """Find the repository root from a notebook working directory."""
    current = Path(start or Path.cwd()).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    return current


def resolve_report_path(project_root: Path, requested: str | Path | None = None) -> Path:
    """Resolve the quality JSON report with a practical top60 fallback."""
    candidates = []
    if requested:
        candidates.append(Path(requested))
    candidates.extend(
        [
            Path(gettempdir()) / "top60_quality_new.json",
            project_root / "quality_report_top60_h4_2022_01_2026_04.json",
        ]
    )
    for candidate in candidates:
        path = candidate if candidate.is_absolute() else project_root / candidate
        if path.exists():
            return path
    return candidates[0] if Path(candidates[0]).is_absolute() else project_root / candidates[0]


def init_notebook() -> None:
    display(HTML(DARK_NOTEBOOK_CSS))
    output_notebook()


def _apply_dark_plot(plot):
    plot.background_fill_color = DARK_PANEL
    plot.border_fill_color = DARK_BACKGROUND
    plot.outline_line_color = DARK_BORDER
    plot.title.text_color = DARK_TEXT
    plot.title.text_font_size = "13px"
    plot.title.text_font_style = "bold"

    for axis in plot.axis:
        axis.axis_line_color = DARK_BORDER
        axis.major_tick_line_color = DARK_BORDER
        axis.minor_tick_line_color = DARK_BORDER
        axis.major_label_text_color = DARK_MUTED
        axis.axis_label_text_color = DARK_TEXT

    for grid in [*plot.xgrid, *plot.ygrid]:
        grid.grid_line_color = DARK_GRID
        grid.grid_line_alpha = 0.65
        grid.minor_grid_line_color = DARK_GRID
        grid.minor_grid_line_alpha = 0.18

    return plot


def _apply_dark_legend(plot) -> None:
    for legend in plot.legend:
        legend.background_fill_color = DARK_PANEL
        legend.background_fill_alpha = 0.86
        legend.border_line_color = DARK_BORDER
        legend.label_text_color = DARK_TEXT


def _dark_table(
    source: ColumnDataSource,
    columns: Sequence[TableColumn],
    *,
    width: int,
    height: int,
) -> DataTable:
    return DataTable(
        source=source,
        columns=list(columns),
        width=width,
        height=height,
        index_position=None,
        selectable=True,
        styles={"background-color": DARK_PANEL, "color": DARK_TEXT},
        stylesheets=[DARK_DATATABLE_CSS],
    )


def _dark_div(text: str, width: int = 1180) -> Div:
    return Div(
        text=f"<div class='dq-dark-div'>{text}</div>",
        width=width,
        styles={"background-color": DARK_BACKGROUND, "color": DARK_TEXT},
        stylesheets=[
            f"""
            .dq-dark-div {{
                color: {DARK_TEXT};
                background: {DARK_BACKGROUND};
                border: 1px solid {DARK_BORDER};
                border-radius: 6px;
                padding: 8px 10px;
                font-size: 13px;
                line-height: 1.45;
            }}
            .dq-dark-div b {{
                color: #ffffff;
            }}
            """
        ],
    )


def load_quality_findings(
    report_path: str | Path,
    import_anomalies_jsonl: str | Path | None = None,
) -> pd.DataFrame:
    """Load quality findings JSON and optional import-anomaly JSONL."""
    path = Path(report_path)
    records: list[dict[str, Any]] = []
    if path.exists():
        records.extend(json.loads(path.read_text(encoding="utf-8")))
    if import_anomalies_jsonl:
        records.extend(_load_import_anomaly_records(import_anomalies_jsonl))
    return _normalize_findings(records)


def _load_import_anomaly_records(path: str | Path) -> list[dict[str, Any]]:
    report_path = Path(path)
    if not report_path.exists():
        return []

    records = []
    with report_path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            records.append(
                {
                    "severity": "WARN",
                    "table": record.get("table") or record.get("table_name"),
                    "check_name": record.get("check_name") or "import_anomaly",
                    "message": record.get("message") or "Import anomaly",
                    "count": int(record.get("count") or 0),
                    "symbol": record.get("symbol"),
                    "interval": record.get("interval"),
                    "first_timestamp": record.get("first_timestamp") or record.get("timestamp"),
                    "last_timestamp": record.get("last_timestamp") or record.get("timestamp"),
                    "metadata": record.get("metadata") or record,
                }
            )
    return records


def _normalize_findings(records: Sequence[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "severity",
        "table",
        "check_name",
        "message",
        "count",
        "symbol",
        "interval",
        "first_timestamp",
        "last_timestamp",
        "metadata",
    ]
    df = pd.DataFrame(records)
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns].copy()
    if df.empty:
        df["metadata_text"] = pd.Series(dtype="object")
        return df

    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["first_timestamp"] = pd.to_datetime(df["first_timestamp"], errors="coerce")
    df["last_timestamp"] = pd.to_datetime(df["last_timestamp"], errors="coerce")
    df["metadata"] = df["metadata"].apply(lambda value: value if isinstance(value, dict) else {})
    df["metadata_text"] = df["metadata"].apply(
        lambda value: json.dumps(value, default=str, sort_keys=True)
    )
    return df


def summarize_findings(findings: pd.DataFrame) -> pd.DataFrame:
    if findings.empty:
        return findings.copy()
    severity_order = {"ERROR": 0, "WARN": 1}
    summary = findings.copy()
    summary["severity_rank"] = summary["severity"].map(severity_order).fillna(9)
    return summary.sort_values(
        ["severity_rank", "table", "symbol", "interval", "check_name"],
        na_position="last",
    ).drop(columns=["severity_rank"])


def show_findings_table(findings: pd.DataFrame, height: int = 320, width: int = 1180) -> None:
    """Display the quality report as a sortable Bokeh table."""
    view = summarize_findings(findings)
    if view.empty:
        display(Markdown("No quality findings loaded."))
        return

    source = ColumnDataSource(
        view[
            [
                "severity",
                "table",
                "symbol",
                "interval",
                "check_name",
                "count",
                "first_timestamp",
                "last_timestamp",
                "message",
                "metadata_text",
            ]
        ]
    )
    columns = [
        TableColumn(field="severity", title="Severity", width=70),
        TableColumn(field="table", title="Table", width=90),
        TableColumn(field="symbol", title="Symbol", width=110),
        TableColumn(field="interval", title="Interval", width=70),
        TableColumn(field="check_name", title="Check", width=180),
        TableColumn(field="count", title="Count", width=70),
        TableColumn(
            field="first_timestamp",
            title="First",
            width=150,
            formatter=DateFormatter(format="%F %T"),
        ),
        TableColumn(
            field="last_timestamp",
            title="Last",
            width=150,
            formatter=DateFormatter(format="%F %T"),
        ),
        TableColumn(field="message", title="Message", width=320),
        TableColumn(field="metadata_text", title="Metadata", width=320),
    ]
    show(_dark_table(source, columns, width=width, height=height))


def finding_counts(findings: pd.DataFrame) -> pd.DataFrame:
    if findings.empty:
        return pd.DataFrame()
    return (
        findings.groupby(["severity", "table", "check_name"], dropna=False)
        .agg(findings=("check_name", "size"), rows=("count", "sum"))
        .reset_index()
        .sort_values(["severity", "rows"], ascending=[True, False])
    )


def load_kline_data(db_path: str | Path, table: str, symbol: str, interval: str) -> pd.DataFrame:
    if table not in KLINE_TABLES:
        raise ValueError(f"{table!r} is not a kline table")
    with duckdb.connect(str(db_path), read_only=True) as conn:
        df = conn.execute(
            f"""
            SELECT
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trades_count,
                taker_buy_base_volume,
                taker_buy_quote_volume
            FROM {table}
            WHERE symbol = ? AND interval = ?
            ORDER BY timestamp
            """,
            [symbol, interval],
        ).fetchdf()
    return _add_kline_diagnostics(df, interval)


def _add_kline_diagnostics(
    data: pd.DataFrame,
    interval: str,
    thresholds: dict[str, float] | None = None,
) -> pd.DataFrame:
    cfg = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    df = data.copy()
    if df.empty:
        return df

    expected_seconds = INTERVAL_SECONDS.get(interval)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["prev_timestamp"] = df["timestamp"].shift()
    df["delta_seconds"] = (df["timestamp"] - df["prev_timestamp"]).dt.total_seconds()
    df["expected_seconds"] = expected_seconds

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trades_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["is_up"] = df["close"] >= df["open"]
    df["body_top"] = df[["open", "close"]].max(axis=1)
    df["body_bottom"] = df[["open", "close"]].min(axis=1)

    positive_close = (df["close"] > 0) & (df["close"].shift() > 0)
    ratio = df["close"] / df["close"].shift()
    df["relative_return"] = np.where(positive_close, ratio - 1, np.nan)
    df["log_return"] = np.where(positive_close, np.log(ratio), np.nan)

    valid_ohlc = (
        (df["open"] > 0)
        & (df["high"] > 0)
        & (df["low"] > 0)
        & (df["close"] > 0)
        & (df["high"] >= df["low"])
        & (df["high"] >= df["open"])
        & (df["high"] >= df["close"])
        & (df["low"] <= df["open"])
        & (df["low"] <= df["close"])
    )
    df["range_pct"] = np.where(valid_ohlc, (df["high"] - df["low"]) / df["close"], np.nan)
    df["upper_wick_pct"] = np.where(valid_ohlc, (df["high"] - df["body_top"]) / df["close"], np.nan)
    df["lower_wick_pct"] = np.where(
        valid_ohlc,
        (df["body_bottom"] - df["low"]) / df["close"],
        np.nan,
    )

    if expected_seconds and expected_seconds <= 86_400:
        epoch_seconds = _timestamp_seconds(df["timestamp"])
        offsets = epoch_seconds % expected_seconds
        tolerance = int(cfg["timestamp_alignment_tolerance_seconds"])
        df["timestamp_distance_seconds"] = np.minimum(offsets, expected_seconds - offsets)
        df["timestamp_alignment"] = (offsets > tolerance) & (
            expected_seconds - offsets > tolerance
        )
    else:
        df["timestamp_distance_seconds"] = np.nan
        df["timestamp_alignment"] = False

    df["time_gaps"] = (
        df["delta_seconds"].notna()
        & pd.notna(expected_seconds)
        & (df["delta_seconds"] > expected_seconds)
    )

    volume = df["volume"]
    quote_volume = df["quote_volume"]
    trades = df["trades_count"].fillna(0)
    tol = float(cfg["quote_volume_price_band_tolerance"])
    df["quote_price"] = np.where(volume > 0, quote_volume / volume, np.nan)
    df["positive_volume_zero_quote"] = (volume > 0) & (quote_volume == 0)
    df["positive_quote_zero_volume"] = (quote_volume > 0) & (volume == 0)
    df["zero_trades_positive_volume"] = (trades == 0) & ((volume > 0) | (quote_volume > 0))
    df["quote_volume_price_band"] = (
        (volume > 0)
        & (quote_volume > 0)
        & (df["low"] > 0)
        & (df["high"] >= df["low"])
        & ((quote_volume < df["low"] * volume * (1 - tol)) | (quote_volume > df["high"] * volume * (1 + tol)))
    )
    df["taker_base_exceeds_volume"] = df["taker_buy_base_volume"] > volume * (1 + tol)
    df["taker_quote_exceeds_quote_volume"] = df["taker_buy_quote_volume"] > quote_volume * (1 + tol)
    df["volume_price_inconsistency"] = (
        df["positive_volume_zero_quote"]
        | df["positive_quote_zero_volume"]
        | df["zero_trades_positive_volume"]
        | df["quote_volume_price_band"]
        | df["taker_base_exceeds_volume"]
        | df["taker_quote_exceeds_quote_volume"]
    )

    returns = df["log_return"].dropna()
    if len(returns) >= int(cfg["min_outlier_observations"]) and returns.std(ddof=1) > 0:
        mean_return = returns.mean()
        std_return = returns.std(ddof=1)
        df["price_return_outliers"] = (
            (df["log_return"] - mean_return).abs() > float(cfg["price_return_sigma"]) * std_return
        )
    else:
        df["price_return_outliers"] = False

    if len(returns) >= int(cfg["min_outlier_observations"]):
        median_return = returns.median()
        abs_deviation = (df["log_return"] - median_return).abs()
        mad_return = (returns - median_return).abs().median()
        robust_deviation = (
            abs_deviation > float(cfg["price_jump_mad_multiplier"]) * 1.4826 * mad_return
            if mad_return > 0
            else abs_deviation > 0
        )
        df["robust_price_jumps"] = (
            df["relative_return"].abs() > float(cfg["price_jump_abs_return_warn"])
        ) & robust_deviation
    else:
        df["robust_price_jumps"] = False

    df["wick_outliers"] = (
        (df["range_pct"] > float(cfg["wick_range_pct_warn"]))
        | (df["upper_wick_pct"] > float(cfg["wick_side_pct_warn"]))
        | (df["lower_wick_pct"] > float(cfg["wick_side_pct_warn"]))
    )

    positive_volume = df.loc[df["volume"] > 0, "volume"].dropna()
    if len(positive_volume) >= int(cfg["min_outlier_observations"]):
        q1 = positive_volume.quantile(0.25)
        q3 = positive_volume.quantile(0.75)
        iqr = q3 - q1
        if iqr > 0:
            df["volume_outliers"] = (df["volume"] < q1 - float(cfg["volume_iqr_multiplier"]) * iqr) | (
                df["volume"] > q3 + float(cfg["volume_iqr_multiplier"]) * iqr
            )
        else:
            df["volume_outliers"] = False
    else:
        df["volume_outliers"] = False

    df["is_stale"] = (
        df["open"].notna()
        & df["high"].notna()
        & df["low"].notna()
        & df["close"].notna()
        & (df["open"] == df["high"])
        & (df["high"] == df["low"])
        & (df["low"] == df["close"])
        & (df["volume"] == 0)
        & (df["quote_volume"] == 0)
        & (trades == 0)
    )
    contiguous = df["delta_seconds"].eq(expected_seconds)
    continuing_stale = df["is_stale"] & df["is_stale"].shift(fill_value=False) & contiguous
    df["stale_segment_id"] = (~continuing_stale).cumsum()
    df["stale_price_runs"] = False
    if expected_seconds:
        stale_runs = (
            df.loc[df["is_stale"]]
            .groupby("stale_segment_id")
            .agg(
                first_timestamp=("timestamp", "min"),
                last_timestamp=("timestamp", "max"),
                observations=("timestamp", "size"),
            )
        )
        stale_runs["duration_seconds"] = (
            (stale_runs["last_timestamp"] - stale_runs["first_timestamp"]).dt.total_seconds()
            + expected_seconds
        )
        valid_segments = stale_runs.index[
            (stale_runs["observations"] >= int(cfg["stale_run_min_observations"]))
            & (stale_runs["duration_seconds"] >= int(cfg["stale_run_min_duration_seconds"]))
        ]
        df["stale_price_runs"] = df["stale_segment_id"].isin(valid_segments)

    return df


def load_funding_data(db_path: str | Path, symbol: str) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        df = conn.execute(
            """
            SELECT timestamp, funding_rate
            FROM funding_rates
            WHERE symbol = ?
            ORDER BY timestamp
            """,
            [symbol],
        ).fetchdf()
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
    df["prev_timestamp"] = df["timestamp"].shift()
    df["delta_seconds"] = (df["timestamp"] - df["prev_timestamp"]).dt.total_seconds()
    expected = DEFAULT_THRESHOLDS["funding_expected_seconds"]
    df["time_gaps"] = df["delta_seconds"].notna() & (df["delta_seconds"] > expected)
    df["funding_rate_extremes"] = df["funding_rate"].abs() > DEFAULT_THRESHOLDS["funding_rate_abs_warn"]
    return df


def build_kline_anomaly_rows(  # noqa: C901
    data: pd.DataFrame,
    findings: pd.DataFrame,
    table: str,
    symbol: str,
    interval: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    selected_findings = _selected_findings(findings, table, symbol, interval)

    def severity_for(check_name: str) -> str:
        matching = selected_findings[selected_findings["check_name"] == check_name]
        if (matching["severity"] == "ERROR").any():
            return "ERROR"
        return "WARN"

    def append_point(
        row: pd.Series,
        check_name: str,
        detail: str,
        value: float | None = None,
        marker_y: float | None = None,
    ) -> None:
        rows.append(
            {
                "severity": severity_for(check_name),
                "table": table,
                "symbol": symbol,
                "interval": interval,
                "check_name": check_name,
                "timestamp": row["timestamp"],
                "end_timestamp": row["timestamp"],
                "prev_timestamp": row.get("prev_timestamp"),
                "close": row.get("close"),
                "marker_y": row.get("close") if marker_y is None else marker_y,
                "value": value,
                "detail": detail,
            }
        )

    if data.empty:
        return _normalize_anomaly_rows(rows)

    for _, row in data.loc[data["time_gaps"]].iterrows():
        append_point(
            row,
            "time_gaps",
            f"gap={row['delta_seconds'] / 3600:.2f}h expected={row['expected_seconds'] / 3600:.2f}h",
            row["delta_seconds"],
        )
    for _, row in data.loc[data["timestamp_alignment"]].iterrows():
        append_point(
            row,
            "timestamp_alignment",
            f"distance={row['timestamp_distance_seconds']:.0f}s",
            row["timestamp_distance_seconds"],
        )
    for _, row in data.loc[data["volume_price_inconsistency"]].iterrows():
        flags = [
            name
            for name in [
                "positive_volume_zero_quote",
                "positive_quote_zero_volume",
                "zero_trades_positive_volume",
                "quote_volume_price_band",
                "taker_base_exceeds_volume",
                "taker_quote_exceeds_quote_volume",
            ]
            if bool(row.get(name, False))
        ]
        append_point(row, "volume_price_inconsistency", ", ".join(flags), row.get("quote_price"))
    for _, row in data.loc[data["price_return_outliers"]].iterrows():
        append_point(
            row,
            "price_return_outliers",
            f"log_return={row['log_return']:.4f}",
            row["log_return"],
        )
    for _, row in data.loc[data["robust_price_jumps"]].iterrows():
        append_point(
            row,
            "robust_price_jumps",
            f"relative_return={row['relative_return']:.2%}",
            row["relative_return"],
        )
    for _, row in data.loc[data["wick_outliers"]].iterrows():
        append_point(
            row,
            "wick_outliers",
            (
                f"range={row['range_pct']:.2%}, upper={row['upper_wick_pct']:.2%}, "
                f"lower={row['lower_wick_pct']:.2%}"
            ),
            row["range_pct"],
            row.get("high"),
        )
    for _, row in data.loc[data["volume_outliers"]].iterrows():
        append_point(row, "volume_outliers", f"volume={row['volume']:.6g}", row["volume"])

    stale = data.loc[data["stale_price_runs"]].copy()
    if not stale.empty:
        for _, run in stale.groupby("stale_segment_id").agg(
            timestamp=("timestamp", "min"),
            end_timestamp=("timestamp", "max"),
            observations=("timestamp", "size"),
            close=("close", "first"),
        ).reset_index().iterrows():
            rows.append(
                {
                    "severity": severity_for("stale_price_runs"),
                    "table": table,
                    "symbol": symbol,
                    "interval": interval,
                    "check_name": "stale_price_runs",
                    "timestamp": run["timestamp"],
                    "end_timestamp": run["end_timestamp"],
                    "prev_timestamp": pd.NaT,
                    "close": run["close"],
                    "marker_y": run["close"],
                    "value": run["observations"],
                    "detail": f"observations={int(run['observations'])}",
                }
            )

    represented = {row["check_name"] for row in rows}
    for _, finding in selected_findings.iterrows():
        if finding["check_name"] in represented:
            continue
        rows.append(_summary_anomaly_row(finding))

    return _normalize_anomaly_rows(rows)


def build_funding_anomaly_rows(
    data: pd.DataFrame,
    findings: pd.DataFrame,
    symbol: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    selected_findings = _selected_findings(findings, "funding_rates", symbol, None)

    def severity_for(check_name: str) -> str:
        matching = selected_findings[selected_findings["check_name"] == check_name]
        if (matching["severity"] == "ERROR").any():
            return "ERROR"
        return "WARN"

    if not data.empty:
        for _, row in data.loc[data["time_gaps"]].iterrows():
            rows.append(
                {
                    "severity": severity_for("time_gaps"),
                    "table": "funding_rates",
                    "symbol": symbol,
                    "interval": None,
                    "check_name": "time_gaps",
                    "timestamp": row["timestamp"],
                    "end_timestamp": row["timestamp"],
                    "prev_timestamp": row["prev_timestamp"],
                    "close": row["funding_rate"],
                    "marker_y": row["funding_rate"],
                    "value": row["delta_seconds"],
                    "detail": f"gap={row['delta_seconds'] / 3600:.2f}h expected=8.00h",
                }
            )
        for _, row in data.loc[data["funding_rate_extremes"]].iterrows():
            rows.append(
                {
                    "severity": severity_for("funding_rate_extremes"),
                    "table": "funding_rates",
                    "symbol": symbol,
                    "interval": None,
                    "check_name": "funding_rate_extremes",
                    "timestamp": row["timestamp"],
                    "end_timestamp": row["timestamp"],
                    "prev_timestamp": row["prev_timestamp"],
                    "close": row["funding_rate"],
                    "marker_y": row["funding_rate"],
                    "value": row["funding_rate"],
                    "detail": f"funding_rate={row['funding_rate']:.6f}",
                }
            )

    represented = {row["check_name"] for row in rows}
    for _, finding in selected_findings.iterrows():
        if finding["check_name"] in represented:
            continue
        rows.append(_summary_anomaly_row(finding))
    return _normalize_anomaly_rows(rows)


def _selected_findings(
    findings: pd.DataFrame,
    table: str,
    symbol: str | None,
    interval: str | None,
) -> pd.DataFrame:
    if findings.empty:
        return findings.copy()
    mask = findings["table"].eq(table)
    if symbol is not None:
        mask &= findings["symbol"].eq(symbol)
    if interval is not None:
        mask &= findings["interval"].isna() | findings["interval"].eq(interval)
    return findings.loc[mask].copy()


def _summary_anomaly_row(finding: pd.Series) -> dict[str, Any]:
    return {
        "severity": finding["severity"],
        "table": finding["table"],
        "symbol": finding["symbol"],
        "interval": finding["interval"],
        "check_name": finding["check_name"],
        "timestamp": finding["first_timestamp"],
        "end_timestamp": finding["last_timestamp"],
        "prev_timestamp": pd.NaT,
        "close": np.nan,
        "marker_y": np.nan,
        "value": finding["count"],
        "detail": finding["message"],
    }


def _normalize_anomaly_rows(rows: Sequence[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "severity",
        "table",
        "symbol",
        "interval",
        "check_name",
        "timestamp",
        "end_timestamp",
        "prev_timestamp",
        "close",
        "marker_y",
        "value",
        "detail",
    ]
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        df = pd.DataFrame({col: pd.Series(dtype="object") for col in columns})
    for col in ["timestamp", "end_timestamp", "prev_timestamp"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["timestamp_ms"] = _timestamp_ms(df["timestamp"])
    df["end_timestamp_ms"] = _timestamp_ms(df["end_timestamp"]).fillna(df["timestamp_ms"])
    return df.sort_values(["severity", "timestamp", "check_name"], na_position="last")


def _timestamp_ms(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    out = pd.Series(np.nan, index=timestamps.index, dtype="float64")
    mask = timestamps.notna()
    if mask.any():
        out.loc[mask] = timestamps.loc[mask].to_numpy(dtype="datetime64[ms]").astype("int64")
    return out


def _timestamp_seconds(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    out = pd.Series(np.nan, index=timestamps.index, dtype="float64")
    mask = timestamps.notna()
    if mask.any():
        out.loc[mask] = timestamps.loc[mask].to_numpy(dtype="datetime64[s]").astype("int64")
    return out


def render_inspector(
    db_path: str | Path,
    findings: pd.DataFrame,
    symbol: str,
    table: str = "futures",
    interval: str = "4h",
    checks: Sequence[str] | None = None,
    window_days: int = 30,
    focus_on_findings: bool = True,
) -> None:
    if table in KLINE_TABLES:
        render_kline_inspector(
            db_path,
            findings,
            symbol,
            table=table,
            interval=interval,
            checks=checks,
            window_days=window_days,
            focus_on_findings=focus_on_findings,
        )
    elif table == "funding_rates":
        render_funding_inspector(
            db_path,
            findings,
            symbol,
            checks=checks,
            window_days=window_days,
            focus_on_findings=focus_on_findings,
        )
    else:
        display(Markdown(f"Table `{table}` has no time-series plotter yet. Showing findings only."))
        show_findings_table(_selected_findings(findings, table, symbol, interval))


def render_kline_inspector(
    db_path: str | Path,
    findings: pd.DataFrame,
    symbol: str,
    table: str = "futures",
    interval: str = "4h",
    checks: Sequence[str] | None = None,
    window_days: int = 30,
    focus_on_findings: bool = True,
) -> None:
    data = load_kline_data(db_path, table, symbol, interval)
    if data.empty:
        display(Markdown(f"No `{table}` rows found for `{symbol}` / `{interval}`."))
        return

    anomalies = build_kline_anomaly_rows(data, findings, table, symbol, interval)
    if checks:
        anomalies = anomalies[anomalies["check_name"].isin(checks)].copy()

    _show_context_summary(findings, anomalies, table, symbol, interval)
    show(_kline_layout(data, anomalies, table, symbol, interval, window_days, focus_on_findings))


def render_funding_inspector(
    db_path: str | Path,
    findings: pd.DataFrame,
    symbol: str,
    checks: Sequence[str] | None = None,
    window_days: int = 30,
    focus_on_findings: bool = True,
) -> None:
    data = load_funding_data(db_path, symbol)
    if data.empty:
        display(Markdown(f"No `funding_rates` rows found for `{symbol}`."))
        return

    anomalies = build_funding_anomaly_rows(data, findings, symbol)
    if checks:
        anomalies = anomalies[anomalies["check_name"].isin(checks)].copy()

    _show_context_summary(findings, anomalies, "funding_rates", symbol, None)
    show(_funding_layout(data, anomalies, symbol, window_days, focus_on_findings))


def _show_context_summary(
    findings: pd.DataFrame,
    anomalies: pd.DataFrame,
    table: str,
    symbol: str,
    interval: str | None,
) -> None:
    selected = _selected_findings(findings, table, symbol, interval)
    title_interval = f" / {interval}" if interval else ""
    display(Markdown(f"### {table} / {symbol}{title_interval}"))
    if selected.empty:
        display(Markdown("No report findings for this selection. Recomputed plot checks may still show rows."))
    else:
        display(
            selected[
                [
                    "severity",
                    "check_name",
                    "count",
                    "first_timestamp",
                    "last_timestamp",
                    "message",
                    "metadata_text",
                ]
            ].sort_values(["severity", "check_name"])
        )
    display(Markdown(f"Detailed anomaly markers in plot: **{len(anomalies)}**"))


def _initial_x_range(
    data: pd.DataFrame,
    anomalies: pd.DataFrame,
    window_days: int,
    focus_on_findings: bool,
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    if focus_on_findings and not anomalies.empty and anomalies["timestamp"].notna().any():
        start = anomalies["timestamp"].min() - pd.Timedelta(days=window_days)
        end_source = anomalies["end_timestamp"].where(anomalies["end_timestamp"].notna(), anomalies["timestamp"])
        end = end_source.max() + pd.Timedelta(days=window_days)
        if pd.notna(start) and pd.notna(end) and start < end:
            return start, end
    if data["timestamp"].notna().any():
        return data["timestamp"].min(), data["timestamp"].max()
    return None


def _kline_layout(
    data: pd.DataFrame,
    anomalies: pd.DataFrame,
    table: str,
    symbol: str,
    interval: str,
    window_days: int,
    focus_on_findings: bool,
):
    data = data.copy()
    anomalies = anomalies.copy()
    candle_width_ms = int(INTERVAL_SECONDS.get(interval, 14_400) * 1_000 * 0.7)
    x_range = _initial_x_range(data, anomalies, window_days, focus_on_findings)

    price = figure(
        title=f"{table} {symbol} {interval} raw OHLC with anomaly markers",
        x_axis_type="datetime",
        x_range=x_range,
        width=1180,
        height=430,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    _apply_dark_plot(price)
    source = ColumnDataSource(data)
    price.segment("timestamp", "low", "timestamp", "high", source=source, color="#8a97a8", alpha=0.75)

    up_source = ColumnDataSource(data.loc[data["is_up"]])
    down_source = ColumnDataSource(data.loc[~data["is_up"]])
    price.vbar(
        "timestamp",
        candle_width_ms,
        "body_top",
        "body_bottom",
        source=up_source,
        fill_color="#2f9e44",
        line_color="#2f9e44",
        alpha=0.8,
    )
    price.vbar(
        "timestamp",
        candle_width_ms,
        "body_top",
        "body_bottom",
        source=down_source,
        fill_color="#e03131",
        line_color="#e03131",
        alpha=0.8,
    )
    price.line("timestamp", "close", source=source, color="#d9e2f1", alpha=0.55, line_width=1)

    price.add_tools(
        HoverTool(
            tooltips=[
                ("time", "@timestamp{%F %T}"),
                ("open", "@open{0,0.########}"),
                ("high", "@high{0,0.########}"),
                ("low", "@low{0,0.########}"),
                ("close", "@close{0,0.########}"),
                ("volume", "@volume{0,0.##}"),
                ("quote_volume", "@quote_volume{0,0.##}"),
                ("return", "@relative_return{0.00%}"),
                ("range", "@range_pct{0.00%}"),
            ],
            formatters={"@timestamp": "datetime"},
        )
    )
    _add_anomaly_layers(price, anomalies)

    volume = figure(
        title="Volume",
        x_axis_type="datetime",
        x_range=price.x_range,
        width=1180,
        height=180,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    _apply_dark_plot(volume)
    volume.vbar(
        "timestamp",
        candle_width_ms,
        "volume",
        source=source,
        fill_color="#78909c",
        line_color="#78909c",
        alpha=0.65,
    )
    volume.add_tools(
        HoverTool(
            tooltips=[("time", "@timestamp{%F %T}"), ("volume", "@volume{0,0.##}")],
            formatters={"@timestamp": "datetime"},
        )
    )

    returns = figure(
        title="Returns and candle range",
        x_axis_type="datetime",
        x_range=price.x_range,
        width=1180,
        height=190,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    _apply_dark_plot(returns)
    returns.line("timestamp", "relative_return", source=source, color="#ff6b6b", legend_label="relative return")
    returns.line("timestamp", "range_pct", source=source, color="#ffd43b", legend_label="range pct")
    returns.legend.click_policy = "hide"
    _apply_dark_legend(returns)

    quote = figure(
        title="Quote-volume implied price vs low/high band",
        x_axis_type="datetime",
        x_range=price.x_range,
        width=1180,
        height=210,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    _apply_dark_plot(quote)
    quote.line("timestamp", "quote_price", source=source, color="#c084fc", legend_label="quote_volume / volume")
    quote.line("timestamp", "low", source=source, color="#64b5f6", alpha=0.72, legend_label="low")
    quote.line("timestamp", "high", source=source, color="#81c784", alpha=0.72, legend_label="high")
    quote.legend.click_policy = "hide"
    _apply_dark_legend(quote)

    anomaly_table, anomaly_source = _anomaly_table(anomalies)
    _wire_table_zoom(anomaly_source, price.x_range, window_days)

    header = _dark_div(
        f"<b>{len(data):,}</b> candles loaded. "
        f"<b>{len(anomalies):,}</b> anomaly rows/runs shown. "
        "Select a row in the anomaly table to zoom around it."
    )
    return column(
        header,
        anomaly_table,
        price,
        volume,
        returns,
        quote,
        styles={"background-color": DARK_BACKGROUND},
    )


def _funding_layout(
    data: pd.DataFrame,
    anomalies: pd.DataFrame,
    symbol: str,
    window_days: int,
    focus_on_findings: bool,
):
    x_range = _initial_x_range(data, anomalies, window_days, focus_on_findings)
    source = ColumnDataSource(data)
    plot = figure(
        title=f"funding_rates {symbol} raw series with anomaly markers",
        x_axis_type="datetime",
        x_range=x_range,
        width=1180,
        height=430,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    _apply_dark_plot(plot)
    plot.line("timestamp", "funding_rate", source=source, color="#26a69a", line_width=2)
    plot.add_tools(
        HoverTool(
            tooltips=[("time", "@timestamp{%F %T}"), ("funding_rate", "@funding_rate{0.000000}")],
            formatters={"@timestamp": "datetime"},
        )
    )
    _add_anomaly_layers(plot, anomalies)
    anomaly_table, anomaly_source = _anomaly_table(anomalies)
    _wire_table_zoom(anomaly_source, plot.x_range, window_days)
    header = _dark_div(
        f"<b>{len(data):,}</b> funding rows loaded. "
        f"<b>{len(anomalies):,}</b> anomaly rows shown. "
        "Select a row in the anomaly table to zoom around it."
    )
    return column(header, anomaly_table, plot, styles={"background-color": DARK_BACKGROUND})


def _add_anomaly_layers(plot, anomalies: pd.DataFrame) -> None:
    if anomalies.empty:
        return

    for _, row in anomalies.loc[anomalies["check_name"] == "time_gaps"].iterrows():
        if pd.notna(row["prev_timestamp"]) and pd.notna(row["timestamp"]):
            plot.add_layout(
                BoxAnnotation(
                    left=row["prev_timestamp"],
                    right=row["timestamp"],
                    fill_color=CHECK_COLORS["time_gaps"],
                    fill_alpha=0.10,
                )
            )
    for _, row in anomalies.loc[anomalies["check_name"] == "stale_price_runs"].iterrows():
        if pd.notna(row["timestamp"]) and pd.notna(row["end_timestamp"]):
            plot.add_layout(
                BoxAnnotation(
                    left=row["timestamp"],
                    right=row["end_timestamp"],
                    fill_color=CHECK_COLORS["stale_price_runs"],
                    fill_alpha=0.12,
                )
            )

    for check_name, check_df in anomalies.dropna(subset=["timestamp"]).groupby("check_name"):
        marker_df = check_df.copy()
        if marker_df["marker_y"].isna().all():
            continue
        source = ColumnDataSource(marker_df)
        renderer = plot.scatter(
            "timestamp",
            "marker_y",
            source=source,
            size=11,
            marker=CHECK_MARKERS.get(check_name, "circle"),
            fill_color=CHECK_COLORS.get(check_name, "#000000"),
            line_color=DARK_BACKGROUND,
            line_width=0.5,
            alpha=0.95,
            legend_label=check_name,
        )
        plot.add_tools(
            HoverTool(
                renderers=[renderer],
                tooltips=[
                    ("check", "@check_name"),
                    ("severity", "@severity"),
                    ("time", "@timestamp{%F %T}"),
                    ("end", "@end_timestamp{%F %T}"),
                    ("value", "@value{0,0.######}"),
                    ("detail", "@detail"),
                ],
                formatters={"@timestamp": "datetime", "@end_timestamp": "datetime"},
            )
        )

    if plot.legend:
        plot.legend.click_policy = "hide"
        plot.legend.location = "top_left"
        _apply_dark_legend(plot)


def _anomaly_table(anomalies: pd.DataFrame) -> tuple[DataTable, ColumnDataSource]:
    table_df = anomalies.copy()
    if table_df.empty:
        table_df = _normalize_anomaly_rows([])
    source = ColumnDataSource(table_df)
    columns = [
        TableColumn(field="severity", title="Severity", width=70),
        TableColumn(field="check_name", title="Check", width=190),
        TableColumn(
            field="timestamp",
            title="Timestamp",
            width=160,
            formatter=DateFormatter(format="%F %T"),
        ),
        TableColumn(
            field="end_timestamp",
            title="End",
            width=160,
            formatter=DateFormatter(format="%F %T"),
        ),
        TableColumn(field="value", title="Value", width=110, formatter=NumberFormatter(format="0,0.######")),
        TableColumn(field="detail", title="Detail", width=520),
    ]
    return (
        _dark_table(source, columns, width=1180, height=260),
        source,
    )


def _wire_table_zoom(source: ColumnDataSource, x_range, window_days: int) -> None:
    source.selected.js_on_change(
        "indices",
        CustomJS(
            args={"source": source, "x_range": x_range, "window_ms": window_days * 24 * 3600 * 1000},
            code="""
            const indices = cb_obj.indices;
            if (!indices.length) {
                return;
            }
            const i = indices[0];
            const start = source.data.timestamp_ms[i];
            const end = source.data.end_timestamp_ms[i] || start;
            if (!Number.isFinite(start)) {
                return;
            }
            x_range.start = start - window_ms;
            x_range.end = end + window_ms;
            """,
        ),
    )
