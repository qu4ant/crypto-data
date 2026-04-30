# Audit LLM

## Verdict
ISSUES FOUND - highest risk is data correctness drift in futures symbol normalization, metric cadence assumptions, and broad exception handling that can let partial datasets look successful.

## Counts
- CRITICAL: 0
- HIGH: 8
- MEDIUM: 7
- LOW: 0

## Notes
- Protocol source: `/Users/gt/Documents/GitHub/algo_factory/.claude/commands/audit-llm.md`.
- Scope adapted for this repository: `src/crypto_data/`, `tests/`, and `scripts/`; factor-model, Nautilus, and notebook reviewers were not applicable.
- LLM-only audit: no fixes, no notebook mutation, no ruff/pre-commit/pytest execution.
- Semgrep was unavailable, so the required time-series scan used the protocol's `rg` fallback.
- External references consulted for vendor cadence/unit checks: Binance public-data README and Binance futures connector docs for `openInterestHist`.

## Findings (machine-readable)

```json
{
  "schema_version": "1",
  "verdict": "ISSUES FOUND",
  "scope": {
    "mode": "full_repo",
    "base": "",
    "branch": "main",
    "paths": [
      "src/crypto_data/",
      "tests/",
      "scripts/"
    ],
    "reviewers": [
      "time_series",
      "finance",
      "testing",
      "code_quality"
    ]
  },
  "findings": [
    {
      "id": "F001",
      "severity": "HIGH",
      "reviewer": "code_quality",
      "reviewers": [
        "code_quality"
      ],
      "agreement": 1,
      "file": "src/crypto_data/database_builder.py",
      "line": 60,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/coding-standards.md#error-handling",
      "issue": "_get_existing_universe_dates catches every database error and returns an empty set, so skip_existing=True can treat read/schema/permission failures as no existing snapshots and refetch or rewrite data.",
      "fix": "Catch only expected missing database/table errors and log that fallback; re-raise other DuckDB/runtime errors.",
      "evidence": "except Exception:",
      "confidence": "high"
    },
    {
      "id": "F002",
      "severity": "HIGH",
      "reviewer": "code_quality",
      "reviewers": [
        "code_quality"
      ],
      "agreement": 1,
      "file": "src/crypto_data/binance_pipeline.py",
      "line": 130,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/coding-standards.md#error-handling",
      "issue": "_process_results catches every import or database exception, increments failed stats, and lets update_binance_market_data finish normally; schema/import bugs can leave a partial DB without raising.",
      "fix": "Catch expected corrupt-file or validation exceptions narrowly; re-raise unexpected transaction, database, and programming errors, or raise at pipeline exit when non-404 failures occurred.",
      "evidence": "except Exception as e:",
      "confidence": "medium"
    },
    {
      "id": "F003",
      "severity": "HIGH",
      "reviewer": "code_quality",
      "reviewers": [
        "code_quality"
      ],
      "agreement": 1,
      "file": "src/crypto_data/database_builder.py",
      "line": 339,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/coding-standards.md#error-handling",
      "issue": "Universe snapshot writes catch all exceptions, roll back, increment fail_count, and continue; because the function raises only when all snapshots fail, partial schema/DB write failures can return success with missing dates.",
      "fix": "Separate transient fetch failures from write/schema invariants; abort on unexpected DB/schema errors or raise before returning when any write failed.",
      "evidence": "except Exception as e:",
      "confidence": "high"
    },
    {
      "id": "F004",
      "severity": "HIGH",
      "reviewer": "code_quality",
      "reviewers": [
        "code_quality"
      ],
      "agreement": 1,
      "file": "src/crypto_data/binance_downloader.py",
      "line": 465,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/coding-standards.md#error-handling",
      "issue": "_retry_with_prefix converts retry exceptions into failed results, then returns None when no prefixed attempt succeeds; callers keep the original all-404 result and lose the real retry error.",
      "fix": "Return/log prefixed retry failures and return None only when all prefixed attempts are confirmed not_found; propagate unexpected retry exceptions.",
      "evidence": "raw_results = await asyncio.gather(*tasks, return_exceptions=True)",
      "confidence": "high"
    },
    {
      "id": "F005",
      "severity": "HIGH",
      "reviewer": "finance",
      "reviewers": [
        "finance"
      ],
      "agreement": 1,
      "file": "src/crypto_data/binance_downloader.py",
      "line": 487,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/agents/audit-finance-reviewer.md#asset-class-conventions",
      "issue": "1000-prefix futures files are downloaded under the Binance contract symbol but stored as the unprefixed symbol without price/volume conversion, so PEPEUSDT-style rows can carry 1000PEPEUSDT units.",
      "fix": "Persist the raw Binance symbol separately, or convert price and volume units when removing the 1000 prefix; apply the same rule to REST repair.",
      "evidence": "symbol=symbol,  # Use original symbol",
      "confidence": "high"
    },
    {
      "id": "F006",
      "severity": "HIGH",
      "reviewer": "finance",
      "reviewers": [
        "finance"
      ],
      "agreement": 1,
      "file": "src/crypto_data/tables.py",
      "line": 63,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/agents/audit-finance-reviewer.md#funding-open-interest-units",
      "issue": "Open-interest continuity is hardcoded to 1h while Binance futures open-interest history supports sub-hour periods and Data Vision metrics files commonly contain 5-minute rows; gap/completeness checks can accept missing intra-hour data.",
      "fix": "Set the metric cadence to the actual Data Vision cadence or persist/derive the cadence per source, then update completeness and gap tests.",
      "evidence": "OPEN_INTEREST_EXPECTED_SECONDS = 60 * 60",
      "confidence": "medium"
    },
    {
      "id": "F007",
      "severity": "HIGH",
      "reviewer": "time_series",
      "reviewers": [
        "time_series"
      ],
      "agreement": 1,
      "file": "src/crypto_data/utils/symbols.py",
      "line": 107,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/time-series-safety.md#point-in-time",
      "issue": "Symbol extraction excludes a symbol from the whole download window if any row in the full date range has an excluded tag; a later classification can remove earlier eligible history from the download superset.",
      "fix": "Apply tag filters per snapshot and include a symbol if it is eligible on at least one requested date, while keeping downstream point-in-time membership separate.",
      "evidence": "if has_excluded_tag(tags, exclude_tags):",
      "confidence": "medium"
    },
    {
      "id": "F008",
      "severity": "HIGH",
      "reviewer": "testing",
      "reviewers": [
        "testing"
      ],
      "agreement": 1,
      "file": "src/crypto_data/schemas/checks.py",
      "line": 62,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/testing.md#property-based-tests-hypothesis",
      "issue": "The statistical price-continuity primitive has example tests but no focused property test for constant-series, scale, index/order, or NaN invariants.",
      "fix": "Add a focused Hypothesis test for check_price_continuity, or document why property testing is intentionally not used for this primitive.",
      "evidence": "def check_price_continuity(df: pd.DataFrame, sigma: float = 5.0) -> bool:",
      "confidence": "medium"
    },
    {
      "id": "F009",
      "severity": "MEDIUM",
      "reviewer": "code_quality",
      "reviewers": [
        "code_quality"
      ],
      "agreement": 1,
      "file": "src/crypto_data/binance_repair.py",
      "line": 358,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/coding-standards.md#error-handling",
      "issue": "Gap repair catches every exception from strategy selection, REST fetch, parsing, validation, and missing-count calculation, then returns an unrecoverable gap; internal bugs become data-gap reports.",
      "fix": "Catch expected REST/vendor transient exceptions only; re-raise unsupported table, parsing, schema, and invariant failures.",
      "evidence": "except Exception as exc:",
      "confidence": "medium"
    },
    {
      "id": "F010",
      "severity": "MEDIUM",
      "reviewer": "code_quality",
      "reviewers": [
        "code_quality"
      ],
      "agreement": 1,
      "file": "tests/validation/test_spot_validation.py",
      "line": 129,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/coding-standards.md#error-handling",
      "issue": "The validation loop catches exceptions from both API retrieval and compare_spot_kline, so comparison/code bugs are recorded as skips and the test can pass with zero successful validations.",
      "fix": "Catch API client exceptions only around the API call, let comparison exceptions fail, and assert at least one successful validation before assert_no_mismatches.",
      "evidence": "except Exception as e:",
      "confidence": "high"
    },
    {
      "id": "F011",
      "severity": "MEDIUM",
      "reviewer": "code_quality",
      "reviewers": [
        "code_quality"
      ],
      "agreement": 1,
      "file": "scripts/download_top60_h4_spot_futures_fundings_2022_01_2026_04.py",
      "line": 120,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/design-principles.md#configuration",
      "issue": "The script lets callers choose --universe-frequency for ingestion but hardcodes daily in the inline quality audit, producing false missing-snapshot findings for weekly or monthly runs.",
      "fix": "Pass args.universe_frequency into QualityConfig, or remove the CLI choice if the script must always build a daily universe.",
      "evidence": "universe_frequency=\"daily\",",
      "confidence": "high"
    },
    {
      "id": "F012",
      "severity": "MEDIUM",
      "reviewer": "time_series",
      "reviewers": [
        "time_series"
      ],
      "agreement": 1,
      "file": "src/crypto_data/schemas/checks.py",
      "line": 85,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/time-series-safety.md#multiindex-groupby-before-rolling-shift-expanding",
      "issue": "check_price_continuity computes returns with shift(1) in current row order and no symbol grouping or timestamp sort; multi-symbol or unsorted frames can compare unrelated or future rows.",
      "fix": "Require and enforce a single-symbol monotonic input contract, or sort and group by symbol before shifting.",
      "evidence": "returns = np.log(df[\"close\"] / df[\"close\"].shift(1))",
      "confidence": "high"
    },
    {
      "id": "F013",
      "severity": "MEDIUM",
      "reviewer": "time_series",
      "reviewers": [
        "time_series"
      ],
      "agreement": 1,
      "file": "src/crypto_data/tables.py",
      "line": 64,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/time-series-safety.md#external-fields-point-in-time",
      "issue": "Funding-rate continuity is hardcoded to 8h while Binance funding files expose funding_interval_hours; non-8h symbols would be mis-audited by completeness, gap enumeration, and repair.",
      "fix": "Persist or derive funding_interval_hours per row or symbol and use that cadence in completeness, gap enumeration, and repair.",
      "evidence": "FUNDING_RATES_EXPECTED_SECONDS = 8 * 60 * 60",
      "confidence": "medium"
    },
    {
      "id": "F014",
      "severity": "MEDIUM",
      "reviewer": "testing",
      "reviewers": [
        "testing"
      ],
      "agreement": 1,
      "file": "tests/schemas/test_metrics_schema.py",
      "line": 101,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/testing.md#test-structure",
      "issue": "The open-interest outlier test is tautological: it only asserts that the result is a boolean, not whether the constructed outlier case is detected.",
      "fix": "Assert the expected True/False outcome for the constructed outlier case, or replace it with a focused invariant/property test.",
      "evidence": "assert result in [True, False]",
      "confidence": "high"
    },
    {
      "id": "F015",
      "severity": "MEDIUM",
      "reviewer": "testing",
      "reviewers": [
        "testing"
      ],
      "agreement": 1,
      "file": "tests/repair/test_repair_validation.py",
      "line": 26,
      "rule": "/Users/gt/Documents/GitHub/algo_factory/.claude/rules/testing.md#test-types",
      "issue": "The live Binance REST repair validation calls an external API but is marked only validation/asyncio, so api or slow marker selection does not include it.",
      "fix": "Add @pytest.mark.api and @pytest.mark.slow, or document validation as the sole marker used for all external network tests.",
      "evidence": "async with BinanceRestClient(max_concurrent=1) as client:",
      "confidence": "medium"
    }
  ],
  "bias_leakage_findings": [
    "F007"
  ]
}
```
