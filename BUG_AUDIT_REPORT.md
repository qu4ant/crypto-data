# Code Audit Report: Critical Bugs and Issues
**Date:** 2025-11-05
**Project:** crypto-data v4.0.0
**Auditor:** Claude Code

---

## Executive Summary

This audit identified **12 issues** across 4 severity levels:
- **CRITICAL**: 5 issues (SQL injection, race conditions, resource leaks)
- **HIGH**: 3 issues (data corruption risks, incorrect validation)
- **MEDIUM**: 3 issues (edge cases, incomplete error handling)
- **LOW**: 1 issue (code quality)

**Immediate Action Required:** Issues #1, #2, #4, #5 should be fixed before production use.

---

## CRITICAL SEVERITY

### 1. SQL Injection Vulnerability via Table Name Interpolation
**Location:** `src/crypto_data/utils/database.py:146, 249, 346`
**Severity:** CRITICAL
**Risk:** Code execution, data exfiltration

**Description:**
Table names are constructed using f-strings with the `data_type` parameter without proper sanitization:

```python
# Line 146
conn.execute(f"INSERT INTO {table} SELECT * FROM df")

# Line 67 - table derived from user input
table = data_type  # 'spot' or 'futures'
```

While `data_type` is validated earlier in the call chain, this pattern is dangerous because:
1. Validation happens in different modules (loose coupling)
2. Future code changes could bypass validation
3. DuckDB's parameterized queries don't support table names as parameters

**Attack Vector:**
If validation is bypassed or removed, an attacker could inject:
```python
data_type = "spot; DROP TABLE crypto_universe; --"
```

**Recommendation:**
- Use explicit allowlist validation immediately before SQL construction:
```python
ALLOWED_TABLES = {'spot', 'futures', 'open_interest', 'funding_rates'}
if table not in ALLOWED_TABLES:
    raise ValueError(f"Invalid table name: {table}")
conn.execute(f"INSERT INTO {table} SELECT * FROM df")
```

---

### 2. Race Condition in Transaction Committed Flag
**Location:** `src/crypto_data/ingestion.py:1129-1140, 1169-1181, 1218-1229, 1262-1273, 1312-1323, 1360-1371`
**Severity:** CRITICAL
**Risk:** Data corruption, incomplete transactions

**Description:**
The transaction management pattern has a flaw:

```python
committed = False
try:
    conn.execute("BEGIN TRANSACTION")
    _process_metrics_results(results, conn, stats, symbol)
    conn.execute("COMMIT")
    committed = True
except Exception as e:
    if not committed:
        conn.execute("ROLLBACK")
```

**Problem:**
If an exception occurs between `COMMIT` and `committed = True`, the finally block or exception handler will attempt ROLLBACK on an already-committed transaction. This can cause:
- DuckDB errors about invalid transaction state
- Silent data corruption if partial commits succeed

**Proof of Concept:**
```python
conn.execute("BEGIN TRANSACTION")
conn.execute("INSERT INTO spot ...")  # Succeeds
conn.execute("COMMIT")  # Succeeds
# Exception here (e.g., KeyboardInterrupt, system error)
committed = True  # Never reached
# Exception handler runs ROLLBACK on non-existent transaction
```

**Recommendation:**
Use try-except-else or set flag before COMMIT:
```python
try:
    conn.execute("BEGIN TRANSACTION")
    _process_metrics_results(results, conn, stats, symbol)
except Exception as e:
    conn.execute("ROLLBACK")
    logger.error(f"Transaction failed: {e}")
    raise
else:
    conn.execute("COMMIT")
```

---

### 3. Resource Leak in CoinMarketCapClient Context Manager
**Location:** `src/crypto_data/clients/coinmarketcap.py:72-80`
**Severity:** CRITICAL
**Risk:** Connection exhaustion, memory leak

**Description:**
If `__aenter__` fails after creating the session, `__aexit__` may not be called:

```python
async def __aenter__(self):
    self._session = aiohttp.ClientSession()
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    if self._session:
        await self._session.close()
```

**Problem:**
If an exception occurs after `ClientSession()` but before `return self`, the session is created but never closed because `__aexit__` won't be called.

**Recommendation:**
Wrap session creation in try-except:
```python
async def __aenter__(self):
    try:
        self._session = aiohttp.ClientSession()
        return self
    except Exception:
        if self._session:
            await self._session.close()
        raise
```

Or initialize in `__init__` and create in `__aenter__`:
```python
async def __aenter__(self):
    self._session = aiohttp.ClientSession()
    return self
```

---

### 4. TOCTOU Race Condition in Ticker Mappings Cache
**Location:** `src/crypto_data/ingestion.py:1105-1107, 1144-1187, 1284-1286, 1327-1377`
**Severity:** CRITICAL
**Risk:** Duplicate work, cache inconsistency, potential deadlock

**Description:**
The ticker mapping cache has a Time-Of-Check-Time-Of-Use vulnerability:

```python
# Line 1105 - Check happens OUTSIDE lock
with _ticker_mappings_lock:
    download_symbol = _ticker_mappings.get(symbol, symbol)

# Line 1118 - Download happens WITHOUT lock
results = asyncio.run(
    _download_symbol_open_interest_async(
        symbol=download_symbol,
        ...
    )
)

# Line 1144 - Second check OUTSIDE lock
if (download_symbol == symbol and  # Race condition here!
    len(results) > 0 and
    all(not r['success'] and r['error'] == 'not_found' for r in results)):

    # Line 1165 - Update happens INSIDE lock (but too late)
    with _ticker_mappings_lock:
        _ticker_mappings[symbol] = prefixed_symbol
```

**Attack Scenario:**
1. Thread A checks cache, finds no mapping for PEPEUSDT
2. Thread A starts downloading PEPEUSDT (fails with 404)
3. Thread B checks cache, finds no mapping for PEPEUSDT
4. Thread B starts downloading PEPEUSDT (fails with 404)
5. Thread A discovers 1000PEPEUSDT works, updates cache
6. Thread B discovers 1000PEPEUSDT works, updates cache (duplicate work)

**Recommendation:**
Use double-checked locking pattern:
```python
with _ticker_mappings_lock:
    if symbol in _ticker_mappings:
        download_symbol = _ticker_mappings[symbol]
    else:
        # Mark as "in progress" to prevent duplicate work
        _ticker_mappings[symbol] = None

# Download outside lock
if download_symbol is None:
    # Attempt download with original symbol
    # If fails, try 1000-prefix
    # Update cache with result
```

---

### 5. Incomplete Transaction Rollback in Universe Ingestion
**Location:** `src/crypto_data/ingestion.py:395-420`
**Severity:** CRITICAL
**Risk:** Partial data import, database corruption

**Description:**
Transaction rollback logic has gaps:

```python
committed = False
try:
    conn.execute("BEGIN TRANSACTION")

    # Delete all existing records for this date
    conn.execute("DELETE FROM crypto_universe WHERE date = ?", [date_str])
    logger.debug(f"Deleted existing records for {date_str}")

    # Insert new data if we have any
    if len(df_new) > 0:
        conn.execute("INSERT INTO crypto_universe SELECT * FROM df_new")
        logger.debug(f"Inserted {len(df_new)} new records for {date_str}")

    conn.execute("COMMIT")
    committed = True

except Exception as e:
    if not committed:
        conn.execute("ROLLBACK")
    logger.error(f"Failed to update universe for {date_str}: {e}")
    fail_count += 1
    continue  # <-- Dangerous! Swallows exception
```

**Problems:**
1. DELETE succeeds, then INSERT fails → data loss (mitigated by ROLLBACK)
2. Exception during ROLLBACK is not caught → crash with partial data
3. `continue` statement swallows exception → no visibility into failures
4. DuckDB connection error after BEGIN → no ROLLBACK attempted

**Data Loss Scenario:**
```
1. BEGIN TRANSACTION
2. DELETE FROM crypto_universe WHERE date = '2024-01-01'  ✓ Success (100 rows deleted)
3. INSERT fails due to schema mismatch
4. ROLLBACK fails due to connection error
5. Result: 100 rows permanently deleted, no new data inserted
```

**Recommendation:**
```python
try:
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM crypto_universe WHERE date = ?", [date_str])
        if len(df_new) > 0:
            conn.execute("INSERT INTO crypto_universe SELECT * FROM df_new")
        conn.execute("COMMIT")
        success_count += 1
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Transaction failed for {date_str}: {e}")
        fail_count += 1
except duckdb.Error as e:
    logger.critical(f"Database connection error: {e}")
    raise
```

---

## HIGH SEVERITY

### 6. Silent Duplicate Key Errors Mask Data Integrity Issues
**Location:** `src/crypto_data/utils/database.py:149-155, 252-257, 349-354`
**Severity:** HIGH
**Risk:** Data corruption, inconsistent state

**Description:**
Duplicate key errors are silently ignored:

```python
try:
    conn.execute(f"INSERT INTO {table} SELECT * FROM df")
    logger.debug(f"  Import successful: {len(df)} rows")
except Exception as insert_error:
    # Skip silently if duplicate (données déjà là)
    if "Duplicate key" in str(insert_error):
        logger.debug(f"Skipped duplicate data for {storage_symbol} {table}")
        # Données déjà présentes, on continue sans erreur
    else:
        raise
```

**Problems:**
1. String matching on error message is fragile (locale-dependent, version-dependent)
2. Genuine duplicate key violations (data corruption) are hidden
3. No distinction between "expected duplicate" vs "unexpected duplicate"
4. Logs at DEBUG level → invisible in production

**Scenario:**
```
1. Import BTCUSDT 2024-01 (1000 rows) ✓
2. Database corruption: duplicate timestamps inserted
3. Re-import BTCUSDT 2024-01
4. Duplicate key error silently ignored → corruption persists
5. User queries data → incorrect results
```

**Recommendation:**
```python
try:
    conn.execute(f"INSERT INTO {table} SELECT * FROM df")
    logger.debug(f"  Import successful: {len(df)} rows")
except Exception as insert_error:
    if "Duplicate key" in str(insert_error):
        # Check if duplicates are expected (idempotent re-import)
        logger.warning(f"Duplicate data detected for {storage_symbol} {table}")
        # Verify data consistency before continuing
        verify_data_integrity(conn, table, symbol, month)
    else:
        raise
```

---

### 7. Incorrect Date Comparison Logic in data_exists()
**Location:** `src/crypto_data/utils/database.py:443-445`
**Severity:** HIGH
**Risk:** Incorrect skip logic, missing data

**Description:**
Date comparison uses string comparison after converting datetime:

```python
# Line 443-445
max_date_str = max_timestamp.strftime('%Y-%m-%d')
return max_date_str >= threshold_date  # String comparison!
```

**Problems:**
1. Works for ISO format (YYYY-MM-DD) but fragile
2. If `strftime` format changes → comparison breaks
3. Timestamp object comparison is more reliable
4. Edge case: If max_timestamp is datetime object, strftime might fail

**False Positive Scenario:**
```python
threshold_date = "2024-01-24"
max_timestamp = datetime(2024, 1, 10)  # Day 10
max_date_str = "2024-01-10"

# String comparison: "2024-01-10" >= "2024-01-24" → False ✓ Correct

# But if date format changes:
max_date_str = "01-10-2024"  # MM-DD-YYYY format
# "01-10-2024" >= "2024-01-24" → False ✗ Wrong order
```

**Recommendation:**
```python
from datetime import datetime

# Parse threshold_date to datetime for proper comparison
threshold_datetime = datetime.strptime(threshold_date, '%Y-%m-%d')
return max_timestamp >= threshold_datetime
```

---

### 8. Missing Validation for Exchange Parameter
**Location:** Throughout codebase (database.py, ingestion.py)
**Severity:** HIGH
**Risk:** Invalid data in database, query failures

**Description:**
The `exchange` parameter defaults to 'binance' but is never validated:

```python
# database.py:22
def import_to_duckdb(
    conn,
    file_path: Path,
    symbol: str,
    data_type: str,
    interval: str,
    exchange: str = 'binance',  # <-- No validation
    original_symbol: Optional[str] = None
):
    # ...
    df['exchange'] = exchange  # <-- Blindly inserted
```

**Problems:**
1. User could pass invalid exchange name → garbage data in database
2. Queries use `WHERE exchange = 'binance'` → miss data if wrong value
3. No centralized list of valid exchanges
4. Schema allows any VARCHAR → no constraint

**Recommendation:**
```python
VALID_EXCHANGES = {'binance', 'bybit', 'kraken', 'coinbase'}

def import_to_duckdb(
    conn,
    file_path: Path,
    symbol: str,
    data_type: str,
    interval: str,
    exchange: str = 'binance',
    original_symbol: Optional[str] = None
):
    if exchange not in VALID_EXCHANGES:
        raise ValueError(f"Invalid exchange '{exchange}'. Must be one of: {VALID_EXCHANGES}")
    # ... rest of function
```

---

## MEDIUM SEVERITY

### 9. Timestamp Format Auto-Detection Edge Case
**Location:** `src/crypto_data/utils/database.py:116-121`
**Severity:** MEDIUM
**Risk:** Incorrect timestamp conversion near boundary

**Description:**
Timestamp format detection uses threshold `5e12`:

```python
df['timestamp'] = pd.to_datetime(
    df['close_time'].apply(
        lambda x: x / 1000000.0 if x >= 5e12 else x / 1000.0
    ),
    unit='s'
).dt.ceil('1s')
```

**Problem:**
- Threshold `5e12` = 5,000,000,000,000 ms = January 2128
- Binance timestamps near this boundary could be misclassified
- Floating point comparison with `5e12` could have precision issues
- No handling for timestamps in seconds (10 digits) or nanoseconds (19 digits)

**Edge Case:**
```python
close_time = 4_999_999_999_999  # Just below threshold
# Treated as milliseconds: / 1000.0 → timestamp in seconds
# Result: 4999999999.999 seconds → year 2128 ✓ Correct

close_time = 5_000_000_000_000  # At threshold
# Treated as microseconds: / 1000000.0 → timestamp in seconds
# Result: 5000000.0 seconds → year 1970 + 158 years = 2128 ✓ Correct

# But if Binance changes to nanoseconds (19 digits):
close_time = 1_700_000_000_000_000_000  # Nanoseconds (2023)
# Treated as microseconds: / 1000000.0
# Result: 1700000000000 seconds → year 55809 ✗ Wrong!
```

**Recommendation:**
```python
def detect_timestamp_unit(timestamp_value):
    """Detect timestamp unit based on number of digits."""
    if timestamp_value < 1e10:  # 10 digits or less
        return 1.0  # Already in seconds
    elif timestamp_value < 1e13:  # 10-12 digits
        return 1000.0  # Milliseconds
    elif timestamp_value < 1e16:  # 13-15 digits
        return 1000000.0  # Microseconds
    else:  # 16+ digits
        return 1000000000.0  # Nanoseconds

df['timestamp'] = pd.to_datetime(
    df['close_time'].apply(lambda x: x / detect_timestamp_unit(x)),
    unit='s'
).dt.ceil('1s')
```

---

### 10. Missing Validation in _fetch_snapshot for Malformed API Responses
**Location:** `src/crypto_data/ingestion.py:260-294`
**Severity:** MEDIUM
**Risk:** Crash on malformed API data

**Description:**
No validation that required fields exist in API response:

```python
for coin in coins:
    symbol = coin.get('symbol', '').upper()  # Safe with default
    rank = coin.get('cmcRank', 0)  # Safe with default

    tags = coin.get('tags', [])  # Safe with default
    tags_str = ','.join(tags) if tags else ''  # But what if tags is not a list?

    market_cap = 0
    quotes = coin.get('quotes', [])
    if quotes and len(quotes) > 0:
        market_cap = quotes[0].get('marketCap', 0)  # What if quotes[0] is None?
```

**Problems:**
1. No validation that `coin` is a dict
2. No validation that `tags` is a list → `','.join(tags)` crashes if string
3. No validation that `quotes[0]` is a dict → crash on `None.get()`
4. No validation that `symbol` is non-empty

**Crash Scenario:**
```json
{
  "data": [
    {
      "symbol": "BTC",
      "cmcRank": 1,
      "tags": "defi,layer-1",  // String instead of list
      "quotes": [null]  // None instead of dict
    }
  ]
}
```

Results in:
```
TypeError: 'str' object is not iterable (line 266)
AttributeError: 'NoneType' object has no attribute 'get' (line 284)
```

**Recommendation:**
```python
for coin in coins:
    if not isinstance(coin, dict):
        logger.warning(f"Skipping malformed coin entry: {coin}")
        continue

    symbol = coin.get('symbol', '').upper()
    if not symbol:
        logger.warning(f"Skipping coin with empty symbol: {coin}")
        continue

    rank = coin.get('cmcRank', 0)
    if not isinstance(rank, int) or rank <= 0:
        logger.warning(f"Skipping coin {symbol} with invalid rank: {rank}")
        continue

    # Validate tags is a list
    tags = coin.get('tags', [])
    if not isinstance(tags, list):
        logger.warning(f"Invalid tags for {symbol}, expected list: {tags}")
        tags = []
    tags_str = ','.join(str(t) for t in tags)

    # Validate quotes structure
    market_cap = 0
    quotes = coin.get('quotes', [])
    if isinstance(quotes, list) and len(quotes) > 0:
        quote = quotes[0]
        if isinstance(quote, dict):
            market_cap = quote.get('marketCap', 0)
```

---

### 11. Temp File Cleanup Failure on Exception
**Location:** `src/crypto_data/utils/database.py:161-164, 264-266, 361-363`
**Severity:** MEDIUM
**Risk:** Disk space exhaustion

**Description:**
Temp files are cleaned up in `finally` block, but only for CSV files:

```python
finally:
    # Delete extracted CSV file
    if csv_path.exists():
        csv_path.unlink()
```

**Problem:**
The ZIP file (`file_path`) is never cleaned up by this function. The caller is responsible for cleanup, but if the caller crashes before cleanup:
- ZIP files accumulate in temp directory
- Could exhaust disk space on long-running processes

**Recommendation:**
Document the cleanup responsibility clearly:
```python
def import_to_duckdb(
    conn,
    file_path: Path,
    ...
):
    """
    Import CSV from ZIP file into DuckDB.

    IMPORTANT: This function does NOT delete the input ZIP file.
    The caller is responsible for cleanup.

    Parameters
    ----------
    file_path : Path
        Path to ZIP file. Caller must delete after import completes.
    ...
    """
```

Or add cleanup parameter:
```python
def import_to_duckdb(
    conn,
    file_path: Path,
    ...
    cleanup_zip: bool = False
):
    try:
        # ... import logic
    finally:
        # Delete extracted CSV
        if csv_path.exists():
            csv_path.unlink()

        # Optionally delete ZIP
        if cleanup_zip and file_path.exists():
            file_path.unlink()
```

---

## LOW SEVERITY

### 12. Inconsistent Symbol Normalization Documentation
**Location:** `src/crypto_data/ingestion.py:1105-1187` (open_interest), `1192-1280` (funding_rates)
**Severity:** LOW
**Risk:** Confusion, unexpected behavior

**Description:**
The 1000-prefix auto-discovery is documented for futures in the docstring, but the code also applies it to `open_interest` and `funding_rates`:

```python
# ingest_binance_async() docstring (line 1074-1076):
"""
- Auto-detects 1000-prefix futures: PEPE/SHIB/BONK use 1000PEPEUSDT/1000SHIBUSDT/1000BONKUSDT
  Mappings cached in session to avoid repeated 404s
"""

# But in code (line 1105-1107):
if data_type == 'open_interest':
    # Check if we have a cached ticker mapping (e.g., PEPEUSDT → 1000PEPEUSDT)
    with _ticker_mappings_lock:
        download_symbol = _ticker_mappings.get(symbol, symbol)
```

**Problem:**
Documentation says "futures only", but code applies to all data types. This is actually correct behavior (open_interest and funding_rates are futures-only metrics), but documentation is misleading.

**Recommendation:**
Update docstring:
```python
"""
- Auto-detects 1000-prefix for futures-related data:
  PEPE/SHIB/BONK use 1000PEPEUSDT/1000SHIBUSDT/1000BONKUSDT for:
  * futures klines
  * open_interest metrics
  * funding_rates
  Mappings cached in session to avoid repeated 404s
"""
```

---

## Summary of Recommendations

### Immediate Actions (Before Next Release):
1. **Add table name validation** (Issue #1) - SQL injection protection
2. **Fix transaction handling** (Issues #2, #5) - Prevent data corruption
3. **Fix cache race condition** (Issue #4) - Use proper locking
4. **Add resource cleanup** (Issue #3) - Prevent connection leaks

### High Priority (Next Sprint):
5. **Improve duplicate key handling** (Issue #6) - Better error visibility
6. **Fix date comparison** (Issue #7) - More reliable logic
7. **Add exchange validation** (Issue #8) - Data integrity

### Medium Priority (Backlog):
8. **Improve timestamp detection** (Issue #9) - Handle edge cases
9. **Add API response validation** (Issue #10) - Prevent crashes
10. **Document cleanup responsibility** (Issue #11) - Prevent confusion

### Low Priority (Nice to Have):
11. **Update documentation** (Issue #12) - Clarify 1000-prefix behavior

---

## Testing Recommendations

### Unit Tests Needed:
1. `test_sql_injection_protection()` - Verify table name validation
2. `test_transaction_rollback()` - Verify proper error handling
3. `test_concurrent_ticker_mapping()` - Verify thread safety
4. `test_malformed_api_response()` - Verify graceful degradation
5. `test_timestamp_edge_cases()` - Verify boundary conditions

### Integration Tests Needed:
1. Test partial import failures with rollback
2. Test concurrent ingestion from multiple threads
3. Test recovery from network errors during transaction

### Load Tests Needed:
1. Test temp file cleanup under high load
2. Test connection pool exhaustion
3. Test database lock contention with concurrent writes

---

## Code Quality Observations

### Positive:
- Good use of async/await for performance
- Comprehensive logging throughout
- Transaction safety is a priority
- Good documentation in docstrings

### Negative:
- Inconsistent error handling patterns
- String-based error matching is fragile
- Global state (_ticker_mappings) could be avoided
- Some functions are too long (>200 lines)

---

## Conclusion

This codebase has **5 critical issues** that should be addressed before production use. The most severe are:
1. Transaction handling bugs that could cause data loss
2. Race conditions in concurrent code
3. SQL injection vulnerability (mitigated by validation, but dangerous pattern)

The code is generally well-structured, but needs defensive programming improvements around error handling, validation, and resource management.

**Estimated Effort:** 2-3 days to fix all critical and high-severity issues.
