-- Check for duplicates in each table
-- Run with: duckdb crypto_data.db < scripts/check_duplicates.sql

-- Check SPOT duplicates
SELECT 'SPOT' as table_name, COUNT(*) as duplicate_count FROM (
    SELECT exchange, symbol, interval, timestamp, COUNT(*) as cnt
    FROM spot
    GROUP BY exchange, symbol, interval, timestamp
    HAVING COUNT(*) > 1
);

-- Check FUTURES duplicates
SELECT 'FUTURES' as table_name, COUNT(*) as duplicate_count FROM (
    SELECT exchange, symbol, interval, timestamp, COUNT(*) as cnt
    FROM futures
    GROUP BY exchange, symbol, interval, timestamp
    HAVING COUNT(*) > 1
);

-- Check OPEN_INTEREST duplicates
SELECT 'OPEN_INTEREST' as table_name, COUNT(*) as duplicate_count FROM (
    SELECT exchange, symbol, timestamp, COUNT(*) as cnt
    FROM open_interest
    GROUP BY exchange, symbol, timestamp
    HAVING COUNT(*) > 1
);

-- Check FUNDING_RATES duplicates
SELECT 'FUNDING_RATES' as table_name, COUNT(*) as duplicate_count FROM (
    SELECT exchange, symbol, timestamp, COUNT(*) as cnt
    FROM funding_rates
    GROUP BY exchange, symbol, timestamp
    HAVING COUNT(*) > 1
);

-- Show examples of duplicates in FUNDING_RATES (if any)
SELECT exchange, symbol, timestamp, COUNT(*) as occurrence_count
FROM funding_rates
GROUP BY exchange, symbol, timestamp
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 10;
