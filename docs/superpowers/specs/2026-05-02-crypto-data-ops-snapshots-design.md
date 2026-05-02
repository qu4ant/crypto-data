# Crypto Data Ops Snapshots Design

Date: 2026-05-02
Status: Approved design for V1 implementation

## Objective

Create a minimal ops layer for publishing reproducible DuckDB datasets from the
existing `crypto-data` ingestion package.

The system must support:

- one mutable live DuckDB database on the ops VPS
- immutable release snapshots for laptops and production machines
- a versioned JSON config as the source of truth for each run
- a metadata JSON file next to each snapshot
- checksum files for integrity checks
- monthly retention, keeping the latest 12 published snapshots

The core Python package remains responsible for ingestion. The ops layer is
responsible for orchestration, locking, publication, metadata, and retention.

## Non-Goals

- Do not use a shared network mount as a live DuckDB database.
- Do not let multiple machines write to the same DuckDB file.
- Do not move storage to Hetzner Storage Box or Object Storage in V1.
- Do not introduce Parquet as the source of truth in V1.
- Do not make `current.txt` contain metadata. It should only point to the latest
  published snapshot.

## Target Layout

Repository layout:

```text
ops/
  configs/
    top50_h4_daily.json
  publish_snapshot.py
```

VPS layout:

```text
/srv/crypto-data/
  live/
    crypto_data.duckdb
  releases/
    crypto_data_2026-05.duckdb
    crypto_data_2026-05.sha256
    crypto_data_2026-05.metadata.json
    crypto_data_2026-05.quality.json
  current.txt
  logs/
```

## Design Decisions

### Writer Model

The VPS ops machine is the only writer. It owns:

```text
/srv/crypto-data/live/crypto_data.duckdb
```

Cron or a manual operator runs the update process on the VPS. Laptops and
production machines must not read from or write to `live/`. They consume only
published release snapshots.

### Snapshot Model

Each published snapshot is a full DuckDB file:

```text
releases/crypto_data_YYYY-MM.duckdb
```

At the current expected size, around 1 GB per database, full monthly snapshots
are simpler and safer than incremental files. With an 80 GB VPS disk, keeping 12
monthly snapshots is acceptable.

### Config Source of Truth

V1 uses a versioned JSON config file:

```text
ops/configs/top50_h4_daily.json
```

The hardcoded `scripts/Download_data_universe.py` remains available as a manual
example, but the VPS publication flow does not depend on it.

The production run path is:

```text
ops/configs/*.json
  -> ops/publish_snapshot.py
  -> crypto_data.create_binance_database(...)
  -> releases/*.duckdb + metadata
```

This avoids divergence between the script parameters and the metadata.

## Components

### `ops/configs/top50_h4_daily.json`

Defines the ingestion parameters for a dataset run.

Example:

```json
{
  "name": "top50_h4_daily",
  "db_path": "/srv/crypto-data/live/crypto_data.duckdb",
  "start_date": "2022-01-01",
  "end_date": "2026-04-01",
  "top_n": 50,
  "interval": "HOUR_4",
  "data_types": ["SPOT", "FUTURES", "FUNDING_RATES"],
  "exclude_symbols": ["LUNA", "FTT"],
  "exclude_tags": "DEFAULT_UNIVERSE_EXCLUDE_TAGS",
  "universe_frequency": "daily",
  "skip_existing_universe": true,
  "daily_quota": 200,
  "repair_gaps_via_api": false,
  "retention_months": 12,
  "quality_check": {
    "enabled": true,
    "output_file": "auto"
  }
}
```

Rules:

- `interval` must map to a `crypto_data.Interval` enum member.
- `data_types` must map to `crypto_data.DataType` enum members.
- `exclude_tags` may be the string `DEFAULT_UNIVERSE_EXCLUDE_TAGS` or an explicit
  list of tag strings.
- `db_path` points to the live mutable database on the VPS.
- `retention_months` controls release cleanup.
- `quality_check.enabled` controls whether the quality validation step runs.
- `quality_check.output_file` may be `auto` in V1, which writes the report next
  to the published snapshot using the snapshot id.

### `ops/publish_snapshot.py`

Runs the complete VPS publication workflow from one config file:

```bash
uv run python ops/publish_snapshot.py ops/configs/top50_h4_daily.json
```

Responsibilities:

- parse and validate the JSON config
- convert enum strings to `Interval` and `DataType`
- resolve `DEFAULT_UNIVERSE_EXCLUDE_TAGS`
- acquire a non-blocking file lock
- ensure `/srv/crypto-data/live`, `/srv/crypto-data/releases`, and
  `/srv/crypto-data/logs` exist
- call `setup_colored_logging()`
- call `create_binance_database(...)`
- return non-zero on invalid config or failed ingestion
- run the existing quality validation script when `quality_check.enabled` is true
- copy the live DB to a release file
- compute `sha256`
- write metadata JSON
- update `current.txt`
- prune monthly snapshots beyond the configured retention

The release publish step should avoid exposing partial files. The script should
write temporary files first and then move them into their final names.

V1 intentionally avoids a shell wrapper. Python owns config parsing, metadata,
checksum generation, and retention so the workflow can be tested without
duplicating logic in shell.

## Data Flow

```text
manual run or cron
  -> publish_snapshot.py CONFIG
  -> file lock acquired
  -> create_binance_database(db_path=/srv/crypto-data/live/crypto_data.duckdb, ...)
  -> validate data quality if enabled
  -> copy live DB to releases/crypto_data_YYYY-MM.duckdb.tmp
  -> compute checksum
  -> write metadata tmp
  -> atomic rename tmp files to final release files
  -> write current.txt tmp
  -> atomic rename current.txt
  -> prune old snapshots
```

Consumers read `current.txt`, then copy the referenced release snapshot.

Example:

```bash
SNAPSHOT=$(ssh vps-ops 'cat /srv/crypto-data/current.txt')
rsync -avP "vps-ops:/srv/crypto-data/${SNAPSHOT}" ./crypto_data.duckdb
rsync -avP "vps-ops:/srv/crypto-data/${SNAPSHOT%.duckdb}.sha256" .
```

## Metadata JSON

Each release must have:

```text
releases/crypto_data_YYYY-MM.metadata.json
```

Example:

```json
{
  "schema_version": 1,
  "snapshot_id": "crypto_data_2026-05",
  "created_at_utc": "2026-05-02T05:20:00Z",
  "status": "published",
  "db_file": "crypto_data_2026-05.duckdb",
  "sha256": "abc123...",
  "size_bytes": 1073741824,
  "source": {
    "git_commit": "abc1234",
    "git_branch": "main",
    "command": "uv run python ops/publish_snapshot.py ops/configs/top50_h4_daily.json",
    "hostname": "vps-ops"
  },
  "config": {
    "name": "top50_h4_daily",
    "db_path": "/srv/crypto-data/live/crypto_data.duckdb",
    "start_date": "2022-01-01",
    "end_date": "2026-04-01",
    "top_n": 50,
    "interval": "HOUR_4",
    "data_types": ["SPOT", "FUTURES", "FUNDING_RATES"],
    "exclude_symbols": ["LUNA", "FTT"],
    "exclude_tags": "DEFAULT_UNIVERSE_EXCLUDE_TAGS",
    "universe_frequency": "daily",
    "skip_existing_universe": true,
    "daily_quota": 200,
    "repair_gaps_via_api": false,
    "retention_months": 12,
    "quality_check": {
      "enabled": true,
      "output_file": "auto"
    }
  },
  "validation": {
    "status": "passed",
    "quality_report_file": "crypto_data_2026-05.quality.json"
  }
}
```

Required metadata fields:

- `schema_version`
- `snapshot_id`
- `created_at_utc`
- `status`
- `db_file`
- `sha256`
- `size_bytes`
- `source.git_commit`
- `source.git_branch`
- `source.command`
- `source.hostname`
- `config`
- `validation.status`

## Error Handling

The workflow must fail without publishing a new `current.txt` when:

- the config JSON is invalid
- enum names cannot be resolved
- ingestion fails
- validation fails when enabled
- checksum generation fails
- the release copy fails
- metadata generation fails

If the workflow fails after creating temporary files, the temporary files can be
left for debugging, but they must not be referenced by `current.txt`.

If a release filename already exists, the script must fail instead of
overwriting it.

## Retention

The default retention policy keeps the latest 12 monthly release snapshots.

Retention applies to groups of files sharing the same snapshot id:

```text
crypto_data_YYYY-MM.duckdb
crypto_data_YYYY-MM.sha256
crypto_data_YYYY-MM.metadata.json
crypto_data_YYYY-MM.quality.json
```

The currently referenced snapshot in `current.txt` must never be pruned.
The quality report is optional when `quality_check.enabled` is false, but if it
exists it must be pruned with the matching snapshot group.

## Cron

The cron entry should run on the VPS ops machine.

Example monthly schedule:

```cron
0 3 1 * * cd /path/to/crypto-data && uv run python ops/publish_snapshot.py ops/configs/top50_h4_daily.json >> /srv/crypto-data/logs/monthly_publish.log 2>&1
```

The same script can be run manually for controlled publication.

## Testing

V1 should include tests for `ops/publish_snapshot.py`:

- valid config maps strings to enums correctly
- invalid interval fails clearly
- invalid data type fails clearly
- `DEFAULT_UNIVERSE_EXCLUDE_TAGS` resolves correctly
- explicit `exclude_tags` list is accepted
- `quality_check.enabled=false` records validation as skipped
- existing release filenames are not overwritten
- `current.txt` is not changed when publication fails before the final step
- retention keeps the current snapshot and prunes older snapshot groups

Manual acceptance checklist:

- ingestion writes to `/srv/crypto-data/live/crypto_data.duckdb`
- release DB appears in `/srv/crypto-data/releases`
- `.sha256` validates against the release DB
- `.metadata.json` contains the exact config used
- `current.txt` points to the release DB path
- a consumer can `rsync` the release DB and open it with DuckDB
- rerunning while the lock is held fails without a second writer

## Acceptance Criteria

The V1 implementation is complete when:

- a config JSON can drive ingestion without editing Python constants
- the live DB is updated only on the VPS path
- a release snapshot is published with checksum, metadata, and either a passed
  validation report or an explicit `validation.status=skipped`
- `current.txt` points to the latest published snapshot
- failures do not update `current.txt`
- old monthly releases are pruned according to `retention_months`
- the package ingestion API remains usable independently of the ops layer
