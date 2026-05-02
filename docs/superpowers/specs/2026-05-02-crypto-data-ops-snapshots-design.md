# Crypto Data Ops Snapshots Design

Date: 2026-05-02
Status: Approved design for V1 implementation

## Objective

Create a minimal ops layer for publishing reproducible DuckDB datasets from the
existing `crypto-data` ingestion package.

The system must support:

- one temporary build DuckDB database per publication run
- one immutable full DuckDB release snapshot per month
- a versioned JSON config as the source of truth for each run
- a metadata JSON file next to each snapshot
- checksum files for integrity checks
- an import anomaly JSONL sidecar when import-time anomalies are recorded
- monthly retention, keeping the latest 12 published snapshots

The core Python package remains responsible for ingestion. The ops layer is
responsible for orchestration, locking, publication, metadata, and retention.

## Non-Goals

- Do not use a shared network mount as a build DuckDB database.
- Do not let multiple machines write to the same DuckDB file.
- Do not keep a permanent mutable `latest.duckdb` or `work/crypto_data.duckdb`.
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
  build/
    crypto_data_2026-06.tmp.duckdb
  releases/
    crypto_data_2026-05.duckdb
    crypto_data_2026-05.sha256
    crypto_data_2026-05.metadata.json
    crypto_data_2026-05.import_anomalies.jsonl
    crypto_data_2026-05.quality.json
  current.txt
  logs/
```

## Design Decisions

### Writer Model

The VPS ops machine is the only writer. During each run it owns a temporary
build database:

```text
/srv/crypto-data/build/crypto_data_YYYY-MM.tmp.duckdb
```

Cron or a manual operator runs the update process on the VPS. Laptops and
production machines must not read from or write to `build/`. They consume only
published release snapshots.

This dataset is primarily for historical backtests and research, not a live
market-serving database. `current.txt` means the latest published dataset
snapshot, not a real-time database.

### Snapshot Model

Each published snapshot is a full DuckDB file:

```text
releases/crypto_data_YYYY-MM.duckdb
```

The snapshot id is derived from an explicit `snapshot_month` config value:

```text
snapshot_id = {snapshot_prefix}_{snapshot_month}
```

For example, `snapshot_prefix=crypto_data` and `snapshot_month=2026-05`
publish `releases/crypto_data_2026-05.duckdb`.

`snapshot_month` is the publication month, not necessarily the final covered
data month. The covered data range remains defined only by `start_date` and
`end_date`.

Release files are immutable. A run must fail if the target monthly release file
already exists.

At the current expected size, around 1 GB per database, full monthly snapshots
are simpler and safer than incremental files. With an 80 GB VPS disk, keeping 12
monthly snapshots is acceptable.

### Build Model

The script builds each new monthly snapshot in `build/`.

Default mode:

```text
incremental_from_current
```

In this mode, `publish_snapshot.py` reads `current.txt`, copies the referenced
release snapshot to a temporary build database, then runs ingestion on that copy
with the configured `skip_existing_*` behavior.

This keeps the published snapshots immutable while still avoiding a full
redownload on each monthly run.

If no previous snapshot exists, the script starts from an empty build database.

If the config is structurally incompatible with the previous snapshot, the
script must fail before ingestion unless the config explicitly requests a full
rebuild. Structural compatibility includes at least:

- `start_date`
- `top_n`
- `interval`
- `data_types`
- `exclude_symbols`
- `exclude_tags`
- `universe_frequency`

`end_date` is allowed to advance between monthly snapshots.
Compatibility checks must compare the resolved `config_effective` values from
metadata, not only the raw JSON values. This avoids treating
`DEFAULT_UNIVERSE_EXCLUDE_TAGS` and its expanded list as different configs.

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
  "root_dir": "/srv/crypto-data",
  "snapshot_prefix": "crypto_data",
  "snapshot_month": "2026-05",
  "build_mode": "incremental_from_current",
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
- `root_dir` points to the VPS dataset root.
- `snapshot_prefix` controls release filenames.
- `snapshot_month` is required and must be `YYYY-MM`.
- `build_mode` is `incremental_from_current` by default.
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
- ensure `/srv/crypto-data/build`, `/srv/crypto-data/releases`, and
  `/srv/crypto-data/logs` exist
- derive the target `snapshot_id`, release path, and temporary build DB path
- fail if the target release file already exists
- seed the build DB from `current.txt` when `build_mode=incremental_from_current`
- verify structural config compatibility with the base snapshot metadata
- call `setup_colored_logging()`
- call `create_binance_database(...)` with the temporary build DB path and an
  explicit import anomaly report path
- return non-zero on invalid config or failed ingestion
- run the existing quality validation script when `quality_check.enabled` is true
- publish the temporary build DB as an immutable release file
- publish the import anomaly sidecar when one was produced
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
  -> derive snapshot_id crypto_data_YYYY-MM
  -> fail if releases/crypto_data_YYYY-MM.duckdb exists
  -> copy current snapshot to build/crypto_data_YYYY-MM.tmp.duckdb if compatible
  -> create_binance_database(db_path=/srv/crypto-data/build/crypto_data_YYYY-MM.tmp.duckdb, ...)
  -> validate data quality if enabled
  -> compute checksum
  -> write metadata tmp, including raw and effective config
  -> atomic move build DB and sidecars to final release files
  -> write current.txt tmp
  -> atomic rename current.txt
  -> prune old snapshots
```

Consumers read `current.txt`, then copy the referenced release snapshot.
`current.txt` must contain only the release DB path relative to `root_dir`, for
example:

```text
releases/crypto_data_2026-05.duckdb
```

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
  "immutable": true,
  "db_file": "crypto_data_2026-05.duckdb",
  "sha256": "abc123...",
  "size_bytes": 1073741824,
  "sidecars": {
    "metadata_file": "crypto_data_2026-05.metadata.json",
    "quality_report_file": "crypto_data_2026-05.quality.json",
    "import_anomaly_file": "crypto_data_2026-05.import_anomalies.jsonl"
  },
  "source": {
    "git_commit": "abc1234",
    "git_branch": "main",
    "command": "uv run python ops/publish_snapshot.py ops/configs/top50_h4_daily.json",
    "hostname": "vps-ops"
  },
  "build": {
    "mode": "incremental_from_current",
    "base_snapshot": "releases/crypto_data_2026-04.duckdb",
    "build_db_file": "build/crypto_data_2026-05.tmp.duckdb",
    "config_compatibility": "passed"
  },
  "config_raw": {
    "name": "top50_h4_daily",
    "root_dir": "/srv/crypto-data",
    "snapshot_prefix": "crypto_data",
    "snapshot_month": "2026-05",
    "build_mode": "incremental_from_current",
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
  "config_effective": {
    "name": "top50_h4_daily",
    "root_dir": "/srv/crypto-data",
    "snapshot_prefix": "crypto_data",
    "snapshot_month": "2026-05",
    "build_mode": "incremental_from_current",
    "start_date": "2022-01-01",
    "end_date": "2026-04-01",
    "top_n": 50,
    "interval": "HOUR_4",
    "data_types": ["SPOT", "FUTURES", "FUNDING_RATES"],
    "exclude_symbols": ["LUNA", "FTT"],
    "exclude_tags": [
      "stablecoin",
      "usd-stablecoin",
      "wrapped-tokens",
      "tokenized-gold",
      "tokenized-commodities"
    ],
    "universe_frequency": "daily",
    "skip_existing_universe": true,
    "daily_quota": 200,
    "repair_gaps_via_api": false,
    "retention_months": 12,
    "quality_check": {
      "enabled": true,
      "output_file": "crypto_data_2026-05.quality.json"
    },
    "import_anomaly_file": "crypto_data_2026-05.import_anomalies.jsonl"
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
- `immutable`
- `db_file`
- `sha256`
- `size_bytes`
- `sidecars.metadata_file`
- `sidecars.quality_report_file` (nullable when validation is skipped)
- `sidecars.import_anomaly_file` (nullable when no anomaly file is produced)
- `build.mode`
- `build.base_snapshot` (nullable for the first snapshot or full rebuild)
- `build.config_compatibility`
- `source.git_commit`
- `source.git_branch`
- `source.command`
- `source.hostname`
- `config_raw`
- `config_effective`
- `validation.status`

`config_raw` must preserve the exact JSON config values used for the run.
`config_effective` must contain the resolved values actually passed to the
Python API, including the expanded `DEFAULT_UNIVERSE_EXCLUDE_TAGS` list.

## Error Handling

The workflow must fail without publishing a new `current.txt` when:

- the config JSON is invalid
- enum names cannot be resolved
- the target monthly release already exists
- `current.txt` exists but the referenced base snapshot is missing
- `current.txt` exists but the base snapshot metadata is missing or incompatible
  in incremental mode
- ingestion fails
- validation fails when enabled
- checksum generation fails
- the build DB publish step fails
- metadata generation fails

If the workflow fails after creating temporary files, the temporary files can be
left for debugging, but they must not be referenced by `current.txt`.

If a release filename already exists, the script must fail instead of overwriting
it.

## Retention

The default retention policy keeps the latest 12 monthly release snapshots.

Retention applies to groups of files sharing the same snapshot id:

```text
crypto_data_YYYY-MM.duckdb
crypto_data_YYYY-MM.sha256
crypto_data_YYYY-MM.metadata.json
crypto_data_YYYY-MM.import_anomalies.jsonl
crypto_data_YYYY-MM.quality.json
```

The currently referenced snapshot in `current.txt` must never be pruned.
The quality report is optional when `quality_check.enabled` is false, but if it
exists it must be pruned with the matching snapshot group.
The import anomaly sidecar is optional when no anomalies were recorded, but if it
exists it must be pruned with the matching snapshot group.

Temporary build files are not part of retention. They may be removed after a
successful publication. On failure, they may be left for debugging.

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
- incremental mode seeds the build DB from `current.txt`
- incompatible base snapshot config fails before ingestion
- `quality_check.enabled=false` records validation as skipped
- existing release filenames are not overwritten
- `current.txt` is not changed when publication fails before the final step
- retention keeps the current snapshot and prunes older snapshot groups

Manual acceptance checklist:

- ingestion writes to `/srv/crypto-data/build/crypto_data_YYYY-MM.tmp.duckdb`
- release DB appears in `/srv/crypto-data/releases`
- release DB is never overwritten by a later run
- `.sha256` validates against the release DB
- `.metadata.json` contains the exact config used
- `.metadata.json` contains the effective resolved config used by the API
- import anomalies, when present, are published next to the release DB
- `.metadata.json` records `base_snapshot` and `build_mode`
- `current.txt` contains only the release DB path relative to `root_dir`
- a consumer can `rsync` the release DB and open it with DuckDB
- rerunning while the lock is held fails without a second writer

## Acceptance Criteria

The V1 implementation is complete when:

- a config JSON can drive ingestion without editing Python constants
- each run builds in `/srv/crypto-data/build`
- published monthly DB files are immutable and never overwritten
- a release snapshot is published with checksum, metadata, and either a passed
  validation report or an explicit `validation.status=skipped`
- `current.txt` points to the latest published snapshot using a root-relative
  release DB path
- metadata records both raw config and effective config
- failures do not update `current.txt`
- old monthly releases are pruned according to `retention_months`
- the package ingestion API remains usable independently of the ops layer
