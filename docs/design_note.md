# Design Note

## Purpose

This package builds a production-style batch pipeline for downstream machine-learning consumers of betting data. It validates raw betting records, quarantines invalid rows, writes a curated validated-bets layer, and builds customer-level features from each customer's first N valid bets by authoritative `bet_num`.

The architecture diagram is kept in [architecture.md](architecture.md). This note explains the responsibilities, contracts, and operating behavior behind that diagram.

## Public Workflows

The CLI supports three production workflows:

| Workflow | Command shape | What it does |
| --- | --- | --- |
| Validation only | `bet-pipeline validate --input data/bets.csv --output outputs --run-id 001` | Reads the raw CSV in row batches, writes customer-complete raw partitions, validates each raw partition, writes valid and invalid parquet partitions, and commits a validation run. |
| Validate and build features | `bet-pipeline build-features --input data/bets.csv --output outputs --run-id 001` | Runs raw partitioning and validation first, then builds customer features from the newly created valid-bets partitions. |
| Build from validation checkpoint | `bet-pipeline build-features --from-validation-run outputs/runs/001 --output outputs --run-id 001-features` | Reuses a committed validation run, skips raw CSV validation, and writes a new immutable feature run. |

The checkpoint path is important for large inputs. If validation has already succeeded, feature generation should not reread the raw CSV, rerun validation, or rewrite `valid_bets` and `invalid_bets`.

A typical checkpoint reuse sequence is:

```bash
bet-pipeline validate \
  --input data/bets.csv \
  --output outputs \
  --run-id 001

bet-pipeline build-features \
  --from-validation-run outputs/runs/001 \
  --output outputs \
  --run-id 001-features
```

## Components

The pipeline uses three data layers:

- **Bronze layer: Customer Completeness Partition** stores raw rows in customer-complete parquet partitions. It preserves source records while making sure all rows for the same `customer_id` stay together for later customer-level work.
- **Silver layer: Validated Bets and Invalid-Bet Quarantine** validates each customer-complete raw partition, writes curated valid bets, and isolates invalid records for review.
- **Gold layer: Customer Feature Dataset** builds one customer-level ML feature dataset from the silver valid-bets partitions.

| Component | Responsibility | Data in | Data out |
| --- | --- | --- | --- |
| Raw betting landing area | Holds the immutable source extract for a run. | Raw CSV such as `data/bets.csv` | CSV rows for validation |
| Scheduler or job trigger | Starts `validate` or `build-features` with input paths, run id, and sizing parameters. | Schedule, manual request, rerun, or backfill request | CLI invocation |
| Bronze: `RawBetCustomerCompletePartitionBatchProcess` | Streams raw CSV row batches into customer-complete parquet partitions. | Raw CSV path and partition settings | Raw parquet partition metadata |
| Bronze: `RawBetRowBatchWorker` | Handles one raw CSV row batch end to end for raw partitioning. | Up to `--batch-size` CSV rows | Customer-complete `raw_bets/part-*.parquet` writes |
| Bronze: Customer-complete partition routing | Keeps every `customer_id` in one partition while raw CSV streams through row batches. | Raw row-batch outputs | `raw_bets/part-*.parquet` |
| Silver: `BetValidationPartitionBatchProcess` | Orchestrates validation over customer-complete raw partitions. | Raw parquet partitions and `ValidationBatchSettings` | `PartitionedInput` metadata plus valid/invalid parquet parts |
| Silver: `BetValidationPartitionWorker` | Validates one customer-complete raw partition, applies partition-level uniqueness and sequence checks, and writes valid/invalid parquet parts. | One `raw_bets/part-*.parquet` file | Matching `valid_bets/part-*.parquet` and `invalid_bets/part-*.parquet` |
| Silver: Invalid-record quarantine | Preserves rejected rows for review. | Invalid rows plus validation errors | `validation/invalid_bets/part-*.parquet` |
| `ValidationCheckpointLoader` | Loads a committed validation run for feature generation. | `outputs/runs/<validation_run_id>` | Existing valid-bets partitions and validation report |
| Gold: `BetFeaturePartitionBatchProcess` | Orchestrates feature engineering over valid-bets partitions. | Customer-complete `valid_bets/part-*.parquet` files | Feature count and first-N completeness metrics |
| Gold: `BetFeaturePartitionWorker` | Builds features for one customer-complete valid-bets partition. | One valid-bets parquet part | Matching `customer_features/part-*.parquet` |
| `RunArtifactPublisher` | Owns staging, manifest/report writes, completeness checks, and publish. | Staged validation and/or feature artifacts | Committed `outputs/runs/<run_id>` with `_SUCCESS` |
| Downstream consumers | Read committed feature runs. | `customer_features/part-*.parquet` plus reports/manifest | Training data, scoring input, BI/CRM/decisioning data |

## Batch Model

There are two different batch boundaries:

- **Raw partition row batch:** `--batch-size` raw CSV rows read and routed at a time.
- **Validation partition batch:** one customer-complete `raw_bets/part-*.parquet` partition.
- **Feature batch:** one customer-complete `valid_bets/part-*.parquet` partition.

Raw partition row batches control CSV memory usage. They are not feature boundaries. A customer can appear in many raw row batches, so feature completeness is enforced by customer partitioning, not by CSV read chunks.

Feature partitions are customer-complete. Every row for the same `customer_id` is routed to the same partition, so a feature worker can build the first-N feature row without needing records from another partition.

Concurrency is compatible with this model:

- `--validation-workers` can validate multiple customer-complete raw partitions concurrently.
- `--feature-workers` can process multiple valid-bets partitions concurrently. Each worker writes a distinct feature parquet part, so workers do not share output writers.

## Why Batch

The required output is a reproducible customer-level dataset derived from historical first-N betting behavior. Batch is the right default because training, batch scoring, BI, and CRM activation need stable snapshots with lineage, schema versions, feature versions, and rerun behavior.

Streaming could be useful for real-time risk decisions or live customer activation, but it would need stateful per-customer storage, late-event handling, watermarks, and strict online/offline feature consistency. This implementation is intentionally a bounded batch pipeline. It can read a large CSV as a stream of row batches, but it is not a streaming event pipeline.

## Validation And Schema Safety

Validation enforces the row-level rules from the task:

- `betting_amount > 0`
- `price > 1`
- `category` is `sports` or `racing`
- `stake_type` is `cash` or `bonus`
- `bet_result` is `return` or `no-return`
- payout formula by `bet_result` and `stake_type`
- `return_for_entain` formula by `bet_result` and `stake_type`
- parseable identifiers, timestamps, and numeric values
- duplicate `bet_id` values across the source
- duplicate `(customer_id, bet_num)` values inside customer-complete partitions

Curated outputs use explicit Arrow schemas from `schema.py`. Validation reports include `schema_version`; feature reports include `feature_set_version`, `feature_columns`, and `first_n_bets`. Breaking schema or feature changes should create a new version instead of changing existing semantics in place.

The raw partition stage keeps source-level `bet_id` lineage, so later duplicate `bet_id` rows can be quarantined during partition validation. After customer-complete partition files are written, each validation partition worker checks the raw observed `bet_num` sequence for each customer using both valid and invalid rows in that partition. If a customer's raw sequence has a gap, such as `1, 2, 4`, valid rows for that affected customer are demoted into `invalid_bets` with `customer_bet_num_sequence` before the run is published. Each worker reads one customer-complete partition at a time, so it enforces the rule without loading the full source file into memory.

## Invalid Records

Invalid records are isolated under `validation/invalid_bets/`. They are not used for feature generation and are not silently interpolated.

Each invalid row keeps the original values plus `source_row_number`, `validation_errors`, and `validated_at`. Operators can inspect `validation_report.json`, correct upstream data, then rerun or backfill.

If invalid rows appear inside a customer's first-N window, behavior is deterministic:

1. Invalid rows are quarantined.
2. Feature generation uses only valid rows where `bet_num <= first_n_bets`.
3. Later valid bets are not pulled forward to fill gaps.
4. `bets_used` can be less than `first_n_bets`.
5. The window datetime column is present only when the valid `bet_num == first_n_bets` row exists.
6. The window datetime column name follows the configured window: default `first_n_bets = 20` writes `bet_20_datetime`; `first_n_bets = 10` writes `bet_10_datetime`.
7. `customers_with_incomplete_first_n` is reported.

This avoids inventing or interpolating financial outcomes.

## Feature Definitions

The feature output has one row per `customer_id`. It is built from validated rows only, using each customer's first N bets by authoritative `bet_num`; default N is 20.

| Feature group | Fields | Why it is useful |
| --- | --- | --- |
| Window lineage | `first_bet_datetime`, `bet_<N>_datetime`, `bets_used`, `feature_generated_at` | Shows whether the configured first-N window is complete and when features were produced. With the default first-N value, the dynamic column is `bet_20_datetime`; for first 10 it is `bet_10_datetime`. |
| Stake behavior | `total_betting_amount`, `mean_betting_amount` | Captures early stake volume and typical stake size. |
| Price behavior | `mean_price` | Summarizes early odds profile, which can proxy for betting style or risk preference. |
| Product and funding mix | `pct_racing`, `pct_cash` | Converts categorical behavior into stable numeric features for model consumers. |
| Outcome and value | `pct_return`, `total_payout`, `total_return_for_entain` | Captures early return frequency, payout experience, and value to Entain. |

Sums are used where total exposure matters. Means are used where typical behavior should be comparable across customers with different valid-row counts. Percentages are used for categorical fields so downstream systems receive numeric features.

## Checkpoint Reuse

`build-features --from-validation-run` is the checkpoint pickup path.

It exists to save time on large inputs. Validation can be the expensive part of the workflow because it reads the raw file, parses every row, applies schema and business rules, quarantines invalid records, and writes customer-complete valid/invalid parquet partitions. Once that stage has produced a committed validation run, feature generation can start from the validated parquet checkpoint instead of doing the same validation work again.

It is allowed only when:

- the validation run directory has `_SUCCESS`
- `validation/validation_report.json` exists
- `validation/valid_bets/part-*.parquet` exists for every expected feature partition
- `validation/invalid_bets/part-*.parquet` exists for every expected feature partition
- the validation report `schema_version` matches the current `SCHEMA_VERSION`

When reuse is enabled, the pipeline skips:

- raw CSV reading
- raw partition validation
- payout and `return_for_entain` formula checks
- invalid-record quarantine creation
- customer-complete valid/invalid partition writing

It still runs:

- validation checkpoint safety checks
- feature generation over the existing `valid_bets/part-*.parquet`
- feature report creation
- run manifest creation
- ACID-style staging and publish for the new feature run

The feature run writes a new run id such as `001-features`. Its manifest and feature report record:

- `reused_validation_checkpoint: true`
- `source_validation_run_id: 001`
- validation output paths pointing back to the committed validation run

The feature run does not copy validation artifacts into its own run directory. Its `outputs.validation_dir`, `outputs.valid_bets_dir`, and `outputs.invalid_bets_dir` point back to the committed validation checkpoint. The committed validation run is never mutated. This preserves immutability and avoids mixing feature publication state into a validation checkpoint.

Using the same run id for both validation and feature generation is intentionally avoided. A committed validation run should remain immutable. A feature run should use a new run id and reference the validation checkpoint through lineage metadata.

## Reruns, Backfills, And ACID

Runs write to `outputs/_staging/<run_id>/` first. Consumers should never read staging paths.

For a normal validation or raw feature run, staged artifacts include validation outputs, reports, manifest, and optional feature outputs. For a checkpoint-based feature run, staged artifacts include feature outputs, manifest, and a lineage reference to the existing validation checkpoint.

Before commit, `RunArtifactPublisher` checks required files and partition parts. It writes `_SUCCESS`, then publishes the staged directory to `outputs/runs/<run_id>`.

Local ACID-style behavior:

- **Atomicity:** failed runs are removed from staging; successful runs publish only after required artifacts exist.
- **Consistency:** schemas, reports, manifest, and partition files are checked before commit.
- **Isolation:** consumers read only committed run directories with `_SUCCESS`.
- **Durability:** after the filesystem publish succeeds, committed artifacts remain under `runs/<run_id>`.

Backfills and corrections should create new immutable runs. Reusing a run id should be reserved for controlled reruns where the old staged or committed path has been intentionally removed.

Object stores do not usually provide atomic directory rename. A production cloud implementation should use immutable part files plus a transaction manifest, or a table format such as Iceberg, Delta, or Hudi.

## Downstream Consumption

Downstream consumers should read only committed feature runs that contain `_SUCCESS`.

- Batch training records `run_id`, `schema_version`, and `feature_set_version` with the model artifact.
- Batch scoring checks the expected feature-set version before scoring.
- BI, CRM, and decisioning systems consume committed feature parquet or a promoted serving table derived from it.

The contract for consumers is parquet schema plus `feature_report.json` plus `run_manifest.json`. File names alone are not enough.

## Monitoring And Alerts

Production monitoring should capture:

- total rows, valid rows, invalid rows, and invalid-rate percentage
- failure counts by validation rule
- validation batch count and batch size
- feature partition count and output part count
- feature row count
- customers with incomplete first-N windows
- runtime by validation, checkpoint load, feature generation, and publish stage
- missing part files or missing `_SUCCESS`
- schema version and feature-set version

Alerts should fire on missing required columns, invalid-rate spikes, empty feature output, missing partition files, failed publish, unexpected schema or feature version, or consumers attempting to read uncommitted paths.

## Tests

The repository has three automated check layers:

- **Lint and formatting:** `.github/scripts/run_lint.sh` runs Ruff formatting checks and lint rules against `src` and `tests`.
- **Unit tests:** `.github/scripts/run_tests.sh` runs `unittest` discovery against `tests/unit/`.
- **Docker E2E integration:** `tests/integrate/test_docker_pipeline.py` builds the Docker image, runs the pipeline end to end, checks ACID-style publish behavior, verifies parquet outputs, and covers checkpoint reuse from a committed validation run.

GitHub Actions separates lint, unit tests, and E2E integration tests. CI uploads logs and generated integration outputs as artifacts for review.

## Trade-Offs And Assumptions

The implementation is local and vendor-neutral. It uses parquet for typed batch outputs and JSON for small operational reports.

Dynamic target-row partitioning avoids a full-file row-count pass, but partition count depends on source order and customer skew. If stable partition numbering is required across reruns, use `--feature-partition-count`.

Feature generation reads one valid-bets partition into a worker. That is acceptable when partitions are sized correctly. For very large or skewed customers, production should tune partition settings, increase worker resources, or use a distributed engine that can keep per-customer state safely.
