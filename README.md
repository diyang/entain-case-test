# Entain Bet Pipeline

Local, reproducible batch pipeline for validating betting records and producing customer-level features for downstream ML consumers.

## Project Layout

```text
pyproject.toml
Dockerfile
README.md
src/bet_pipeline/
  __init__.py
  batch.py
  main.py
  schema.py
  validation.py
  features.py
  io.py
tests/
  unit/
  integrate/
outputs/
docs/
  architecture.md
  design_note.md
```

## Install

```bash
uv sync
```

The pipeline writes batch outputs as parquet with explicit schemas. Reports and run manifests are JSON.

## Run

Recommended single-command feature batch:

```bash
uv run bet-pipeline build-features --input data/bets.csv --output outputs/ --run-id local-001
```

This writes validation outputs, feature outputs, and run lineage under `outputs/runs/local-001/`.

You can set the internal row batch size:

```bash
uv run bet-pipeline build-features --input data/bets.csv --output outputs/ --run-id local-001 --batch-size 1000
```

Local execution defaults to one validation worker. You can enable concurrent row-batch validation while keeping partition writes deterministic:

```bash
uv run bet-pipeline build-features --input data/bets.csv --output outputs/ --run-id local-001 --validation-workers 4
```

Local execution also defaults to one feature worker. You can enable concurrent customer-complete feature partition processing:

```bash
uv run bet-pipeline build-features --input data/bets.csv --output outputs/ --run-id local-001 --feature-workers 4
```

You can also set the exact number of customer-hash feature partitions:

```bash
uv run bet-pipeline build-features --input data/bets.csv --output outputs/ --run-id local-001 --feature-partition-count 8
```

Or let the validation pass create customer-complete feature partitions from an approximate target rows-per-feature-partition:

```bash
uv run bet-pipeline build-features --input data/bets.csv --output outputs/ --run-id local-001 --target-feature-partition-rows 1000
```

Validate raw bets only:

```bash
uv run bet-pipeline validate --input data/bets.csv --output outputs/ --run-id local-001
```

Feature engineering is a batch phase inside the public `build-features` command. `RunArtifactPublisher` owns staging, reports, manifest, and commit. During source validation, `BetValidationBatchProcess` reads the CSV in row batches and creates a `BetValidationRowBatchWorker` for each row batch. Each worker applies row-level validation immediately. The validation process then routes valid and invalid rows through `FeaturePartitionRouter` and writes customer-complete parquet partitions in source-row-batch order. For feature runs, `BetFeatureBatchProcess` iterates feature partitions and creates a `BetFeaturePartitionWorker` for each partition to build and write features.

`--batch-size` controls how many source rows are read and validated at a time. It is not a feature boundary. `--validation-workers` defaults to 1 and can be increased for concurrent row-batch validation; partition routing and parquet writes are still coordinated in source-row-batch order to keep output deterministic. `--feature-workers` defaults to 1 and can be increased for concurrent customer-complete feature partition processing; each worker writes a distinct `customer_features/part-*.parquet` file. `--target-feature-partition-rows` is the main sizing input for feature partitions and defaults to the same row-count constant as `--batch-size` (`DEFAULT_BATCH_ROWS = 1000`), so validation row batches and feature partitions are roughly aligned by default. If you change `--batch-size` and want feature partitions to track it, set `--target-feature-partition-rows` to the same value. `--feature-partition-count` controls the exact number of customer-hash feature partitions and overrides the dynamic target. All rows for the same `customer_id` go to the same partition, so customer features are not split across row batches. `--first-n-bets` controls the feature window size and defaults to 20 for the interview task.

## Outputs

Validation writes:

```text
outputs/runs/<run_id>/validation/valid_bets/part-00000.parquet
outputs/runs/<run_id>/validation/invalid_bets/part-00000.parquet
outputs/runs/<run_id>/validation/validation_report.json
```

Feature generation writes:

```text
outputs/runs/<run_id>/features/customer_features/part-00000.parquet
outputs/runs/<run_id>/features/feature_report.json
```

Full batch runs also write:

```text
outputs/runs/<run_id>/run_manifest.json
outputs/runs/<run_id>/_SUCCESS
```

The pipeline writes all run artifacts to `outputs/_staging/<run_id>/` first. `valid_bets/`, `invalid_bets/`, and `customer_features/` are each one logical parquet dataset made from partition parquet files such as `part-00000.parquet` and `part-00001.parquet`. That lets partition processing scale without sharing one large writer. The run commits by validating expected partition files, writing `_SUCCESS`, and atomically publishing the staged directory to `outputs/runs/<run_id>/`. Consumers should only read runs with `_SUCCESS`.

## Tests

Lint and formatting checks:

```bash
uv run ruff format --check src tests
uv run ruff check src tests
```

Unit tests:

```bash
uv run python -m unittest discover tests/unit
```

Coverage:

```bash
uv run python -m coverage run -m unittest discover tests/unit
uv run python -m coverage report -m
```

Docker integration test:

```bash
uv run pytest tests/integrate -v
```

The integration test uses `tests/integrate/fixtures/bets.csv`, not the full `data/bets.csv`, so it can run quickly in CI while still exercising Docker build, validation, feature generation, ACID-style publish, and artifact checks.

## Docker

Build:

```bash
docker build -t entain-bet-pipeline .
```

Validate only:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline validate --input /data/bets.csv --output /outputs/validation/
```

Feature batch:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline build-features --input /data/bets.csv --output /outputs/features/
```

## Validation Rules

The validation job checks required columns, integer `bet_id`, UUID `customer_id`, parseable `bet_datetime`, uniqueness of `bet_id`, per-customer uniqueness, positivity, and sequence of `bet_num`, numeric parsing, business domains, `payout`, and `return_for_entain`.

Invalid rows are written to parquet quarantine with `source_row_number`, `validation_errors`, and `validated_at`. Failure counts are also written to `validation_report.json`.

## Customer Feature Dataset

The feature output has one row per `customer_id`. It is built from validated rows only, using each customer's first N bets by authoritative `bet_num`; the default N is 20.

The selected features summarize early customer betting behavior:

| Feature | Aggregation | Why it is useful |
| --- | --- | --- |
| `first_bet_datetime` | Timestamp from the lowest valid `bet_num` in the first-N window | Gives the start of the observed customer history and supports time-based joins or cohorting. |
| `nth_bet_datetime` | Timestamp where `bet_num == first_n_bets`, when present | Shows whether the customer reached the full first-N window and when that early window completed. |
| `bets_used` | Count of valid bets used in the first-N window | Makes missing or invalid first-N records visible to downstream models. |
| `total_betting_amount` | Sum of `betting_amount` | Captures early customer stake volume. |
| `mean_betting_amount` | Average `betting_amount` | Captures typical stake size without letting customers with more available valid rows dominate only through volume. |
| `mean_price` | Average decimal odds `price` | Summarizes early risk appetite or bet profile. |
| `pct_racing` | Share of first-N bets where `category == racing` | Encodes product preference between racing and sports. |
| `pct_cash` | Share of first-N bets where `stake_type == cash` | Distinguishes real-cash funded behavior from bonus-funded behavior. |
| `pct_return` | Share of first-N bets where `bet_result == return` | Captures early win/return frequency. |
| `total_payout` | Sum of validated `payout` | Captures customer cash-back outcome over the early window. |
| `total_return_for_entain` | Sum of `return_for_entain` | Captures early customer profitability from Entain's perspective. |
| `feature_generated_at` | Batch feature generation timestamp | Provides feature lineage and supports reproducibility checks. |

These are deliberately simple, auditable aggregations. Sums capture total exposure and value, means normalize behavior across customers with different valid row counts, and percentages convert categorical behavior into stable numeric features that batch training, scoring, BI, CRM, or decisioning systems can consume.

## First-N Feature Policy

The pipeline treats `bet_num` as the authoritative order. The default feature window is the first 20 bets, and it can be changed with `--first-n-bets`. If invalid records appear inside a customer's configured first-N window, the batch handles them as follows:

1. The invalid records are written to the `invalid_bets` parquet partition with validation errors.
2. Feature generation continues for that customer using the remaining valid records where `bet_num <= first_n_bets`.
3. Later valid bets, such as `bet_num == first_n_bets + 1`, are not pulled forward to replace invalid records inside the feature window.
4. The feature row shows the effect through `bets_used`; it can be less than `first_n_bets`.
5. `nth_bet_datetime` is only populated when the valid `bet_num == first_n_bets` row exists.
6. `feature_report.json` and `run_manifest.json` include `customers_with_incomplete_first_n` so operators can see how often this happened.

This keeps the feature window deterministic for the same source data and avoids silently changing the definition of the first-N window.
