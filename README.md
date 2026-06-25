# Entain Bet Pipeline

Local, reproducible batch pipeline for validating raw betting records and producing customer-level features for downstream ML consumers.

## Pipeline Layers

The batch run is organized into three local data layers:

- **Bronze layer: Customer Completeness Partition** is a row-batch process that writes raw source rows to `raw/raw_bets/part-*.parquet`. Each `customer_id` is routed to one partition so later customer-level processing does not split a customer's history across workers.
- **Silver layer: Validated Bets and Invalid-Bet Quarantine** is a concurrency-compatible partition batch process. It reads bronze partitions, applies validation, writes curated `validation/valid_bets/part-*.parquet`, and writes rejected rows to `validation/invalid_bets/part-*.parquet`.
- **Gold layer: Customer Feature Dataset** is a concurrency-compatible partition batch process. It reads silver valid-bets partitions and writes customer-level ML features to `features/customer_features/part-*.parquet`.

Runs use ACID-style local publish: artifacts are written under `outputs/_staging/<run_id>/`, checked for completeness, marked with `_SUCCESS`, and then published to `outputs/runs/<run_id>/`. Consumers should only read committed runs with `_SUCCESS`.

For large data, `build-features --from-validation-run outputs/runs/<validation_run_id>` reuses a committed silver-layer validation checkpoint, skips raw CSV validation, writes a new gold-layer feature run, and records `source_validation_run_id` for lineage.

## Docker Quick Start

The Docker image installs dependencies with `uv sync --frozen` from `uv.lock`, so container builds use locked dependency versions instead of resolving open ranges from `pyproject.toml`.

Build:

```bash
docker build -t entain-bet-pipeline .
```

Validate raw bets:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline validate --input /data/bets.csv --output /outputs/
```

Validate and build features:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline build-features --input /data/bets.csv --output /outputs/
```

Build features from an existing validation checkpoint:

```bash
docker run --rm \
  -v $(pwd)/data:/data:ro \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline validate \
  --input /data/bets.csv \
  --output /outputs \
  --run-id 001

docker run --rm \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline build-features \
  --from-validation-run /outputs/runs/001 \
  --output /outputs \
  --run-id 001-features
```

The second command skips raw CSV validation, reads `/outputs/runs/001/validation/valid_bets/part-*.parquet`, writes a new feature run at `/outputs/runs/001-features`, and leaves the committed validation run unchanged.

Optional Docker flags:

| Option | Purpose |
| --- | --- |
| `--batch-size` | Raw CSV rows read per bronze row batch. |
| `--validation-workers` | Concurrent silver partition validation workers. |
| `--feature-workers` | Concurrent gold feature partition workers. |
| `--target-feature-partition-rows` | Approximate source rows per customer-complete feature partition. |
| `--feature-partition-count` | Exact number of customer-hash partitions; overrides target rows. |
| `--first-n-bets` | Feature window size by authoritative `bet_num`; defaults to 20. |

## Optional Local Development

Install:

```bash
uv sync
```

Run the same workflows locally:

```bash
uv run bet-pipeline validate --input data/bets.csv --output outputs/ --run-id local-validation

uv run bet-pipeline build-features --input data/bets.csv --output outputs/ --run-id local-features

uv run bet-pipeline build-features \
  --from-validation-run outputs/runs/local-validation \
  --output outputs/ \
  --run-id local-validation-features
```

The pipeline writes batch outputs as parquet with explicit schemas. Reports and run manifests are JSON.

## Outputs

Bronze raw partitioning writes:

```text
outputs/runs/<run_id>/raw/raw_bets/part-00000.parquet
```

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

`raw_bets/`, `valid_bets/`, `invalid_bets/`, and `customer_features/` are each one logical parquet dataset made from partition parquet files such as `part-00000.parquet` and `part-00001.parquet`. This lets partition processing scale without sharing one large writer.

The current committed sample run under `outputs/runs/run-2026-06-20T10-16-04Z/` contains eight partition files for each parquet dataset:

```text
raw/raw_bets/part-00000.parquet ... part-00007.parquet
validation/valid_bets/part-00000.parquet ... part-00007.parquet
validation/invalid_bets/part-00000.parquet ... part-00007.parquet
features/customer_features/part-00000.parquet ... part-00007.parquet
```

That run processed `372296` rows from `/data/bets.csv`: `371432` valid rows, `864` invalid rows, and `5000` customer feature rows. The validation report records failure counts by rule:

```text
betting_amount_gt_0: 835
return_for_entain_formula: 29
price_gt_1: 5
payout_formula: 3
```

The feature report records `customers_with_incomplete_first_n: 206`, `first_n_bets: 20`, `feature_partition_count: 8`, and `window_datetime_column: bet_20_datetime`.

## Validation Rules

The silver validation stage checks required columns, integer `bet_id`, UUID `customer_id`, parseable `bet_datetime`, positive `bet_num`, numeric parsing, business domains, `betting_amount`, `price`, `payout`, and `return_for_entain`.

It also checks the payout and `return_for_entain` formulas:

- `betting_amount > 0`
- `price > 1`
- `category in {"sports", "racing"}`
- `stake_type in {"cash", "bonus"}`
- `bet_result in {"return", "no-return"}`
- payout by `bet_result` and `stake_type`
- `return_for_entain` by `bet_result` and `stake_type`

Customer ordering is validated after bronze customer-complete partitioning. Duplicate `bet_id`, duplicate `(customer_id, bet_num)`, and raw `bet_num` sequence gaps are quarantined into `invalid_bets`. Invalid rows keep `source_row_number`, `validation_errors`, and `validated_at`; failure counts are written to `validation_report.json`.

## Customer Feature Dataset

The feature output has one row per `customer_id`. It is built from validated rows only, using each customer's first N bets by authoritative `bet_num`; the default N is 20.

| Feature | Aggregation | Why it is useful |
| --- | --- | --- |
| `first_bet_datetime` | Timestamp from the lowest valid `bet_num` in the first-N window | Supports cohorting and time-based joins. |
| `bet_<N>_datetime` | Timestamp where `bet_num == first_n_bets`, when present | Shows whether the customer reached the configured first-N window. |
| `bets_used` | Count of valid bets used in the first-N window | Makes missing or invalid first-N records visible. |
| `total_betting_amount` | Sum of `betting_amount` | Captures early stake volume. |
| `mean_betting_amount` | Average `betting_amount` | Captures typical stake size. |
| `mean_price` | Average decimal odds `price` | Summarizes early odds profile. |
| `pct_racing` | Share of first-N bets where `category == racing` | Encodes product preference. |
| `pct_cash` | Share of first-N bets where `stake_type == cash` | Distinguishes cash-funded and bonus-funded behavior. |
| `pct_return` | Share of first-N bets where `bet_result == return` | Captures early return frequency. |
| `total_payout` | Sum of validated `payout` | Captures customer payout outcome. |
| `total_return_for_entain` | Sum of `return_for_entain` | Captures early customer profitability from Entain's perspective. |
| `feature_generated_at` | Batch feature generation timestamp | Provides feature lineage. |

Sums capture total exposure and value, means normalize behavior across customers with different valid-row counts, and percentages convert categorical behavior into numeric model features.

## First-N Feature Policy

If invalid records appear inside a customer's configured first-N window:

1. Invalid records are written to `invalid_bets` with validation errors.
2. Feature generation uses only valid rows where `bet_num <= first_n_bets`.
3. Later valid bets are not pulled forward to replace invalid records.
4. `bets_used` can be less than `first_n_bets`.
5. The window datetime column is only populated when the valid `bet_num == first_n_bets` row exists.
6. The dynamic datetime column follows the configured window: `--first-n-bets 20` writes `bet_20_datetime`; `--first-n-bets 10` writes `bet_10_datetime`.
7. `feature_report.json` and `run_manifest.json` include `customers_with_incomplete_first_n`.

This keeps the feature window deterministic and avoids interpolating financial outcomes.

## AI-Assisted Production Engineering

AI was used as an engineering assistant to accelerate implementation while keeping production requirements explicit and testable. The author owned the architecture, class boundaries, functional behavior, and core design decisions: concurrency-compatible batch processing, customer-complete partitioning, bronze/silver/gold data layers, validation checkpoint reuse, deterministic first-N feature generation, Docker reproducibility, GitHub Actions CI, and ACID-style local publish through `_staging`, completeness checks, `_SUCCESS`, and committed `runs/<run_id>` outputs.

AI usage was limited to implementation support: drafting code from the author's design, generating small test cases, checking for edge cases, improving wording in documentation, and suggesting cleanup during review. The author reviewed, corrected, and accepted changes against the production requirements before they were kept.

All AI-assisted changes were validated through local linting, unit tests, Docker integration tests, and documented operating commands. The resulting pipeline does not depend on AI at runtime; it depends on explicit contracts, versioned artifacts, reproducible Docker builds, and test-covered behavior.

## Tests

Lint and formatting:

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

The integration test uses `tests/integrate/fixtures/bets.csv`, not the full `data/bets.csv`, so it runs quickly in CI while still exercising Docker build, validation, feature generation, ACID-style publish, and artifact checks.

## Project Layout

```text
pyproject.toml
Dockerfile
README.md
src/bet_pipeline/
  __init__.py
  batch_process/
    __init__.py
    run_artifacts.py
    raw_partition_batch.py
    validation_batch.py
    feature_batch.py
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
