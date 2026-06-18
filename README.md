# Entain Bet Pipeline

Local, reproducible batch pipeline for validating betting records and producing customer-level features for downstream ML consumers.

## Project Layout

```text
pyproject.toml
Dockerfile
README.md
src/bet_pipeline/
  __init__.py
  cli.py
  schema.py
  validation.py
  features.py
  io.py
tests/
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

This writes validation outputs, feature outputs, and `outputs/run_manifest.json` with run lineage.

Validate raw bets only:

```bash
uv run bet-pipeline validate --input data/bets.csv --output outputs/validation/
```

Feature engineering is a batch phase inside the public `build-features` command. A full feature batch always validates first, writes curated `valid_bets.parquet`, and then builds features from that curated output.

## Outputs

Validation writes:

```text
outputs/validation/valid_bets.parquet
outputs/validation/invalid_bets.parquet
outputs/validation/validation_report.json
```

Feature generation writes:

```text
outputs/features/customer_features.parquet
outputs/features/feature_report.json
```

Full batch runs also write:

```text
outputs/run_manifest.json
```

## Tests

Lint and formatting checks:

```bash
uv run ruff format --check src tests
uv run ruff check src tests
```

Unit tests:

```bash
uv run python -m unittest discover tests
```

Coverage:

```bash
uv run python -m coverage run -m unittest discover tests
uv run python -m coverage report -m
```

## Docker

Build:

```bash
docker build -t entain-bet-pipeline .
```

Feature batch:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline build-features --input /data/bets.csv --output /outputs --run-id local-001
```

Validate only:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/outputs:/outputs \
  entain-bet-pipeline validate --input /data/bets.csv --output /outputs/validation/
```

## Validation Rules

The validation job checks required columns, integer `bet_id`, UUID `customer_id`, parseable `bet_datetime`, uniqueness of `bet_id`, per-customer uniqueness, positivity, and sequence of `bet_num`, numeric parsing, business domains, `payout`, and `return_for_entain`.

Invalid rows are written to parquet quarantine with `source_row_number`, `validation_errors`, and `validated_at`. Failure counts are also written to `validation_report.json`.

## First-20 Feature Policy

The pipeline treats `bet_num` as the authoritative order. If invalid records appear within a customer's first 20 bets, the batch handles them as follows:

1. The invalid records are written to `invalid_bets.parquet` with validation errors.
2. Feature generation continues for that customer using the remaining valid records where `bet_num <= 20`.
3. Later valid bets, such as `bet_num == 21`, are not pulled forward to replace invalid first-20 bets.
4. The feature row shows the effect through `bets_used`; it can be less than 20.
5. `twentieth_bet_datetime` is only populated when the valid `bet_num == 20` row exists.
6. `feature_report.json` and `run_manifest.json` include `customers_with_incomplete_first_20` so operators can see how often this happened.

This keeps the feature window deterministic for the same source data and avoids silently changing the definition of "first 20 bets."
