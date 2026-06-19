# Architecture

## Main Batch Flow

```mermaid
flowchart TD
    raw["Raw betting data landing area<br/>data/bets.csv"]
    trigger["Scheduler or job trigger<br/>validate / build-features"]

    subgraph validation["Validation batch process"]
        direction TD
        read["Read CSV in row batches<br/>--batch-size"]
        validate["BetValidationRowBatchWorker<br/>schema + business rules"]
        route["Customer-complete partition routing<br/>customer_id stays in one partition"]
        sequence["Partition sequence finalizer<br/>customer bet_num gap check"]
        valid["Curated validated bets<br/>valid_bets/part-*.parquet"]
        invalid["Invalid-record quarantine<br/>invalid_bets/part-*.parquet"]

        read --> validate --> route --> sequence
        sequence --> valid
        sequence --> invalid
    end

    subgraph features["Feature Engineering partition-based batch process"]
        direction TD
        feature_worker["BetFeaturePartitionWorker<br/>one worker per valid-bets partition"]
        feature_output["Versioned customer features<br/>customer_features/part-*.parquet"]

        feature_worker --> feature_output
    end

    publish["ACID-style publish<br/>_staging -> runs + _SUCCESS"]
    committed["Committed run<br/>outputs/runs/run_id"]
    consumers["Downstream consumers<br/>training / scoring / BI / CRM"]
    review["Operator review and source correction"]
    rerun["Rerun or backfill"]
    contracts["Schema, feature, and run configuration<br/>schema.py / features.py / CLI args"]
    observability["Logging, reports, metrics, and alerts<br/>validation + feature + publish health"]

    raw --> trigger --> read
    valid --> feature_worker
    valid --> publish
    invalid --> publish
    feature_output --> publish
    publish --> committed --> consumers
    invalid --> review --> rerun --> trigger
    contracts -. govern .-> validate
    contracts -. govern .-> feature_worker
    validate -. emit metrics .-> observability
    feature_worker -. emit metrics .-> observability
    publish -. emit status .-> observability
```

## Validation Checkpoint Reuse

```mermaid
flowchart TD
    checkpoint["Committed validation run<br/>outputs/runs/001<br/>_SUCCESS required"]
    loader["ValidationCheckpointLoader<br/>verify schema_version<br/>load valid_bets partitions"]
    feature_worker["Feature Engineering partition-based batch process"]
    feature_run["New feature run<br/>outputs/runs/001-features<br/>source_validation_run_id = 001"]

    checkpoint --> loader --> feature_worker --> feature_run
```

## Execution Model

The pipeline has two public commands:

1. `validate` reads raw CSV, validates row batches, writes customer-complete validation partitions, and commits a validation run.
2. `build-features --input ...` runs validation first, then builds customer features from the new valid-bets partitions.
3. `build-features --from-validation-run outputs/runs/<validation_run_id>` skips raw CSV validation and builds features from an already committed validation checkpoint.

The validation checkpoint path is the faster path for large inputs when validation has already completed. It requires `_SUCCESS`, checks that the validation `schema_version` matches the current code, records `source_validation_run_id` in the feature manifest, writes features into a new run id, and never mutates the committed validation run.

## Batch Boundaries

- Validation batch: `--batch-size` raw CSV rows read and validated at a time.
- Feature batch: one customer-complete `valid_bets/part-*.parquet` partition.
- `--validation-workers` can validate row batches concurrently.
- `--feature-workers` can process feature partitions concurrently.
- Partition routing keeps every `customer_id` in one valid-bets partition, so first-N customer features are complete.

## Production Controls

These controls are intentionally kept out of the main diagram so the data flow stays readable:

- Schema contracts live in `schema.py` and are versioned through `SCHEMA_VERSION`.
- Feature definitions live in `features.py` and are versioned through `FEATURE_SET_VERSION`.
- JSON reports and run manifests record row counts, failure counts, feature counts, schema version, feature version, output paths, and checkpoint lineage.
- Monitoring should alert on failed runs, missing `_SUCCESS`, invalid-rate spikes, empty feature output, missing partition files, or schema/version mismatches.
- Downstream systems should read only committed `outputs/runs/<run_id>/` paths with `_SUCCESS`.
