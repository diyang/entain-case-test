# Architecture

## Main Batch Flow

```mermaid
flowchart TD
    raw["Raw betting data landing area<br/>data/bets.csv"]
    trigger["Scheduler or job trigger<br/>validate / build-features"]

    subgraph bronze["Bronze&nbsp;layer:&nbsp;Customer&nbsp;Completeness&nbsp;Partition"]
        direction TD
        read["Read CSV in row batches<br/>--batch-size"]
        route["RawBetRowBatchWorker<br/>route by customer_id"]
        raw_parts["raw_bets/part-*.parquet<br/>one customer stays in one partition"]

        read --> route --> raw_parts
    end

    subgraph silver["Silver&nbsp;layer:&nbsp;Validated&nbsp;Bets&nbsp;and&nbsp;Invalid-Bet&nbsp;Quarantine"]
        direction TD
        validate["BetValidationPartitionWorker<br/>validate one raw partition"]
        valid["valid_bets/part-*.parquet"]
        invalid["invalid_bets/part-*.parquet"]

        validate --> valid
        validate --> invalid
    end

    subgraph gold["Gold&nbsp;layer:&nbsp;Customer&nbsp;Feature&nbsp;Dataset"]
        direction TD
        feature_worker["BetFeaturePartitionWorker<br/>one worker per valid partition"]
        feature_output["customer_features/part-*.parquet"]

        feature_worker --> feature_output
    end

    publish["ACID-style publish<br/>_staging -> runs + _SUCCESS"]
    committed["Committed run<br/>outputs/runs/run_id"]
    consumers["Downstream consumers<br/>training / scoring / BI / CRM"]
    review["Operator review and source correction"]
    rerun["Rerun or backfill"]

    raw --> trigger --> read
    raw_parts --> validate
    valid --> feature_worker
    valid --> publish
    invalid --> publish
    feature_output --> publish
    publish --> committed --> consumers
    invalid --> review --> rerun --> trigger
```

## Validation Checkpoint Reuse

```mermaid
flowchart TD
    checkpoint["Committed validation run<br/>outputs/runs/001<br/>_SUCCESS required"]
    loader["ValidationCheckpointLoader<br/>load valid_bets partitions"]
    feature_worker["Gold layer<br/>feature partition workers"]
    feature_run["New feature run<br/>outputs/runs/001-features<br/>source_validation_run_id = 001"]

    checkpoint --> loader --> feature_worker --> feature_run
```

## Execution Model

The pipeline has two public commands:

1. `validate` reads raw CSV in row batches, writes customer-complete raw partitions, validates each raw partition, writes valid/invalid partitions, and commits a validation run.
2. `build-features --input ...` runs the same raw partition and validation stages first, then builds customer features from the new valid-bets partitions.
3. `build-features --from-validation-run outputs/runs/<validation_run_id>` skips raw CSV validation and builds features from an already committed validation checkpoint.

The validation checkpoint path is the faster path for large inputs when validation has already completed. It requires `_SUCCESS`, records `source_validation_run_id` in the feature manifest, writes features into a new run id, and never mutates the committed validation run.

## Batch Boundaries

- Raw partition batch: `--batch-size` raw CSV rows read and routed at a time.
- Validation batch: one customer-complete `raw_bets/part-*.parquet` partition.
- Feature batch: one customer-complete `valid_bets/part-*.parquet` partition.
- `--validation-workers` can validate raw partitions concurrently.
- `--feature-workers` can process feature partitions concurrently.
- Partition routing keeps every `customer_id` in one valid-bets partition, so first-N customer features are complete.
