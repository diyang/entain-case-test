from __future__ import annotations

import argparse

from bet_pipeline.batch import (
    DEFAULT_BATCH_ROWS,
    FIRST_N_BETS,
    BetFeatureBatchProcess,
    BetValidationBatchProcess,
    RunArtifactPublisher,
    ValidationBatchSettings,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bet-pipeline", description="Local betting-data batch pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    batch_source_parser = argparse.ArgumentParser(add_help=False)
    batch_source_parser.add_argument("--input", required=True, help="Path to input bets CSV")
    batch_source_parser.add_argument("--output", required=True, help="Directory for batch outputs")
    batch_source_parser.add_argument("--run-id", help="Optional stable run id for lineage and backfills")
    batch_source_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_ROWS,
        help=f"Rows to process per internal validation batch. Default: {DEFAULT_BATCH_ROWS}",
    )
    batch_source_parser.add_argument(
        "--validation-workers",
        type=int,
        default=1,
        help="Concurrent row-batch validation workers. Partition writes remain ordered. Default: 1",
    )
    batch_source_parser.add_argument(
        "--target-feature-partition-rows",
        type=int,
        default=DEFAULT_BATCH_ROWS,
        help=f"Approximate source rows per feature partition. Default: {DEFAULT_BATCH_ROWS}",
    )
    batch_source_parser.add_argument(
        "--feature-partition-count",
        type=int,
        help=(
            "Exact number of customer-hash feature partitions to process. "
            "Overrides --target-feature-partition-rows when supplied."
        ),
    )

    subparsers.add_parser("validate", parents=[batch_source_parser], help="Validate raw betting records")

    features_parser = subparsers.add_parser(
        "build-features",
        parents=[batch_source_parser],
        help="Validate raw bets and build customer features",
    )
    features_parser.add_argument(
        "--first-n-bets",
        type=int,
        default=FIRST_N_BETS,
        help=f"Feature window size by bet_num. Default: {FIRST_N_BETS}",
    )
    features_parser.add_argument(
        "--feature-workers",
        type=int,
        default=1,
        help="Concurrent customer feature partition workers. Default: 1",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command in {"validate", "build-features"}:
        artifact_publisher = RunArtifactPublisher(args.output, args.run_id)
        try:
            validation_batch_process = BetValidationBatchProcess()
            partitioned_input = validation_batch_process.process(
                args.input,
                artifact_publisher.validation_dir,
                ValidationBatchSettings(
                    batch_size=args.batch_size,
                    validation_worker_count=args.validation_workers,
                    feature_partition_count=args.feature_partition_count,
                    target_feature_partition_rows=args.target_feature_partition_rows,
                    generated_at=artifact_publisher.validation_generated_at,
                ),
            )
            report = artifact_publisher.write_validation_report(partitioned_input)

            if args.command == "validate":
                artifact_publisher.write_manifest(partitioned_input, report)
                artifact_publisher.commit(partitioned_input)
                print(
                    f"Validated {report['total_rows']} rows: "
                    f"{report['valid_rows']} valid, {report['invalid_rows']} invalid"
                )
                return 0

            feature_batch_process = BetFeatureBatchProcess()
            feature_result = feature_batch_process.process(
                partitioned_input,
                artifact_publisher.features_dir,
                artifact_publisher.feature_generated_at,
                first_n_bets=args.first_n_bets,
                feature_worker_count=args.feature_workers,
            )
            artifact_publisher.write_feature_report(partitioned_input, report, feature_result)
            manifest = artifact_publisher.write_manifest(partitioned_input, report, feature_result)
            artifact_publisher.commit(partitioned_input)

        except Exception:
            artifact_publisher.abort()
            raise

        print(f"Validated {report['total_rows']} rows: {report['valid_rows']} valid, {report['invalid_rows']} invalid")
        print(
            f"Completed feature batch {manifest['run_id']}: "
            f"{manifest['validation']['valid_rows']} valid rows, "
            f"{manifest['validation']['invalid_rows']} invalid rows, "
            f"{manifest['features']['customers']} customers"
        )
        return 0
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
