from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TextIO

from bet_pipeline.batch_process.feature_batch import BetFeaturePartitionBatchProcess
from bet_pipeline.batch_process.raw_partition_batch import (
    CustomerCompletePartitionSettings,
    RawBetCustomerCompletePartitionBatchProcess,
)
from bet_pipeline.batch_process.run_artifacts import (
    DEFAULT_BATCH_ROWS,
    FIRST_N_BETS,
    RunArtifactPublisher,
    ValidationCheckpointLoader,
)
from bet_pipeline.batch_process.validation_batch import BetValidationPartitionBatchProcess, ValidationBatchSettings


class RunSummaryPrinter:
    """Print a compact operational summary for a committed batch run."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self.stream = sys.stdout if stream is None else stream

    def print_validation_summary(self, manifest: dict) -> None:
        self._print_run_header(manifest)
        self._print_validation(manifest)
        self._print_outputs(manifest, include_features=False)

    def print_feature_summary(self, manifest: dict) -> None:
        self._print_run_header(manifest)
        self._print_validation(manifest)
        self._print_features(manifest)
        self._print_outputs(manifest, include_features=True)

    def _print_run_header(self, manifest: dict) -> None:
        self._write("Batch run completed")
        self._write(f"  run_id: {manifest.get('run_id', 'unknown')}")
        self._write(f"  status: {manifest.get('status', 'unknown')}")
        self._write(f"  input: {manifest.get('input_path', 'unknown')}")
        if manifest.get("reused_validation_checkpoint"):
            self._write(f"  source_validation_run_id: {manifest.get('source_validation_run_id', 'unknown')}")
        self._write(f"  started_at: {manifest.get('started_at', 'unknown')}")
        self._write(f"  finished_at: {manifest.get('finished_at', 'unknown')}")

    def _print_validation(self, manifest: dict) -> None:
        validation = manifest.get("validation", {})
        total_rows = self._int_value(validation.get("total_rows"))
        valid_rows = self._int_value(validation.get("valid_rows"))
        invalid_rows = self._int_value(validation.get("invalid_rows"))

        self._write("Validation")
        self._write(
            f"  rows: {total_rows} total, {valid_rows} valid, "
            f"{invalid_rows} invalid ({self._percentage(invalid_rows, total_rows)} invalid)"
        )
        self._write(
            f"  row_batches: {validation.get('batches_processed', 'unknown')}, "
            f"batch_size: {validation.get('batch_size', 'unknown')}"
        )
        self._write(f"  customer_partitions: {validation.get('feature_partition_count', 'unknown')}")

        failures = validation.get("failure_counts_by_rule", {})
        if failures:
            self._write("  validation_failures:")
            for rule, count in self._sorted_failures(failures):
                self._write(f"    {rule}: {count}")
        else:
            self._write("  validation_failures: none")

    def _print_features(self, manifest: dict) -> None:
        features = manifest.get("features", {})
        self._write("Features")
        self._write(f"  customers: {features.get('customers', 'unknown')}")
        self._write(f"  first_n_bets: {features.get('first_n_bets', 'unknown')}")
        incomplete_customers = features.get("customers_with_incomplete_first_n", "unknown")
        self._write(f"  customers_with_incomplete_first_n: {incomplete_customers}")
        self._write(f"  feature_partitions: {features.get('feature_partition_count', 'unknown')}")
        self._write(f"  feature_workers: {features.get('feature_worker_count', 'unknown')}")

    def _print_outputs(self, manifest: dict, include_features: bool) -> None:
        outputs = manifest.get("outputs", {})
        features = manifest.get("features", {})
        self._write("Outputs")
        self._write(f"  run_dir: {outputs.get('run_dir', 'unknown')}")
        self._write(f"  raw_bets: {outputs.get('raw_bets_dir', 'unknown')}")
        self._write(f"  valid_bets: {outputs.get('valid_bets_dir', 'unknown')}")
        self._write(f"  invalid_bets: {outputs.get('invalid_bets_dir', 'unknown')}")
        if include_features:
            self._write(f"  customer_features: {features.get('feature_dir', 'unknown')}")
        self._write(f"  manifest: {outputs.get('manifest', 'unknown')}")
        self._write(f"  success_marker: {outputs.get('success_marker', 'unknown')}")

    def _sorted_failures(self, failures: dict) -> list[tuple[str, object]]:
        return sorted(failures.items(), key=lambda item: (-self._int_value(item[1]), item[0]))

    def _percentage(self, numerator: int, denominator: int) -> str:
        if denominator == 0:
            return "0.00%"
        return f"{(numerator / denominator) * 100:.2f}%"

    def _int_value(self, value: object) -> int:
        if isinstance(value, int):
            return value
        return 0

    def _write(self, text: str) -> None:
        self.stream.write(f"{text}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bet-pipeline", description="Local betting-data batch pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    batch_source_parser = argparse.ArgumentParser(add_help=False)
    batch_source_parser.add_argument(
        "--output",
        required=True,
        help=(
            "Directory for committed batch run outputs. If it ends with validation or features, "
            "the parent directory is used as the batch run root."
        ),
    )
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
        help="Concurrent partition-level validation workers. Default: 1",
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

    validate_parser = subparsers.add_parser(
        "validate", parents=[batch_source_parser], help="Validate raw betting records"
    )
    validate_parser.add_argument("--input", required=True, help="Path to input bets CSV")

    features_parser = subparsers.add_parser(
        "build-features",
        parents=[batch_source_parser],
        help="Validate raw bets and build customer features",
    )
    features_parser.add_argument(
        "--input", help="Path to input bets CSV. Required unless --from-validation-run is used."
    )
    features_parser.add_argument(
        "--from-validation-run",
        help="Committed validation run directory to reuse, for example /outputs/runs/001.",
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


def batch_output_root(command: str, output_dir: str) -> Path:
    output_path = Path(output_dir)
    if command == "validate" and output_path.name == "validation":
        return output_path.parent
    if command == "build-features" and output_path.name == "features":
        return output_path.parent
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command in {"validate", "build-features"}:
        artifact_publisher = RunArtifactPublisher(batch_output_root(args.command, args.output), args.run_id)
        try:
            if args.command == "build-features" and args.from_validation_run is not None:
                if args.input is not None:
                    parser.error("build-features accepts either --input or --from-validation-run, not both")
                checkpoint = ValidationCheckpointLoader(args.from_validation_run).load()
                partitioned_input = checkpoint.partitioned_input
                report = checkpoint.validation_report
            else:
                if args.command == "build-features" and args.input is None:
                    parser.error("build-features requires either --input or --from-validation-run")

                # Bronze layer: stream raw CSV into customer-complete raw parquet partitions.
                raw_partition_process = RawBetCustomerCompletePartitionBatchProcess()
                raw_partitioned_input = raw_partition_process.process(
                    args.input,
                    artifact_publisher.raw_dir,
                    CustomerCompletePartitionSettings(
                        batch_size=args.batch_size,
                        feature_partition_count=args.feature_partition_count,
                        target_feature_partition_rows=args.target_feature_partition_rows,
                    ),
                )

                # Silver layer: validate each customer-complete partition into valid and invalid bets.
                validation_process = BetValidationPartitionBatchProcess()
                partitioned_input = validation_process.process(
                    raw_partitioned_input,
                    artifact_publisher.validation_dir,
                    ValidationBatchSettings(
                        validation_worker_count=args.validation_workers,
                        generated_at=artifact_publisher.validation_generated_at,
                    ),
                )
                report = artifact_publisher.write_validation_report(partitioned_input)

            if args.command == "validate":
                manifest = artifact_publisher.write_manifest(partitioned_input, report)
                artifact_publisher.commit(partitioned_input)
                RunSummaryPrinter().print_validation_summary(manifest)
                return 0

            # Gold layer: build customer-level ML features from validated bet partitions.
            feature_process = BetFeaturePartitionBatchProcess()
            feature_result = feature_process.process(
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

        RunSummaryPrinter().print_feature_summary(manifest)
        return 0
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
