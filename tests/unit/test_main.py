from __future__ import annotations

import unittest
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bet_pipeline import main
from bet_pipeline.batch_process.raw_partition_batch import CustomerCompletePartitionSettings
from bet_pipeline.batch_process.run_artifacts import ValidationCheckpoint
from bet_pipeline.batch_process.validation_batch import ValidationBatchSettings


def _manifest() -> dict:
    return {
        "run_id": "run",
        "status": "success",
        "input_path": "data/bets.csv",
        "reused_validation_checkpoint": False,
        "source_validation_run_id": None,
        "started_at": "2024-01-01T00:00:00+00:00",
        "finished_at": "2024-01-01T00:00:01+00:00",
        "validation": {
            "total_rows": 3,
            "valid_rows": 2,
            "invalid_rows": 1,
            "batches_processed": 2,
            "batch_size": 2,
            "feature_partition_count": 1,
            "failure_counts_by_rule": {"betting_amount_gt_0": 1},
        },
        "features": {
            "customers": 1,
            "customers_with_incomplete_first_n": 0,
            "first_n_bets": 20,
            "feature_partition_count": 1,
            "feature_worker_count": 1,
            "feature_dir": "outputs/runs/run/features/customer_features",
        },
        "outputs": {
            "run_dir": "outputs/runs/run",
            "raw_bets_dir": "outputs/runs/run/raw/raw_bets",
            "valid_bets_dir": "outputs/runs/run/validation/valid_bets",
            "invalid_bets_dir": "outputs/runs/run/validation/invalid_bets",
            "manifest": "outputs/runs/run/run_manifest.json",
            "success_marker": "outputs/runs/run/_SUCCESS",
        },
    }


class MainTests(unittest.TestCase):
    def test_validate_command_uses_source_level_validation(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.RawBetCustomerCompletePartitionBatchProcess") as raw_partition_process_class,
            patch("bet_pipeline.main.BetValidationPartitionBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeaturePartitionBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.raw_dir = "outputs/_staging/run/raw"
            run.validation_dir = "outputs/_staging/run/validation"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            raw_partitioned_input = SimpleNamespace()
            partitioned_input = SimpleNamespace()
            report = {
                "total_rows": 3,
                "valid_rows": 2,
                "invalid_rows": 1,
            }
            raw_partition_process = raw_partition_process_class.return_value
            validation_process = validation_process_class.return_value
            raw_partition_process.process.return_value = raw_partitioned_input
            validation_process.process.return_value = partitioned_input
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = _manifest()

            exit_code = main.main(
                [
                    "validate",
                    "--input",
                    "data/bets.csv",
                    "--output",
                    "outputs",
                    "--batch-size",
                    "1000",
                    "--feature-partition-count",
                    "8",
                ]
            )

            self.assertEqual(exit_code, 0)
            run_class.assert_called_once_with(Path("outputs"), None)
            raw_partition_process.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run/raw",
                CustomerCompletePartitionSettings(
                    batch_size=1000,
                    feature_partition_count=8,
                    target_feature_partition_rows=1000,
                ),
            )
            validation_process.process.assert_called_once_with(
                raw_partitioned_input,
                "outputs/_staging/run/validation",
                ValidationBatchSettings(
                    validation_worker_count=1,
                    generated_at="2024-01-01T00:00:00+00:00",
                ),
            )
            run.write_manifest.assert_called_once_with(partitioned_input, report)
            run.commit.assert_called_once_with(partitioned_input)
            feature_process_class.assert_not_called()

    def test_build_features_command_validates_then_builds_from_validated_result(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.RawBetCustomerCompletePartitionBatchProcess") as raw_partition_process_class,
            patch("bet_pipeline.main.BetValidationPartitionBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeaturePartitionBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.raw_dir = "outputs/_staging/run-1/raw"
            run.validation_dir = "outputs/_staging/run-1/validation"
            run.features_dir = "outputs/_staging/run-1/features"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            run.feature_generated_at = "2024-01-01T00:00:01+00:00"
            raw_partitioned_input = SimpleNamespace()
            partitioned_input = SimpleNamespace()
            report = {
                "total_rows": 3,
                "valid_rows": 2,
                "invalid_rows": 1,
            }
            feature_result = SimpleNamespace(feature_count=1, incomplete_first_n_count=0)
            raw_partition_process = raw_partition_process_class.return_value
            validation_process = validation_process_class.return_value
            feature_process = feature_process_class.return_value
            raw_partition_process.process.return_value = raw_partitioned_input
            validation_process.process.return_value = partitioned_input
            feature_process.process.return_value = feature_result
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = _manifest()

            exit_code = main.main(
                [
                    "build-features",
                    "--input",
                    "data/bets.csv",
                    "--output",
                    "outputs",
                    "--run-id",
                    "run-1",
                    "--batch-size",
                    "500",
                    "--validation-workers",
                    "3",
                    "--feature-workers",
                    "2",
                    "--feature-partition-count",
                    "4",
                ]
            )

            self.assertEqual(exit_code, 0)
            run_class.assert_called_once_with(Path("outputs"), "run-1")
            raw_partition_process.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run-1/raw",
                CustomerCompletePartitionSettings(
                    batch_size=500,
                    feature_partition_count=4,
                    target_feature_partition_rows=1000,
                ),
            )
            validation_process.process.assert_called_once_with(
                raw_partitioned_input,
                "outputs/_staging/run-1/validation",
                ValidationBatchSettings(
                    validation_worker_count=3,
                    generated_at="2024-01-01T00:00:00+00:00",
                ),
            )
            feature_process.process.assert_called_once_with(
                partitioned_input,
                "outputs/_staging/run-1/features",
                "2024-01-01T00:00:01+00:00",
                first_n_bets=20,
                feature_worker_count=2,
            )
            run.write_feature_report.assert_called_once_with(
                partitioned_input,
                report,
                feature_result,
            )
            run.write_manifest.assert_called_once_with(partitioned_input, report, feature_result)
            run.commit.assert_called_once_with(partitioned_input)

    def test_build_features_passes_target_feature_partition_rows_to_validation(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.RawBetCustomerCompletePartitionBatchProcess") as raw_partition_process_class,
            patch("bet_pipeline.main.BetValidationPartitionBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeaturePartitionBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.raw_dir = "outputs/_staging/run/raw"
            run.validation_dir = "outputs/_staging/run/validation"
            run.features_dir = "outputs/_staging/run/features"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            run.feature_generated_at = "2024-01-01T00:00:01+00:00"
            raw_partitioned_input = SimpleNamespace()
            partitioned_input = SimpleNamespace()
            report = {"total_rows": 10, "valid_rows": 10, "invalid_rows": 0}
            feature_result = SimpleNamespace(feature_count=2, incomplete_first_n_count=0, first_n_bets=10)
            raw_partition_process_class.return_value.process.return_value = raw_partitioned_input
            validation_process_class.return_value.process.return_value = partitioned_input
            feature_process_class.return_value.process.return_value = feature_result
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = _manifest()

            exit_code = main.main(
                [
                    "build-features",
                    "--input",
                    "data/bets.csv",
                    "--output",
                    "outputs",
                    "--target-feature-partition-rows",
                    "1000",
                    "--first-n-bets",
                    "10",
                ]
            )

            self.assertEqual(exit_code, 0)
            raw_partition_process_class.return_value.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run/raw",
                CustomerCompletePartitionSettings(
                    batch_size=1000,
                    feature_partition_count=None,
                    target_feature_partition_rows=1000,
                ),
            )
            validation_process_class.return_value.process.assert_called_once_with(
                raw_partitioned_input,
                "outputs/_staging/run/validation",
                ValidationBatchSettings(
                    validation_worker_count=1,
                    generated_at="2024-01-01T00:00:00+00:00",
                ),
            )
            feature_process_class.return_value.process.assert_called_once_with(
                partitioned_input,
                "outputs/_staging/run/features",
                "2024-01-01T00:00:01+00:00",
                first_n_bets=10,
                feature_worker_count=1,
            )

    def test_target_feature_partition_rows_uses_default_batch_rows(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.RawBetCustomerCompletePartitionBatchProcess") as raw_partition_process_class,
            patch("bet_pipeline.main.BetValidationPartitionBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeaturePartitionBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.raw_dir = "outputs/_staging/run/raw"
            run.validation_dir = "outputs/_staging/run/validation"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            raw_partitioned_input = SimpleNamespace()
            partitioned_input = SimpleNamespace()
            report = {"total_rows": 10, "valid_rows": 10, "invalid_rows": 0}
            raw_partition_process_class.return_value.process.return_value = raw_partitioned_input
            validation_process_class.return_value.process.return_value = partitioned_input
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = _manifest()

            exit_code = main.main(
                [
                    "validate",
                    "--input",
                    "data/bets.csv",
                    "--output",
                    "outputs",
                    "--batch-size",
                    "750",
                ]
            )

            self.assertEqual(exit_code, 0)
            raw_partition_process_class.return_value.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run/raw",
                CustomerCompletePartitionSettings(
                    batch_size=750,
                    feature_partition_count=None,
                    target_feature_partition_rows=1000,
                ),
            )
            validation_process_class.return_value.process.assert_called_once_with(
                raw_partitioned_input,
                "outputs/_staging/run/validation",
                ValidationBatchSettings(
                    validation_worker_count=1,
                    generated_at="2024-01-01T00:00:00+00:00",
                ),
            )
            run.write_manifest.assert_called_once_with(partitioned_input, report)
            run.commit.assert_called_once_with(partitioned_input)
            feature_process_class.assert_not_called()

    def test_build_features_normalizes_features_output_to_batch_root(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.RawBetCustomerCompletePartitionBatchProcess") as raw_partition_process_class,
            patch("bet_pipeline.main.BetValidationPartitionBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeaturePartitionBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.raw_dir = "outputs/_staging/run/raw"
            run.validation_dir = "outputs/_staging/run/validation"
            run.features_dir = "outputs/_staging/run/features"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            run.feature_generated_at = "2024-01-01T00:00:01+00:00"
            raw_partitioned_input = SimpleNamespace()
            partitioned_input = SimpleNamespace()
            report = {"total_rows": 1, "valid_rows": 1, "invalid_rows": 0}
            feature_result = SimpleNamespace(feature_count=1, incomplete_first_n_count=0)
            raw_partition_process_class.return_value.process.return_value = raw_partitioned_input
            validation_process_class.return_value.process.return_value = partitioned_input
            feature_process_class.return_value.process.return_value = feature_result
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = _manifest()

            exit_code = main.main(
                [
                    "build-features",
                    "--input",
                    "data/bets.csv",
                    "--output",
                    "outputs/features",
                ]
            )

            self.assertEqual(exit_code, 0)
            run_class.assert_called_once_with(Path("outputs"), None)

    def test_build_features_reuses_validation_checkpoint(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.ValidationCheckpointLoader") as checkpoint_loader_class,
            patch("bet_pipeline.main.BetValidationPartitionBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeaturePartitionBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.features_dir = "outputs/_staging/features-001/features"
            run.feature_generated_at = "2024-01-01T00:00:01+00:00"
            partitioned_input = SimpleNamespace(source_validation_run_id="001")
            report = {"total_rows": 10, "valid_rows": 9, "invalid_rows": 1}
            feature_result = SimpleNamespace(feature_count=2, incomplete_first_n_count=0)
            checkpoint_loader_class.return_value.load.return_value = ValidationCheckpoint(partitioned_input, report)
            feature_process_class.return_value.process.return_value = feature_result
            run.write_manifest.return_value = _manifest()

            exit_code = main.main(
                [
                    "build-features",
                    "--from-validation-run",
                    "outputs/runs/001",
                    "--output",
                    "outputs",
                    "--run-id",
                    "001-features",
                ]
            )

            self.assertEqual(exit_code, 0)
            run_class.assert_called_once_with(Path("outputs"), "001-features")
            checkpoint_loader_class.assert_called_once_with("outputs/runs/001")
            validation_process_class.assert_not_called()
            run.write_validation_report.assert_not_called()
            feature_process_class.return_value.process.assert_called_once_with(
                partitioned_input,
                "outputs/_staging/features-001/features",
                "2024-01-01T00:00:01+00:00",
                first_n_bets=20,
                feature_worker_count=1,
            )
            run.write_feature_report.assert_called_once_with(partitioned_input, report, feature_result)
            run.write_manifest.assert_called_once_with(partitioned_input, report, feature_result)
            run.commit.assert_called_once_with(partitioned_input)

    def test_validate_normalizes_validation_output_to_batch_root(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.RawBetCustomerCompletePartitionBatchProcess") as raw_partition_process_class,
            patch("bet_pipeline.main.BetValidationPartitionBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeaturePartitionBatchProcess"),
        ):
            run = run_class.return_value
            run.raw_dir = "outputs/_staging/run/raw"
            run.validation_dir = "outputs/_staging/run/validation"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            raw_partitioned_input = SimpleNamespace()
            partitioned_input = SimpleNamespace()
            report = {"total_rows": 1, "valid_rows": 1, "invalid_rows": 0}
            raw_partition_process_class.return_value.process.return_value = raw_partitioned_input
            validation_process_class.return_value.process.return_value = partitioned_input
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = _manifest()

            exit_code = main.main(
                [
                    "validate",
                    "--input",
                    "data/bets.csv",
                    "--output",
                    "outputs/validation",
                ]
            )

            self.assertEqual(exit_code, 0)
            run_class.assert_called_once_with(Path("outputs"), None)

    def test_run_summary_printer_outputs_operational_metrics(self) -> None:
        stream = StringIO()
        main.RunSummaryPrinter(stream).print_feature_summary(_manifest())

        output = stream.getvalue()
        self.assertIn("Batch run completed", output)
        self.assertIn("run_id: run", output)
        self.assertIn("rows: 3 total, 2 valid, 1 invalid (33.33% invalid)", output)
        self.assertIn("validation_failures:", output)
        self.assertIn("betting_amount_gt_0: 1", output)
        self.assertIn("customers: 1", output)
        self.assertIn("customer_features: outputs/runs/run/features/customer_features", output)


if __name__ == "__main__":
    unittest.main()
