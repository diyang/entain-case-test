from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bet_pipeline import main
from bet_pipeline.batch import ValidationBatchSettings


class MainTests(unittest.TestCase):
    def test_validate_command_uses_source_level_validation(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.BetValidationBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeatureBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.validation_dir = "outputs/_staging/run/validation"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            partitioned_input = SimpleNamespace()
            report = {
                "total_rows": 3,
                "valid_rows": 2,
                "invalid_rows": 1,
            }
            validation_process = validation_process_class.return_value
            validation_process.process.return_value = partitioned_input
            run.write_validation_report.return_value = report

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
            run_class.assert_called_once_with("outputs", None)
            validation_process.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run/validation",
                ValidationBatchSettings(
                    batch_size=1000,
                    validation_worker_count=1,
                    feature_partition_count=8,
                    target_feature_partition_rows=1000,
                    generated_at="2024-01-01T00:00:00+00:00",
                ),
            )
            run.write_manifest.assert_called_once_with(partitioned_input, report)
            run.commit.assert_called_once_with(partitioned_input)
            feature_process_class.assert_not_called()

    def test_build_features_command_validates_then_builds_from_validated_result(self) -> None:
        with (
            patch("bet_pipeline.main.RunArtifactPublisher") as run_class,
            patch("bet_pipeline.main.BetValidationBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeatureBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.validation_dir = "outputs/_staging/run-1/validation"
            run.features_dir = "outputs/_staging/run-1/features"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            run.feature_generated_at = "2024-01-01T00:00:01+00:00"
            partitioned_input = SimpleNamespace()
            report = {
                "total_rows": 3,
                "valid_rows": 2,
                "invalid_rows": 1,
            }
            feature_result = SimpleNamespace(feature_count=1, incomplete_first_n_count=0)
            manifest = {
                "run_id": "run-1",
                "validation": {
                    "valid_rows": 2,
                    "invalid_rows": 1,
                },
                "features": {
                    "customers": 1,
                },
            }
            validation_process = validation_process_class.return_value
            feature_process = feature_process_class.return_value
            validation_process.process.return_value = partitioned_input
            feature_process.process.return_value = feature_result
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = manifest

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
            run_class.assert_called_once_with("outputs", "run-1")
            validation_process.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run-1/validation",
                ValidationBatchSettings(
                    batch_size=500,
                    validation_worker_count=3,
                    feature_partition_count=4,
                    target_feature_partition_rows=1000,
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
            patch("bet_pipeline.main.BetValidationBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeatureBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.validation_dir = "outputs/_staging/run/validation"
            run.features_dir = "outputs/_staging/run/features"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            run.feature_generated_at = "2024-01-01T00:00:01+00:00"
            partitioned_input = SimpleNamespace()
            report = {"total_rows": 10, "valid_rows": 10, "invalid_rows": 0}
            feature_result = SimpleNamespace(feature_count=2, incomplete_first_n_count=0, first_n_bets=10)
            manifest = {
                "run_id": "run",
                "validation": {"valid_rows": 10, "invalid_rows": 0},
                "features": {"customers": 2},
            }
            validation_process_class.return_value.process.return_value = partitioned_input
            feature_process_class.return_value.process.return_value = feature_result
            run.write_validation_report.return_value = report
            run.write_manifest.return_value = manifest

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
            validation_process_class.return_value.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run/validation",
                ValidationBatchSettings(
                    batch_size=1000,
                    validation_worker_count=1,
                    feature_partition_count=None,
                    target_feature_partition_rows=1000,
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
            patch("bet_pipeline.main.BetValidationBatchProcess") as validation_process_class,
            patch("bet_pipeline.main.BetFeatureBatchProcess") as feature_process_class,
        ):
            run = run_class.return_value
            run.validation_dir = "outputs/_staging/run/validation"
            run.validation_generated_at = "2024-01-01T00:00:00+00:00"
            partitioned_input = SimpleNamespace()
            report = {"total_rows": 10, "valid_rows": 10, "invalid_rows": 0}
            validation_process_class.return_value.process.return_value = partitioned_input
            run.write_validation_report.return_value = report

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
            validation_process_class.return_value.process.assert_called_once_with(
                "data/bets.csv",
                "outputs/_staging/run/validation",
                ValidationBatchSettings(
                    batch_size=750,
                    validation_worker_count=1,
                    feature_partition_count=None,
                    target_feature_partition_rows=1000,
                    generated_at="2024-01-01T00:00:00+00:00",
                ),
            )
            run.write_manifest.assert_called_once_with(partitioned_input, report)
            run.commit.assert_called_once_with(partitioned_input)
            feature_process_class.assert_not_called()


if __name__ == "__main__":
    unittest.main()
