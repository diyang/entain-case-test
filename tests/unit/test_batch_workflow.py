from __future__ import annotations

import unittest
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from bet_pipeline.batch_process.feature_batch import BetFeaturePartitionBatchProcess
from bet_pipeline.batch_process.raw_partition_batch import (
    CustomerCompletePartitionSettings,
    RawBetCustomerCompletePartitionBatchProcess,
)
from bet_pipeline.batch_process.run_artifacts import RunArtifactPublisher, ValidationCheckpointLoader
from bet_pipeline.batch_process.validation_batch import BetValidationPartitionBatchProcess, ValidationBatchSettings
from bet_pipeline.io import read_parquet, write_csv
from bet_pipeline.schema import EXPECTED_COLUMNS


def _row(bet_id: str, bet_num: str, amount: str = "10") -> dict[str, str]:
    return {
        "bet_id": bet_id,
        "customer_id": "00000000-0000-4000-8000-000000000001",
        "bet_datetime": f"2024-08-{int(bet_num):02d} 00:00:00.000",
        "bet_num": bet_num,
        "betting_amount": amount,
        "price": "2",
        "category": "racing",
        "stake_type": "cash",
        "bet_result": "no-return",
        "payout": "0",
        "return_for_entain": amount,
    }


@dataclass(frozen=True)
class ValidationSourceConfig:
    raw_dir: Path | None = None
    batch_size: int = 1000
    feature_partition_count: int | None = None
    target_feature_partition_rows: int = 1000
    validation_worker_count: int = 1
    generated_at: str | None = None


def _validate_source(
    input_path: Path,
    validation_dir: Path,
    config: ValidationSourceConfig | None = None,
):
    if config is None:
        config = ValidationSourceConfig()
    raw_dir = config.raw_dir
    if raw_dir is None:
        raw_dir = validation_dir.parent / f"{validation_dir.name}-raw"
    raw_partitioned_input = RawBetCustomerCompletePartitionBatchProcess().process(
        input_path,
        raw_dir,
        CustomerCompletePartitionSettings(
            batch_size=config.batch_size,
            feature_partition_count=config.feature_partition_count,
            target_feature_partition_rows=config.target_feature_partition_rows,
        ),
    )
    return BetValidationPartitionBatchProcess().process(
        raw_partitioned_input,
        validation_dir,
        ValidationBatchSettings(
            validation_worker_count=config.validation_worker_count,
            generated_at=config.generated_at,
        ),
    )


class BatchWorkflowTests(unittest.TestCase):
    def test_target_feature_partition_rows_creates_dynamic_feature_partitions(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            rows = [_row(str(index), "1") for index in range(1, 6)]
            for index, row in enumerate(rows, start=1):
                row["customer_id"] = f"00000000-0000-4000-8000-{index:012d}"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            partitioned_input = _validate_source(
                input_path,
                tmp_path / "validation",
                ValidationSourceConfig(target_feature_partition_rows=2),
            )
            self.assertEqual(partitioned_input.feature_partition_count, 3)
            self.assertEqual(len(partitioned_input.partition_paths), 3)

            fixed_partition_input = _validate_source(
                input_path,
                tmp_path / "fixed-validation",
                ValidationSourceConfig(feature_partition_count=7, target_feature_partition_rows=2),
            )
            self.assertEqual(fixed_partition_input.feature_partition_count, 7)
            self.assertEqual(len(fixed_partition_input.partition_paths), 7)

            with self.assertRaises(ValueError):
                _validate_source(
                    input_path,
                    tmp_path / "bad-validation",
                    ValidationSourceConfig(target_feature_partition_rows=0),
                )

    def test_validate_batch_writes_validation_outputs(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "validation"
            write_csv(input_path, [_row("1", "1")], EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="validation-batch-test")
            partitioned_input = _validate_source(
                input_path,
                run.validation_dir,
                ValidationSourceConfig(raw_dir=run.raw_dir, generated_at=run.validation_generated_at),
            )
            report = run.write_validation_report(partitioned_input)
            manifest = run.write_manifest(partitioned_input, report)
            run.commit(partitioned_input)

            self.assertEqual(manifest["validation"]["run_id"], "validation-batch-test")
            run_dir = output_dir / "runs" / "validation-batch-test"
            self.assertTrue((run_dir / "_SUCCESS").exists())
            self.assertTrue((run_dir / "validation" / "valid_bets" / "part-00000.parquet").exists())
            self.assertFalse((output_dir / "_staging" / "validation-batch-test").exists())

    def test_validation_batch_quarantines_later_global_duplicates(self) -> None:
        rows = [
            _row("1", "1"),
            _row("1", "2"),
            _row("3", "1"),
        ]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            partitioned_input = _validate_source(
                input_path,
                tmp_path / "validation",
                ValidationSourceConfig(batch_size=1),
            )
            invalid_rows = []
            for partition_path in partitioned_input.invalid_partition_paths:
                _, partition_rows = read_parquet(partition_path)
                invalid_rows.extend(partition_rows)

            self.assertEqual(partitioned_input.valid_rows, 1)
            self.assertEqual(partitioned_input.invalid_rows, 2)
            self.assertEqual(partitioned_input.failure_counts_by_rule["bet_id_unique"], 1)
            self.assertEqual(partitioned_input.failure_counts_by_rule["customer_bet_num_unique"], 1)
            self.assertEqual(
                sorted(row["validation_errors"] for row in invalid_rows),
                ["bet_id_unique", "customer_bet_num_unique"],
            )

    def test_validation_batch_demotes_customer_rows_with_sequence_gap_after_partitioning(self) -> None:
        rows = [
            _row("1", "1"),
            _row("3", "3"),
        ]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            partitioned_input = _validate_source(
                input_path,
                tmp_path / "validation",
                ValidationSourceConfig(batch_size=1),
            )
            valid_rows = []
            invalid_rows = []
            for partition_path in partitioned_input.partition_paths:
                _, partition_rows = read_parquet(partition_path)
                valid_rows.extend(partition_rows)
            for partition_path in partitioned_input.invalid_partition_paths:
                _, partition_rows = read_parquet(partition_path)
                invalid_rows.extend(partition_rows)

            self.assertEqual(partitioned_input.valid_rows, 0)
            self.assertEqual(partitioned_input.invalid_rows, 2)
            self.assertEqual(partitioned_input.failure_counts_by_rule["customer_bet_num_sequence"], 2)
            self.assertEqual(valid_rows, [])
            self.assertEqual(sorted(row["bet_id"] for row in invalid_rows), ["1", "3"])
            self.assertEqual({row["validation_errors"] for row in invalid_rows}, {"customer_bet_num_sequence"})
            self.assertEqual(sorted(row["source_row_number"] for row in invalid_rows), [2, 3])

    def test_invalid_row_inside_sequence_does_not_create_sequence_gap(self) -> None:
        rows = [
            _row("1", "1"),
            _row("2", "2", amount="-1"),
            _row("3", "3"),
        ]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            partitioned_input = _validate_source(
                input_path,
                tmp_path / "validation",
                ValidationSourceConfig(batch_size=1),
            )
            invalid_rows = []
            for partition_path in partitioned_input.invalid_partition_paths:
                _, partition_rows = read_parquet(partition_path)
                invalid_rows.extend(partition_rows)

            self.assertEqual(partitioned_input.valid_rows, 2)
            self.assertEqual(partitioned_input.invalid_rows, 1)
            self.assertNotIn("customer_bet_num_sequence", partitioned_input.failure_counts_by_rule)
            self.assertEqual(invalid_rows[0]["bet_id"], "2")
            self.assertEqual(invalid_rows[0]["validation_errors"], "betting_amount_gt_0")

    def test_feature_batch_writes_manifest_validation_and_features(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, [_row("1", "1"), _row("2", "2", amount="-1")], EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="platform-test-run")
            partitioned_input = _validate_source(
                input_path,
                run.validation_dir,
                ValidationSourceConfig(
                    raw_dir=run.raw_dir,
                    feature_partition_count=2,
                    generated_at=run.validation_generated_at,
                ),
            )
            report = run.write_validation_report(partitioned_input)
            feature_result = BetFeaturePartitionBatchProcess().process(
                partitioned_input,
                run.features_dir,
                run.feature_generated_at,
            )
            run.write_feature_report(partitioned_input, report, feature_result)
            manifest = run.write_manifest(partitioned_input, report, feature_result)
            run.commit(partitioned_input)

            self.assertEqual(manifest["run_id"], "platform-test-run")
            self.assertEqual(manifest["status"], "success")
            self.assertEqual(manifest["validation"]["invalid_rows"], 1)
            self.assertEqual(manifest["features"]["customers"], 1)
            run_dir = output_dir / "runs" / "platform-test-run"
            self.assertTrue((run_dir / "_SUCCESS").exists())
            self.assertTrue((run_dir / "run_manifest.json").exists())
            self.assertTrue((run_dir / "validation" / "valid_bets" / "part-00000.parquet").exists())
            self.assertTrue((run_dir / "validation" / "invalid_bets" / "part-00000.parquet").exists())
            self.assertTrue((run_dir / "features" / "customer_features" / "part-00000.parquet").exists())
            self.assertFalse((output_dir / "_staging" / "platform-test-run").exists())

    def test_feature_batch_can_reuse_committed_validation_checkpoint(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, [_row("1", "1"), _row("2", "2", amount="-1")], EXPECTED_COLUMNS)

            validation_run = RunArtifactPublisher(output_dir, run_id="001")
            partitioned_input = _validate_source(
                input_path,
                validation_run.validation_dir,
                ValidationSourceConfig(
                    raw_dir=validation_run.raw_dir,
                    feature_partition_count=2,
                    generated_at=validation_run.validation_generated_at,
                ),
            )
            validation_report = validation_run.write_validation_report(partitioned_input)
            validation_run.write_manifest(partitioned_input, validation_report)
            validation_run.commit(partitioned_input)

            checkpoint = ValidationCheckpointLoader(output_dir / "runs" / "001").load()
            feature_run = RunArtifactPublisher(output_dir, run_id="001-features")
            feature_result = BetFeaturePartitionBatchProcess().process(
                checkpoint.partitioned_input,
                feature_run.features_dir,
                feature_run.feature_generated_at,
            )
            feature_run.write_feature_report(
                checkpoint.partitioned_input,
                checkpoint.validation_report,
                feature_result,
            )
            manifest = feature_run.write_manifest(
                checkpoint.partitioned_input,
                checkpoint.validation_report,
                feature_result,
            )
            feature_run.commit(checkpoint.partitioned_input)

            validation_run_dir = output_dir / "runs" / "001"
            feature_run_dir = output_dir / "runs" / "001-features"
            self.assertEqual(manifest["source_validation_run_id"], "001")
            self.assertTrue(manifest["reused_validation_checkpoint"])
            self.assertEqual(manifest["outputs"]["validation_dir"], str(validation_run_dir / "validation"))
            self.assertTrue((validation_run_dir / "_SUCCESS").exists())
            self.assertTrue((feature_run_dir / "_SUCCESS").exists())
            self.assertTrue((feature_run_dir / "features" / "customer_features" / "part-00000.parquet").exists())
            self.assertFalse((feature_run_dir / "validation").exists())

    def test_validation_checkpoint_requires_success_marker(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            validation_run_dir = tmp_path / "outputs" / "runs" / "001"
            (validation_run_dir / "validation" / "valid_bets").mkdir(parents=True)
            (validation_run_dir / "validation" / "invalid_bets").mkdir(parents=True)

            with self.assertRaises(FileNotFoundError):
                ValidationCheckpointLoader(validation_run_dir).load()

    def test_customer_hash_partitions_keep_customer_feature_complete(self) -> None:
        customer_b = "00000000-0000-4000-8000-000000000002"
        rows = [
            _row("1", "1", amount="10"),
            _row("2", "1", amount="20"),
            _row("3", "2", amount="30"),
            _row("4", "2", amount="40"),
        ]
        rows[1]["customer_id"] = customer_b
        rows[3]["customer_id"] = customer_b

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="partition-test-run")
            partitioned_input = _validate_source(
                input_path,
                run.validation_dir,
                ValidationSourceConfig(
                    raw_dir=run.raw_dir,
                    batch_size=1,
                    feature_partition_count=3,
                    generated_at=run.validation_generated_at,
                ),
            )
            report = run.write_validation_report(partitioned_input)

            self.assertEqual(
                [path.name for path in partitioned_input.partition_paths],
                ["part-00000.parquet", "part-00001.parquet", "part-00002.parquet"],
            )
            customer_partition_indices = {}
            customer_row_counts = {}
            for partition_index, partition_path in enumerate(partitioned_input.partition_paths):
                _, valid_rows = read_parquet(partition_path)
                for row in valid_rows:
                    customer_partition_indices.setdefault(str(row["customer_id"]), set()).add(partition_index)
                    customer_row_counts[str(row["customer_id"])] = (
                        customer_row_counts.get(str(row["customer_id"]), 0) + 1
                    )

            self.assertEqual(len(customer_partition_indices["00000000-0000-4000-8000-000000000001"]), 1)
            self.assertEqual(len(customer_partition_indices[customer_b]), 1)
            self.assertEqual(customer_row_counts["00000000-0000-4000-8000-000000000001"], 2)
            self.assertEqual(customer_row_counts[customer_b], 2)

            feature_result = BetFeaturePartitionBatchProcess().process(
                partitioned_input,
                run.features_dir,
                run.feature_generated_at,
            )
            run.write_feature_report(partitioned_input, report, feature_result)
            manifest = run.write_manifest(partitioned_input, report, feature_result)
            run.commit(partitioned_input)

            self.assertEqual(manifest["features"]["customers"], 2)
            self.assertEqual(manifest["validation"]["feature_partition_count"], 3)

    def test_concurrent_validation_workers_keep_customer_partitions_complete(self) -> None:
        rows = []
        for customer_index in range(1, 5):
            customer_id = f"00000000-0000-4000-8000-{customer_index:012d}"
            first_row = _row(str((customer_index * 2) - 1), "1", amount="10")
            second_row = _row(str(customer_index * 2), "2", amount="10")
            first_row["customer_id"] = customer_id
            second_row["customer_id"] = customer_id
            rows.extend([first_row, second_row])

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="concurrent-validation-test")
            partitioned_input = _validate_source(
                input_path,
                run.validation_dir,
                ValidationSourceConfig(
                    raw_dir=run.raw_dir,
                    batch_size=2,
                    validation_worker_count=3,
                    target_feature_partition_rows=3,
                    generated_at=run.validation_generated_at,
                ),
            )
            report = run.write_validation_report(partitioned_input)
            feature_result = BetFeaturePartitionBatchProcess().process(
                partitioned_input,
                run.features_dir,
                run.feature_generated_at,
            )
            run.write_feature_report(partitioned_input, report, feature_result)
            manifest = run.write_manifest(partitioned_input, report, feature_result)
            run.commit(partitioned_input)

            self.assertEqual(manifest["validation"]["batches_processed"], 4)
            self.assertEqual(manifest["validation"]["valid_rows"], 8)
            self.assertGreaterEqual(manifest["validation"]["feature_partition_count"], 2)
            self.assertEqual(manifest["features"]["customers"], 4)

    def test_concurrent_feature_workers_write_partitioned_outputs(self) -> None:
        rows = []
        for customer_index in range(1, 7):
            row = _row(str(customer_index), "1", amount="10")
            row["customer_id"] = f"00000000-0000-4000-8000-{customer_index:012d}"
            rows.append(row)

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="concurrent-feature-test")
            partitioned_input = _validate_source(
                input_path,
                run.validation_dir,
                ValidationSourceConfig(
                    raw_dir=run.raw_dir,
                    batch_size=2,
                    feature_partition_count=4,
                    generated_at=run.validation_generated_at,
                ),
            )
            report = run.write_validation_report(partitioned_input)
            feature_result = BetFeaturePartitionBatchProcess().process(
                partitioned_input,
                run.features_dir,
                run.feature_generated_at,
                feature_worker_count=3,
            )
            run.write_feature_report(partitioned_input, report, feature_result)
            manifest = run.write_manifest(partitioned_input, report, feature_result)
            run.commit(partitioned_input)

            feature_dir = output_dir / "runs" / "concurrent-feature-test" / "features" / "customer_features"
            self.assertEqual(manifest["features"]["customers"], 6)
            self.assertEqual(manifest["features"]["feature_worker_count"], 3)
            self.assertEqual(
                [path.name for path in sorted(feature_dir.glob("part-*.parquet"))],
                [
                    "part-00000.parquet",
                    "part-00001.parquet",
                    "part-00002.parquet",
                    "part-00003.parquet",
                ],
            )


if __name__ == "__main__":
    unittest.main()
