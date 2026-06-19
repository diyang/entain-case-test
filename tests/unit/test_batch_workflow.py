from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bet_pipeline.batch import (
    BetFeatureBatchProcess,
    BetValidationBatchProcess,
    RunArtifactPublisher,
    ValidationBatchSettings,
)
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


class BatchWorkflowTests(unittest.TestCase):
    def test_target_feature_partition_rows_creates_dynamic_feature_partitions(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            rows = [_row(str(index), "1") for index in range(1, 6)]
            for index, row in enumerate(rows, start=1):
                row["customer_id"] = f"00000000-0000-4000-8000-{index:012d}"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                tmp_path / "validation",
                ValidationBatchSettings(target_feature_partition_rows=2),
            )
            self.assertEqual(partitioned_input.feature_partition_count, 3)
            self.assertEqual(len(partitioned_input.partition_paths), 3)

            fixed_partition_input = BetValidationBatchProcess().process(
                input_path,
                tmp_path / "fixed-validation",
                ValidationBatchSettings(
                    feature_partition_count=7,
                    target_feature_partition_rows=2,
                ),
            )
            self.assertEqual(fixed_partition_input.feature_partition_count, 7)
            self.assertEqual(len(fixed_partition_input.partition_paths), 7)

            with self.assertRaises(ValueError):
                BetValidationBatchProcess().process(
                    input_path,
                    tmp_path / "bad-validation",
                    ValidationBatchSettings(target_feature_partition_rows=0),
                )

    def test_validate_batch_writes_validation_outputs(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "validation"
            write_csv(input_path, [_row("1", "1")], EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="validation-batch-test")
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(generated_at=run.validation_generated_at),
            )
            report = run.write_validation_report(partitioned_input)
            manifest = run.write_manifest(partitioned_input, report)
            run.commit(partitioned_input)

            self.assertEqual(manifest["validation"]["run_id"], "validation-batch-test")
            run_dir = output_dir / "runs" / "validation-batch-test"
            self.assertTrue((run_dir / "_SUCCESS").exists())
            self.assertTrue((run_dir / "validation" / "valid_bets" / "part-00000.parquet").exists())
            self.assertFalse((output_dir / "_staging" / "validation-batch-test").exists())

    def test_feature_batch_writes_manifest_validation_and_features(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, [_row("1", "1"), _row("2", "2", amount="-1")], EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="platform-test-run")
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(
                    feature_partition_count=2,
                    target_feature_partition_rows=1000,
                    generated_at=run.validation_generated_at,
                ),
            )
            report = run.write_validation_report(partitioned_input)
            feature_result = BetFeatureBatchProcess().process(
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
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(
                    batch_size=1,
                    feature_partition_count=3,
                    target_feature_partition_rows=1000,
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

            feature_result = BetFeatureBatchProcess().process(
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
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(
                    batch_size=2,
                    validation_worker_count=3,
                    target_feature_partition_rows=3,
                    generated_at=run.validation_generated_at,
                ),
            )
            report = run.write_validation_report(partitioned_input)
            feature_result = BetFeatureBatchProcess().process(
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
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(
                    batch_size=2,
                    feature_partition_count=4,
                    generated_at=run.validation_generated_at,
                ),
            )
            report = run.write_validation_report(partitioned_input)
            feature_result = BetFeatureBatchProcess().process(
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
