from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bet_pipeline.batch import (
    BetFeatureBatchProcess,
    BetValidationBatchProcess,
    RunArtifactPublisher,
    ValidationBatchSettings,
)
from bet_pipeline.features import BetFeatureBuilder
from bet_pipeline.io import read_parquet, write_csv
from bet_pipeline.schema import EXPECTED_COLUMNS
from bet_pipeline.validation import BetValidator, ValidationInput


def _read_feature_rows(run_dir: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    feature_dir = run_dir / "features" / "customer_features"
    for partition_path in sorted(feature_dir.glob("part-*.parquet")):
        _, partition_rows = read_parquet(partition_path)
        rows.extend(partition_rows)
    return rows


def _row(
    bet_num: int, amount: str, category: str = "racing", stake_type: str = "cash", result: str = "no-return"
) -> dict[str, str]:
    payout = "0" if result == "no-return" else amount
    return_for_entain = amount if result == "no-return" and stake_type == "cash" else "0"
    return {
        "bet_id": str(bet_num),
        "customer_id": "00000000-0000-4000-8000-000000000001",
        "bet_datetime": f"2024-08-{bet_num:02d} 00:00:00.000",
        "bet_num": str(bet_num),
        "betting_amount": amount,
        "price": "2",
        "category": category,
        "stake_type": stake_type,
        "bet_result": result,
        "payout": payout,
        "return_for_entain": return_for_entain,
    }


class FeatureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.builder = BetFeatureBuilder()

    def test_customer_features_use_first_twenty_bet_numbers(self) -> None:
        rows = [_row(i, "10") for i in range(1, 22)]

        valid_rows = BetValidator().validate(ValidationInput(EXPECTED_COLUMNS, rows))["valid_rows"]
        features = self.builder.build(valid_rows)

        self.assertEqual(len(features), 1)
        feature = features[0]
        self.assertEqual(feature["bets_used"], 20)
        self.assertEqual(feature["total_betting_amount"], 200.0)
        self.assertEqual(feature["mean_betting_amount"], 10.0)
        self.assertEqual(feature["nth_bet_datetime"].isoformat(), "2024-08-20T00:00:00")

    def test_customer_features_can_use_configured_first_n_bet_numbers(self) -> None:
        rows = [_row(i, "10") for i in range(1, 13)]

        valid_rows = BetValidator().validate(ValidationInput(EXPECTED_COLUMNS, rows))["valid_rows"]
        features = BetFeatureBuilder(first_n_bets=10).build(valid_rows)

        self.assertEqual(features[0]["bets_used"], 10)
        self.assertEqual(features[0]["total_betting_amount"], 100.0)
        self.assertEqual(features[0]["nth_bet_datetime"].isoformat(), "2024-08-10T00:00:00")

    def test_customer_percentages_are_computed(self) -> None:
        rows = [
            _row(1, "10", category="racing", stake_type="cash", result="no-return"),
            _row(2, "20", category="sports", stake_type="bonus", result="return"),
        ]
        rows[1]["payout"] = "20"
        rows[1]["return_for_entain"] = "-20"

        valid_rows = BetValidator().validate(ValidationInput(EXPECTED_COLUMNS, rows))["valid_rows"]
        feature = self.builder.build(valid_rows)[0]

        self.assertEqual(feature["pct_racing"], 0.5)
        self.assertEqual(feature["pct_cash"], 0.5)
        self.assertEqual(feature["pct_return"], 0.5)

    def test_customer_records_split_across_batches_build_one_feature_row(self) -> None:
        customer_a = "00000000-0000-4000-8000-000000000001"
        customer_b = "00000000-0000-4000-8000-000000000002"
        rows = [
            _row(1, "10"),
            _row(1, "20"),
            _row(2, "30"),
            _row(2, "40"),
        ]
        rows[1]["customer_id"] = customer_b
        rows[1]["bet_id"] = "2"
        rows[2]["bet_id"] = "3"
        rows[3]["customer_id"] = customer_b
        rows[3]["bet_id"] = "4"

        valid_rows = BetValidator().validate(ValidationInput(EXPECTED_COLUMNS, rows))["valid_rows"]
        features = self.builder.build(valid_rows)

        self.assertEqual(len(features), 2)
        self.assertEqual(features[0]["customer_id"], customer_a)
        self.assertEqual(features[0]["bets_used"], 2)
        self.assertEqual(features[0]["total_betting_amount"], 40.0)
        self.assertEqual(features[1]["customer_id"], customer_b)
        self.assertEqual(features[1]["bets_used"], 2)
        self.assertEqual(features[1]["total_betting_amount"], 60.0)

    def test_customer_records_out_of_order_across_batches_are_sorted_before_features(self) -> None:
        rows = [_row(i, "10") for i in [1, 2, 3, 4, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 5, 6, 7, 8, 9, 10]]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="out-of-order-batch-test")
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(
                    batch_size=4,
                    feature_partition_count=3,
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
            run.write_manifest(partitioned_input, report, feature_result)
            run.commit(partitioned_input)
            run_dir = output_dir / "runs" / "out-of-order-batch-test"
            features = _read_feature_rows(run_dir)

            self.assertEqual(len(features), 1)
            self.assertEqual(features[0]["bets_used"], 20)
            self.assertEqual(features[0]["total_betting_amount"], 200.0)
            self.assertEqual(features[0]["first_bet_datetime"].isoformat(), "2024-08-01T00:00:00")
            self.assertEqual(features[0]["nth_bet_datetime"].isoformat(), "2024-08-20T00:00:00")

    def test_feature_batch_process_accepts_first_n_bets_argument(self) -> None:
        rows = [_row(i, "10") for i in range(1, 13)]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="first-n-batch-test")
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(generated_at=run.validation_generated_at),
            )
            report = run.write_validation_report(partitioned_input)
            feature_result = BetFeatureBatchProcess().process(
                partitioned_input,
                run.features_dir,
                run.feature_generated_at,
                first_n_bets=10,
            )
            run.write_feature_report(partitioned_input, report, feature_result)
            manifest = run.write_manifest(partitioned_input, report, feature_result)
            run.commit(partitioned_input)
            features = _read_feature_rows(output_dir / "runs" / "first-n-batch-test")

            self.assertEqual(feature_result.first_n_bets, 10)
            self.assertEqual(manifest["features"]["first_n_bets"], 10)
            self.assertEqual(features[0]["bets_used"], 10)
            self.assertEqual(features[0]["total_betting_amount"], 100.0)

    def test_file_processor_excludes_invalid_rows_before_feature_building(self) -> None:
        rows = [_row(1, "10"), _row(2, "-5")]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="feature-test-run")
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(
                    batch_size=1,
                    feature_partition_count=2,
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
            run_dir = output_dir / "runs" / "feature-test-run"
            features = _read_feature_rows(run_dir)

            self.assertEqual(features[0]["bets_used"], 1)
            self.assertEqual(manifest["validation"]["invalid_rows"], 1)
            self.assertEqual(manifest["validation"]["batch_size"], 1)
            self.assertEqual(manifest["validation"]["batches_processed"], 2)
            self.assertEqual(manifest["features"]["batch_size"], 1)
            self.assertTrue((run_dir / "_SUCCESS").exists())
            self.assertTrue((run_dir / "features" / "feature_report.json").exists())
            self.assertTrue((run_dir / "features" / "customer_features" / "part-00000.parquet").exists())
            self.assertFalse((run_dir / "features" / "customer_features" / "part-00000.parquet.tmp").exists())
            self.assertFalse((output_dir / "_staging" / "feature-test-run").exists())
            self.assertEqual(
                read_parquet(run_dir / "features" / "customer_features" / "part-00000.parquet")[0][0],
                "customer_id",
            )

    def test_invalid_first_twenty_bets_are_not_replaced_by_later_bets(self) -> None:
        rows = [_row(i, "10") for i in range(1, 22)]
        rows[1]["betting_amount"] = "-5"

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            run = RunArtifactPublisher(output_dir, run_id="first-n-policy-test")
            partitioned_input = BetValidationBatchProcess().process(
                input_path,
                run.validation_dir,
                ValidationBatchSettings(generated_at=run.validation_generated_at),
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
            run_dir = output_dir / "runs" / "first-n-policy-test"
            features = _read_feature_rows(run_dir)
            feature_report = json.loads((run_dir / "features" / "feature_report.json").read_text())

            self.assertEqual(features[0]["bets_used"], 19)
            self.assertEqual(features[0]["total_betting_amount"], 190.0)
            self.assertEqual(features[0]["nth_bet_datetime"].isoformat(), "2024-08-20T00:00:00")
            self.assertEqual(manifest["features"]["customers_with_incomplete_first_n"], 1)
            self.assertEqual(feature_report["customers_with_incomplete_first_n"], 1)

    def test_feature_builder_uses_validated_batch_data_only(self) -> None:
        rows = [_row(1, "10"), _row(2, "20")]
        valid_rows = BetValidator().validate(ValidationInput(EXPECTED_COLUMNS, rows))["valid_rows"]

        features = self.builder.build(valid_rows)

        self.assertEqual(features[0]["bets_used"], 2)

    def test_batch_size_must_be_positive(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            write_csv(input_path, [_row(1, "10")], EXPECTED_COLUMNS)

            with self.assertRaises(ValueError):
                BetValidationBatchProcess().process(
                    input_path,
                    tmp_path / "outputs",
                    ValidationBatchSettings(batch_size=0),
                )


if __name__ == "__main__":
    unittest.main()
