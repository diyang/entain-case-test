from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bet_pipeline.features import BetFeatureBuilder
from bet_pipeline.io import read_parquet, write_csv
from bet_pipeline.schema import EXPECTED_COLUMNS


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

        features = self.builder.build_rows(rows)

        self.assertEqual(len(features), 1)
        feature = features[0]
        self.assertEqual(feature["bets_used"], 20)
        self.assertEqual(feature["total_betting_amount"], 200.0)
        self.assertEqual(feature["mean_betting_amount"], 10.0)
        self.assertEqual(feature["twentieth_bet_datetime"].isoformat(), "2024-08-20T00:00:00")

    def test_customer_percentages_are_computed(self) -> None:
        rows = [
            _row(1, "10", category="racing", stake_type="cash", result="no-return"),
            _row(2, "20", category="sports", stake_type="bonus", result="return"),
        ]
        rows[1]["payout"] = "20"
        rows[1]["return_for_entain"] = "0"

        feature = self.builder.build_rows(rows)[0]

        self.assertEqual(feature["pct_racing"], 0.5)
        self.assertEqual(feature["pct_cash"], 0.5)
        self.assertEqual(feature["pct_return"], 0.5)

    def test_build_feature_batch_excludes_invalid_raw_rows(self) -> None:
        rows = [_row(1, "10"), _row(2, "-5")]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            manifest = self.builder.build_feature_batch(input_path, output_dir, run_id="feature-test-run")
            _, features = read_parquet(output_dir / "features" / "customer_features.parquet")

            self.assertEqual(features[0]["bets_used"], 1)
            self.assertEqual(manifest["validation"]["invalid_rows"], 1)
            self.assertTrue((output_dir / "features" / "feature_report.json").exists())
            self.assertTrue((output_dir / "features" / "customer_features.parquet").exists())
            self.assertFalse((output_dir / "features" / "customer_features.parquet.tmp").exists())
            self.assertEqual(read_parquet(output_dir / "features" / "customer_features.parquet")[0][0], "customer_id")

    def test_invalid_first_twenty_bets_are_not_replaced_by_later_bets(self) -> None:
        rows = [_row(i, "10") for i in range(1, 22)]
        rows[1]["betting_amount"] = "-5"

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            manifest = self.builder.build_feature_batch(input_path, output_dir, run_id="first-20-policy-test")
            _, features = read_parquet(output_dir / "features" / "customer_features.parquet")
            feature_report = json.loads((output_dir / "features" / "feature_report.json").read_text())

            self.assertEqual(features[0]["bets_used"], 19)
            self.assertEqual(features[0]["total_betting_amount"], 190.0)
            self.assertEqual(features[0]["twentieth_bet_datetime"].isoformat(), "2024-08-20T00:00:00")
            self.assertEqual(manifest["features"]["customers_with_incomplete_first_20"], 1)
            self.assertEqual(feature_report["customers_with_incomplete_first_20"], 1)

    def test_build_features_file_can_assume_validated_input(self) -> None:
        rows = [_row(1, "10"), _row(2, "-5")]

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "valid_bets.csv"
            output_dir = tmp_path / "features"
            write_csv(input_path, rows, EXPECTED_COLUMNS)

            features = self.builder.build_file(input_path, output_dir)

            self.assertEqual(features[0]["bets_used"], 2)


if __name__ == "__main__":
    unittest.main()
