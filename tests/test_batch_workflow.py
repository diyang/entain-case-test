from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bet_pipeline.features import BetFeatureBuilder
from bet_pipeline.io import write_csv
from bet_pipeline.schema import EXPECTED_COLUMNS
from bet_pipeline.validation import BetValidator


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
    def test_validate_batch_writes_validation_outputs(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "validation"
            write_csv(input_path, [_row("1", "1")], EXPECTED_COLUMNS)

            result = BetValidator().validate_file(input_path, output_dir, run_id="validation-batch-test")

            self.assertEqual(result["report"]["run_id"], "validation-batch-test")
            self.assertTrue((output_dir / "valid_bets.parquet").exists())

    def test_feature_batch_writes_manifest_validation_and_features(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "outputs"
            write_csv(input_path, [_row("1", "1"), _row("2", "2", amount="-1")], EXPECTED_COLUMNS)

            manifest = BetFeatureBuilder().build_feature_batch(input_path, output_dir, run_id="platform-test-run")

            self.assertEqual(manifest["run_id"], "platform-test-run")
            self.assertEqual(manifest["status"], "success")
            self.assertEqual(manifest["validation"]["invalid_rows"], 1)
            self.assertEqual(manifest["features"]["customers"], 1)
            self.assertTrue((output_dir / "run_manifest.json").exists())
            self.assertTrue((output_dir / "validation" / "valid_bets.parquet").exists())
            self.assertTrue((output_dir / "validation" / "invalid_bets.parquet").exists())
            self.assertTrue((output_dir / "features" / "customer_features.parquet").exists())


if __name__ == "__main__":
    unittest.main()
