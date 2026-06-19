from __future__ import annotations

import unittest

from bet_pipeline.schema import EXPECTED_COLUMNS
from bet_pipeline.validation import BetValidator, ValidationInput


def _row(**overrides: str) -> dict[str, str]:
    row = {
        "bet_id": "1",
        "customer_id": "00000000-0000-4000-8000-000000000001",
        "bet_datetime": "2024-08-01 00:00:00.000",
        "bet_num": "1",
        "betting_amount": "10",
        "price": "3",
        "category": "sports",
        "stake_type": "cash",
        "bet_result": "return",
        "payout": "30",
        "return_for_entain": "-20",
    }
    row.update(overrides)
    return row


class ValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validator = BetValidator()

    def test_valid_cash_return_row_passes(self) -> None:
        result = self.validator.validate(ValidationInput(EXPECTED_COLUMNS, [_row()], run_id="test-run"))

        self.assertEqual(result["report"]["valid_rows"], 1)
        self.assertEqual(result["report"]["invalid_rows"], 0)
        self.assertEqual(result["report"]["run_id"], "test-run")
        self.assertEqual(result["report"]["schema_version"], "bets-v1")

    def test_invalid_amount_and_price_are_reported(self) -> None:
        result = self.validator.validate(
            ValidationInput(
                EXPECTED_COLUMNS, [_row(betting_amount="-1", price="1", payout="-1", return_for_entain="0")]
            )
        )

        errors = result["invalid_rows"][0]["validation_errors"]
        self.assertIn("betting_amount_gt_0", errors)
        self.assertIn("price_gt_1", errors)
        self.assertEqual(result["report"]["failure_counts_by_rule"]["betting_amount_gt_0"], 1)

    def test_bonus_return_formula(self) -> None:
        result = self.validator.validate(
            ValidationInput(
                EXPECTED_COLUMNS,
                [
                    _row(
                        stake_type="bonus",
                        betting_amount="10",
                        price="3",
                        payout="20",
                        return_for_entain="-20",
                    )
                ],
            )
        )

        self.assertEqual(result["report"]["valid_rows"], 1)

    def test_all_business_formula_branches_are_valid(self) -> None:
        rows = [
            _row(
                bet_id="1",
                bet_num="1",
                betting_amount="10",
                price="3",
                stake_type="cash",
                bet_result="no-return",
                payout="0",
                return_for_entain="10",
            ),
            _row(
                bet_id="2",
                bet_num="2",
                betting_amount="10",
                price="3",
                stake_type="bonus",
                bet_result="no-return",
                payout="0",
                return_for_entain="0",
            ),
            _row(
                bet_id="3",
                bet_num="3",
                betting_amount="10",
                price="3",
                stake_type="cash",
                bet_result="return",
                payout="30",
                return_for_entain="-20",
            ),
            _row(
                bet_id="4",
                bet_num="4",
                betting_amount="10",
                price="3",
                stake_type="bonus",
                bet_result="return",
                payout="20",
                return_for_entain="-20",
            ),
        ]

        result = self.validator.validate(ValidationInput(EXPECTED_COLUMNS, rows))

        self.assertEqual(result["report"]["valid_rows"], 4)
        self.assertEqual(result["report"]["invalid_rows"], 0)

    def test_domain_rules_are_enforced(self) -> None:
        result = self.validator.validate(
            ValidationInput(
                EXPECTED_COLUMNS,
                [
                    _row(
                        category="casino",
                        stake_type="free",
                        bet_result="void",
                        payout="0",
                        return_for_entain="0",
                    )
                ],
            )
        )

        errors = result["invalid_rows"][0]["validation_errors"]
        self.assertIn("category_domain", errors)
        self.assertIn("stake_type_domain", errors)
        self.assertIn("bet_result_domain", errors)

    def test_bad_return_for_entain_formula_is_invalid(self) -> None:
        result = self.validator.validate(ValidationInput(EXPECTED_COLUMNS, [_row(return_for_entain="-19")]))

        self.assertEqual(result["report"]["invalid_rows"], 1)
        self.assertIn("return_for_entain_formula", result["invalid_rows"][0]["validation_errors"])

    def test_bad_payout_formula_is_invalid(self) -> None:
        result = self.validator.validate(
            ValidationInput(EXPECTED_COLUMNS, [_row(payout="29", return_for_entain="-19")])
        )

        self.assertEqual(result["report"]["invalid_rows"], 1)
        self.assertIn("payout_formula", result["invalid_rows"][0]["validation_errors"])

    def test_missing_formula_columns_do_not_crash_validation(self) -> None:
        fieldnames = [column for column in EXPECTED_COLUMNS if column != "bet_result"]
        row = _row()
        del row["bet_result"]

        result = self.validator.validate(ValidationInput(fieldnames, [row]))

        self.assertEqual(result["report"]["invalid_rows"], 1)
        self.assertIn("missing_column:bet_result", result["invalid_rows"][0]["validation_errors"])

    def test_duplicate_customer_bet_num_is_invalid(self) -> None:
        result = self.validator.validate(ValidationInput(EXPECTED_COLUMNS, [_row(bet_id="1"), _row(bet_id="2")]))

        self.assertEqual(result["report"]["invalid_rows"], 2)
        self.assertEqual(result["report"]["failure_counts_by_rule"]["customer_bet_num_unique"], 2)

    def test_customer_id_must_be_uuid(self) -> None:
        result = self.validator.validate(ValidationInput(EXPECTED_COLUMNS, [_row(customer_id="customer-1")]))

        self.assertEqual(result["report"]["invalid_rows"], 1)
        self.assertIn("customer_id_uuid", result["invalid_rows"][0]["validation_errors"])

    def test_bet_num_sequence_must_start_at_one_without_gaps(self) -> None:
        result = self.validator.validate(ValidationInput(EXPECTED_COLUMNS, [_row(bet_id="2", bet_num="2")]))

        self.assertEqual(result["report"]["invalid_rows"], 1)
        self.assertIn("customer_bet_num_sequence", result["invalid_rows"][0]["validation_errors"])


if __name__ == "__main__":
    unittest.main()
