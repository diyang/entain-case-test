from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Sequence, Set
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import UUID

from bet_pipeline.schema import (
    ALLOWED_BET_RESULTS,
    ALLOWED_CATEGORIES,
    ALLOWED_STAKE_TYPES,
    EXPECTED_COLUMNS,
    SCHEMA_VERSION,
)

DATETIME_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
DEFAULT_TOLERANCE = Decimal("0.00001")


@dataclass(frozen=True)
class ValidationInput:
    fieldnames: Sequence[str]
    rows: list[dict[str, str]]
    run_id: str | None = None
    source_row_start: int = 2
    bet_id_counts: Counter[str] | None = None
    generated_at: str | None = None


class BetValidator:
    """Validate raw betting rows."""

    def __init__(
        self,
        expected_columns: Sequence[str] = EXPECTED_COLUMNS,
        allowed_categories: Set[str] = ALLOWED_CATEGORIES,
        allowed_stake_types: Set[str] = ALLOWED_STAKE_TYPES,
        allowed_bet_results: Set[str] = ALLOWED_BET_RESULTS,
        tolerance: Decimal = DEFAULT_TOLERANCE,
    ) -> None:
        self.expected_columns = expected_columns
        self.allowed_categories = allowed_categories
        self.allowed_stake_types = allowed_stake_types
        self.allowed_bet_results = allowed_bet_results
        self.tolerance = tolerance
        self.schema_version = SCHEMA_VERSION

    def validate(self, validation_input: ValidationInput) -> dict:
        bet_id_counts = Counter(row.get("bet_id", "") for row in validation_input.rows)
        if validation_input.bet_id_counts is not None:
            bet_id_counts = validation_input.bet_id_counts
        customer_bet_nums = self._customer_bet_num_counts(validation_input.rows)
        customer_bet_num_gaps = self._customer_bet_num_gaps(validation_input.rows)

        return self._validate_with_errors(
            validation_input,
            lambda row, missing_columns: self._row_errors(
                row,
                missing_columns,
                bet_id_counts,
                customer_bet_nums,
                customer_bet_num_gaps,
            ),
        )

    def validate_rows(self, validation_input: ValidationInput) -> dict:
        return self._validate_with_errors(validation_input, self._row_level_errors)

    def _validate_with_errors(
        self,
        validation_input: ValidationInput,
        row_error_builder: Callable[[dict[str, str], Sequence[str]], list[str]],
    ) -> dict:
        generated_at = validation_input.generated_at
        if generated_at is None:
            generated_at = self._now_iso()
        run_id = validation_input.run_id
        if run_id is None:
            run_id = generated_at.replace(":", "").replace("+", "Z")
        missing_columns = [column for column in self.expected_columns if column not in validation_input.fieldnames]
        extra_columns = [column for column in validation_input.fieldnames if column not in self.expected_columns]

        valid_rows: list[dict[str, object]] = []
        invalid_rows: list[dict[str, object]] = []
        failure_counts: Counter[str] = Counter()
        row_number = validation_input.source_row_start

        for row in validation_input.rows:
            errors = row_error_builder(row, missing_columns)
            source_row_number = self._source_row_number(row, row_number)
            if errors:
                failure_counts.update(errors)
                invalid_rows.append(self._typed_invalid_row(row, source_row_number, errors, generated_at))
            else:
                valid_rows.append(self._typed_valid_row(row))
            row_number += 1

        return {
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "report": {
                "run_id": run_id,
                "generated_at": generated_at,
                "schema_version": self.schema_version,
                "total_rows": len(validation_input.rows),
                "valid_rows": len(valid_rows),
                "invalid_rows": len(invalid_rows),
                "missing_columns": missing_columns,
                "extra_columns": extra_columns,
                "failure_counts_by_rule": dict(sorted(failure_counts.items())),
            },
        }

    def _row_errors(
        self,
        row: dict[str, str],
        missing_columns: Sequence[str],
        bet_id_counts: Counter[str],
        customer_bet_nums: dict[str, Counter[str]],
        customer_bet_num_gaps: set[tuple[str, str]],
    ) -> list[str]:
        amount = self._to_decimal(row.get("betting_amount", ""))
        price = self._to_decimal(row.get("price", ""))
        payout = self._to_decimal(row.get("payout", ""))
        return_for_entain = self._to_decimal(row.get("return_for_entain", ""))

        return (
            self._presence_errors(row, missing_columns)
            + self._identity_errors(row, bet_id_counts, customer_bet_nums, customer_bet_num_gaps)
            + self._value_errors(row, amount, price, payout, return_for_entain)
            + self._formula_errors(row, amount, price, payout, return_for_entain)
        )

    def _row_level_errors(self, row: dict[str, str], missing_columns: Sequence[str]) -> list[str]:
        amount = self._to_decimal(row.get("betting_amount", ""))
        price = self._to_decimal(row.get("price", ""))
        payout = self._to_decimal(row.get("payout", ""))
        return_for_entain = self._to_decimal(row.get("return_for_entain", ""))

        return (
            self._presence_errors(row, missing_columns)
            + self._row_identity_errors(row)
            + self._value_errors(row, amount, price, payout, return_for_entain)
            + self._formula_errors(row, amount, price, payout, return_for_entain)
        )

    def _presence_errors(self, row: dict[str, str], missing_columns: Sequence[str]) -> list[str]:
        errors = [f"missing_column:{column}" for column in missing_columns]
        errors.extend(
            f"missing_value:{column}" for column in self.expected_columns if column in row and row[column] == ""
        )
        return errors

    def _identity_errors(
        self,
        row: dict[str, str],
        bet_id_counts: Counter[str],
        customer_bet_nums: dict[str, Counter[str]],
        customer_bet_num_gaps: set[tuple[str, str]],
    ) -> list[str]:
        errors: list[str] = []
        bet_id = row.get("bet_id", "")
        customer_id = row.get("customer_id", "")
        bet_num_value = row.get("bet_num", "")
        bet_num = self._to_int(bet_num_value)

        if self._to_int(bet_id) is None:
            errors.append("bet_id_integer")
        elif bet_id_counts[bet_id] > 1:
            errors.append("bet_id_unique")

        if not self._is_uuid(customer_id):
            errors.append("customer_id_uuid")

        if not self._is_datetime(row.get("bet_datetime", "")):
            errors.append("bet_datetime_parseable")

        if bet_num is None:
            errors.append("bet_num_integer")
        elif bet_num < 1:
            errors.append("bet_num_positive")
        elif customer_bet_nums[customer_id][bet_num_value] > 1:
            errors.append("customer_bet_num_unique")
        elif (customer_id, bet_num_value) in customer_bet_num_gaps:
            errors.append("customer_bet_num_sequence")

        return errors

    def _row_identity_errors(self, row: dict[str, str]) -> list[str]:
        errors: list[str] = []
        bet_id = row.get("bet_id", "")
        customer_id = row.get("customer_id", "")
        bet_num_value = row.get("bet_num", "")
        bet_num = self._to_int(bet_num_value)

        if self._to_int(bet_id) is None:
            errors.append("bet_id_integer")

        if not self._is_uuid(customer_id):
            errors.append("customer_id_uuid")

        if not self._is_datetime(row.get("bet_datetime", "")):
            errors.append("bet_datetime_parseable")

        if bet_num is None:
            errors.append("bet_num_integer")
        elif bet_num < 1:
            errors.append("bet_num_positive")

        return errors

    def _value_errors(
        self,
        row: dict[str, str],
        amount: Decimal | None,
        price: Decimal | None,
        payout: Decimal | None,
        return_for_entain: Decimal | None,
    ) -> list[str]:
        errors: list[str] = []

        if amount is None:
            errors.append("betting_amount_numeric")
        elif amount <= 0:
            errors.append("betting_amount_gt_0")

        if price is None:
            errors.append("price_numeric")
        elif price <= 1:
            errors.append("price_gt_1")

        if payout is None:
            errors.append("payout_numeric")

        if return_for_entain is None:
            errors.append("return_for_entain_numeric")

        if row.get("category") not in self.allowed_categories:
            errors.append("category_domain")
        if row.get("stake_type") not in self.allowed_stake_types:
            errors.append("stake_type_domain")
        if row.get("bet_result") not in self.allowed_bet_results:
            errors.append("bet_result_domain")

        return errors

    def _formula_errors(
        self,
        row: dict[str, str],
        amount: Decimal | None,
        price: Decimal | None,
        payout: Decimal | None,
        return_for_entain: Decimal | None,
    ) -> list[str]:
        errors: list[str] = []

        if (
            {"bet_result", "stake_type"}.issubset(row)
            and amount is not None
            and price is not None
            and payout is not None
        ):
            expected_payout = self._expected_payout(row, amount, price)
            if expected_payout is None:
                errors.append("payout_rule_not_evaluable")
            elif not self._decimal_equal(payout, expected_payout):
                errors.append("payout_formula")

        if (
            {"bet_result", "stake_type"}.issubset(row)
            and amount is not None
            and payout is not None
            and return_for_entain is not None
        ):
            expected_return = self._expected_return_for_entain(row, amount, payout)
            if expected_return is None:
                errors.append("return_for_entain_rule_not_evaluable")
            elif not self._decimal_equal(return_for_entain, expected_return):
                errors.append("return_for_entain_formula")

        return errors

    def _expected_payout(self, row: dict[str, str], amount: Decimal, price: Decimal) -> Decimal | None:
        bet_result = row.get("bet_result")
        stake_type = row.get("stake_type")
        if bet_result == "no-return":
            return Decimal("0")
        if bet_result == "return" and stake_type == "cash":
            return amount * price
        if bet_result == "return" and stake_type == "bonus":
            return amount * (price - Decimal("1"))
        return None

    def _expected_return_for_entain(self, row: dict[str, str], amount: Decimal, payout: Decimal) -> Decimal | None:
        bet_result = row.get("bet_result")
        stake_type = row.get("stake_type")
        if bet_result == "no-return" and stake_type == "cash":
            return amount
        if bet_result == "no-return" and stake_type == "bonus":
            return Decimal("0")
        if bet_result == "return" and stake_type == "cash":
            return amount - payout
        if bet_result == "return" and stake_type == "bonus":
            return -payout
        return None

    def _customer_bet_num_counts(self, rows: list[dict[str, str]]) -> dict[str, Counter[str]]:
        customer_bet_nums: dict[str, Counter[str]] = defaultdict(Counter)
        for row in rows:
            customer_bet_nums[row.get("customer_id", "")][row.get("bet_num", "")] += 1
        return customer_bet_nums

    def _customer_bet_num_gaps(self, rows: list[dict[str, str]]) -> set[tuple[str, str]]:
        by_customer: dict[str, list[int]] = defaultdict(list)
        for row in rows:
            bet_num = self._to_int(row.get("bet_num", ""))
            if bet_num is not None:
                by_customer[row.get("customer_id", "")].append(bet_num)

        affected: set[tuple[str, str]] = set()
        for customer_id, bet_nums in by_customer.items():
            unique_bet_nums = sorted(set(bet_nums))
            if unique_bet_nums and unique_bet_nums != list(range(1, unique_bet_nums[-1] + 1)):
                affected.update((customer_id, str(bet_num)) for bet_num in unique_bet_nums)
        return affected

    def _decimal_equal(self, left: Decimal, right: Decimal) -> bool:
        return abs(left - right) <= self.tolerance

    def _typed_valid_row(self, row: dict[str, str]) -> dict[str, object]:
        return {
            "bet_id": int(row["bet_id"]),
            "customer_id": row["customer_id"],
            "bet_datetime": self._parse_datetime(row["bet_datetime"]),
            "bet_num": int(row["bet_num"]),
            "betting_amount": float(row["betting_amount"]),
            "price": float(row["price"]),
            "category": row["category"],
            "stake_type": row["stake_type"],
            "bet_result": row["bet_result"],
            "payout": float(row["payout"]),
            "return_for_entain": float(row["return_for_entain"]),
            "source_row_number": self._source_row_number(row, -1),
        }

    def _typed_invalid_row(
        self, row: dict[str, str], source_row_number: int, errors: list[str], generated_at: str
    ) -> dict[str, object]:
        return {
            **{column: row.get(column, "") for column in self.expected_columns},
            "source_row_number": source_row_number,
            "validation_errors": "|".join(sorted(set(errors))),
            "validated_at": self._parse_utc_datetime(generated_at),
        }

    def _source_row_number(self, row: dict[str, str], fallback: int) -> int:
        value = row.get("_source_row_number")
        if value is None:
            return fallback
        parsed = self._to_int(value)
        if parsed is None:
            return fallback
        return parsed

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _to_decimal(self, value: str) -> Decimal | None:
        try:
            return Decimal(value)
        except (InvalidOperation, TypeError):
            return None

    def _to_int(self, value: str) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _is_uuid(self, value: str) -> bool:
        try:
            UUID(value)
            return True
        except (TypeError, ValueError):
            return False

    def _is_datetime(self, value: str) -> bool:
        return self._parse_datetime_or_none(value) is not None

    def _parse_datetime(self, value: str) -> datetime:
        parsed = self._parse_datetime_or_none(value)
        if parsed is None:
            raise ValueError(f"Invalid datetime: {value}")
        return parsed

    def _parse_datetime_or_none(self, value: str) -> datetime | None:
        if not value:
            return None
        for fmt in DATETIME_FORMATS:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _parse_utc_datetime(self, value: str) -> datetime:
        return datetime.fromisoformat(value).astimezone(timezone.utc)
