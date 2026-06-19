from __future__ import annotations

from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from bet_pipeline.batch_process.run_artifacts import PartitionedInput, RawBetPartitionedInput, parquet_partition_file
from bet_pipeline.io import read_parquet, write_parquet
from bet_pipeline.schema import EXPECTED_COLUMNS, INVALID_BETS_SCHEMA, VALID_BETS_SCHEMA
from bet_pipeline.validation import BetValidator, ValidationInput


@dataclass(frozen=True)
class ValidationBatchSettings:
    validation_worker_count: int = 1
    generated_at: str | None = None


@dataclass(frozen=True)
class ValidationPartitionConfig:
    partition_index: int
    raw_partition_path: Path
    validation_dir: Path
    fieldnames: list[str]
    bet_id_first_source_rows: dict[str, int]
    generated_at: str


@dataclass(frozen=True)
class ValidationPartitionResult:
    valid_rows: int
    invalid_rows: int
    failure_counts_by_rule: dict[str, int]


class BetValidationPartitionWorker:
    """Validate one customer-complete raw partition and publish valid and invalid bets."""

    def __init__(
        self,
        config: ValidationPartitionConfig,
        validator: BetValidator | None = None,
    ) -> None:
        self.config = config
        self.validator = BetValidator() if validator is None else validator
        self.generated_at = datetime.fromisoformat(config.generated_at).astimezone(timezone.utc)

    def process(self) -> ValidationPartitionResult:
        _, raw_rows = read_parquet(self.config.raw_partition_path)
        validation_rows = self._validation_rows(raw_rows)
        row_level_result = self.validator.validate_rows(
            ValidationInput(
                fieldnames=self.config.fieldnames,
                rows=validation_rows,
                generated_at=self.config.generated_at,
            )
        )
        validation_result = self._apply_partition_errors(row_level_result, validation_rows)

        write_parquet(self._valid_partition_path(), validation_result["valid_rows"], VALID_BETS_SCHEMA)
        write_parquet(self._invalid_partition_path(), validation_result["invalid_rows"], INVALID_BETS_SCHEMA)

        return ValidationPartitionResult(
            valid_rows=len(validation_result["valid_rows"]),
            invalid_rows=len(validation_result["invalid_rows"]),
            failure_counts_by_rule=validation_result["report"]["failure_counts_by_rule"],
        )

    def _apply_partition_errors(self, validation_result: dict, rows: list[dict[str, str]]) -> dict:
        valid_rows: list[dict[str, object]] = []
        invalid_rows: list[dict[str, object]] = []
        failure_counts = Counter(validation_result["report"]["failure_counts_by_rule"])
        customer_bet_num_first_rows = self._customer_bet_num_first_rows(rows)
        affected_sequence_customers = self._customers_with_sequence_gaps(rows)

        for row_status, record in self._source_ordered_records(validation_result):
            errors = self._partition_errors(record, customer_bet_num_first_rows, affected_sequence_customers)
            if row_status == "invalid":
                if errors:
                    failure_counts.update(errors)
                invalid_rows.append(self._invalid_record_with_extra_errors(record, errors))
            elif errors:
                failure_counts.update(errors)
                invalid_rows.append(self._invalid_record(record, errors))
            else:
                valid_rows.append(record)

        return {
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "report": {
                **validation_result["report"],
                "valid_rows": len(valid_rows),
                "invalid_rows": len(invalid_rows),
                "failure_counts_by_rule": dict(sorted(failure_counts.items())),
            },
        }

    def _source_ordered_records(self, validation_result: dict) -> list[tuple[str, dict[str, object]]]:
        records = [("valid", record) for record in validation_result["valid_rows"]]
        records.extend(("invalid", record) for record in validation_result["invalid_rows"])
        return sorted(records, key=lambda item: self._source_row_number(item[1]))

    def _partition_errors(
        self,
        record: dict[str, object],
        customer_bet_num_first_rows: dict[tuple[str, int], int],
        affected_sequence_customers: set[str],
    ) -> list[str]:
        errors: list[str] = []
        source_row_number = self._source_row_number(record)
        bet_id = self._string_value(record.get("bet_id"))
        customer_id = self._string_value(record.get("customer_id"))
        bet_num = self._int_value(record.get("bet_num"))

        if bet_id and self.config.bet_id_first_source_rows.get(bet_id) != source_row_number:
            errors.append("bet_id_unique")

        if customer_id and bet_num is not None:
            if customer_bet_num_first_rows.get((customer_id, bet_num)) != source_row_number:
                errors.append("customer_bet_num_unique")
            if bet_num > 0 and customer_id in affected_sequence_customers:
                errors.append("customer_bet_num_sequence")

        return errors

    def _customer_bet_num_first_rows(self, rows: list[dict[str, str]]) -> dict[tuple[str, int], int]:
        first_rows: dict[tuple[str, int], int] = {}
        for row in rows:
            customer_id = self._string_value(row.get("customer_id"))
            bet_num = self._int_value(row.get("bet_num"))
            if customer_id and bet_num is not None:
                first_rows.setdefault((customer_id, bet_num), self._source_row_number(row))
        return first_rows

    def _customers_with_sequence_gaps(self, rows: list[dict[str, str]]) -> set[str]:
        customer_bet_nums: dict[str, set[int]] = {}
        for row in rows:
            customer_id = self._string_value(row.get("customer_id"))
            bet_num = self._int_value(row.get("bet_num"))
            if customer_id and bet_num is not None and bet_num > 0:
                customer_bet_nums.setdefault(customer_id, set()).add(bet_num)

        affected_customers = set()
        for customer_id, bet_nums in customer_bet_nums.items():
            ordered_bet_nums = sorted(bet_nums)
            if ordered_bet_nums != list(range(1, ordered_bet_nums[-1] + 1)):
                affected_customers.add(customer_id)
        return affected_customers

    def _invalid_record(self, record: dict[str, object], errors: list[str]) -> dict[str, object]:
        return {
            **{column: self._string_value(record.get(column)) for column in EXPECTED_COLUMNS},
            "source_row_number": self._source_row_number(record),
            "validation_errors": "|".join(sorted(set(errors))),
            "validated_at": self.generated_at,
        }

    def _invalid_record_with_extra_errors(self, record: dict[str, object], errors: list[str]) -> dict[str, object]:
        if not errors:
            return record
        existing_errors = str(record.get("validation_errors", "")).split("|")
        validation_errors = sorted(error for error in {*existing_errors, *errors} if error)
        return {
            **record,
            "validation_errors": "|".join(validation_errors),
        }

    def _source_row_number(self, record: dict[str, object]) -> int:
        parsed_value = self._int_value(record.get("source_row_number"))
        if parsed_value is None:
            parsed_value = self._int_value(record.get("_source_row_number"))
        if parsed_value is None:
            return -1
        return parsed_value

    def _int_value(self, value: object) -> int | None:
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    def _string_value(self, value: object) -> str:
        if value is None:
            return ""
        return str(value)

    def _validation_rows(self, raw_rows: list[dict[str, object]]) -> list[dict[str, str]]:
        rows = []
        for raw_row in raw_rows:
            row = {key: "" if value is None else str(value) for key, value in raw_row.items()}
            if "source_row_number" in row:
                row["_source_row_number"] = row["source_row_number"]
            rows.append(row)
        return rows

    def _valid_partition_path(self) -> Path:
        return parquet_partition_file(self.config.validation_dir, "valid_bets", self.config.partition_index)

    def _invalid_partition_path(self) -> Path:
        return parquet_partition_file(self.config.validation_dir, "invalid_bets", self.config.partition_index)


class BetValidationPartitionBatchProcess:
    """Run validation over customer-complete raw partitions."""

    def __init__(self, validator: BetValidator | None = None) -> None:
        self.validator = validator

    def process(
        self,
        raw_partitioned_input: RawBetPartitionedInput,
        validation_dir: str | Path,
        settings: ValidationBatchSettings | None = None,
    ) -> PartitionedInput:
        if settings is None:
            settings = ValidationBatchSettings()
        if settings.validation_worker_count < 1:
            raise ValueError("validation_worker_count must be greater than 0")

        generated_at = settings.generated_at
        if generated_at is None:
            generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

        validation_dir = Path(validation_dir)
        if settings.validation_worker_count == 1:
            results = [
                self._worker(raw_partitioned_input, validation_dir, generated_at, partition_index, raw_path).process()
                for partition_index, raw_path in enumerate(raw_partitioned_input.partition_paths)
            ]
        else:
            results = self._concurrent_results(
                raw_partitioned_input,
                validation_dir,
                generated_at,
                settings.validation_worker_count,
            )

        failure_counts: Counter[str] = Counter()
        for result in results:
            failure_counts.update(result.failure_counts_by_rule)

        return PartitionedInput(
            fieldnames=raw_partitioned_input.fieldnames,
            partition_paths=[
                parquet_partition_file(validation_dir, "valid_bets", partition_index)
                for partition_index in range(raw_partitioned_input.feature_partition_count)
            ],
            invalid_partition_paths=[
                parquet_partition_file(validation_dir, "invalid_bets", partition_index)
                for partition_index in range(raw_partitioned_input.feature_partition_count)
            ],
            total_rows=raw_partitioned_input.total_rows,
            batches_processed=raw_partitioned_input.batches_processed,
            valid_rows=sum(result.valid_rows for result in results),
            invalid_rows=sum(result.invalid_rows for result in results),
            failure_counts_by_rule=dict(sorted(failure_counts.items())),
            source_path=raw_partitioned_input.source_path,
            source_fingerprint=raw_partitioned_input.source_fingerprint,
            batch_size=raw_partitioned_input.batch_size,
            feature_partition_count=raw_partitioned_input.feature_partition_count,
        )

    def _concurrent_results(
        self,
        raw_partitioned_input: RawBetPartitionedInput,
        validation_dir: Path,
        generated_at: str,
        worker_count: int,
    ) -> list[ValidationPartitionResult]:
        pending: deque[Future[ValidationPartitionResult]] = deque()
        results: list[ValidationPartitionResult] = []
        max_pending = worker_count * 2

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for partition_index, raw_path in enumerate(raw_partitioned_input.partition_paths):
                worker = self._worker(raw_partitioned_input, validation_dir, generated_at, partition_index, raw_path)
                pending.append(executor.submit(worker.process))

                if len(pending) >= max_pending:
                    results.append(pending.popleft().result())

            while pending:
                results.append(pending.popleft().result())

        return results

    def _worker(
        self,
        raw_partitioned_input: RawBetPartitionedInput,
        validation_dir: Path,
        generated_at: str,
        partition_index: int,
        raw_path: Path,
    ) -> BetValidationPartitionWorker:
        return BetValidationPartitionWorker(
            ValidationPartitionConfig(
                partition_index=partition_index,
                raw_partition_path=raw_path,
                validation_dir=validation_dir,
                fieldnames=raw_partitioned_input.fieldnames,
                bet_id_first_source_rows=raw_partitioned_input.bet_id_first_source_rows,
                generated_at=generated_at,
            ),
            self.validator,
        )
