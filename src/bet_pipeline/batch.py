from __future__ import annotations

import hashlib
import os
import shutil
from collections import Counter, deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from bet_pipeline.features import FEATURE_COLUMNS, VALIDATED_INPUT_POLICY, BetFeatureBuilder
from bet_pipeline.io import (
    ParquetBatchWriter,
    ensure_dir,
    iter_csv_batches,
    read_csv_fieldnames,
    read_parquet,
    write_json,
    write_parquet,
)
from bet_pipeline.schema import (
    CUSTOMER_FEATURES_SCHEMA,
    EXPECTED_COLUMNS,
    FEATURE_SET_VERSION,
    INVALID_BETS_SCHEMA,
    SCHEMA_VERSION,
    VALID_BETS_SCHEMA,
)
from bet_pipeline.validation import BetValidator, ValidationInput

SOURCE_ROW_NUMBER = "_source_row_number"
DEFAULT_BATCH_ROWS = 1000
FIRST_N_BETS = 20


def parquet_partition_file(root_dir: Path, dataset_name: str, partition_index: int) -> Path:
    return root_dir / dataset_name / f"part-{partition_index:05d}.parquet"


def source_file_snapshot(path: str | Path) -> dict[str, object]:
    source = Path(path)
    stat = source.stat()
    return {
        "path": str(source),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


@dataclass(frozen=True)
class PartitionedInput:
    fieldnames: list[str]
    partition_paths: list[Path]
    invalid_partition_paths: list[Path]
    total_rows: int
    batches_processed: int
    valid_rows: int
    invalid_rows: int
    failure_counts_by_rule: dict[str, int]
    source_path: str
    source_fingerprint: dict[str, object]
    batch_size: int
    feature_partition_count: int


@dataclass(frozen=True)
class FeaturePartitionConfig:
    partition_index: int
    partition_path: Path
    features_dir: Path
    feature_generated_at: datetime
    first_n_bets: int


@dataclass(frozen=True)
class FeatureBatchResult:
    feature_count: int
    incomplete_first_n_count: int
    first_n_bets: int
    feature_worker_count: int = 1


@dataclass(frozen=True)
class ValidationRowBatchConfig:
    fieldnames: list[str]
    rows: list[dict[str, str]]
    source_row_start: int
    generated_at: str


@dataclass(frozen=True)
class ValidationBatchSettings:
    batch_size: int = DEFAULT_BATCH_ROWS
    validation_worker_count: int = 1
    feature_partition_count: int | None = None
    target_feature_partition_rows: int = DEFAULT_BATCH_ROWS
    generated_at: str | None = None


class ValidationPartitionWriters:
    """Own parquet writers for validation output partitions."""

    def __init__(self, stack: ExitStack, validation_dir: str | Path) -> None:
        self.stack = stack
        self.validation_dir = ensure_dir(validation_dir)
        self.valid_writers: dict[int, ParquetBatchWriter] = {}
        self.invalid_writers: dict[int, ParquetBatchWriter] = {}

    def write_valid(self, partition_index: int, rows: list[dict[str, object]]) -> None:
        if rows:
            self._valid_writer(partition_index).write(rows)

    def write_invalid(self, partition_index: int, rows: list[dict[str, object]]) -> None:
        if rows:
            self._invalid_writer(partition_index).write(rows)

    def ensure_partition_files(self, partition_count: int) -> None:
        for partition_index in range(partition_count):
            self._valid_writer(partition_index)
            self._invalid_writer(partition_index)

    def valid_partition_paths(self, partition_count: int) -> list[Path]:
        return [
            parquet_partition_file(self.validation_dir, "valid_bets", partition_index)
            for partition_index in range(partition_count)
        ]

    def invalid_partition_paths(self, partition_count: int) -> list[Path]:
        return [
            parquet_partition_file(self.validation_dir, "invalid_bets", partition_index)
            for partition_index in range(partition_count)
        ]

    def _valid_writer(self, partition_index: int) -> ParquetBatchWriter:
        if partition_index not in self.valid_writers:
            self.valid_writers[partition_index] = self.stack.enter_context(
                ParquetBatchWriter(
                    parquet_partition_file(self.validation_dir, "valid_bets", partition_index),
                    VALID_BETS_SCHEMA,
                )
            )
        return self.valid_writers[partition_index]

    def _invalid_writer(self, partition_index: int) -> ParquetBatchWriter:
        if partition_index not in self.invalid_writers:
            self.invalid_writers[partition_index] = self.stack.enter_context(
                ParquetBatchWriter(
                    parquet_partition_file(self.validation_dir, "invalid_bets", partition_index),
                    INVALID_BETS_SCHEMA,
                )
            )
        return self.invalid_writers[partition_index]


class CustomerPartitionRouter:
    """Assign customer ids to customer-complete partitions during validation."""

    def __init__(self, feature_partition_count: int | None, target_partition_rows: int) -> None:
        if feature_partition_count is not None and feature_partition_count < 1:
            raise ValueError("feature_partition_count must be greater than 0")
        if target_partition_rows < 1:
            raise ValueError("target_feature_partition_rows must be greater than 0")

        self.feature_partition_count = feature_partition_count
        self.target_partition_rows = target_partition_rows
        self.customer_partitions: dict[str, int] = {}
        self.partition_row_counts: Counter[int] = Counter()
        self.current_dynamic_partition = 0

    def partition_for_customer(self, customer_id: str) -> int:
        if self.feature_partition_count is not None:
            return self._hash_partition(customer_id)

        if customer_id not in self.customer_partitions:
            if self.partition_row_counts[self.current_dynamic_partition] >= self.target_partition_rows:
                self.current_dynamic_partition += 1
            self.customer_partitions[customer_id] = self.current_dynamic_partition
        return self.customer_partitions[customer_id]

    def record_row(self, partition_index: int) -> None:
        self.partition_row_counts[partition_index] += 1

    @property
    def partition_count(self) -> int:
        if self.feature_partition_count is not None:
            return self.feature_partition_count
        return max(1, self.current_dynamic_partition + 1)

    def _hash_partition(self, customer_id: str) -> int:
        if self.feature_partition_count is None:
            raise ValueError("feature_partition_count is required for hash partitioning")
        digest = hashlib.sha256(customer_id.encode("utf-8")).hexdigest()
        return int(digest, 16) % self.feature_partition_count


@dataclass(frozen=True)
class ValidationRowBatchResult:
    total_rows: int
    valid_rows: int
    invalid_rows: int
    valid_records: list[dict[str, object]]
    invalid_records: list[dict[str, object]]
    failure_counts_by_rule: dict[str, int]
    next_source_row_number: int


class BetValidationRowBatchWorker:
    """Validate one source row batch."""

    def __init__(
        self,
        config: ValidationRowBatchConfig,
        validator: BetValidator | None = None,
    ) -> None:
        self.config = config
        self.validator = BetValidator() if validator is None else validator

    def process(self) -> ValidationRowBatchResult:
        rows_with_source = self._rows_with_source_numbers()
        validation_result = self.validator.validate_rows(
            ValidationInput(
                fieldnames=self.config.fieldnames,
                rows=rows_with_source,
                source_row_start=self.config.source_row_start,
                generated_at=self.config.generated_at,
            )
        )
        return ValidationRowBatchResult(
            total_rows=len(self.config.rows),
            valid_rows=len(validation_result["valid_rows"]),
            invalid_rows=len(validation_result["invalid_rows"]),
            valid_records=validation_result["valid_rows"],
            invalid_records=validation_result["invalid_rows"],
            failure_counts_by_rule=validation_result["report"]["failure_counts_by_rule"],
            next_source_row_number=self.config.source_row_start + len(self.config.rows),
        )

    def _rows_with_source_numbers(self) -> list[dict[str, str]]:
        rows_with_source = []
        source_row_number = self.config.source_row_start
        for row in self.config.rows:
            rows_with_source.append({**row, SOURCE_ROW_NUMBER: str(source_row_number)})
            source_row_number += 1
        return rows_with_source


class BetValidationBatchProcess:
    """Orchestrate validation over source row batches."""

    def __init__(self, validator: BetValidator | None = None) -> None:
        self.validator = BetValidator() if validator is None else validator

    def process(
        self,
        input_path: str | Path,
        validation_dir: str | Path,
        settings: ValidationBatchSettings | None = None,
    ) -> PartitionedInput:
        if settings is None:
            settings = ValidationBatchSettings()
        if settings.validation_worker_count < 1:
            raise ValueError("validation_worker_count must be greater than 0")

        fieldnames = read_csv_fieldnames(input_path)
        router = CustomerPartitionRouter(settings.feature_partition_count, settings.target_feature_partition_rows)
        generated_at = settings.generated_at
        if generated_at is None:
            generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        total_rows = 0
        valid_rows = 0
        invalid_rows = 0
        failure_counts: Counter[str] = Counter()
        batches_processed = 0

        with ExitStack() as stack:
            writers = ValidationPartitionWriters(stack, validation_dir)
            validation_results = self._validation_results(
                input_path,
                fieldnames,
                settings.batch_size,
                settings.validation_worker_count,
                generated_at,
            )
            for result in validation_results:
                batches_processed += 1
                total_rows += result.total_rows
                valid_rows += result.valid_rows
                invalid_rows += result.invalid_rows
                failure_counts.update(result.failure_counts_by_rule)
                self._write_partitions(result, router, writers)

            writers.ensure_partition_files(router.partition_count)
            partition_paths = writers.valid_partition_paths(router.partition_count)
            invalid_partition_paths = writers.invalid_partition_paths(router.partition_count)

        return PartitionedInput(
            fieldnames=fieldnames,
            partition_paths=partition_paths,
            invalid_partition_paths=invalid_partition_paths,
            total_rows=total_rows,
            batches_processed=batches_processed,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            failure_counts_by_rule=dict(sorted(failure_counts.items())),
            source_path=str(input_path),
            source_fingerprint=source_file_snapshot(input_path),
            batch_size=settings.batch_size,
            feature_partition_count=router.partition_count,
        )

    def _validation_results(
        self,
        input_path: str | Path,
        fieldnames: list[str],
        batch_size: int,
        validation_worker_count: int,
        generated_at: str,
    ) -> Iterator[ValidationRowBatchResult]:
        if validation_worker_count == 1:
            source_row_number = 2
            for row_batch in iter_csv_batches(input_path, batch_size):
                result = self._process_row_batch(fieldnames, row_batch, source_row_number, generated_at)
                source_row_number = result.next_source_row_number
                yield result
            return

        yield from self._concurrent_validation_results(
            input_path,
            fieldnames,
            batch_size,
            validation_worker_count,
            generated_at,
        )

    def _concurrent_validation_results(
        self,
        input_path: str | Path,
        fieldnames: list[str],
        batch_size: int,
        validation_worker_count: int,
        generated_at: str,
    ) -> Iterator[ValidationRowBatchResult]:
        max_pending = validation_worker_count * 2
        pending: deque[Future[ValidationRowBatchResult]] = deque()
        source_row_number = 2

        with ThreadPoolExecutor(max_workers=validation_worker_count) as executor:
            for row_batch in iter_csv_batches(input_path, batch_size):
                future = executor.submit(
                    self._process_row_batch,
                    fieldnames,
                    row_batch,
                    source_row_number,
                    generated_at,
                )
                pending.append(future)
                source_row_number += len(row_batch)

                if len(pending) >= max_pending:
                    yield pending.popleft().result()

            while pending:
                yield pending.popleft().result()

    def _process_row_batch(
        self,
        fieldnames: list[str],
        rows: list[dict[str, str]],
        source_row_start: int,
        generated_at: str,
    ) -> ValidationRowBatchResult:
        return BetValidationRowBatchWorker(
            ValidationRowBatchConfig(
                fieldnames=fieldnames,
                rows=rows,
                source_row_start=source_row_start,
                generated_at=generated_at,
            ),
            self.validator,
        ).process()

    def _write_partitions(
        self,
        result: ValidationRowBatchResult,
        router: CustomerPartitionRouter,
        writers: ValidationPartitionWriters,
    ) -> None:
        valid_partition_rows, invalid_partition_rows = self._partition_rows(
            result.valid_records,
            result.invalid_records,
            router,
        )
        for partition, rows in valid_partition_rows.items():
            writers.write_valid(partition, rows)
        for partition, rows in invalid_partition_rows.items():
            writers.write_invalid(partition, rows)

    def _partition_rows(
        self,
        valid_rows: list[dict[str, object]],
        invalid_rows: list[dict[str, object]],
        router: CustomerPartitionRouter,
    ) -> tuple[dict[int, list[dict[str, object]]], dict[int, list[dict[str, object]]]]:
        valid_partition_rows: dict[int, list[dict[str, object]]] = {}
        invalid_partition_rows: dict[int, list[dict[str, object]]] = {}

        for row in valid_rows:
            partition = router.partition_for_customer(str(row.get("customer_id", "")))
            router.record_row(partition)
            valid_partition_rows.setdefault(partition, []).append(row)
        for row in invalid_rows:
            partition = router.partition_for_customer(str(row.get("customer_id", "")))
            router.record_row(partition)
            invalid_partition_rows.setdefault(partition, []).append(row)

        return valid_partition_rows, invalid_partition_rows


class BetFeaturePartitionWorker:
    """Build and publish features for one customer-complete feature partition."""

    def __init__(
        self,
        config: FeaturePartitionConfig,
        feature_builder: BetFeatureBuilder | None = None,
    ) -> None:
        self.config = config
        self.feature_builder = (
            BetFeatureBuilder(first_n_bets=config.first_n_bets) if feature_builder is None else feature_builder
        )

    def process(self) -> FeatureBatchResult:
        _, valid_rows = read_parquet(self.config.partition_path)
        feature_rows = self.feature_builder.build(
            valid_rows,
            generated_at=self.config.feature_generated_at,
        )
        if feature_rows:
            feature_rows = sorted(feature_rows, key=lambda row: str(row["customer_id"]))
        write_parquet(
            self._partition_file(self.config.features_dir, "customer_features"),
            feature_rows,
            CUSTOMER_FEATURES_SCHEMA,
        )
        return FeatureBatchResult(
            feature_count=len(feature_rows),
            incomplete_first_n_count=self._incomplete_first_n_count(feature_rows),
            first_n_bets=self.config.first_n_bets,
        )

    def _partition_file(self, root_dir: Path, dataset_name: str) -> Path:
        return parquet_partition_file(root_dir, dataset_name, self.config.partition_index)

    def _incomplete_first_n_count(self, features: list[dict[str, object]]) -> int:
        return sum(int(feature["bets_used"]) < self.config.first_n_bets for feature in features)


class BetFeatureBatchProcess:
    """Orchestrate feature generation over customer-complete feature partitions."""

    def __init__(self, feature_builder: BetFeatureBuilder | None = None) -> None:
        self.feature_builder = feature_builder

    def process(
        self,
        partitioned_input: PartitionedInput,
        features_dir: str | Path,
        generated_at: datetime,
        first_n_bets: int = FIRST_N_BETS,
        feature_worker_count: int = 1,
    ) -> FeatureBatchResult:
        if first_n_bets < 1:
            raise ValueError("first_n_bets must be greater than 0")
        if feature_worker_count < 1:
            raise ValueError("feature_worker_count must be greater than 0")

        feature_count = 0
        incomplete_first_n_count = 0

        for feature_result in self._feature_results(
            partitioned_input,
            Path(features_dir),
            generated_at,
            first_n_bets,
            feature_worker_count,
        ):
            feature_count += feature_result.feature_count
            incomplete_first_n_count += feature_result.incomplete_first_n_count

        return FeatureBatchResult(feature_count, incomplete_first_n_count, first_n_bets, feature_worker_count)

    def _feature_results(
        self,
        partitioned_input: PartitionedInput,
        features_dir: Path,
        generated_at: datetime,
        first_n_bets: int,
        feature_worker_count: int,
    ) -> Iterator[FeatureBatchResult]:
        if feature_worker_count == 1:
            for partition_index, partition_path in enumerate(partitioned_input.partition_paths):
                yield self._process_partition(
                    partition_index,
                    partition_path,
                    features_dir,
                    generated_at,
                    first_n_bets,
                )
            return

        yield from self._concurrent_feature_results(
            partitioned_input,
            features_dir,
            generated_at,
            first_n_bets,
            feature_worker_count,
        )

    def _concurrent_feature_results(
        self,
        partitioned_input: PartitionedInput,
        features_dir: Path,
        generated_at: datetime,
        first_n_bets: int,
        feature_worker_count: int,
    ) -> Iterator[FeatureBatchResult]:
        max_pending = feature_worker_count * 2
        pending: deque[Future[FeatureBatchResult]] = deque()

        with ThreadPoolExecutor(max_workers=feature_worker_count) as executor:
            for partition_index, partition_path in enumerate(partitioned_input.partition_paths):
                future = executor.submit(
                    self._process_partition,
                    partition_index,
                    partition_path,
                    features_dir,
                    generated_at,
                    first_n_bets,
                )
                pending.append(future)

                if len(pending) >= max_pending:
                    yield pending.popleft().result()

            while pending:
                yield pending.popleft().result()

    def _process_partition(
        self,
        partition_index: int,
        partition_path: Path,
        features_dir: Path,
        generated_at: datetime,
        first_n_bets: int,
    ) -> FeatureBatchResult:
        return BetFeaturePartitionWorker(
            FeaturePartitionConfig(
                partition_index=partition_index,
                partition_path=partition_path,
                features_dir=features_dir,
                feature_generated_at=generated_at,
                first_n_bets=first_n_bets,
            ),
            self.feature_builder,
        ).process()


class RunArtifactPublisher:
    """Create staged run artifacts and publish them after completeness checks."""

    def __init__(self, output_dir: str | Path, run_id: str | None = None) -> None:
        self.started_at = self._now()
        output_root = Path(output_dir)
        self.run_id = self._resolve_run_id(run_id, self.started_at, output_root)
        self.staging_dir = output_root / "_staging" / self.run_id
        self.committed_dir = output_root / "runs" / self.run_id
        if self.staging_dir.exists():
            raise FileExistsError(f"Staging run already exists: {self.staging_dir}")
        if self.committed_dir.exists():
            raise FileExistsError(f"Committed run already exists: {self.committed_dir}")

        self.validation_dir = ensure_dir(self.staging_dir / "validation")
        self.features_dir = self.staging_dir / "features"
        self.validation_generated_at = self._now_iso()
        self.feature_generated_at = self._now()

    def write_validation_report(self, partitioned_input: PartitionedInput) -> dict:
        report = self._validation_report(partitioned_input)
        write_json(self.validation_dir / "validation_report.json", report)
        return report

    def write_feature_report(
        self,
        partitioned_input: PartitionedInput,
        validation_report: dict,
        feature_result: FeatureBatchResult,
    ) -> None:
        write_json(
            self.features_dir / "feature_report.json",
            {
                "input_path": partitioned_input.source_path,
                "input_fingerprint": partitioned_input.source_fingerprint,
                "validated_before_feature_generation": True,
                "invalid_rows_excluded": validation_report["invalid_rows"],
                "run_id": self.run_id,
                "generated_at": self.feature_generated_at.isoformat(timespec="seconds"),
                "feature_set_version": FEATURE_SET_VERSION,
                "feature_columns": list(FEATURE_COLUMNS),
                "customers": feature_result.feature_count,
                "customers_with_incomplete_first_n": feature_result.incomplete_first_n_count,
                "first_n_bets": feature_result.first_n_bets,
                "feature_worker_count": feature_result.feature_worker_count,
                "batch_size": validation_report["batch_size"],
                "feature_partition_count": validation_report["feature_partition_count"],
                "output_format": "parquet",
                "output_layout": "partitioned_by_customer_hash",
                "first_n_policy": VALIDATED_INPUT_POLICY,
            },
        )

    def write_manifest(
        self,
        partitioned_input: PartitionedInput,
        validation_report: dict,
        feature_result: FeatureBatchResult | None = None,
    ) -> dict:
        manifest = self._manifest(partitioned_input, validation_report, feature_result)
        write_json(self.staging_dir / "run_manifest.json", manifest)
        return manifest

    def commit(self, partitioned_input: PartitionedInput) -> None:
        self._assert_staged_artifacts(partitioned_input)
        write_json(
            self.staging_dir / "_SUCCESS",
            {
                "run_id": self.run_id,
                "committed_at": self._now_iso(),
            },
        )
        ensure_dir(self.committed_dir.parent)
        os.replace(self.staging_dir, self.committed_dir)

    def abort(self) -> None:
        if self.staging_dir.exists():
            shutil.rmtree(self.staging_dir)

    def _validation_report(self, partitioned_input: PartitionedInput) -> dict:
        missing_columns = [column for column in EXPECTED_COLUMNS if column not in partitioned_input.fieldnames]
        extra_columns = [column for column in partitioned_input.fieldnames if column not in EXPECTED_COLUMNS]
        return {
            "run_id": self.run_id,
            "generated_at": self.validation_generated_at,
            "schema_version": SCHEMA_VERSION,
            "total_rows": partitioned_input.total_rows,
            "valid_rows": partitioned_input.valid_rows,
            "invalid_rows": partitioned_input.invalid_rows,
            "batch_size": partitioned_input.batch_size,
            "batches_processed": partitioned_input.batches_processed,
            "feature_partition_count": partitioned_input.feature_partition_count,
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "failure_counts_by_rule": partitioned_input.failure_counts_by_rule,
            "input_path": partitioned_input.source_path,
            "input_fingerprint": partitioned_input.source_fingerprint,
        }

    def _manifest(
        self,
        partitioned_input: PartitionedInput,
        validation_report: dict,
        feature_result: FeatureBatchResult | None,
    ) -> dict:
        feature_count = 0
        incomplete_first_n_count = 0
        first_n_bets = None
        if feature_result is not None:
            feature_count = feature_result.feature_count
            incomplete_first_n_count = feature_result.incomplete_first_n_count
            first_n_bets = feature_result.first_n_bets
            feature_worker_count = feature_result.feature_worker_count
        else:
            feature_worker_count = None

        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "finished_at": self._now().isoformat(timespec="seconds"),
            "status": "success",
            "input_path": partitioned_input.source_path,
            "outputs": {
                "run_dir": str(self.committed_dir),
                "validation_dir": str(self.committed_dir / "validation"),
                "valid_bets_dir": str(self.committed_dir / "validation" / "valid_bets"),
                "invalid_bets_dir": str(self.committed_dir / "validation" / "invalid_bets"),
                "features_dir": str(self.committed_dir / "features"),
                "manifest": str(self.committed_dir / "run_manifest.json"),
                "success_marker": str(self.committed_dir / "_SUCCESS"),
            },
            "validation": validation_report,
            "features": {
                "customers": feature_count,
                "customers_with_incomplete_first_n": incomplete_first_n_count,
                "first_n_bets": first_n_bets,
                "feature_worker_count": feature_worker_count,
                "batch_size": partitioned_input.batch_size,
                "feature_partition_count": partitioned_input.feature_partition_count,
                "feature_dir": str(self.committed_dir / "features" / "customer_features"),
                "feature_report": str(self.committed_dir / "features" / "feature_report.json"),
            },
        }

    def _assert_staged_artifacts(self, partitioned_input: PartitionedInput) -> None:
        required_paths = [
            self.validation_dir / "valid_bets",
            self.validation_dir / "invalid_bets",
            self.validation_dir / "validation_report.json",
            self.staging_dir / "run_manifest.json",
        ]
        if self.features_dir.exists():
            required_paths.extend(
                [
                    self.features_dir / "customer_features",
                    self.features_dir / "feature_report.json",
                ]
            )
        missing_paths = [path for path in required_paths if not path.exists()]
        missing_paths.extend(self._missing_partition_files(partitioned_input.feature_partition_count))
        if missing_paths:
            missing = ", ".join(str(path) for path in missing_paths)
            raise FileNotFoundError(f"Cannot commit incomplete run. Missing: {missing}")

    def _missing_partition_files(self, feature_partition_count: int) -> list[Path]:
        required_files = []
        for partition_index in range(feature_partition_count):
            required_files.extend(
                [
                    parquet_partition_file(self.validation_dir, "valid_bets", partition_index),
                    parquet_partition_file(self.validation_dir, "invalid_bets", partition_index),
                ]
            )
            if self.features_dir.exists():
                required_files.append(parquet_partition_file(self.features_dir, "customer_features", partition_index))
        return [path for path in required_files if not path.exists()]

    def _resolve_run_id(self, run_id: str | None, started_at: datetime, output_root: Path) -> str:
        if run_id is not None:
            return run_id
        run_id = started_at.astimezone(timezone.utc).strftime("run-%Y-%m-%dT%H-%M-%SZ")
        if (output_root / "_staging" / run_id).exists() or (output_root / "runs" / run_id).exists():
            raise FileExistsError(f"Generated run_id already exists: {run_id}")
        return run_id

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _now_iso(self) -> str:
        return self._now().isoformat(timespec="seconds")
