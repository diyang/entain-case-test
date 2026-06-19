from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from bet_pipeline.features import VALIDATED_INPUT_POLICY
from bet_pipeline.io import ensure_dir, read_json, write_json
from bet_pipeline.schema import EXPECTED_COLUMNS, FEATURE_SET_VERSION, SCHEMA_VERSION

if TYPE_CHECKING:
    from bet_pipeline.batch_process.feature_batch import FeatureBatchResult

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
class RawBetPartitionedInput:
    fieldnames: list[str]
    partition_paths: list[Path]
    total_rows: int
    batches_processed: int
    bet_id_counts: dict[str, int]
    bet_id_first_source_rows: dict[str, int]
    source_path: str
    source_fingerprint: dict[str, object]
    batch_size: int
    feature_partition_count: int


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
    source_validation_run_id: str | None = None
    source_validation_run_dir: str | None = None


@dataclass(frozen=True)
class ValidationCheckpoint:
    partitioned_input: PartitionedInput
    validation_report: dict


class ValidationCheckpointLoader:
    """Load a committed validation run as the input to feature generation."""

    def __init__(self, validation_run_dir: str | Path) -> None:
        self.validation_run_dir = Path(validation_run_dir)
        self.validation_dir = self.validation_run_dir / "validation"
        self.valid_bets_dir = self.validation_dir / "valid_bets"
        self.invalid_bets_dir = self.validation_dir / "invalid_bets"

    def load(self) -> ValidationCheckpoint:
        self._assert_committed_run()
        validation_report = read_json(self.validation_dir / "validation_report.json")
        self._assert_schema_compatible(validation_report)
        partition_count = self._int_value(validation_report, "feature_partition_count")
        partition_paths = self._partition_paths(self.valid_bets_dir, partition_count)
        invalid_partition_paths = self._partition_paths(self.invalid_bets_dir, partition_count)

        return ValidationCheckpoint(
            partitioned_input=PartitionedInput(
                fieldnames=list(EXPECTED_COLUMNS),
                partition_paths=partition_paths,
                invalid_partition_paths=invalid_partition_paths,
                total_rows=self._int_value(validation_report, "total_rows"),
                batches_processed=self._int_value(validation_report, "batches_processed"),
                valid_rows=self._int_value(validation_report, "valid_rows"),
                invalid_rows=self._int_value(validation_report, "invalid_rows"),
                failure_counts_by_rule=self._failure_counts(validation_report),
                source_path=str(validation_report.get("input_path", "")),
                source_fingerprint=self._dict_value(validation_report, "input_fingerprint"),
                batch_size=self._int_value(validation_report, "batch_size"),
                feature_partition_count=partition_count,
                source_validation_run_id=self._source_validation_run_id(),
                source_validation_run_dir=str(self.validation_run_dir),
            ),
            validation_report=validation_report,
        )

    def _assert_committed_run(self) -> None:
        success_marker = self.validation_run_dir / "_SUCCESS"
        if not success_marker.is_file():
            raise FileNotFoundError(f"Validation checkpoint is not committed: missing {success_marker}")
        required_paths = [
            self.validation_dir / "validation_report.json",
            self.valid_bets_dir,
            self.invalid_bets_dir,
        ]
        missing_paths = [path for path in required_paths if not path.exists()]
        if missing_paths:
            missing = ", ".join(str(path) for path in missing_paths)
            raise FileNotFoundError(f"Validation checkpoint is incomplete. Missing: {missing}")

    def _assert_schema_compatible(self, validation_report: dict) -> None:
        schema_version = validation_report.get("schema_version")
        if schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"Validation checkpoint schema_version {schema_version!r} does not match expected {SCHEMA_VERSION!r}"
            )

    def _partition_paths(self, partition_dir: Path, partition_count: int) -> list[Path]:
        partition_paths = [
            partition_dir / f"part-{partition_index:05d}.parquet" for partition_index in range(partition_count)
        ]
        missing_paths = [path for path in partition_paths if not path.is_file()]
        if missing_paths:
            missing = ", ".join(str(path) for path in missing_paths)
            raise FileNotFoundError(f"Validation checkpoint is missing partition files: {missing}")
        return partition_paths

    def _failure_counts(self, validation_report: dict) -> dict[str, int]:
        failure_counts = validation_report.get("failure_counts_by_rule", {})
        if not isinstance(failure_counts, dict):
            return {}
        return {str(rule): int(count) for rule, count in failure_counts.items()}

    def _dict_value(self, payload: dict, key: str) -> dict[str, object]:
        value = payload.get(key, {})
        if isinstance(value, dict):
            return value
        return {}

    def _int_value(self, payload: dict, key: str) -> int:
        value = payload.get(key)
        if isinstance(value, int):
            return value
        raise ValueError(f"Validation checkpoint report field {key!r} must be an integer")

    def _source_validation_run_id(self) -> str:
        return self.validation_run_dir.name


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

        self.raw_dir = self.staging_dir / "raw"
        self.validation_dir = self.staging_dir / "validation"
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
                "reused_validation_checkpoint": partitioned_input.source_validation_run_id is not None,
                "source_validation_run_id": partitioned_input.source_validation_run_id,
                "source_validation_run_dir": partitioned_input.source_validation_run_dir,
                "invalid_rows_excluded": validation_report["invalid_rows"],
                "run_id": self.run_id,
                "generated_at": self.feature_generated_at.isoformat(timespec="seconds"),
                "feature_set_version": FEATURE_SET_VERSION,
                "feature_columns": list(feature_result.feature_columns),
                "window_datetime_column": feature_result.window_datetime_column,
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
        feature_columns: list[str] = []
        feature_window_datetime_column = None
        if feature_result is not None:
            feature_count = feature_result.feature_count
            incomplete_first_n_count = feature_result.incomplete_first_n_count
            first_n_bets = feature_result.first_n_bets
            feature_columns = list(feature_result.feature_columns)
            feature_window_datetime_column = feature_result.window_datetime_column
            feature_worker_count = feature_result.feature_worker_count
        else:
            feature_worker_count = None
        raw_dir = self._published_raw_dir(partitioned_input)
        validation_dir = self._published_validation_dir(partitioned_input)

        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "finished_at": self._now().isoformat(timespec="seconds"),
            "status": "success",
            "input_path": partitioned_input.source_path,
            "reused_validation_checkpoint": partitioned_input.source_validation_run_id is not None,
            "source_validation_run_id": partitioned_input.source_validation_run_id,
            "outputs": {
                "run_dir": str(self.committed_dir),
                "raw_bets_dir": str(raw_dir / "raw_bets"),
                "validation_dir": str(validation_dir),
                "valid_bets_dir": str(validation_dir / "valid_bets"),
                "invalid_bets_dir": str(validation_dir / "invalid_bets"),
                "features_dir": str(self.committed_dir / "features"),
                "manifest": str(self.committed_dir / "run_manifest.json"),
                "success_marker": str(self.committed_dir / "_SUCCESS"),
            },
            "validation": validation_report,
            "features": {
                "customers": feature_count,
                "customers_with_incomplete_first_n": incomplete_first_n_count,
                "first_n_bets": first_n_bets,
                "feature_columns": feature_columns,
                "window_datetime_column": feature_window_datetime_column,
                "feature_worker_count": feature_worker_count,
                "batch_size": partitioned_input.batch_size,
                "feature_partition_count": partitioned_input.feature_partition_count,
                "feature_dir": str(self.committed_dir / "features" / "customer_features"),
                "feature_report": str(self.committed_dir / "features" / "feature_report.json"),
            },
        }

    def _assert_staged_artifacts(self, partitioned_input: PartitionedInput) -> None:
        required_paths = [self.staging_dir / "run_manifest.json"]
        if partitioned_input.source_validation_run_dir is None:
            required_paths.extend(
                [
                    self.raw_dir / "raw_bets",
                    self.validation_dir / "valid_bets",
                    self.validation_dir / "invalid_bets",
                    self.validation_dir / "validation_report.json",
                ]
            )
        else:
            required_paths.extend(self._source_validation_paths(partitioned_input))
        if self.features_dir.exists():
            required_paths.extend(
                [
                    self.features_dir / "customer_features",
                    self.features_dir / "feature_report.json",
                ]
            )
        missing_paths = [path for path in required_paths if not path.exists()]
        missing_paths.extend(self._missing_partition_files(partitioned_input))
        if missing_paths:
            missing = ", ".join(str(path) for path in missing_paths)
            raise FileNotFoundError(f"Cannot commit incomplete run. Missing: {missing}")

    def _source_validation_paths(self, partitioned_input: PartitionedInput) -> list[Path]:
        if partitioned_input.source_validation_run_dir is None:
            return []
        source_validation_run_dir = Path(partitioned_input.source_validation_run_dir)
        return [
            source_validation_run_dir / "_SUCCESS",
            source_validation_run_dir / "validation" / "validation_report.json",
            source_validation_run_dir / "validation" / "valid_bets",
            source_validation_run_dir / "validation" / "invalid_bets",
        ]

    def _missing_partition_files(self, partitioned_input: PartitionedInput) -> list[Path]:
        required_files = []
        if partitioned_input.source_validation_run_dir is None:
            for partition_index in range(partitioned_input.feature_partition_count):
                required_files.extend(
                    [
                        parquet_partition_file(self.raw_dir, "raw_bets", partition_index),
                        parquet_partition_file(self.validation_dir, "valid_bets", partition_index),
                        parquet_partition_file(self.validation_dir, "invalid_bets", partition_index),
                    ]
                )
        else:
            required_files.extend(partitioned_input.partition_paths)
            required_files.extend(partitioned_input.invalid_partition_paths)

        for partition_index in range(partitioned_input.feature_partition_count):
            if self.features_dir.exists():
                required_files.append(parquet_partition_file(self.features_dir, "customer_features", partition_index))
        return [path for path in required_files if not path.exists()]

    def _published_validation_dir(self, partitioned_input: PartitionedInput) -> Path:
        if partitioned_input.source_validation_run_dir is not None:
            return Path(partitioned_input.source_validation_run_dir) / "validation"
        return self.committed_dir / "validation"

    def _published_raw_dir(self, partitioned_input: PartitionedInput) -> Path:
        if partitioned_input.source_validation_run_dir is not None:
            return Path(partitioned_input.source_validation_run_dir) / "raw"
        return self.committed_dir / "raw"

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
