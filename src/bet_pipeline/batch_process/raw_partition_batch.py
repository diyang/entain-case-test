from __future__ import annotations

import hashlib
from collections import Counter
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from bet_pipeline.batch_process.run_artifacts import (
    DEFAULT_BATCH_ROWS,
    RawBetPartitionedInput,
    parquet_partition_file,
    source_file_snapshot,
)
from bet_pipeline.io import ParquetBatchWriter, ensure_dir, iter_csv_batches, read_csv_fieldnames
from bet_pipeline.schema import EXPECTED_COLUMNS, RAW_BETS_SCHEMA


@dataclass(frozen=True)
class CustomerCompletePartitionSettings:
    batch_size: int = DEFAULT_BATCH_ROWS
    feature_partition_count: int | None = None
    target_feature_partition_rows: int = DEFAULT_BATCH_ROWS


@dataclass(frozen=True)
class RawBetRowBatchConfig:
    rows: list[dict[str, str]]
    source_row_start: int


@dataclass(frozen=True)
class RawBetRowBatchResult:
    total_rows: int
    bet_id_counts: Counter[str]
    bet_id_first_source_rows: dict[str, int]
    next_source_row_number: int


class CustomerCompletePartitionRouter:
    """Assign customer ids to customer-complete feature partitions."""

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


class RawBetPartitionWriters:
    """Own parquet writers for raw customer-complete partitions."""

    def __init__(self, stack: ExitStack, raw_dir: str | Path) -> None:
        self.stack = stack
        self.raw_dir = ensure_dir(raw_dir)
        self.writers: dict[int, ParquetBatchWriter] = {}

    def write(self, partition_index: int, rows: list[dict[str, object]]) -> None:
        if rows:
            self._writer(partition_index).write(rows)

    def ensure_partition_files(self, partition_count: int) -> None:
        for partition_index in range(partition_count):
            self._writer(partition_index)

    def partition_paths(self, partition_count: int) -> list[Path]:
        return [
            parquet_partition_file(self.raw_dir, "raw_bets", partition_index)
            for partition_index in range(partition_count)
        ]

    def _writer(self, partition_index: int) -> ParquetBatchWriter:
        if partition_index not in self.writers:
            self.writers[partition_index] = self.stack.enter_context(
                ParquetBatchWriter(
                    parquet_partition_file(self.raw_dir, "raw_bets", partition_index),
                    RAW_BETS_SCHEMA,
                )
            )
        return self.writers[partition_index]


class RawBetRowBatchWorker:
    """Write one raw source row batch into customer-complete parquet partitions."""

    def __init__(
        self,
        config: RawBetRowBatchConfig,
        router: CustomerCompletePartitionRouter,
        writers: RawBetPartitionWriters,
    ) -> None:
        self.config = config
        self.router = router
        self.writers = writers

    def process(self) -> RawBetRowBatchResult:
        bet_id_counts: Counter[str] = Counter()
        bet_id_first_source_rows: dict[str, int] = {}
        partition_rows: dict[int, list[dict[str, object]]] = {}
        source_row_number = self.config.source_row_start

        for row in self.config.rows:
            bet_id = str(row.get("bet_id", ""))
            bet_id_counts[bet_id] += 1
            bet_id_first_source_rows.setdefault(bet_id, source_row_number)
            partition = self.router.partition_for_customer(str(row.get("customer_id", "")))
            self.router.record_row(partition)
            partition_rows.setdefault(partition, []).append(self._raw_record(row, source_row_number))
            source_row_number += 1

        for partition, rows in partition_rows.items():
            self.writers.write(partition, rows)

        return RawBetRowBatchResult(
            total_rows=len(self.config.rows),
            bet_id_counts=bet_id_counts,
            bet_id_first_source_rows=bet_id_first_source_rows,
            next_source_row_number=source_row_number,
        )

    def _raw_record(self, row: dict[str, str], source_row_number: int) -> dict[str, object]:
        return {
            **{column: row.get(column, "") for column in EXPECTED_COLUMNS},
            "source_row_number": source_row_number,
        }


class RawBetCustomerCompletePartitionBatchProcess:
    """Stream raw bet CSV rows into customer-complete parquet partitions."""

    def process(
        self,
        input_path: str | Path,
        raw_dir: str | Path,
        settings: CustomerCompletePartitionSettings | None = None,
    ) -> RawBetPartitionedInput:
        if settings is None:
            settings = CustomerCompletePartitionSettings()
        if settings.batch_size < 1:
            raise ValueError("batch_size must be greater than 0")

        fieldnames = read_csv_fieldnames(input_path)
        router = CustomerCompletePartitionRouter(
            settings.feature_partition_count,
            settings.target_feature_partition_rows,
        )
        total_rows = 0
        batches_processed = 0
        bet_id_counts: Counter[str] = Counter()
        bet_id_first_source_rows: dict[str, int] = {}
        source_row_number = 2

        with ExitStack() as stack:
            writers = RawBetPartitionWriters(stack, raw_dir)
            for row_batch in iter_csv_batches(input_path, settings.batch_size):
                worker = RawBetRowBatchWorker(
                    RawBetRowBatchConfig(rows=row_batch, source_row_start=source_row_number),
                    router,
                    writers,
                )
                result = worker.process()
                total_rows += result.total_rows
                batches_processed += 1
                bet_id_counts.update(result.bet_id_counts)
                for bet_id, first_source_row in result.bet_id_first_source_rows.items():
                    bet_id_first_source_rows.setdefault(bet_id, first_source_row)
                source_row_number = result.next_source_row_number

            writers.ensure_partition_files(router.partition_count)
            partition_paths = writers.partition_paths(router.partition_count)

        return RawBetPartitionedInput(
            fieldnames=fieldnames,
            partition_paths=partition_paths,
            total_rows=total_rows,
            batches_processed=batches_processed,
            bet_id_counts=dict(sorted(bet_id_counts.items())),
            bet_id_first_source_rows=dict(sorted(bet_id_first_source_rows.items())),
            source_path=str(input_path),
            source_fingerprint=source_file_snapshot(input_path),
            batch_size=settings.batch_size,
            feature_partition_count=router.partition_count,
        )
