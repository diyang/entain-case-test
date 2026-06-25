from __future__ import annotations

from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from bet_pipeline.batch_process.run_artifacts import FIRST_N_BETS, PartitionedInput, parquet_partition_file
from bet_pipeline.features import BetFeatureBuilder, feature_columns_for_window, window_datetime_column
from bet_pipeline.io import read_parquet, write_parquet
from bet_pipeline.schema import customer_features_schema


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
    feature_columns: tuple[str, ...]
    window_datetime_column: str
    feature_worker_count: int = 1


class BetFeaturePartitionWorker:
    """Build and publish features for one customer-complete feature partition."""

    def __init__(
        self,
        config: FeaturePartitionConfig,
        feature_builder: BetFeatureBuilder | None = None,
    ) -> None:
        self.config = config
        if feature_builder is None:
            self.feature_builder = BetFeatureBuilder(first_n_bets=config.first_n_bets)
            return
        if feature_builder.first_n_bets != config.first_n_bets:
            self.feature_builder = BetFeatureBuilder(first_n_bets=config.first_n_bets)
            return
        self.feature_builder = feature_builder

    def process(self) -> FeatureBatchResult:
        _, valid_rows = read_parquet(self.config.partition_path)
        feature_rows = self.feature_builder.build(
            valid_rows,
            generated_at=self.config.feature_generated_at,
        )
        if feature_rows:
            feature_rows = sorted(feature_rows, key=lambda row: str(row["customer_id"]))
        write_parquet(
            parquet_partition_file(self.config.features_dir, "customer_features", self.config.partition_index),
            feature_rows,
            customer_features_schema(self.feature_builder.window_datetime_column),
        )
        return FeatureBatchResult(
            feature_count=len(feature_rows),
            incomplete_first_n_count=sum(
                int(feature["bets_used"]) < self.config.first_n_bets for feature in feature_rows
            ),
            first_n_bets=self.config.first_n_bets,
            feature_columns=tuple(self.feature_builder.feature_columns),
            window_datetime_column=self.feature_builder.window_datetime_column,
        )


class BetFeaturePartitionBatchProcess:
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
        feature_output_dir = Path(features_dir)

        if feature_worker_count == 1:
            for partition_index, partition_path in enumerate(partitioned_input.partition_paths):
                feature_result = BetFeaturePartitionWorker(
                    FeaturePartitionConfig(
                        partition_index=partition_index,
                        partition_path=partition_path,
                        features_dir=feature_output_dir,
                        feature_generated_at=generated_at,
                        first_n_bets=first_n_bets,
                    ),
                    self.feature_builder,
                ).process()
                feature_count += feature_result.feature_count
                incomplete_first_n_count += feature_result.incomplete_first_n_count
        else:
            max_pending = feature_worker_count * 2
            pending: deque[Future[FeatureBatchResult]] = deque()
            with ThreadPoolExecutor(max_workers=feature_worker_count) as executor:
                for partition_index, partition_path in enumerate(partitioned_input.partition_paths):
                    worker = BetFeaturePartitionWorker(
                        FeaturePartitionConfig(
                            partition_index=partition_index,
                            partition_path=partition_path,
                            features_dir=feature_output_dir,
                            feature_generated_at=generated_at,
                            first_n_bets=first_n_bets,
                        ),
                        self.feature_builder,
                    )
                    pending.append(executor.submit(worker.process))

                    if len(pending) >= max_pending:
                        feature_result = pending.popleft().result()
                        feature_count += feature_result.feature_count
                        incomplete_first_n_count += feature_result.incomplete_first_n_count

                while pending:
                    feature_result = pending.popleft().result()
                    feature_count += feature_result.feature_count
                    incomplete_first_n_count += feature_result.incomplete_first_n_count

        return FeatureBatchResult(
            feature_count=feature_count,
            incomplete_first_n_count=incomplete_first_n_count,
            first_n_bets=first_n_bets,
            feature_columns=feature_columns_for_window(first_n_bets),
            window_datetime_column=window_datetime_column(first_n_bets),
            feature_worker_count=feature_worker_count,
        )
