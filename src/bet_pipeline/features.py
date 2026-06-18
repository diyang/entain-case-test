from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from bet_pipeline.io import ensure_dir, file_fingerprint, read_csv, read_parquet, write_json, write_parquet
from bet_pipeline.schema import CUSTOMER_FEATURES_SCHEMA, EXPECTED_COLUMNS, FEATURE_SET_VERSION
from bet_pipeline.validation import BetValidator

FEATURE_COLUMNS = (
    "customer_id",
    "first_bet_datetime",
    "twentieth_bet_datetime",
    "bets_used",
    "total_betting_amount",
    "mean_betting_amount",
    "mean_price",
    "pct_racing",
    "pct_cash",
    "pct_return",
    "total_payout",
    "total_return_for_entain",
    "feature_generated_at",
)

VALIDATED_INPUT_POLICY = (
    "Rows are validated first. Feature generation uses only valid rows with bet_num <= 20; "
    "later valid bets are not pulled forward to replace invalid first-20 bets."
)


class BetFeatureBuilder:
    """Validate raw bets and build customer-level feature batches."""

    def __init__(
        self,
        validator: BetValidator | None = None,
        expected_columns: Sequence[str] = EXPECTED_COLUMNS,
        feature_columns: Sequence[str] = FEATURE_COLUMNS,
        first_n_bets: int = 20,
        feature_set_version: str = FEATURE_SET_VERSION,
    ) -> None:
        self.validator = BetValidator() if validator is None else validator
        self.expected_columns = expected_columns
        self.feature_columns = feature_columns
        self.first_n_bets = first_n_bets
        self.feature_set_version = feature_set_version

    def build_feature_batch(
        self,
        input_path: str | Path,
        output_dir: str | Path,
        run_id: str | None = None,
    ) -> dict:
        started_at = self._now()
        if run_id is None:
            run_id = self._run_id_from_datetime(started_at)
        output_root = ensure_dir(output_dir)
        validation_dir = ensure_dir(output_root / "validation")
        features_dir = ensure_dir(output_root / "features")

        validation_result = self.validator.validate_file(input_path, validation_dir, run_id=run_id)
        features = self.build_file(
            validation_dir / "valid_bets.parquet",
            features_dir,
            run_id=run_id,
        )

        manifest = {
            "run_id": run_id,
            "started_at": started_at.isoformat(timespec="seconds"),
            "finished_at": self._now().isoformat(timespec="seconds"),
            "status": "success",
            "input_path": str(input_path),
            "outputs": {
                "validation_dir": str(validation_dir),
                "features_dir": str(features_dir),
                "manifest": str(output_root / "run_manifest.json"),
            },
            "validation": validation_result["report"],
            "features": {
                "customers": len(features),
                "customers_with_incomplete_first_20": self._incomplete_first_n_count(features),
                "feature_file": str(features_dir / "customer_features.parquet"),
                "feature_report": str(features_dir / "feature_report.json"),
            },
        }
        write_json(output_root / "run_manifest.json", manifest)
        return manifest

    def build_file(
        self,
        input_path: str | Path,
        output_dir: str | Path,
        run_id: str | None = None,
    ) -> list[dict[str, object]]:
        generated_at = self._now()
        if run_id is None:
            run_id = self._run_id_from_datetime(generated_at)
        fieldnames, rows = self._read_input(input_path)
        missing_columns = [column for column in self.expected_columns if column not in fieldnames]
        if missing_columns:
            raise ValueError(f"Input is missing required columns: {', '.join(missing_columns)}")

        valid_rows = [{column: row[column] for column in self.expected_columns} for row in rows]
        features = self.build_rows(valid_rows, generated_at=generated_at)
        target_dir = ensure_dir(output_dir)
        write_parquet(target_dir / "customer_features.parquet", features, CUSTOMER_FEATURES_SCHEMA)
        write_json(
            target_dir / "feature_report.json",
            {
                "input_path": str(input_path),
                "input_fingerprint": file_fingerprint(input_path),
                "validated_before_feature_generation": True,
                "invalid_rows_excluded": 0,
                "run_id": run_id,
                "generated_at": generated_at.isoformat(timespec="seconds"),
                "feature_set_version": self.feature_set_version,
                "feature_columns": list(self.feature_columns),
                "customers": len(features),
                "customers_with_incomplete_first_20": self._incomplete_first_n_count(features),
                "first_n_bets": self.first_n_bets,
                "output_format": "parquet",
                "first_20_policy": VALIDATED_INPUT_POLICY,
            },
        )
        return features

    def build_rows(
        self,
        rows: list[dict[str, object]],
        generated_at: datetime | None = None,
    ) -> list[dict[str, object]]:
        if generated_at is None:
            generated_at = self._now()
        features: list[dict[str, object]] = []

        for customer_id, grouped_rows in self._group_first_n_rows(rows).items():
            customer_rows = self._sort_customer_rows(grouped_rows)
            if customer_rows:
                features.append(self._customer_feature_row(customer_id, customer_rows, generated_at))

        return features

    def _group_first_n_rows(self, rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in rows:
            bet_num = int(row["bet_num"])
            if bet_num <= self.first_n_bets:
                grouped[str(row["customer_id"])].append(row)
        return dict(sorted(grouped.items()))

    def _sort_customer_rows(self, rows: list[dict[str, object]]) -> list[dict[str, object]]:
        return sorted(rows, key=lambda row: (int(row["bet_num"]), row["bet_datetime"], int(row["bet_id"])))

    def _customer_feature_row(
        self, customer_id: str, rows: list[dict[str, object]], generated_at: datetime
    ) -> dict[str, object]:
        denominator = Decimal(len(rows))
        amounts = [Decimal(str(row["betting_amount"])) for row in rows]
        prices = [Decimal(str(row["price"])) for row in rows]
        payouts = [Decimal(str(row["payout"])) for row in rows]
        returns_for_entain = [Decimal(str(row["return_for_entain"])) for row in rows]

        return {
            "customer_id": customer_id,
            "first_bet_datetime": self._coerce_datetime(rows[0]["bet_datetime"]),
            "twentieth_bet_datetime": self._nth_bet_datetime(rows),
            "bets_used": len(rows),
            "total_betting_amount": float(sum(amounts, Decimal("0"))),
            "mean_betting_amount": float(sum(amounts, Decimal("0")) / denominator),
            "mean_price": float(sum(prices, Decimal("0")) / denominator),
            "pct_racing": float(Decimal(sum(row["category"] == "racing" for row in rows)) / denominator),
            "pct_cash": float(Decimal(sum(row["stake_type"] == "cash" for row in rows)) / denominator),
            "pct_return": float(Decimal(sum(row["bet_result"] == "return" for row in rows)) / denominator),
            "total_payout": float(sum(payouts, Decimal("0"))),
            "total_return_for_entain": float(sum(returns_for_entain, Decimal("0"))),
            "feature_generated_at": generated_at,
        }

    def _nth_bet_datetime(self, rows: list[dict[str, object]]) -> datetime | None:
        for row in rows:
            if int(row["bet_num"]) == self.first_n_bets:
                return self._coerce_datetime(row["bet_datetime"])
        return None

    def _incomplete_first_n_count(self, features: list[dict[str, object]]) -> int:
        return sum(int(feature["bets_used"]) < self.first_n_bets for feature in features)

    def _read_input(self, input_path: str | Path) -> tuple[list[str], list[dict[str, object]]]:
        path = Path(input_path)
        if path.suffix == ".parquet":
            return read_parquet(path)
        return read_csv(path)

    def _coerce_datetime(self, value: object) -> datetime:
        if isinstance(value, datetime):
            return value
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        raise ValueError(f"Invalid datetime: {value}")

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _run_id_from_datetime(self, value: datetime) -> str:
        return value.isoformat(timespec="seconds").replace(":", "").replace("+", "Z")
