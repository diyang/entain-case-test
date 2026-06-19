from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Decimal

from bet_pipeline.schema import EXPECTED_COLUMNS, FEATURE_SET_VERSION


def window_datetime_column(first_n_bets: int) -> str:
    if first_n_bets < 1:
        raise ValueError("first_n_bets must be greater than 0")
    return f"bet_{first_n_bets}_datetime"


def feature_columns_for_window(first_n_bets: int) -> tuple[str, ...]:
    return (
        "customer_id",
        "first_bet_datetime",
        window_datetime_column(first_n_bets),
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


FEATURE_COLUMNS = feature_columns_for_window(20)

VALIDATED_INPUT_POLICY = (
    "Rows are validated first. Feature generation uses only valid rows within the configured first-N bet window; "
    "later valid bets are not pulled forward to replace invalid rows inside that window."
)


class BetFeatureBuilder:
    """Build customer-level features from validated customer-complete rows."""

    def __init__(
        self,
        expected_columns: Sequence[str] = EXPECTED_COLUMNS,
        feature_columns: Sequence[str] | None = None,
        first_n_bets: int = 20,
        feature_set_version: str = FEATURE_SET_VERSION,
    ) -> None:
        if first_n_bets < 1:
            raise ValueError("first_n_bets must be greater than 0")

        self.expected_columns = expected_columns
        self.first_n_bets = first_n_bets
        self.window_datetime_column = window_datetime_column(first_n_bets)
        if feature_columns is None:
            self.feature_columns = feature_columns_for_window(first_n_bets)
        else:
            self.feature_columns = feature_columns
        self.feature_set_version = feature_set_version

    def build(
        self,
        rows: list[dict[str, object]],
        generated_at: datetime | None = None,
    ) -> list[dict[str, object]]:
        if generated_at is None:
            generated_at = self._now()
        grouped_rows = self._group_first_n_rows(rows)
        return self._build_grouped_features(grouped_rows, generated_at)

    def _build_grouped_features(
        self, grouped_rows: dict[str, list[dict[str, object]]], generated_at: datetime
    ) -> list[dict[str, object]]:
        features: list[dict[str, object]] = []
        for customer_id, customer_rows in grouped_rows.items():
            sorted_rows = self._sort_customer_rows(customer_rows)
            if sorted_rows:
                features.append(self._customer_feature_row(customer_id, sorted_rows, generated_at))
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
            self.window_datetime_column: self._window_bet_datetime(rows),
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

    def _window_bet_datetime(self, rows: list[dict[str, object]]) -> datetime | None:
        for row in rows:
            if int(row["bet_num"]) == self.first_n_bets:
                return self._coerce_datetime(row["bet_datetime"])
        return None

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
