from __future__ import annotations

import pyarrow as pa

SCHEMA_VERSION = "bets-v1"
FEATURE_SET_VERSION = "customer-first-n-v1"

EXPECTED_COLUMNS = (
    "bet_id",
    "customer_id",
    "bet_datetime",
    "bet_num",
    "betting_amount",
    "price",
    "category",
    "stake_type",
    "bet_result",
    "payout",
    "return_for_entain",
)

ALLOWED_CATEGORIES = frozenset({"sports", "racing"})
ALLOWED_STAKE_TYPES = frozenset({"cash", "bonus"})
ALLOWED_BET_RESULTS = frozenset({"return", "no-return"})

RAW_BETS_SCHEMA = pa.schema(
    [(column, pa.string()) for column in EXPECTED_COLUMNS] + [("source_row_number", pa.int64())]
)

VALID_BETS_SCHEMA = pa.schema(
    [
        ("bet_id", pa.int64()),
        ("customer_id", pa.string()),
        ("bet_datetime", pa.timestamp("us")),
        ("bet_num", pa.int64()),
        ("betting_amount", pa.float64()),
        ("price", pa.float64()),
        ("category", pa.string()),
        ("stake_type", pa.string()),
        ("bet_result", pa.string()),
        ("payout", pa.float64()),
        ("return_for_entain", pa.float64()),
        ("source_row_number", pa.int64()),
    ]
)

INVALID_BETS_SCHEMA = pa.schema(
    [
        ("bet_id", pa.string()),
        ("customer_id", pa.string()),
        ("bet_datetime", pa.string()),
        ("bet_num", pa.string()),
        ("betting_amount", pa.string()),
        ("price", pa.string()),
        ("category", pa.string()),
        ("stake_type", pa.string()),
        ("bet_result", pa.string()),
        ("payout", pa.string()),
        ("return_for_entain", pa.string()),
        ("source_row_number", pa.int64()),
        ("validation_errors", pa.string()),
        ("validated_at", pa.timestamp("us", tz="UTC")),
    ]
)


def customer_features_schema(window_datetime_column: str) -> pa.Schema:
    return pa.schema(
        [
            ("customer_id", pa.string()),
            ("first_bet_datetime", pa.timestamp("us")),
            (window_datetime_column, pa.timestamp("us")),
            ("bets_used", pa.int64()),
            ("total_betting_amount", pa.float64()),
            ("mean_betting_amount", pa.float64()),
            ("mean_price", pa.float64()),
            ("pct_racing", pa.float64()),
            ("pct_cash", pa.float64()),
            ("pct_return", pa.float64()),
            ("total_payout", pa.float64()),
            ("total_return_for_entain", pa.float64()),
            ("feature_generated_at", pa.timestamp("us", tz="UTC")),
        ]
    )


CUSTOMER_FEATURES_SCHEMA = customer_features_schema("bet_20_datetime")
