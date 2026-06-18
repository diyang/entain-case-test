from __future__ import annotations

import argparse

from bet_pipeline.features import BetFeatureBuilder
from bet_pipeline.validation import BetValidator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bet-pipeline", description="Local betting-data batch pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate", help="Validate raw betting records")
    validate_parser.add_argument("--input", required=True, help="Path to input bets CSV")
    validate_parser.add_argument("--output", required=True, help="Directory for validation outputs")
    validate_parser.add_argument("--run-id", help="Optional stable run id for lineage and backfills")

    features_parser = subparsers.add_parser(
        "build-features", help="Validate raw bets and build customer features as one batch"
    )
    features_parser.add_argument("--input", required=True, help="Path to input bets CSV")
    features_parser.add_argument("--output", required=True, help="Directory for batch outputs")
    features_parser.add_argument("--run-id", help="Optional stable run id for lineage and backfills")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "validate":
        bet_validator = BetValidator()
        result = bet_validator.validate_file(args.input, args.output, run_id=args.run_id)
        report = result["report"]
        print(f"Validated {report['total_rows']} rows: {report['valid_rows']} valid, {report['invalid_rows']} invalid")
        return 0
    if args.command == "build-features":
        feature_builder = BetFeatureBuilder()
        manifest = feature_builder.build_feature_batch(args.input, args.output, run_id=args.run_id)
        print(
            f"Completed feature batch {manifest['run_id']}: "
            f"{manifest['validation']['valid_rows']} valid rows, "
            f"{manifest['validation']['invalid_rows']} invalid rows, "
            f"{manifest['features']['customers']} customers"
        )
        return 0
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
