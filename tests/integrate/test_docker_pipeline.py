from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pytest


class TestDockerPipelineIntegration:
    def test_docker_build_features_end_to_end(self, tmp_path: Path) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        fixture_dir = repo_root / "tests" / "integrate" / "fixtures"
        image_name = os.environ.get("IMAGE_NAME", "entain-bet-pipeline:integration")
        run_id = os.environ.get("RUN_ID", "integration-test")
        output_dir = self._output_dir(tmp_path)

        self._run(["docker", "build", "-t", image_name, str(repo_root)], cwd=repo_root)
        self._run(self._docker_build_features_command(fixture_dir, output_dir, image_name, run_id), cwd=repo_root)

        run_dir = output_dir / "runs" / run_id
        staging_dir = output_dir / "_staging" / run_id
        validation_dir = run_dir / "validation"
        features_dir = run_dir / "features"

        assert (run_dir / "_SUCCESS").is_file()
        assert not staging_dir.exists()
        assert not (output_dir / "features" / "runs" / run_id).exists()
        assert (run_dir / "run_manifest.json").is_file()
        assert (validation_dir / "validation_report.json").is_file()
        assert (features_dir / "feature_report.json").is_file()
        assert (validation_dir / "valid_bets").is_dir()
        assert (validation_dir / "invalid_bets").is_dir()
        assert (features_dir / "customer_features").is_dir()

        valid_parts = self._parquet_parts(validation_dir / "valid_bets")
        invalid_parts = self._parquet_parts(validation_dir / "invalid_bets")
        feature_parts = self._parquet_parts(features_dir / "customer_features")
        assert valid_parts
        assert invalid_parts
        assert feature_parts
        assert len(valid_parts) == len(invalid_parts) == len(feature_parts)

        manifest = self._read_json(run_dir / "run_manifest.json")
        success_marker = self._read_json(run_dir / "_SUCCESS")
        validation_report = self._read_json(validation_dir / "validation_report.json")
        feature_report = self._read_json(features_dir / "feature_report.json")
        valid_rows = self._read_parquet_rows(validation_dir / "valid_bets")
        invalid_rows = self._read_parquet_rows(validation_dir / "invalid_bets")
        feature_rows = self._read_parquet_rows(features_dir / "customer_features")
        container_run_dir = f"/outputs/runs/{run_id}"

        assert manifest["status"] == "success"
        assert manifest["input_path"] == "/data/bets.csv"
        assert manifest["outputs"]["run_dir"] == container_run_dir
        assert manifest["outputs"]["success_marker"] == f"{container_run_dir}/_SUCCESS"
        assert success_marker["run_id"] == run_id
        assert manifest["validation"]["total_rows"] == validation_report["total_rows"]
        assert validation_report["total_rows"] == 5
        assert validation_report["valid_rows"] == 4
        assert validation_report["invalid_rows"] == 1
        assert feature_report["validated_before_feature_generation"] is True
        assert feature_report["customers"] == 2
        assert feature_report["first_n_bets"] == 2
        assert feature_report["invalid_rows_excluded"] == 1
        assert feature_report["feature_partition_count"] == len(feature_parts)

        assert len(valid_rows) == 4
        assert len(invalid_rows) == 1
        assert len(feature_rows) == 2
        assert sorted(row["bet_id"] for row in valid_rows) == [1, 2, 4, 5]

        invalid_row = invalid_rows[0]
        assert invalid_row["bet_id"] == "3"
        assert invalid_row["source_row_number"] == 4
        assert "betting_amount_gt_0" in invalid_row["validation_errors"]

        self._assert_customer_complete_partitions(valid_parts)
        features_by_customer = {row["customer_id"]: row for row in feature_rows}
        self._assert_customer_one_features(features_by_customer["00000000-0000-4000-8000-000000000001"])
        self._assert_customer_two_features(features_by_customer["00000000-0000-4000-8000-000000000002"])

        failed_run_id = f"{run_id}-failed"
        failed_command = self._docker_build_features_command(fixture_dir, output_dir, image_name, failed_run_id)
        failed_command[failed_command.index("--batch-size") + 1] = "0"
        failed_run = self._run_without_check(failed_command, cwd=repo_root)

        assert failed_run.returncode != 0
        assert not (output_dir / "runs" / failed_run_id).exists()
        assert not (output_dir / "_staging" / failed_run_id).exists()
        assert not (output_dir / "features" / "runs" / failed_run_id).exists()

    def _output_dir(self, tmp_path: Path) -> Path:
        configured_output_dir = os.environ.get("OUTPUT_DIR")
        output_dir = tmp_path / "integration_outputs" if configured_output_dir is None else Path(configured_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir.resolve()

    def _docker_build_features_command(
        self, fixture_dir: Path, output_dir: Path, image_name: str, run_id: str
    ) -> list[str]:
        return [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{fixture_dir}:/data:ro",
            "-v",
            f"{output_dir}:/outputs",
            image_name,
            "build-features",
            "--input",
            "/data/bets.csv",
            "--output",
            "/outputs/features",
            "--run-id",
            run_id,
            "--batch-size",
            os.environ.get("BATCH_SIZE", "2"),
            "--target-feature-partition-rows",
            os.environ.get("TARGET_FEATURE_PARTITION_ROWS", "3"),
            "--validation-workers",
            os.environ.get("VALIDATION_WORKERS", "1"),
            "--feature-workers",
            os.environ.get("FEATURE_WORKERS", "1"),
            "--first-n-bets",
            os.environ.get("FIRST_N_BETS", "2"),
        ]

    def _run(self, command: list[str], cwd: Path) -> str:
        completed = self._run_without_check(command, cwd)
        if completed.returncode != 0:
            command_text = " ".join(command)
            pytest.fail(f"Command failed with exit code {completed.returncode}: {command_text}\n{completed.stdout}")
        return completed.stdout

    def _run_without_check(self, command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            command,
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        return completed

    def _parquet_parts(self, path: Path) -> list[Path]:
        return sorted(path.glob("part-*.parquet"))

    def _read_parquet_rows(self, path: Path) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for part in self._parquet_parts(path):
            rows.extend(pq.read_table(part).to_pylist())
        return rows

    def _assert_customer_complete_partitions(self, valid_parts: list[Path]) -> None:
        customer_partitions: dict[str, str] = {}
        for part in valid_parts:
            for row in pq.read_table(part).to_pylist():
                customer_id = row["customer_id"]
                if customer_id in customer_partitions:
                    assert customer_partitions[customer_id] == part.name
                customer_partitions[customer_id] = part.name

    def _assert_customer_one_features(self, row: dict[str, object]) -> None:
        assert row["bets_used"] == 2
        assert row["first_bet_datetime"].isoformat() == "2024-08-01T00:00:00"
        assert row["nth_bet_datetime"].isoformat() == "2024-08-02T00:00:00"
        assert row["total_betting_amount"] == pytest.approx(20.0)
        assert row["mean_betting_amount"] == pytest.approx(10.0)
        assert row["mean_price"] == pytest.approx(2.5)
        assert row["pct_racing"] == pytest.approx(0.0)
        assert row["pct_cash"] == pytest.approx(1.0)
        assert row["pct_return"] == pytest.approx(0.5)
        assert row["total_payout"] == pytest.approx(25.0)
        assert row["total_return_for_entain"] == pytest.approx(-5.0)

    def _assert_customer_two_features(self, row: dict[str, object]) -> None:
        assert row["bets_used"] == 2
        assert row["first_bet_datetime"].isoformat() == "2024-08-01T00:00:00"
        assert row["nth_bet_datetime"].isoformat() == "2024-08-02T00:00:00"
        assert row["total_betting_amount"] == pytest.approx(12.0)
        assert row["mean_betting_amount"] == pytest.approx(6.0)
        assert row["mean_price"] == pytest.approx(2.5)
        assert row["pct_racing"] == pytest.approx(1.0)
        assert row["pct_cash"] == pytest.approx(0.0)
        assert row["pct_return"] == pytest.approx(0.5)
        assert row["total_payout"] == pytest.approx(10.0)
        assert row["total_return_for_entain"] == pytest.approx(-10.0)

    def _read_json(self, path: Path) -> dict[str, object]:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
