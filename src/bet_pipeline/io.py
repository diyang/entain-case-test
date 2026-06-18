from __future__ import annotations

import csv
import hashlib
import json
import os
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def ensure_dir(path: str | Path) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def read_csv(path: str | Path) -> tuple[list[str], list[dict[str, str]]]:
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return [], []
        return list(reader.fieldnames), list(reader)


def read_parquet(path: str | Path) -> tuple[list[str], list[dict[str, object]]]:
    table = pq.read_table(path)
    rows = table.to_pylist()
    return list(table.column_names), rows


def file_fingerprint(path: str | Path) -> dict[str, object]:
    source = Path(path)
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": str(source),
        "size_bytes": source.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def write_parquet(path: str | Path, rows: Iterable[dict[str, object]], schema: pa.Schema) -> None:
    rows = list(rows)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_suffix(target.suffix + ".tmp")
    columns = {field.name: [row.get(field.name) for row in rows] for field in schema}
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, temp_target, compression="zstd")
    os.replace(temp_target, target)


def write_csv(path: str | Path, rows: Iterable[dict[str, object]], fieldnames: list[str]) -> None:
    rows = list(rows)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_suffix(target.suffix + ".tmp")
    with temp_target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp_target, target)


def write_json(path: str | Path, payload: dict) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_suffix(target.suffix + ".tmp")
    with temp_target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temp_target, target)
