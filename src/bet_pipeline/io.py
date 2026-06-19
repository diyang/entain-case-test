from __future__ import annotations

import csv
import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def ensure_dir(path: str | Path) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def read_csv_fieldnames(path: str | Path) -> list[str]:
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return []
        return list(reader.fieldnames)


def iter_csv_batches(path: str | Path, batch_size: int) -> Iterator[list[dict[str, str]]]:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than 0")
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        batch: list[dict[str, str]] = []
        for row in reader:
            batch.append(row)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def read_parquet(path: str | Path) -> tuple[list[str], list[dict[str, object]]]:
    table = pq.read_table(path)
    rows = table.to_pylist()
    return list(table.column_names), rows


def read_json(path: str | Path) -> dict:
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def write_parquet(path: str | Path, rows: Iterable[dict[str, object]], schema: pa.Schema) -> None:
    rows = list(rows)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_suffix(target.suffix + ".tmp")
    columns = {field.name: [row.get(field.name) for row in rows] for field in schema}
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, temp_target, compression="zstd")
    os.replace(temp_target, target)


def _table_from_rows(rows: Iterable[dict[str, object]], schema: pa.Schema) -> pa.Table:
    rows = list(rows)
    columns = {field.name: [row.get(field.name) for row in rows] for field in schema}
    return pa.Table.from_pydict(columns, schema=schema)


class ParquetBatchWriter:
    def __init__(self, path: str | Path, schema: pa.Schema) -> None:
        self.target = Path(path)
        self.schema = schema
        self.temp_target = self.target.with_suffix(self.target.suffix + ".tmp")
        self.writer: pq.ParquetWriter | None = None
        self.wrote_batch = False

    def __enter__(self) -> ParquetBatchWriter:
        self.target.parent.mkdir(parents=True, exist_ok=True)
        return self

    def write(self, rows: Iterable[dict[str, object]]) -> None:
        table = _table_from_rows(rows, self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.temp_target, self.schema, compression="zstd")
        self.writer.write_table(table)
        self.wrote_batch = True

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None

        if exc_type is not None:
            if self.temp_target.exists():
                self.temp_target.unlink()
            return

        if not self.wrote_batch:
            table = _table_from_rows([], self.schema)
            pq.write_table(table, self.temp_target, compression="zstd")
        os.replace(self.temp_target, self.target)


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
