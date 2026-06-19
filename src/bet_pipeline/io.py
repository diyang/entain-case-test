from __future__ import annotations

import csv
import hashlib
import json
import os
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import TypeVar

import pyarrow as pa
import pyarrow.parquet as pq

T = TypeVar("T")


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


def read_parquet_fieldnames(path: str | Path) -> list[str]:
    return list(pq.read_schema(path).names)


def iter_parquet_batches(path: str | Path, batch_size: int) -> Iterator[list[dict[str, object]]]:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than 0")
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        yield batch.to_pylist()


def iter_batches(rows: Sequence[T], batch_size: int) -> Iterator[Sequence[T]]:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than 0")
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


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


def write_parquet_batches(
    path: str | Path,
    row_batches: Iterable[Iterable[dict[str, object]]],
    schema: pa.Schema,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_suffix(target.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    wrote_batch = False

    try:
        for rows in row_batches:
            table = _table_from_rows(rows, schema)
            if writer is None:
                writer = pq.ParquetWriter(temp_target, schema, compression="zstd")
            writer.write_table(table)
            wrote_batch = True

        if writer is None:
            table = _table_from_rows([], schema)
            pq.write_table(table, temp_target, compression="zstd")
        else:
            writer.close()
            writer = None

        os.replace(temp_target, target)
    finally:
        if writer is not None:
            writer.close()
        if temp_target.exists() and not wrote_batch:
            temp_target.unlink()


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
