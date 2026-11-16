"""Build Bronze, Silver, and Gold SQLite tables from the synthetic source CSVs."""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

SOURCE_FILES = {
    "rx_claims": "rx_claims.csv",
    "formulary": "formulary.csv",
    "hub_events": "hub_events.csv",
    "sp_fills": "sp_fills.csv",
}


def run_pipeline(source_dir: Path, database_path: Path) -> dict[str, int]:
    """Create a fresh local lakehouse database and return key row-count checks."""
    database_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{database_path.name}.", suffix=".tmp", dir=database_path.parent
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        connection = sqlite3.connect(temporary_path)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(
                """CREATE TABLE silver_quarantine (
                    source_name TEXT NOT NULL, source_key TEXT NOT NULL,
                    reason TEXT NOT NULL, raw_record TEXT NOT NULL
                )"""
            )
            for source_name, filename in SOURCE_FILES.items():
                _build_bronze(connection, source_name, source_dir / filename)
            for source_name in SOURCE_FILES:
                _build_silver(connection, source_name)
            connection.executescript((SQL_DIR / "gold.sql").read_text(encoding="utf-8"))
            _build_quality_checks(connection)

            results = {
                "bronze_rx_rows": _count(connection, "bronze_rx_claims"),
                "silver_rx_rows": _count(connection, "silver_rx_claims"),
                "quarantined_rows": _count(connection, "silver_quarantine"),
                "net_paid_claims": _scalar(
                    connection, "SELECT COUNT(*) FROM gold_fact_rx_transaction WHERE is_net_paid = 1"
                ),
                "patient_cases": _count(connection, "gold_mart_patient_access"),
                "unmatched_sp_fills": _scalar(connection, "SELECT COUNT(*) FROM gold_dq_unmatched_sp_fills"),
            }
        finally:
            connection.close()
        temporary_path.replace(database_path)
        return results
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _build_bronze(connection: sqlite3.Connection, source_name: str, path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Missing source file: {path}")
    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        columns = reader.fieldnames
        if not columns:
            raise ValueError(f"Source has no header: {path}")
        table = f"bronze_{source_name}"
        definitions = ", ".join(f'"{column}" TEXT' for column in columns)
        connection.execute(f'CREATE TABLE "{table}" ({definitions}, _source_file TEXT NOT NULL)')
        rows = [tuple(row.get(column, "") or "" for column in columns) + (path.name,) for row in reader]
        placeholders = ", ".join("?" for _ in range(len(columns) + 1))
        connection.executemany(f'INSERT INTO "{table}" VALUES ({placeholders})', rows)


def _build_silver(connection: sqlite3.Connection, source_name: str) -> None:
    rows = connection.execute(f"SELECT * FROM bronze_{source_name}").fetchall()
    columns = [item[0] for item in connection.execute(f"SELECT * FROM bronze_{source_name} LIMIT 0").description]
    records = [dict(zip(columns, row, strict=True)) for row in rows]
    if source_name == "rx_claims":
        _silver_rx(connection, records)
    elif source_name == "formulary":
        _silver_formulary(connection, records)
    elif source_name == "hub_events":
        _silver_hub(connection, records)
    else:
        _silver_fills(connection, records)


def _silver_rx(connection: sqlite3.Connection, records: list[dict[str, Any]]) -> None:
    connection.execute(
        """CREATE TABLE silver_rx_claims (
            transaction_id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, product_id TEXT NOT NULL,
            payer_id TEXT NOT NULL, plan_id TEXT NOT NULL, service_date TEXT NOT NULL,
            transaction_type TEXT NOT NULL, reverses_transaction_id TEXT, paid_amount REAL NOT NULL,
            reject_code TEXT, _source_file TEXT NOT NULL
        )"""
    )
    seen: set[str] = set()
    accepted = []
    for row in records:
        key = str(row["transaction_id"]).strip()
        if key in seen:
            _quarantine(connection, "rx_claims", key, "duplicate_transaction_id", row)
            continue
        seen.add(key)
        try:
            service_date = date.fromisoformat(str(row["service_date"]))
            paid_amount = float(row["paid_amount"])
            transaction_type = str(row["transaction_type"]).strip().upper()
            if not key or not row["patient_id"] or not row["product_id"]:
                raise ValueError("missing required key")
            if transaction_type not in {"PAID", "REJECTED", "REVERSAL"}:
                raise ValueError("invalid_transaction_type")
            if paid_amount < 0:
                raise ValueError("negative_paid_amount")
        except (TypeError, ValueError) as error:
            _quarantine(connection, "rx_claims", key or "<missing>", str(error), row)
            continue
        accepted.append(
            (
                key,
                row["patient_id"],
                row["product_id"],
                row["payer_id"],
                row["plan_id"],
                service_date.isoformat(),
                transaction_type,
                row["reverses_transaction_id"] or None,
                paid_amount,
                row["reject_code"] or None,
                row["_source_file"],
            )
        )
    connection.executemany("INSERT INTO silver_rx_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", accepted)


def _silver_formulary(connection: sqlite3.Connection, records: list[dict[str, Any]]) -> None:
    connection.execute(
        """CREATE TABLE silver_formulary (
            plan_id TEXT NOT NULL, payer_id TEXT NOT NULL, product_id TEXT NOT NULL,
            effective_start TEXT NOT NULL, effective_end TEXT NOT NULL, formulary_status TEXT NOT NULL,
            coverage_tier TEXT NOT NULL, _source_file TEXT NOT NULL,
            PRIMARY KEY (plan_id, product_id, effective_start)
        )"""
    )
    accepted = []
    for row in records:
        try:
            start = date.fromisoformat(row["effective_start"])
            end = date.fromisoformat(row["effective_end"])
            if start > end:
                raise ValueError("effective_start_after_end")
        except (TypeError, ValueError) as error:
            _quarantine(connection, "formulary", row.get("plan_id", "<missing>"), str(error), row)
            continue
        accepted.append(
            (
                row["plan_id"],
                row["payer_id"],
                row["product_id"],
                start.isoformat(),
                end.isoformat(),
                row["formulary_status"].strip().upper(),
                row["coverage_tier"].strip().upper(),
                row["_source_file"],
            )
        )
    connection.executemany("INSERT INTO silver_formulary VALUES (?, ?, ?, ?, ?, ?, ?, ?)", accepted)


def _silver_hub(connection: sqlite3.Connection, records: list[dict[str, Any]]) -> None:
    connection.execute(
        """CREATE TABLE silver_hub_events (
            event_id TEXT PRIMARY KEY, case_id TEXT NOT NULL, patient_id TEXT NOT NULL,
            product_id TEXT NOT NULL, payer_id TEXT NOT NULL, event_type TEXT NOT NULL,
            event_date TEXT NOT NULL, _source_file TEXT NOT NULL
        )"""
    )
    accepted = []
    seen: set[str] = set()
    for row in records:
        key = str(row["event_id"]).strip()
        try:
            event_date = date.fromisoformat(row["event_date"])
            if not key or key in seen:
                raise ValueError("duplicate_or_missing_event_id")
            seen.add(key)
            if row["event_type"] not in {"ENROLLED", "BENEFITS_VERIFIED", "PA_APPROVED", "PA_DENIED"}:
                raise ValueError("invalid_event_type")
        except (TypeError, ValueError) as error:
            _quarantine(connection, "hub_events", key or "<missing>", str(error), row)
            continue
        accepted.append(
            (
                key,
                row["case_id"],
                row["patient_id"],
                row["product_id"],
                row["payer_id"],
                row["event_type"],
                event_date.isoformat(),
                row["_source_file"],
            )
        )
    connection.executemany("INSERT INTO silver_hub_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)", accepted)


def _silver_fills(connection: sqlite3.Connection, records: list[dict[str, Any]]) -> None:
    connection.execute(
        """CREATE TABLE silver_sp_fills (
            fill_id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, product_id TEXT NOT NULL,
            fill_date TEXT NOT NULL, fill_status TEXT NOT NULL, days_supply INTEGER NOT NULL,
            transaction_id TEXT, _source_file TEXT NOT NULL
        )"""
    )
    accepted = []
    seen: set[str] = set()
    for row in records:
        key = str(row["fill_id"]).strip()
        try:
            fill_date = date.fromisoformat(row["fill_date"])
            days_supply = int(row["days_supply"])
            if not key or key in seen:
                raise ValueError("duplicate_or_missing_fill_id")
            seen.add(key)
            if days_supply <= 0 or row["fill_status"] not in {"DISPENSED", "REVERSED"}:
                raise ValueError("invalid_fill_attributes")
        except (TypeError, ValueError) as error:
            _quarantine(connection, "sp_fills", key or "<missing>", str(error), row)
            continue
        accepted.append(
            (
                key,
                row["patient_id"],
                row["product_id"],
                fill_date.isoformat(),
                row["fill_status"],
                days_supply,
                row["transaction_id"] or None,
                row["_source_file"],
            )
        )
    connection.executemany("INSERT INTO silver_sp_fills VALUES (?, ?, ?, ?, ?, ?, ?, ?)", accepted)


def _quarantine(
    connection: sqlite3.Connection, source_name: str, source_key: str, reason: str, record: dict[str, Any]
) -> None:
    connection.execute(
        "INSERT INTO silver_quarantine VALUES (?, ?, ?, ?)",
        (source_name, source_key, reason, json.dumps(record, sort_keys=True)),
    )


def _build_quality_checks(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """CREATE TABLE gold_dq_checks (
            check_name TEXT PRIMARY KEY, check_value INTEGER NOT NULL, status TEXT NOT NULL
        );
        INSERT INTO gold_dq_checks
        SELECT 'duplicate_rx_transactions_quarantined', COUNT(*), 'PASS'
        FROM silver_quarantine WHERE source_name = 'rx_claims' AND reason = 'duplicate_transaction_id';
        INSERT INTO gold_dq_checks
        SELECT 'malformed_rows_quarantined', COUNT(*), 'PASS' FROM silver_quarantine;
        INSERT INTO gold_dq_checks
        SELECT 'unmatched_reversals', COUNT(*),
            CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'REVIEW' END
        FROM silver_rx_claims r
        WHERE r.transaction_type = 'REVERSAL'
          AND NOT EXISTS (
              SELECT 1 FROM silver_rx_claims p
              WHERE p.transaction_id = r.reverses_transaction_id AND p.transaction_type = 'PAID'
          );
        INSERT INTO gold_dq_checks
        SELECT 'unmatched_sp_fills', COUNT(*),
            CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'REVIEW' END
        FROM gold_dq_unmatched_sp_fills;
        INSERT INTO gold_dq_checks
        SELECT 'rx_source_reconciliation', ABS(
            (SELECT COUNT(*) FROM bronze_rx_claims)
            - (SELECT COUNT(*) FROM silver_rx_claims)
            - (SELECT COUNT(*) FROM silver_quarantine WHERE source_name = 'rx_claims')
        ), CASE WHEN ABS(
            (SELECT COUNT(*) FROM bronze_rx_claims)
            - (SELECT COUNT(*) FROM silver_rx_claims)
            - (SELECT COUNT(*) FROM silver_quarantine WHERE source_name = 'rx_claims')
        ) = 0 THEN 'PASS' ELSE 'FAIL' END;
        """
    )


def _count(connection: sqlite3.Connection, table: str) -> int:
    return _scalar(connection, f'SELECT COUNT(*) FROM "{table}"')


def _scalar(connection: sqlite3.Connection, query: str) -> int:
    return int(connection.execute(query).fetchone()[0])
