import sqlite3
from pathlib import Path

import pytest

from pharma_access.dashboard import build_dashboard
from pharma_access.generate import generate_sources
from pharma_access.pipeline import run_pipeline


@pytest.fixture
def database(tmp_path: Path) -> Path:
    sources = tmp_path / "sources"
    generate_sources(sources)
    db_path = tmp_path / "lakehouse.db"
    run_pipeline(sources, db_path)
    return db_path


def test_pipeline_resolves_duplicates_and_reversals(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        duplicate_count = connection.execute(
            "SELECT check_value FROM gold_dq_checks WHERE check_name = 'duplicate_rx_transactions_quarantined'"
        ).fetchone()[0]
        net_paid_count = connection.execute(
            "SELECT COUNT(*) FROM gold_fact_rx_transaction WHERE is_net_paid = 1"
        ).fetchone()[0]
        reversed_paid_count = connection.execute(
            """SELECT COUNT(*) FROM gold_fact_rx_transaction p
               WHERE p.transaction_type = 'PAID' AND p.is_net_paid = 0"""
        ).fetchone()[0]
    assert duplicate_count == 9
    assert net_paid_count > 0
    assert reversed_paid_count == 9


def test_patient_access_mart_has_one_row_per_enrolled_case(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        enrolled_count = connection.execute(
            "SELECT COUNT(DISTINCT case_id) FROM silver_hub_events WHERE event_type = 'ENROLLED'"
        ).fetchone()[0]
        mart_count = connection.execute("SELECT COUNT(*) FROM gold_mart_patient_access").fetchone()[0]
        orphan_count = connection.execute("SELECT COUNT(*) FROM gold_dq_unmatched_sp_fills").fetchone()[0]
    assert mart_count == enrolled_count == 36
    assert orphan_count == 1


def test_reconciliation_and_quality_statuses(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        checks = dict(connection.execute("SELECT check_name, status FROM gold_dq_checks"))
        quarantined = connection.execute("SELECT COUNT(*) FROM silver_quarantine").fetchone()[0]
        rates = connection.execute(
            "SELECT no_start_rate_30d FROM gold_monthly_access_metrics"
        ).fetchall()
    assert checks["rx_source_reconciliation"] == "PASS"
    assert checks["unmatched_reversals"] == "REVIEW"
    assert checks["unmatched_sp_fills"] == "REVIEW"
    assert quarantined >= 10
    assert rates and all(0 <= row[0] <= 1 for row in rates)


def test_generator_rejects_too_few_cases(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least 4"):
        generate_sources(tmp_path / "sources", case_count=3)


def test_dashboard_renders_metrics_and_data_quality(database: Path, tmp_path: Path) -> None:
    dashboard_path = tmp_path / "dashboard.html"
    build_dashboard(database, dashboard_path)
    html = dashboard_path.read_text(encoding="utf-8")
    assert "Patient access overview" in html
    assert "No-start rate (30 days)" in html
    assert "unmatched sp fills" in html
    assert "REVIEW" in html
