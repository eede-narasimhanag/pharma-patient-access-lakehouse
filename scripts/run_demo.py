from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from pharma_access.dashboard import build_dashboard
from pharma_access.generate import generate_sources
from pharma_access.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the synthetic pharma patient-access lakehouse demo.")
    parser.add_argument("--output", type=Path, default=Path("data/generated"))
    parser.add_argument("--cases", type=int, default=36)
    args = parser.parse_args()

    output_dir = args.output.resolve()
    counts = generate_sources(output_dir, args.cases)
    database_path = output_dir / "patient_access.db"
    results = run_pipeline(output_dir, database_path)
    dashboard_path = output_dir / "dashboard.html"
    build_dashboard(database_path, dashboard_path)

    print("Synthetic source rows:", counts)
    print("Pipeline checks:", results)
    with sqlite3.connect(database_path) as connection:
        print("\nMonthly access metrics:")
        for row in connection.execute(
            """SELECT enrollment_month, payer_id, product_id, enrolled_cases,
                      starts_within_30d, no_start_within_30d, dispensed_cases, no_start_rate_30d
               FROM gold_monthly_access_metrics
               ORDER BY enrollment_month, payer_id, product_id"""
        ):
            print(row)
        print("\nData quality checks:")
        for row in connection.execute("SELECT check_name, check_value, status FROM gold_dq_checks ORDER BY check_name"):
            print(row)
    print(f"\nSQLite lakehouse: {database_path}")
    print(f"HTML dashboard: {dashboard_path}")


if __name__ == "__main__":
    main()
