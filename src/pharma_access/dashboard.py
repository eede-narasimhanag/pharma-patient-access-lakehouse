"""Render a self-contained HTML summary from the Gold tables."""

from __future__ import annotations

import html
import sqlite3
from pathlib import Path


def build_dashboard(database_path: Path, output_path: Path) -> None:
    """Write a static dashboard using the current Gold tables."""
    with sqlite3.connect(database_path) as connection:
        totals = connection.execute(
            """SELECT COUNT(*),
                      SUM(started_within_30d),
                      SUM(CASE WHEN first_dispense_date IS NOT NULL THEN 1 ELSE 0 END),
                      SUM(CASE WHEN started_within_30d = 0 THEN 1 ELSE 0 END)
               FROM gold_mart_patient_access"""
        ).fetchone()
        monthly = connection.execute(
            """SELECT enrollment_month, payer_id, product_id, enrolled_cases,
                      starts_within_30d, no_start_within_30d, dispensed_cases, no_start_rate_30d
               FROM gold_monthly_access_metrics
               ORDER BY enrollment_month, payer_id, product_id"""
        ).fetchall()
        quality = connection.execute(
            "SELECT check_name, check_value, status FROM gold_dq_checks ORDER BY check_name"
        ).fetchall()

    enrollments, starts, dispensed, no_starts = (int(value or 0) for value in totals)
    no_start_rate = no_starts / enrollments if enrollments else 0
    monthly_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(month)}</td><td>{html.escape(payer)}</td><td>{html.escape(product)}</td>"
        f"<td>{enrolled}</td><td>{started}</td><td>{no_start}</td><td>{filled}</td>"
        f"<td><span class=\"bar\" style=\"--rate:{rate * 100:.0f}%\"></span> {rate:.0%}</td>"
        "</tr>"
        for month, payer, product, enrolled, started, no_start, filled, rate in monthly
    )
    quality_rows = "\n".join(
        f"<tr><td>{html.escape(name.replace('_', ' '))}</td><td>{value}</td>"
        f"<td class=\"{status.lower()}\">{status}</td></tr>"
        for name, value, status in quality
    )

    document = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Patient Access Metrics | Synthetic Lakehouse</title>
  <style>
    :root {{ color-scheme: light; font-family: Segoe UI, Arial, sans-serif; color: #172b4d; background: #f3f6fa; }}
    body {{ margin: 0; }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 32px 20px 56px; }}
    h1 {{ margin-bottom: 8px; }}
    .note {{ color: #52647a; line-height: 1.5; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin: 24px 0; }}
    .card, section {{ background: white; border: 1px solid #dce4ee; border-radius: 10px; padding: 18px; }}
    .card span {{ display: block; color: #52647a; font-size: .9rem; }}
    .card strong {{ display: block; margin-top: 8px; font-size: 1.8rem; }}
    section {{ margin-top: 18px; overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 760px; }}
    th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #e8edf3; white-space: nowrap; }}
    th {{ color: #52647a; font-size: .82rem; }}
    .bar {{ display: inline-block; height: 9px; width: var(--rate); max-width: 90px;
      background: #6483d8; border-radius: 8px; }}
    .pass {{ color: #177245; font-weight: 600; }} .review {{ color: #9a5a00; font-weight: 600; }}
    footer {{ margin-top: 22px; color: #52647a; font-size: .86rem; }}
  </style>
</head>
<body>
<main>
  <h1>Patient access overview</h1>
  <p class="note">Fictional, generated data only. A 30-day start is a net-paid Rx claim within 30 days of HUB
  enrollment. Reversed claims are excluded. This demonstration does not represent actual patient behavior or
  commercial performance.</p>
  <div class="cards">
    <div class="card"><span>HUB enrolled cases</span><strong>{enrollments}</strong></div>
    <div class="card"><span>Net-paid starts within 30 days</span><strong>{starts}</strong></div>
    <div class="card"><span>Dispensed cases</span><strong>{dispensed}</strong></div>
    <div class="card"><span>No-start rate (30 days)</span><strong>{no_start_rate:.0%}</strong></div>
  </div>
  <section>
    <h2>Monthly access by payer and product</h2>
    <table>
      <thead><tr><th>Enrollment month</th><th>Payer</th><th>Product</th><th>Enrolled</th>
      <th>Starts ≤30d</th><th>No-start ≤30d</th><th>Dispensed</th><th>No-start rate</th></tr></thead>
      <tbody>{monthly_rows}</tbody>
    </table>
  </section>
  <section>
    <h2>Data quality and reconciliation</h2>
    <p class="note">REVIEW indicates a deliberately injected exception that needs investigation,
    not a pipeline failure.</p>
    <table>
      <thead><tr><th>Check</th><th>Count / difference</th><th>Status</th></tr></thead>
      <tbody>{quality_rows}</tbody>
    </table>
  </section>
  <footer>All products, payers, cases, transactions, and events in this dashboard are synthetic.</footer>
</main>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(document, encoding="utf-8")
