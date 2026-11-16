"""Generate deterministic, fictional patient-access source files."""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

PRODUCTS = ("DRUG-A", "DRUG-B")
PAYERS = (("PAY-01", "PLAN-01"), ("PAY-02", "PLAN-02"), ("PAY-03", "PLAN-03"))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_sources(output_dir: Path, case_count: int = 36) -> dict[str, int]:
    """Write four source CSVs and return their physical row counts."""
    if case_count < 4:
        raise ValueError("case_count must be at least 4 to include each journey scenario")

    rx_claims: list[dict[str, str]] = []
    hub_events: list[dict[str, str]] = []
    sp_fills: list[dict[str, str]] = []
    formulary: list[dict[str, str]] = []

    for payer_id, plan_id in PAYERS:
        for product_id in PRODUCTS:
            status = "NOT_COVERED" if payer_id == "PAY-03" and product_id == "DRUG-B" else (
                "PREFERRED" if product_id == "DRUG-A" else "NON_PREFERRED"
            )
            formulary.append(
                {
                    "plan_id": plan_id,
                    "payer_id": payer_id,
                    "product_id": product_id,
                    "effective_start": "2024-01-01",
                    "effective_end": "2025-12-31",
                    "formulary_status": status,
                    "coverage_tier": "2" if status == "PREFERRED" else ("EXCLUDED" if status == "NOT_COVERED" else "4"),
                }
            )

    # Cycle through complete, rejected, reversed, and delayed-start journeys.
    for index in range(1, case_count + 1):
        patient_id = f"SYN-P{index:04d}"
        case_id = f"CASE-{index:04d}"
        product_id = PRODUCTS[(index - 1) % len(PRODUCTS)]
        payer_id, plan_id = PAYERS[(index - 1) % len(PAYERS)]
        enrollment = date(2025, 1, 1) + timedelta(days=(index - 1) * 3)
        scenario = (index - 1) % 4
        hub_events.extend(
            [
                _event(case_id, patient_id, product_id, payer_id, "ENROLLED", enrollment),
                _event(case_id, patient_id, product_id, payer_id, "BENEFITS_VERIFIED", enrollment + timedelta(days=1)),
                _event(
                    case_id,
                    patient_id,
                    product_id,
                    payer_id,
                    "PA_APPROVED" if scenario in (0, 3) else "PA_DENIED",
                    enrollment + timedelta(days=3),
                ),
            ]
        )

        if scenario in (0, 2, 3):
            paid_date = enrollment + timedelta(days=40 if scenario == 3 else 5)
            transaction_id = f"RX-{index:04d}-01"
            paid_claim = _claim(
                transaction_id, patient_id, product_id, payer_id, plan_id, paid_date, "PAID", "", "1250.00", ""
            )
            rx_claims.append(paid_claim)
            if scenario == 0:
                # Deliberate duplicate source delivery; silver should retain one transaction.
                rx_claims.append(paid_claim.copy())
            if scenario == 2:
                rx_claims.append(
                    _claim(
                        f"RX-{index:04d}-02",
                        patient_id,
                        product_id,
                        payer_id,
                        plan_id,
                        paid_date + timedelta(days=2),
                        "REVERSAL",
                        transaction_id,
                        "1250.00",
                        "",
                    )
                )
            if scenario in (0, 3):
                sp_fills.append(
                    {
                        "fill_id": f"FILL-{index:04d}",
                        "patient_id": patient_id,
                        "product_id": product_id,
                        "fill_date": (paid_date + timedelta(days=1)).isoformat(),
                        "fill_status": "DISPENSED",
                        "days_supply": "30",
                        "transaction_id": transaction_id,
                    }
                )
        elif scenario == 1:
            rx_claims.append(
                _claim(
                    f"RX-{index:04d}-01",
                    patient_id,
                    product_id,
                    payer_id,
                    plan_id,
                    enrollment + timedelta(days=5),
                    "REJECTED",
                    "",
                    "0.00",
                    "PA_REQUIRED",
                )
            )

    # Explicit quality-control examples: one malformed claim, one orphan fill, one unmatched reversal.
    rx_claims.extend(
        [
            _claim(
                "RX-BAD-DATE", "SYN-P0001", "DRUG-A", "PAY-01", "PLAN-01",
                "not-a-date", "REJECTED", "", "0.00", "OTHER",
            ),
            _claim(
                "RX-ORPHAN-REV", "SYN-P0001", "DRUG-A", "PAY-01", "PLAN-01",
                "2025-06-01", "REVERSAL", "RX-MISSING", "20.00", "",
            ),
        ]
    )
    sp_fills.append(
        {
            "fill_id": "FILL-ORPHAN",
            "patient_id": "SYN-P9999",
            "product_id": "DRUG-A",
            "fill_date": "2025-06-02",
            "fill_status": "DISPENSED",
            "days_supply": "30",
            "transaction_id": "",
        }
    )

    _write_csv(
        output_dir / "rx_claims.csv",
        [
            "transaction_id",
            "patient_id",
            "product_id",
            "payer_id",
            "plan_id",
            "service_date",
            "transaction_type",
            "reverses_transaction_id",
            "paid_amount",
            "reject_code",
        ],
        rx_claims,
    )
    _write_csv(
        output_dir / "formulary.csv",
        ["plan_id", "payer_id", "product_id", "effective_start", "effective_end", "formulary_status", "coverage_tier"],
        formulary,
    )
    _write_csv(
        output_dir / "hub_events.csv",
        ["event_id", "case_id", "patient_id", "product_id", "payer_id", "event_type", "event_date"],
        hub_events,
    )
    _write_csv(
        output_dir / "sp_fills.csv",
        ["fill_id", "patient_id", "product_id", "fill_date", "fill_status", "days_supply", "transaction_id"],
        sp_fills,
    )
    return {
        "rx_claims": len(rx_claims),
        "formulary": len(formulary),
        "hub_events": len(hub_events),
        "sp_fills": len(sp_fills),
    }


def _event(
    case_id: str, patient_id: str, product_id: str, payer_id: str, event_type: str, event_date: date
) -> dict[str, str]:
    suffix = {"ENROLLED": "01", "BENEFITS_VERIFIED": "02", "PA_APPROVED": "03", "PA_DENIED": "03"}[event_type]
    return {
        "event_id": f"{case_id}-EV{suffix}",
        "case_id": case_id,
        "patient_id": patient_id,
        "product_id": product_id,
        "payer_id": payer_id,
        "event_type": event_type,
        "event_date": event_date.isoformat(),
    }


def _claim(
    transaction_id: str,
    patient_id: str,
    product_id: str,
    payer_id: str,
    plan_id: str,
    service_date: date | str,
    transaction_type: str,
    reverses_transaction_id: str,
    paid_amount: str,
    reject_code: str,
) -> dict[str, str]:
    return {
        "transaction_id": transaction_id,
        "patient_id": patient_id,
        "product_id": product_id,
        "payer_id": payer_id,
        "plan_id": plan_id,
        "service_date": service_date.isoformat() if isinstance(service_date, date) else service_date,
        "transaction_type": transaction_type,
        "reverses_transaction_id": reverses_transaction_id,
        "paid_amount": paid_amount,
        "reject_code": reject_code,
    }
