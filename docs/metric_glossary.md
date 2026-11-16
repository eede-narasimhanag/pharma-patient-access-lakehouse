# Metric glossary

All definitions below apply only to the generated demonstration data. They are illustrative and are not validated
pharma business definitions.

| Metric | Definition | Grain / caveat |
|---|---|---|
| Enrolled cases | Distinct HUB `case_id` with an `ENROLLED` event | One row per case in `gold_mart_patient_access` |
| Net-paid transaction | `PAID` Rx transaction not referenced by a valid `REVERSAL` transaction | One source transaction; a paid transaction can be reversed later |
| Start within 30 days | First net-paid claim date is from enrollment through enrollment + 30 calendar days, inclusive | Patient-product case; only synthetic case-enrollment data |
| No-start rate (30d) | Enrolled cases without a net-paid claim within 30 days / all enrolled cases | Cohort is considered mature because generated dates are historical relative to the demo's run date |
| Dispensed cases | Cases with at least one `DISPENSED` specialty-pharmacy fill | Does not establish adherence, persistence, or clinical outcome |
| Days to dispense | First dispense date minus enrollment date | Null when no dispense exists; negative values should be treated as a data-quality issue |

`gold_monthly_access_metrics` groups cases by enrollment month, payer, and fictional product. It does not represent
market share, real-world access, prescription volume, or actual patient behavior.
