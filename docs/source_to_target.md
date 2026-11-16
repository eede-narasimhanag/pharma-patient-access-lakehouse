# Source-to-target mapping

All source identifiers and content are generated for this demo. Patient IDs are synthetic tokens, not transformed
real-world identifiers.

| Source | Source grain | Bronze | Silver | Gold |
|---|---|---|---|---|
| Rx claims | One claim transaction, including rejects and reversals | `bronze_rx_claims` keeps CSV values as text | `silver_rx_claims` types dates and amount; deduplicates transaction ID; quarantines malformed rows | `gold_fact_rx_transaction` retains transaction grain and resolves net-paid status |
| Formulary | One plan-product effective-date interval | `bronze_formulary` keeps source values | `silver_formulary` validates interval boundaries | `gold_dim_plan_product` preserves effective-dated plan-product rows |
| HUB events | One event in a patient access case | `bronze_hub_events` keeps source values | `silver_hub_events` validates event/date and unique event ID | `gold_fact_hub_event`; case-level metrics derive enrollment date |
| Specialty pharmacy | One fill event | `bronze_sp_fills` keeps source values | `silver_sp_fills` types dates/days supply and validates status | `gold_fact_sp_fill`; unmatched patient references are exposed for review |

## Gold model grains

- `gold_dim_patient`: one synthetic patient token; no demographics or direct identifiers.
- `gold_dim_product`: one fictional product.
- `gold_dim_payer`: one fictional payer.
- `gold_dim_plan_product`: one plan/product effective interval; this is the effective-dated access reference.
- `gold_fact_rx_transaction`: one unique Rx transaction, including rejected and reversed transactions.
- `gold_fact_hub_event`: one HUB event.
- `gold_fact_sp_fill`: one specialty-pharmacy fill event.
- `gold_mart_patient_access`: one HUB case, with first net-paid and first dispensed dates.
- `gold_monthly_access_metrics`: enrollment month x payer x product.

The demo joins case, claims, and fills by synthetic patient/product identifiers and date windows. It does not model a
real vendor's identity-resolution rules or a production source contract.
