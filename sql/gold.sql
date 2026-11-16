CREATE TABLE gold_dim_patient AS
SELECT patient_id, 'SYNTHETIC' AS data_classification
FROM (
    SELECT patient_id FROM silver_hub_events
    UNION
    SELECT patient_id FROM silver_rx_claims
);

CREATE TABLE gold_dim_product AS
SELECT 'DRUG-A' AS product_id, 'Fictional Product A' AS product_label
UNION ALL
SELECT 'DRUG-B', 'Fictional Product B';

CREATE TABLE gold_dim_payer AS
SELECT 'PAY-01' AS payer_id, 'Fictional Payer 01' AS payer_label
UNION ALL SELECT 'PAY-02', 'Fictional Payer 02'
UNION ALL SELECT 'PAY-03', 'Fictional Payer 03';

CREATE TABLE gold_dim_plan_product AS
SELECT
    plan_id,
    payer_id,
    product_id,
    effective_start,
    effective_end,
    formulary_status,
    coverage_tier
FROM silver_formulary;

CREATE TABLE gold_fact_rx_transaction AS
SELECT
    r.transaction_id,
    r.patient_id,
    r.product_id,
    r.payer_id,
    r.plan_id,
    r.service_date,
    r.transaction_type,
    r.reverses_transaction_id,
    r.paid_amount,
    r.reject_code,
    CASE
        WHEN r.transaction_type = 'PAID'
         AND NOT EXISTS (
             SELECT 1 FROM silver_rx_claims reversal
             WHERE reversal.transaction_type = 'REVERSAL'
               AND reversal.reverses_transaction_id = r.transaction_id
         )
        THEN 1 ELSE 0
    END AS is_net_paid,
    COALESCE(f.formulary_status, 'UNKNOWN') AS formulary_status
FROM silver_rx_claims r
LEFT JOIN silver_formulary f
    ON f.plan_id = r.plan_id
   AND f.product_id = r.product_id
   AND r.service_date BETWEEN f.effective_start AND f.effective_end;

CREATE TABLE gold_fact_hub_event AS
SELECT event_id, case_id, patient_id, product_id, payer_id, event_type, event_date
FROM silver_hub_events;

CREATE TABLE gold_fact_sp_fill AS
SELECT fill_id, patient_id, product_id, fill_date, fill_status, days_supply, transaction_id
FROM silver_sp_fills;

CREATE TABLE gold_mart_patient_access AS
WITH enrollments AS (
    SELECT case_id, patient_id, product_id, payer_id, MIN(event_date) AS enrollment_date
    FROM silver_hub_events
    WHERE event_type = 'ENROLLED'
    GROUP BY case_id, patient_id, product_id, payer_id
),
first_paid AS (
    SELECT patient_id, product_id, MIN(service_date) AS first_net_paid_date
    FROM gold_fact_rx_transaction
    WHERE is_net_paid = 1
    GROUP BY patient_id, product_id
),
first_dispense AS (
    SELECT patient_id, product_id, MIN(fill_date) AS first_dispense_date
    FROM silver_sp_fills
    WHERE fill_status = 'DISPENSED'
    GROUP BY patient_id, product_id
)
SELECT
    e.case_id,
    e.patient_id,
    e.product_id,
    e.payer_id,
    e.enrollment_date,
    p.first_net_paid_date,
    d.first_dispense_date,
    CAST(julianday(p.first_net_paid_date) - julianday(e.enrollment_date) AS INTEGER) AS days_to_net_paid,
    CAST(julianday(d.first_dispense_date) - julianday(e.enrollment_date) AS INTEGER) AS days_to_dispense,
    CASE
        WHEN p.first_net_paid_date BETWEEN e.enrollment_date AND date(e.enrollment_date, '+30 days')
        THEN 1 ELSE 0
    END AS started_within_30d,
    CASE
        WHEN d.first_dispense_date IS NOT NULL THEN 'DISPENSED'
        WHEN p.first_net_paid_date IS NOT NULL THEN 'PAID_NOT_DISPENSED'
        ELSE 'NO_NET_PAID_CLAIM'
    END AS journey_status
FROM enrollments e
LEFT JOIN first_paid p ON p.patient_id = e.patient_id AND p.product_id = e.product_id
LEFT JOIN first_dispense d ON d.patient_id = e.patient_id AND d.product_id = e.product_id;

CREATE TABLE gold_monthly_access_metrics AS
SELECT
    strftime('%Y-%m', enrollment_date) AS enrollment_month,
    payer_id,
    product_id,
    COUNT(*) AS enrolled_cases,
    SUM(started_within_30d) AS starts_within_30d,
    SUM(CASE WHEN started_within_30d = 0 THEN 1 ELSE 0 END) AS no_start_within_30d,
    SUM(CASE WHEN first_dispense_date IS NOT NULL THEN 1 ELSE 0 END) AS dispensed_cases,
    ROUND(
        1.0 * SUM(CASE WHEN started_within_30d = 0 THEN 1 ELSE 0 END) / COUNT(*),
        4
    ) AS no_start_rate_30d
FROM gold_mart_patient_access
GROUP BY strftime('%Y-%m', enrollment_date), payer_id, product_id;

CREATE TABLE gold_dq_unmatched_sp_fills AS
SELECT f.fill_id, f.patient_id, f.product_id, f.fill_date
FROM silver_sp_fills f
LEFT JOIN gold_dim_patient p ON p.patient_id = f.patient_id
LEFT JOIN gold_dim_product d ON d.product_id = f.product_id
WHERE p.patient_id IS NULL OR d.product_id IS NULL;
