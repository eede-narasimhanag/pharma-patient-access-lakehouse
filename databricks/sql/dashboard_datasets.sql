-- Dataset 1: monthly enrollment, starts, and dispensing trend.
SELECT
    enrollment_month,
    SUM(enrolled_cases) AS enrolled_cases,
    SUM(starts_within_30d) AS starts_within_30d,
    SUM(dispensed_cases) AS dispensed_cases,
    ROUND(
        100.0 * SUM(no_start_within_30d) / NULLIF(SUM(enrolled_cases), 0),
        1
    ) AS no_start_rate_pct
FROM workspace.gold.monthly_access_metrics
GROUP BY enrollment_month
ORDER BY enrollment_month;

-- Dataset 2: payer and product access comparison.
SELECT
    payer_id,
    product_id,
    SUM(enrolled_cases) AS enrolled_cases,
    SUM(starts_within_30d) AS starts_within_30d,
    SUM(dispensed_cases) AS dispensed_cases,
    ROUND(
        100.0 * SUM(no_start_within_30d) / NULLIF(SUM(enrolled_cases), 0),
        1
    ) AS no_start_rate_pct
FROM workspace.gold.monthly_access_metrics
GROUP BY payer_id, product_id
ORDER BY payer_id, product_id;

-- Dataset 3: case outcomes for a simple journey-status chart.
SELECT
    journey_status,
    COUNT(*) AS case_count
FROM workspace.gold.mart_patient_access
GROUP BY journey_status
ORDER BY journey_status;
