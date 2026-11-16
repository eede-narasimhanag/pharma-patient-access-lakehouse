# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold analytics model
# MAGIC Builds Delta dimensions, facts, the patient-access mart, and monthly metrics.

# COMMAND ----------

CATALOG = "workspace"


def create_gold_table(table_name: str, query: str) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {CATALOG}.gold.{table_name}
        USING DELTA
        AS {query}
        """
    )

# COMMAND ----------

create_gold_table(
    "dim_patient",
    """
    SELECT patient_id, 'SYNTHETIC' AS data_classification
    FROM (
        SELECT patient_id FROM workspace.silver.hub_events
        UNION
        SELECT patient_id FROM workspace.silver.rx_claims
    )
    """,
)

create_gold_table(
    "dim_product",
    """
    SELECT 'DRUG-A' AS product_id, 'Fictional Product A' AS product_label
    UNION ALL
    SELECT 'DRUG-B', 'Fictional Product B'
    """,
)

create_gold_table(
    "dim_payer",
    """
    SELECT 'PAY-01' AS payer_id, 'Fictional Payer 01' AS payer_label
    UNION ALL SELECT 'PAY-02', 'Fictional Payer 02'
    UNION ALL SELECT 'PAY-03', 'Fictional Payer 03'
    """,
)

create_gold_table(
    "dim_plan_product",
    """
    SELECT
        plan_id,
        payer_id,
        product_id,
        effective_start,
        effective_end,
        formulary_status,
        coverage_tier
    FROM workspace.silver.formulary
    """,
)

# COMMAND ----------

create_gold_table(
    "fact_rx_transaction",
    """
    WITH reversals AS (
        SELECT DISTINCT reverses_transaction_id
        FROM workspace.silver.rx_claims
        WHERE transaction_type = 'REVERSAL'
          AND reverses_transaction_id IS NOT NULL
          AND reverses_transaction_id <> ''
    )
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
            WHEN r.transaction_type = 'PAID' AND reversal.reverses_transaction_id IS NULL THEN 1
            ELSE 0
        END AS is_net_paid,
        COALESCE(f.formulary_status, 'UNKNOWN') AS formulary_status
    FROM workspace.silver.rx_claims r
    LEFT JOIN reversals reversal
      ON reversal.reverses_transaction_id = r.transaction_id
    LEFT JOIN workspace.silver.formulary f
      ON f.plan_id = r.plan_id
     AND f.product_id = r.product_id
     AND r.service_date BETWEEN f.effective_start AND f.effective_end
    """,
)

create_gold_table(
    "fact_hub_event",
    """
    SELECT event_id, case_id, patient_id, product_id, payer_id, event_type, event_date
    FROM workspace.silver.hub_events
    """,
)

create_gold_table(
    "fact_sp_fill",
    """
    SELECT fill_id, patient_id, product_id, fill_date, fill_status, days_supply, transaction_id
    FROM workspace.silver.sp_fills
    """,
)

# COMMAND ----------

create_gold_table(
    "mart_patient_access",
    """
    WITH enrollments AS (
        SELECT case_id, patient_id, product_id, payer_id, MIN(event_date) AS enrollment_date
        FROM workspace.silver.hub_events
        WHERE event_type = 'ENROLLED'
        GROUP BY case_id, patient_id, product_id, payer_id
    ),
    first_paid AS (
        SELECT e.case_id, MIN(r.service_date) AS first_net_paid_date
        FROM enrollments e
        JOIN workspace.gold.fact_rx_transaction r
          ON r.patient_id = e.patient_id
         AND r.product_id = e.product_id
         AND r.is_net_paid = 1
         AND r.service_date >= e.enrollment_date
        GROUP BY e.case_id
    ),
    first_dispense AS (
        SELECT e.case_id, MIN(f.fill_date) AS first_dispense_date
        FROM enrollments e
        JOIN workspace.silver.sp_fills f
          ON f.patient_id = e.patient_id
         AND f.product_id = e.product_id
         AND f.fill_status = 'DISPENSED'
         AND f.fill_date >= e.enrollment_date
        GROUP BY e.case_id
    )
    SELECT
        e.case_id,
        e.patient_id,
        e.product_id,
        e.payer_id,
        e.enrollment_date,
        p.first_net_paid_date,
        d.first_dispense_date,
        DATEDIFF(p.first_net_paid_date, e.enrollment_date) AS days_to_net_paid,
        DATEDIFF(d.first_dispense_date, e.enrollment_date) AS days_to_dispense,
        CASE
            WHEN p.first_net_paid_date <= DATE_ADD(e.enrollment_date, 30) THEN 1
            ELSE 0
        END AS started_within_30d,
        CASE
            WHEN d.first_dispense_date IS NOT NULL THEN 'DISPENSED'
            WHEN p.first_net_paid_date IS NOT NULL THEN 'PAID_NOT_DISPENSED'
            ELSE 'NO_NET_PAID_CLAIM'
        END AS journey_status
    FROM enrollments e
    LEFT JOIN first_paid p ON p.case_id = e.case_id
    LEFT JOIN first_dispense d ON d.case_id = e.case_id
    """,
)

create_gold_table(
    "monthly_access_metrics",
    """
    SELECT
        DATE_FORMAT(enrollment_date, 'yyyy-MM') AS enrollment_month,
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
    FROM workspace.gold.mart_patient_access
    GROUP BY DATE_FORMAT(enrollment_date, 'yyyy-MM'), payer_id, product_id
    """,
)

create_gold_table(
    "dq_unmatched_sp_fills",
    """
    SELECT f.fill_id, f.patient_id, f.product_id, f.fill_date
    FROM workspace.silver.sp_fills f
    LEFT JOIN workspace.gold.dim_patient p ON p.patient_id = f.patient_id
    LEFT JOIN workspace.gold.dim_product d ON d.product_id = f.product_id
    WHERE p.patient_id IS NULL OR d.product_id IS NULL
    """,
)

# COMMAND ----------

for table_name in (
    "dim_patient",
    "dim_product",
    "dim_payer",
    "dim_plan_product",
    "fact_rx_transaction",
    "fact_hub_event",
    "fact_sp_fill",
    "mart_patient_access",
    "monthly_access_metrics",
    "dq_unmatched_sp_fills",
):
    print(f"gold.{table_name}: {spark.table(f'{CATALOG}.gold.{table_name}').count()} rows")

display(
    spark.sql(
        """
        SELECT enrollment_month, payer_id, product_id, enrolled_cases,
               starts_within_30d, no_start_within_30d, dispensed_cases, no_start_rate_30d
        FROM workspace.gold.monthly_access_metrics
        ORDER BY enrollment_month, payer_id, product_id
        """
    )
)
