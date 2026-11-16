# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data quality and reconciliation checks
# MAGIC Persists check results and fails the run only for broken pipeline invariants.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"


def table_count(table_name: str) -> int:
    return spark.table(f"{CATALOG}.{table_name}").count()


def scalar(query: str) -> int:
    return int(spark.sql(query).first()[0])

# COMMAND ----------

bronze_rx_count = table_count("bronze.rx_claims")
silver_rx_count = table_count("silver.rx_claims")
rx_quarantine = spark.table(f"{CATALOG}.silver.quarantine").filter(F.col("source_name") == "rx_claims")
rx_quarantine_count = rx_quarantine.count()
duplicate_rx_count = rx_quarantine.filter(F.col("reason") == "duplicate_transaction_id").count()
invalid_rx_count = rx_quarantine.filter(F.col("reason") == "invalid_required_field_or_value").count()
rx_reconciliation_difference = abs(bronze_rx_count - silver_rx_count - rx_quarantine_count)

unmatched_reversals = scalar(
    f"""
    SELECT COUNT(*)
    FROM {CATALOG}.silver.rx_claims reversal
    LEFT ANTI JOIN {CATALOG}.silver.rx_claims paid
      ON paid.transaction_id = reversal.reverses_transaction_id
     AND paid.transaction_type = 'PAID'
    WHERE reversal.transaction_type = 'REVERSAL'
    """
)

unmatched_sp_fills = table_count("gold.dq_unmatched_sp_fills")
formulary_unknowns = scalar(
    f"SELECT COUNT(*) FROM {CATALOG}.gold.fact_rx_transaction WHERE formulary_status = 'UNKNOWN'"
)
duplicate_gold_rx_ids = scalar(
    f"""
    SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
    FROM {CATALOG}.gold.fact_rx_transaction
    """
)
enrollment_count = scalar(
    f"SELECT COUNT(DISTINCT case_id) FROM {CATALOG}.silver.hub_events WHERE event_type = 'ENROLLED'"
)
mart_case_count = table_count("gold.mart_patient_access")
mart_reconciliation_difference = abs(enrollment_count - mart_case_count)
invalid_metric_rows = scalar(
    f"""
    SELECT COUNT(*)
    FROM {CATALOG}.gold.monthly_access_metrics
    WHERE no_start_rate_30d < 0 OR no_start_rate_30d > 1 OR no_start_rate_30d IS NULL
    """
)

# COMMAND ----------

checks = [
    (
        "rx_source_row_reconciliation_difference",
        rx_reconciliation_difference,
        "PASS" if rx_reconciliation_difference == 0 else "FAIL",
    ),
    (
        "duplicate_rx_transactions_quarantined",
        duplicate_rx_count,
        "PASS" if duplicate_rx_count <= rx_quarantine_count else "FAIL",
    ),
    (
        "invalid_rx_rows_quarantined",
        invalid_rx_count,
        "PASS" if invalid_rx_count <= rx_quarantine_count else "FAIL",
    ),
    (
        "unmatched_reversals",
        unmatched_reversals,
        "REVIEW" if unmatched_reversals > 0 else "PASS",
    ),
    (
        "unmatched_sp_fills",
        unmatched_sp_fills,
        "REVIEW" if unmatched_sp_fills > 0 else "PASS",
    ),
    (
        "claims_without_formulary_match",
        formulary_unknowns,
        "REVIEW" if formulary_unknowns > 0 else "PASS",
    ),
    (
        "duplicate_gold_rx_transaction_ids",
        duplicate_gold_rx_ids,
        "FAIL" if duplicate_gold_rx_ids > 0 else "PASS",
    ),
    (
        "mart_enrollment_reconciliation_difference",
        mart_reconciliation_difference,
        "PASS" if mart_reconciliation_difference == 0 else "FAIL",
    ),
    (
        "invalid_monthly_access_metric_rows",
        invalid_metric_rows,
        "FAIL" if invalid_metric_rows > 0 else "PASS",
    ),
]

checks_df = (
    spark.createDataFrame(checks, ["check_name", "check_value", "status"])
    .withColumn("checked_at", F.current_timestamp())
)
(
    checks_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dq_checks")
)

display(checks_df.orderBy("check_name"))

failures = [name for name, _, status in checks if status == "FAIL"]
if failures:
    raise ValueError(f"Data quality checks failed: {', '.join(failures)}")
