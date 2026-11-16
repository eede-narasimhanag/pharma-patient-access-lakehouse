# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver transformations
# MAGIC Casts source fields, removes duplicate event keys, and quarantines malformed rows.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "workspace"
QUARANTINE_COLUMNS = ["source_name", "source_key", "reason", "raw_record"]
quarantine_frames = []

# COMMAND ----------

def bronze_with_raw_record(table_name: str):
    source = spark.table(f"{CATALOG}.bronze.{table_name}")
    return source.withColumn(
        "_raw_record",
        F.to_json(F.struct(*[F.col(column) for column in source.columns])),
    )


def quarantine_rows(frame, source_name: str, key_column: str, reason):
    if isinstance(reason, str):
        reason = F.lit(reason)
    return frame.select(
        F.lit(source_name).alias("source_name"),
        F.col(key_column).cast("string").alias("source_key"),
        reason.alias("reason"),
        F.col("_raw_record").alias("raw_record"),
    )


def write_silver(frame, table_name: str) -> None:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.{table_name}")
    )

# COMMAND ----------

rx = bronze_with_raw_record("rx_claims")
rx_typed = (
    rx.withColumn("transaction_id", F.trim("transaction_id"))
    .withColumn("patient_id", F.trim("patient_id"))
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("payer_id", F.trim("payer_id"))
    .withColumn("plan_id", F.trim("plan_id"))
    .withColumn("transaction_type", F.upper(F.trim("transaction_type")))
    .withColumn("service_date", F.expr("try_cast(service_date AS DATE)"))
    .withColumn("paid_amount", F.expr("try_cast(paid_amount AS DECIMAL(12,2))"))
)
rx_invalid = (
    F.col("transaction_id").isNull()
    | (F.col("transaction_id") == "")
    | F.col("patient_id").isNull()
    | (F.col("patient_id") == "")
    | F.col("product_id").isNull()
    | (F.col("product_id") == "")
    | F.col("payer_id").isNull()
    | F.col("plan_id").isNull()
    | F.col("service_date").isNull()
    | F.col("paid_amount").isNull()
    | (F.col("paid_amount") < 0)
    | F.col("transaction_type").isNull()
    | (~F.col("transaction_type").isin("PAID", "REJECTED", "REVERSAL"))
)
rx_bad = rx_typed.filter(rx_invalid)
quarantine_frames.append(
    quarantine_rows(rx_bad, "rx_claims", "transaction_id", "invalid_required_field_or_value")
)

rx_valid = rx_typed.filter(~rx_invalid)
rx_window = Window.partitionBy("transaction_id").orderBy(
    F.col("_ingest_ts").asc(), F.col("_source_file").asc()
)
rx_ranked = rx_valid.withColumn("_transaction_rank", F.row_number().over(rx_window))
rx_duplicates = rx_ranked.filter(F.col("_transaction_rank") > 1)
quarantine_frames.append(
    quarantine_rows(rx_duplicates, "rx_claims", "transaction_id", "duplicate_transaction_id")
)
rx_silver = rx_ranked.filter(F.col("_transaction_rank") == 1).select(
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
    "_source_file",
    "_ingest_ts",
)
write_silver(rx_silver, "rx_claims")

# COMMAND ----------

formulary = bronze_with_raw_record("formulary")
formulary_typed = (
    formulary.withColumn("plan_id", F.trim("plan_id"))
    .withColumn("payer_id", F.trim("payer_id"))
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("effective_start", F.expr("try_cast(effective_start AS DATE)"))
    .withColumn("effective_end", F.expr("try_cast(effective_end AS DATE)"))
    .withColumn("formulary_status", F.upper(F.trim("formulary_status")))
    .withColumn("coverage_tier", F.upper(F.trim("coverage_tier")))
)
formulary_invalid = (
    F.col("plan_id").isNull()
    | F.col("payer_id").isNull()
    | F.col("product_id").isNull()
    | F.col("effective_start").isNull()
    | F.col("effective_end").isNull()
    | (F.col("effective_start") > F.col("effective_end"))
    | F.col("formulary_status").isNull()
)
formulary_bad = formulary_typed.filter(formulary_invalid)
quarantine_frames.append(
    quarantine_rows(formulary_bad, "formulary", "plan_id", "invalid_effective_dated_record")
)
formulary_silver = formulary_typed.filter(~formulary_invalid).select(
    "plan_id",
    "payer_id",
    "product_id",
    "effective_start",
    "effective_end",
    "formulary_status",
    "coverage_tier",
    "_source_file",
    "_ingest_ts",
)
write_silver(formulary_silver, "formulary")

# COMMAND ----------

hub = bronze_with_raw_record("hub_events")
hub_typed = (
    hub.withColumn("event_id", F.trim("event_id"))
    .withColumn("case_id", F.trim("case_id"))
    .withColumn("patient_id", F.trim("patient_id"))
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("payer_id", F.trim("payer_id"))
    .withColumn("event_type", F.upper(F.trim("event_type")))
    .withColumn("event_date", F.expr("try_cast(event_date AS DATE)"))
)
hub_invalid = (
    F.col("event_id").isNull()
    | F.col("case_id").isNull()
    | F.col("patient_id").isNull()
    | F.col("product_id").isNull()
    | F.col("payer_id").isNull()
    | F.col("event_date").isNull()
    | F.col("event_type").isNull()
    | (~F.col("event_type").isin("ENROLLED", "BENEFITS_VERIFIED", "PA_APPROVED", "PA_DENIED"))
)
hub_bad = hub_typed.filter(hub_invalid)
quarantine_frames.append(
    quarantine_rows(hub_bad, "hub_events", "event_id", "invalid_required_field_or_value")
)
hub_valid = hub_typed.filter(~hub_invalid)
hub_window = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").asc(), F.col("_source_file").asc())
hub_ranked = hub_valid.withColumn("_event_rank", F.row_number().over(hub_window))
hub_duplicates = hub_ranked.filter(F.col("_event_rank") > 1)
quarantine_frames.append(
    quarantine_rows(hub_duplicates, "hub_events", "event_id", "duplicate_event_id")
)
hub_silver = hub_ranked.filter(F.col("_event_rank") == 1).select(
    "event_id",
    "case_id",
    "patient_id",
    "product_id",
    "payer_id",
    "event_type",
    "event_date",
    "_source_file",
    "_ingest_ts",
)
write_silver(hub_silver, "hub_events")

# COMMAND ----------

fills = bronze_with_raw_record("sp_fills")
fills_typed = (
    fills.withColumn("fill_id", F.trim("fill_id"))
    .withColumn("patient_id", F.trim("patient_id"))
    .withColumn("product_id", F.trim("product_id"))
    .withColumn("fill_date", F.expr("try_cast(fill_date AS DATE)"))
    .withColumn("fill_status", F.upper(F.trim("fill_status")))
    .withColumn("days_supply", F.expr("try_cast(days_supply AS INT)"))
)
fill_invalid = (
    F.col("fill_id").isNull()
    | F.col("patient_id").isNull()
    | F.col("product_id").isNull()
    | F.col("fill_date").isNull()
    | F.col("days_supply").isNull()
    | (F.col("days_supply") <= 0)
    | F.col("fill_status").isNull()
    | (~F.col("fill_status").isin("DISPENSED", "REVERSED"))
)
fill_bad = fills_typed.filter(fill_invalid)
quarantine_frames.append(
    quarantine_rows(fill_bad, "sp_fills", "fill_id", "invalid_required_field_or_value")
)
fills_valid = fills_typed.filter(~fill_invalid)
fill_window = Window.partitionBy("fill_id").orderBy(F.col("_ingest_ts").asc(), F.col("_source_file").asc())
fill_ranked = fills_valid.withColumn("_fill_rank", F.row_number().over(fill_window))
fill_duplicates = fill_ranked.filter(F.col("_fill_rank") > 1)
quarantine_frames.append(
    quarantine_rows(fill_duplicates, "sp_fills", "fill_id", "duplicate_fill_id")
)
fills_silver = fill_ranked.filter(F.col("_fill_rank") == 1).select(
    "fill_id",
    "patient_id",
    "product_id",
    "fill_date",
    "fill_status",
    "days_supply",
    "transaction_id",
    "_source_file",
    "_ingest_ts",
)
write_silver(fills_silver, "sp_fills")

# COMMAND ----------

quarantine = quarantine_frames[0]
for additional_frame in quarantine_frames[1:]:
    quarantine = quarantine.unionByName(additional_frame)

(
    quarantine.select(*QUARANTINE_COLUMNS)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.quarantine")
)

for table_name in ("rx_claims", "formulary", "hub_events", "sp_fills", "quarantine"):
    print(f"silver.{table_name}: {spark.table(f'{CATALOG}.silver.{table_name}').count()} rows")
