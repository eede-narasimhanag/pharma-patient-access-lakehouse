# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion
# MAGIC Reads each uploaded CSV incrementally with Auto Loader and writes raw-string Delta tables.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/raw_landing"
CHECKPOINTS = f"/Volumes/{CATALOG}/bronze/checkpoints"
ENTITIES = ("rx_claims", "formulary", "hub_events", "sp_fills")

# COMMAND ----------

def ingest(entity: str) -> None:
    source_path = f"{LANDING}/{entity}/"
    schema_path = f"{CHECKPOINTS}/{entity}/schema"
    checkpoint_path = f"{CHECKPOINTS}/{entity}/checkpoint"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .load(source_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )

    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.bronze.{entity}")
    )
    query.awaitTermination()

    row_count = spark.table(f"{CATALOG}.bronze.{entity}").count()
    print(f"{entity}: {row_count} total Bronze rows")

# COMMAND ----------

for entity_name in ENTITIES:
    ingest(entity_name)
