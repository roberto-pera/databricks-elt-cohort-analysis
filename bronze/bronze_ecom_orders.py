from pyspark.sql import functions as F

# Source (ingested by Fivetran or uploaded)
source_table = "workspace.bigquery_db_cohort_db.ecom_orders"

# Target Bronze table
bronze_table = "workspace.bigquery_db_cohort_db.bronze_ecom_orders"

# Read raw data
df_raw = spark.read.table(source_table)

# Optional: basic technical metadata
df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

# Write as Delta (idempotent)
(
    df_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table)
)
