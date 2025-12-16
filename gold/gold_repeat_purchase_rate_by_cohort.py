from pyspark.sql import functions as F

silver_table = "workspace.bigquery_db_cohort_db.silver_cohort_analysis"
gold_table = "workspace.bigquery_db_cohort_db.gold_repeat_purchase_rate_by_cohort"

df = spark.read.table(silver_table)

df = df.withColumn("cohort_month", F.date_trunc("month", "first_purchase_date"))

gold_df = (
    df
    .groupBy("cohort_month")
    .agg(
        F.count("*").alias("num_customers"),
        F.sum(F.when(F.col("total_orders") >= 2, 1).otherwise(0)).alias("num_2plus_orders"),
        F.sum(F.when(F.col("total_orders") >= 3, 1).otherwise(0)).alias("num_3plus_orders"),
        F.sum(F.when(F.col("total_orders") >= 4, 1).otherwise(0)).alias("num_4plus_orders"),
        F.round(F.avg(F.when(F.col("total_orders") >= 2, 1).otherwise(0)) * 100, 2).alias("pct_2plus_orders"),
        F.round(F.avg(F.when(F.col("total_orders") >= 3, 1).otherwise(0)) * 100, 2).alias("pct_3plus_orders"),
        F.round(F.avg(F.when(F.col("total_orders") >= 4, 1).otherwise(0)) * 100, 2).alias("pct_4plus_orders"),
    )
    .orderBy("cohort_month")
)

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)
