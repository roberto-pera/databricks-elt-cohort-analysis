from pyspark.sql import functions as F

silver_table = "workspace.bigquery_db_cohort_db.silver_cohort_analysis"
gold_table = "workspace.bigquery_db_cohort_db.gold_retention_rate_by_cohort"

df = spark.read.table(silver_table)

df_with_diff = (
    df
    .withColumn("cohort_month", F.date_trunc("month", "first_purchase_date"))
    .withColumn(
        "month_diff",
        F.floor(F.months_between("second_purchase_date", "first_purchase_date"))
    )
)

gold_df = (
    df_with_diff
    .groupBy("cohort_month")
    .agg(
        F.count("*").alias("num_customers"),
        F.round(F.avg(F.when(F.col("month_diff") <= 1, 1).otherwise(0)) * 100, 2).alias("retention_rate_1m"),
        F.round(F.avg(F.when(F.col("month_diff") <= 2, 1).otherwise(0)) * 100, 2).alias("retention_rate_2m"),
        F.round(F.avg(F.when(F.col("month_diff") <= 3, 1).otherwise(0)) * 100, 2).alias("retention_rate_3m"),
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
