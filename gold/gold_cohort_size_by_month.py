from pyspark.sql import functions as F

silver_table = "workspace.bigquery_db_cohort_db.silver_cohort_analysis"
gold_table = "workspace.bigquery_db_cohort_db.gold_cohort_size_by_month"

df = spark.read.table(silver_table)

gold_df = (
    df
    .withColumn("cohort_month", F.date_trunc("month", "first_purchase_date"))
    .groupBy("cohort_month")
    .agg(F.count("*").alias("num_customers"))
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
