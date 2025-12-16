from pyspark.sql import functions as F

bronze_table = "workspace.bigquery_db_cohort_db.bronze_ecom_orders"
silver_table = "workspace.bigquery_db_cohort_db.silver_cohort_analysis"

orders = spark.read.table(bronze_table)

# First purchase + total orders
first_purchases = (
    orders
    .groupBy("customer_id")
    .agg(
        F.min("order_date").alias("first_purchase_date"),
        F.count("*").alias("total_orders")
    )
)

# Second purchase
second_purchases = (
    orders.alias("o")
    .join(first_purchases.alias("f"), "customer_id")
    .where(F.col("o.order_date") > F.col("f.first_purchase_date"))
    .groupBy("customer_id")
    .agg(F.min("order_date").alias("second_purchase_date"))
)

# Final silver table
silver_df = (
    first_purchases
    .join(second_purchases, "customer_id", "left")
    .withColumn(
        "days_between_first_and_second",
        F.datediff("second_purchase_date", "first_purchase_date")
    )
)

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)
