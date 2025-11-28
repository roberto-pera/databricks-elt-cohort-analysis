--- Cohort Size by Month ---
-- Write a query to count the number of new customers (i.e., first orders) acquired in each month.

CREATE OR REPLACE TABLE workspace.bigquery_db_cohort_db.gold_cohort_size_by_month AS

-- Prepare the cohort month from SILVER layer
WITH customer_cohorts AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', first_purchase_date) AS cohort_month
    FROM workspace.bigquery_db_cohort_db.silver_cohort_analysis
)

-- Aggregate cohort sizes
SELECT
    DATE_FORMAT(cohort_month, 'yyyy-MM') AS cohort_month,
    COUNT(*) AS num_customers
FROM customer_cohorts
GROUP BY cohort_month
ORDER BY cohort_month;
