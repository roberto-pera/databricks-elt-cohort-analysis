--- Create a Query to Compute Repeat Purchase Rates ---
-- Write a query that calculates, for each customer, the first purchase date and total orders.
-- Then, group customers into monthly cohorts and compute the percentage of customers who placed at least a 2nd, 3rd, and 4th order.*/

CREATE OR REPLACE TABLE workspace.bigquery_db_cohort_db.gold_repeat_purchase_rate_by_cohort AS

-- 1) Prepare customer cohort data from SILVER layer
WITH customer_cohorts AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', first_purchase_date) AS cohort_month,
        total_orders
    FROM workspace.bigquery_db_cohort_db.silver_cohort_analysis
)

-- 2) Aggregate repeat purchase counts & percentages per cohort
SELECT
    DATE_FORMAT(cohort_month, 'yyyy-MM') AS cohort_month,
    COUNT(*) AS num_customers,  -- total customers in cohort

    -- absolute counts
    SUM(CASE WHEN total_orders >= 2 THEN 1 END) AS num_2plus_orders,
    SUM(CASE WHEN total_orders >= 3 THEN 1 END) AS num_3plus_orders,
    SUM(CASE WHEN total_orders >= 4 THEN 1 END) AS num_4plus_orders,

    -- percentages
    ROUND(100.0 * SUM(CASE WHEN total_orders >= 2 THEN 1 END) / COUNT(*)) AS pct_2plus_orders,
    ROUND(100.0 * SUM(CASE WHEN total_orders >= 3 THEN 1 END) / COUNT(*)) AS pct_3plus_orders,
    ROUND(100.0 * SUM(CASE WHEN total_orders >= 4 THEN 1 END) / COUNT(*)) AS pct_4plus_orders

FROM customer_cohorts
GROUP BY cohort_month
ORDER BY cohort_month;
