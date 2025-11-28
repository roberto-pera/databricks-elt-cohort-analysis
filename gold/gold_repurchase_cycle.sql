SELECT 
  ROUND(AVG(days_between_first_and_second)) AS avg_days_to_repurchase
FROM workspace.bigquery_db_cohort_db.silver_cohort_analysis
WHERE second_purchase_date IS NOT NULL
