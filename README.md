# **Cohort Analysis with Databricks and a Modern ELT Pipeline**

This repository contains the SQL models, transformation logic, and dashboard assets for a cohort analysis project built with a modern ELT architecture. The goal is to understand customer retention, repeat purchases, and repurchase timing across monthly customer cohorts.

---

## **1. Project Overview** 

This project demonstrates how to transform raw e-commerce order data into actionable customer insights using:

- **Cloud SQL (BigQuery)** as the data source
- **Fivetran** for automated ingestion
- **Databricks (Delta Lake + SQL)** for transformations
- **Databricks Dashboard** for visualization
- **GitHub** for version control and reproducibility

The underlying dataset covers **Jan–Dec 2024**, while all customers’ **first purchases occurred between January and June**.

This results in **six monthly cohorts**.

---

## **2. Objectives** 

- Derive each customer’s **first** and **second** purchase dates
- Calculate:
    - 1/2/3-month retention
    - Repeat purchase rates (2nd, 3rd, 4th orders)
    - Repurchase cycle (days until second order)
    - Cohort sizes (new customers per month)
- Build dashboard visualizations for cohort comparison and behavioral insights
- Sync SQL models and dashboards with GitHub for reproducibility

> Interpretation Note:
> 
> 
> Cohort sizes are small, and no business/industry context is available.
> 
> Findings should be treated as *directional*, not statistically definitive.
> 

---

## **3. ELT Data Stack** 

This project uses a **modern ELT process**:

1. **Extract** – Orders sourced from Cloud SQL
2. **Load** – Fivetran replicates raw data into Databricks (Bronze Layer)
3. **Transform** – Databricks SQL produces Silver & Gold analytical models

Benefits:

- Full raw data preserved
- Reproducible modeling logic
- Scalable Delta Lake transformations

---

## **4. Medallion Architecture** 

### **Bronze Layer (Raw Data)**

Replicated table `ecom_orders` ([bronze_ecom_orders.csv](bronze/bronze_ecom_orders.csv)).

---

### **Silver Layer (Customer-Level Transformations)**

Transforms raw events into customer-centric features:

- First and second purchase
- Days to repurchase
- Total order count per customer

**Silver SQL model:**

- `silver/silver_cohort_analysis.sql`

---

### **Gold Layer (Cohort-Level Aggregations)**

Behavioral aggregations used directly in the dashboard:

**Gold SQL models:**

- `gold/gold_retention_rate_by_cohort.sql`
- `gold/gold_repeat_purchase_rate_by_cohort.sql`
- `gold/gold_cohort_size_by_month.sql`
- `gold/gold_repurchase_cycle.sql`

Metrics include:

- Cohort month
- Retention after 1, 2, 3 months
- Repeat purchase funnel (2+, 3+, 4+ orders)
- Cohort sizes
- Average time to second purchase

---

## **5. Dashboard** 

A Databricks dashboard visualizes:

- Overall KPIs across all cohorts
- Retention trends
- Repeat purchase rates
- Cohort size trend
- Average days to second purchase (additional visualization beyond assignment)

**Dashboard screenshot:**

![Dashboard Screenshot](dashboard/dashboard_screenshot.png)

---

## **6. Repository Structure**

- **bronze/**
  - [bronze_ecom_orders.csv](bronze/bronze_ecom_orders.csv)

- **silver/**
  - [silver_cohort_analysis.sql](silver/silver_cohort_analysis.sql)

- **gold/**
  - [gold_retention_rate_by_cohort.sql](gold/gold_retention_rate_by_cohort.sql)
  - [gold_repeat_purchase_rate_by_cohort.sql](gold/gold_repeat_purchase_rate_by_cohort.sql)
  - [gold_cohort_size_by_month.sql](gold/gold_cohort_size_by_month.sql)
  - [gold_repurchase_cycle.sql](gold/gold_repurchase_cycle.sql)

- **dashboard/**
  - [dashboard_screenshot.png](dashboard/dashboard_screenshot.png)

- **slides/**
  - [elt_databricks_project_cohort_analysis.pptx](slides/elt_databricks_project_cohort_analysis.pptx)
  - [elt_databricks_project_cohort_analysis.pdf](slides/elt_databricks_project_cohort_analysis.pdf)


## **7. Data Context & Limitations**

- Data covers the full year (Jan–Dec 2024)
- First purchases cluster exclusively in **Jan–Jun**, forming six cohorts
- Cohort sizes vary widely (10–66 customers)
- Unknown factors:
    - Marketing strategy
    - Industry or seasonality
    - Product and pricing context
    - Customer segments
- -> Insights should be viewed as **exploratory**, not conclusive.

---

## **8. How to Run the SQL Models**

1. Connect your Databricks workspace to this GitHub repo
2. Execute the Silver model:
    - `silver/silver_cohort_analysis.sql`
3. Execute the Gold models:
    - All files in `/gold/`
4. Refresh the Databricks dashboard
5. Optional: Schedule models using Databricks Jobs

---

## **9. Suggested Next Steps**

- Automate transformations via Databricks Workflows
- Extend Gold layer with LTV, churn prediction, or RFM segmentation
- Add acquisition metadata (channels, campaigns)
- Validate retention/repurchase trends with larger datasets
