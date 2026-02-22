# BigQuery Cost Optimization Demo
### CMS Medicare Inpatient Charges Analysis (2011–2015)

---

## Project Overview

This project demonstrates BigQuery cost optimization techniques using publicly available CMS Medicare inpatient charges data. The goal is to show how thoughtful query design and table architecture can dramatically reduce the amount of data scanned — and therefore the cost — of running analytical queries in BigQuery.

The project consolidates five years of Medicare inpatient data (2011–2015) into a single partitioned table aggregated at the DRG (Diagnosis Related Group) level, enabling efficient year-over-year trend analysis.

**Target use case:** Healthcare data engineering — managing sensitive datasets through reliable, cost-efficient, and auditable data pipelines.

---

## Key Findings

| Technique | Bytes Scanned | Savings |
|---|---|---|
| `SELECT *` on raw source table (no optimization) | 34.25 MB | baseline |
| Column pruning (2 columns only) | 7.43 MB | ~80% reduction |
| Partition filter on consolidated table (single year) | 0 B | ~100% reduction |

> **Insight:** In BigQuery, cost is driven by bytes scanned — not rows returned or query runtime. `LIMIT` does not reduce scan cost. Optimization happens at the column and partition level.

---

## Techniques Demonstrated

### 1. Column Pruning
Selecting only the columns needed for analysis instead of using `SELECT *`. Since BigQuery uses columnar storage, each column is stored and read independently. Selecting 2 columns instead of all columns reduced data scanned by ~80%.

### 2. Table Partitioning
Partitioning physically separates table data into distinct storage segments by a chosen column — in this case `inpatient_year`. When a query filters on the partition column, BigQuery skips all other partitions entirely and never reads them. This is more powerful than indexing because data is never read at all, rather than being filtered after the fact.

### 3. Weighted Averages
The source data is already aggregated at the provider + DRG level, so simple `AVG()` across providers would be statistically incorrect. Weighted averages account for provider volume, ensuring high-volume providers have proportionally more influence on the result.

**Formula:**
```sql
SUM(metric * total_discharges) / SUM(total_discharges)
```

### 4. UNION ALL vs UNION
`UNION` removes duplicate rows, requiring BigQuery to scan and compare every row — extra work and extra cost. `UNION ALL` retains all rows without comparison, making it faster and cheaper. Since each CTE represents a distinct year, duplicates are not a concern.

---

## Metrics Glossary

| Metric | Description |
|---|---|
| `drg_definition` | Diagnosis Related Group code and description — the clinical category of the inpatient stay |
| `inpatient_year` | Reporting year of the data, hardcoded as January 1st of each year (e.g. `2013-01-01`) |
| `total_discharges` | Total number of Medicare inpatient discharges for this DRG nationally in the given year |
| `average_covered_charges` | Weighted average of what providers billed for this DRG |
| `average_total_payments` | Weighted average of total reimbursement received by providers |
| `average_medicare_payments` | Weighted average of Medicare's portion of total reimbursement |
| `charge_to_payment_ratio` | Ratio of covered charges to total payments — how many times more providers billed vs. received. A ratio of 3.5 means providers billed 3.5x what they were paid |
| `medicare_total_payment_share` | Medicare payments as a proportion of total payments — Medicare's share of reimbursement |

---

## Table Schema

**Table:** `bq-cost-optimization-demo.cms_medicare_inpatient.metrics_by_drg_year`  
**Partition column:** `inpatient_year` (DATE)  
**Grain:** One row per DRG per year  
**Years covered:** 2011–2015  

Column order follows the convention: identifiers → volume → raw financials → derived metrics.

---

## Data Source

**Google BigQuery Public Dataset:** `bigquery-public-data.cms_medicare`  
**Tables used:** `inpatient_charges_2011` through `inpatient_charges_2015`  

The source tables are pre-aggregated at the provider + DRG level by CMS. This project further aggregates to the national DRG level by year, enabling trend analysis across the full dataset.

---

## How to Run

1. Open [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Create a project called `bq-cost-optimization-demo`
3. Create a dataset called `cms_medicare_inpatient`
4. Run `create_metrics_by_drg_year.sql` to build the partitioned table
5. Query the table using a partition filter for cost-efficient access:

```sql
SELECT *
FROM `bq-cost-optimization-demo.cms_medicare_inpatient.metrics_by_drg_year`
WHERE inpatient_year = '2013-01-01'
```
