-- create_metrics_by_drg_year.sql
-- Builds a partitioned table aggregating CMS Medicare inpatient charges
-- at the DRG level across 5 years (2011-2015).
-- Techniques: column pruning, UNION ALL, weighted averages, date partitioning
-- See README.md for full documentation and key findings.

-- SELECT * FROM `bigquery-public-data.cms_medicare.inpatient_charges_2015` LIMIT 5;
/* This query will process 34.25 MB when run.SELECT * forces BigQuery to read every column in the table. BigQuery charges based on data scanned, not rows returned. */

-- SELECT 
--   p.provider_name
--   , p.average_total_payments
-- FROM `bigquery-public-data.cms_medicare.inpatient_charges_2015` AS p 
-- LIMIT 5;
-- By using column pruning I was able to cut the processing burden by nearly 80%

CREATE OR REPLACE TABLE `bq-cost-optimization-demo.cms_medicare_inpatient.metrics_by_drg_year`
PARTITION BY inpatient_year AS
WITH inpatient_2011 AS(
SELECT 
  p.drg_definition
  , DATE '2011-01-01' as inpatient_year
  , SUM(total_discharges) as total_discharges
  , SUM(average_covered_charges * total_discharges) / SUM(total_discharges) as average_covered_charges
  , SUM(average_total_payments * total_discharges) / SUM(total_discharges) AS average_total_payments
  , SUM(average_medicare_payments * total_discharges) / SUM(total_discharges) as average_medicare_payments
  , SUM(average_covered_charges * total_discharges) / SUM(average_total_payments * total_discharges) as charge_to_payment_ratio
  , SUM(average_medicare_payments * total_discharges) / SUM(average_total_payments * total_discharges) AS medicare_total_payment_share

FROM `bigquery-public-data.cms_medicare.inpatient_charges_2011` AS p 
GROUP BY p.drg_definition

),
  inpatient_2012 AS(
SELECT 
  p.drg_definition
  , DATE '2012-01-01' as inpatient_year
  , SUM(total_discharges) as total_discharges
  , SUM(average_covered_charges * total_discharges) / SUM(total_discharges) as average_covered_charges
  , SUM(average_total_payments * total_discharges) / SUM(total_discharges) AS average_total_payments
  , SUM(average_medicare_payments * total_discharges) / SUM(total_discharges) as average_medicare_payments
  , SUM(average_covered_charges * total_discharges) / SUM(average_total_payments * total_discharges) as charge_to_payment_ratio
  , SUM(average_medicare_payments * total_discharges) / SUM(average_total_payments * total_discharges) AS medicare_total_payment_share

FROM `bigquery-public-data.cms_medicare.inpatient_charges_2012` AS p 
GROUP BY p.drg_definition
),
  inpatient_2013 AS(
SELECT 
  p.drg_definition
  , DATE '2013-01-01' as inpatient_year
  , SUM(total_discharges) as total_discharges
  , SUM(average_covered_charges * total_discharges) / SUM(total_discharges) as average_covered_charges
  , SUM(average_total_payments * total_discharges) / SUM(total_discharges) AS average_total_payments
  , SUM(average_medicare_payments * total_discharges) / SUM(total_discharges) as average_medicare_payments
  , SUM(average_covered_charges * total_discharges) / SUM(average_total_payments * total_discharges) as charge_to_payment_ratio
  , SUM(average_medicare_payments * total_discharges) / SUM(average_total_payments * total_discharges) AS medicare_total_payment_share

FROM `bigquery-public-data.cms_medicare.inpatient_charges_2013` AS p 
GROUP BY p.drg_definition
),
  inpatient_2014 AS(
SELECT 
  p.drg_definition
  , DATE '2014-01-01' as inpatient_year
  , SUM(total_discharges) as total_discharges
  , SUM(average_covered_charges * total_discharges) / SUM(total_discharges) as average_covered_charges
  , SUM(average_total_payments * total_discharges) / SUM(total_discharges) AS average_total_payments
  , SUM(average_medicare_payments * total_discharges) / SUM(total_discharges) as average_medicare_payments
  , SUM(average_covered_charges * total_discharges) / SUM(average_total_payments * total_discharges) as charge_to_payment_ratio
  , SUM(average_medicare_payments * total_discharges) / SUM(average_total_payments * total_discharges) AS medicare_total_payment_share

FROM `bigquery-public-data.cms_medicare.inpatient_charges_2014` AS p 
GROUP BY p.drg_definition
),
  inpatient_2015 AS(
SELECT 
  p.drg_definition
  , DATE '2015-01-01' as inpatient_year
  , SUM(total_discharges) as total_discharges
  , SUM(average_covered_charges * total_discharges) / SUM(total_discharges) as average_covered_charges
  , SUM(average_total_payments * total_discharges) / SUM(total_discharges) AS average_total_payments
  , SUM(average_medicare_payments * total_discharges) / SUM(total_discharges) as average_medicare_payments
  , SUM(average_covered_charges * total_discharges) / SUM(average_total_payments * total_discharges) as charge_to_payment_ratio
  , SUM(average_medicare_payments * total_discharges) / SUM(average_total_payments * total_discharges) AS medicare_total_payment_share

FROM `bigquery-public-data.cms_medicare.inpatient_charges_2015` AS p 
GROUP BY p.drg_definition
) 

SELECT * FROM inpatient_2011
  UNION ALL
  SELECT * FROM inpatient_2012
    UNION ALL
  SELECT * FROM inpatient_2013
    UNION ALL
  SELECT * FROM inpatient_2014
    UNION ALL
  SELECT * FROM inpatient_2015
;
-- Baseline scan cost without partitioning: 66.97 MB across all 5 years
-- After adding PARTITION BY inpatient_year, queries filtering by year will scan only ~1/5 of the data
-- Checkout Job information after running. In BigQuery, "bytes processed" = data scanned. This is what drives query cost.



