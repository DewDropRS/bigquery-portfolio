
-- Exploratory data analysis
-- total rows in the dataset= 8M
-- cost optimization is important
-- Note: Cache results was disabled to ensure accurate, fresh query results.
-- Cached results were causing inconsistent row counts across runs.

SELECT COUNT(*) AS row_count
FROM `bigquery-public-data.nppes.npi_raw`;

-- What is the breakdown of `entity_type_code` values?
-- 1 = Individual and 2 = Organization.
-- Cost note: Selecting only entity_type_code on 8M rows = 58.52 MB scanned
-- Column pruning keeps cost low despite large row count
SELECT 
  npi.entity_type_code
  , COUNT(*) as count
  , ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100,1) as pct_of_total
FROM `bigquery-public-data.nppes.npi_raw` as npi
GROUP BY 1
;


-- What is the breakdown of `provider_gender_code`?
SELECT 
  npi.provider_gender_code
  , COUNT(*) as count
  , ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100,1) as pct_of_total
FROM `bigquery-public-data.nppes.npi_raw` as npi
GROUP BY 1
;

