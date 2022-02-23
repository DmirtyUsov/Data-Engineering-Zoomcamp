Question 1: What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)
SELECT
  COUNT(1)
FROM
  `de-zoomcamp-339014.dbt_dusov.fact_trips`
WHERE
  FORMAT_DATE('%Y', pickup_datetime) IN ('2019','2020')
61635154

Question 2: What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos . (Yellow/Green)
89.9/10.1

Question 3: What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled
SELECT count(1) FROM `de-zoomcamp-339014.dbt_dusov.stg_fhv_tripdata` 
42084899

Question 4: What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled 
22,676,253

Question 5: What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table
January