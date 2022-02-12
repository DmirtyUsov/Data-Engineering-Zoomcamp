-- Question 1: What is count for fhv vehicles data for year 2019 
-- Number of rows 42,084,899 
SELECT
  COUNT(*)
FROM
  `de-zoomcamp-339014.trips_data_all.fhv_tripdata`
WHERE
  FORMAT_DATE('%Y', pickup_datetime)='2019';

-- Question 2: How many distinct dispatching_base_num we have in fhv for 2019 
-- 792
SELECT
  COUNT(DISTINCT dispatching_base_num) AS UniqueBaseNum
FROM
  `de-zoomcamp-339014.trips_data_all.fhv_tripdata`;

-- Question 3: Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num 
-- Partition by dropoff_datetime and cluster by dispatching_base_num


-- Question 4: What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE
  de-zoomcamp-339014.trips_data_all.fhv_q4
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  dispatching_base_num AS
SELECT * FROM `de-zoomcamp-339014.trips_data_all.fhv_tripdata_ext`;

SELECT COUNT(*) AS Total
FROM
  `de-zoomcamp-339014.trips_data_all.fhv_q4`
WHERE
  DATE(pickup_datetime) BETWEEN "2019-01-01" AND "2019-03-31"
  AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
-- This query will process 400.1 MiB when run
-- 148.2 MB processed
-- 26647
