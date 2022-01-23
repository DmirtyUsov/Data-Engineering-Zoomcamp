--Question 3. Count records
--How many taxi trips were there on January 15?
--Consider only trips that started on January 15.
-- 53 024
SELECT
    CAST(tpep_pickup_datetime as DATE) AS "day",
    COUNT(1) AS "total_trips"
FROM yellow_taxi_trips
WHERE CAST(tpep_pickup_datetime as DATE) = CAST('2021-01-15' AS DATE)
GROUP BY "day";

--Question 4. Largest tip for each day
--Find the largest tip for each day. On which day it was the largest tip in January?
--Use the pick up time for your calculations.
--1140.44 2021-01-20
SELECT
    CAST(tpep_pickup_datetime as DATE) AS "day",
    MAX(tip_amount) AS "largest_tip"
FROM yellow_taxi_trips
GROUP BY "day"
ORDER BY "largest_tip" DESC;

--Question 5. Most popular destination
--What was the most popular destination for passengers picked up in central park on January 14?
--Use the pick up time for your calculations.
--Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"
-- Upper East Side South 97

SELECT 
    MAX("PULocationID"),
    MAX(CONCAT(puzones."Borough", '/', puzones."Zone")) AS "PULocation",
    MAX(puzones."Zone") AS "PUZone",
    MAX("DOLocationID"),
    MAX(CONCAT(dozones."Borough", '/', dozones."Zone")) AS "PDOLocation",
    MAX(dozones."Zone") AS "DOZone",
    COUNT(1) AS total_pikedups
FROM yellow_taxi_trips trips
	JOIN zones puzones
		ON trips."PULocationID" = puzones."LocationID"
	JOIN zones dozones
		ON trips."DOLocationID" = dozones."LocationID"
WHERE "PULocationID" = 43 AND 
	CAST(tpep_pickup_datetime as DATE) = CAST('2021-01-14' AS DATE)
GROUP BY "DOLocationID"
ORDER BY total_pikedups DESC;

--Question 6. Most expensive locations
--What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)?
--Enter two zone names separated by a slash
--For example:
--"Jamaica Bay / Clinton East"
--If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".
-- 	Alphabet City / Unknown 2292.4
SELECT
    MAX("pairPU-DO"),
    AVG(total_amount) AS avg_amount
FROM
    (SELECT 
        CONCAT(puzones."Zone", ' / ', dozones."Zone") AS "pairPU-DO",
        total_amount
    FROM yellow_taxi_trips trips
        JOIN zones puzones
            ON trips."PULocationID" = puzones."LocationID"
        JOIN zones dozones
            ON trips."DOLocationID" = dozones."LocationID"
    ) AS temptable
GROUP BY "pairPU-DO"
ORDER BY avg_amount DESC;
