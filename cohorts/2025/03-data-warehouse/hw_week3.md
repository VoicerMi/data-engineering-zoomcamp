
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.yellow_tripdata_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kelly-de-kestra/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `zoomcamp.yellow_tripdata_2024`
AS SELECT * FROM `zoomcamp.yellow_tripdata_2024_external`;

__q1__

SELECT count(*) FROM zoomcamp.yellow_tripdata_2024;

__q2__

SELECT count(distinct PULocationID) FROM zoomcamp.yellow_tripdata_2024_external;
SELECT count(distinct PULocationID) FROM zoomcamp.yellow_tripdata_2024;

__q3__

SELECT PULocationID FROM zoomcamp.yellow_tripdata_2024;
SELECT PULocationID, DOLocationID from zoomcamp.yellow_tripdata_2024;

__q4__

SELECT count(*) FROM zoomcamp.yellow_tripdata_2024
WHERE fare_amount=0;

__q5__

CREATE TABLE zoomcamp.yellow_tripdata_2024_partition_cluster
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT * FROM zoomcamp.yellow_tripdata_2024_external;

__q6__

SELECT DISTINCT VendorID FROM zoomcamp.yellow_tripdata
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID FROM zoomcamp.yellow_tripdata_2024_partition_cluster
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
