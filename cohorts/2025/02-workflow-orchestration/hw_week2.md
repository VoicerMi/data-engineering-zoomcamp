-- q3
select count(*) from `zoomcamp.yellow_tripdata`
where filename = 'yellow_tripdata_2020%'
-- q4
select count(*) from `zoomcamp.green_tripdata`
where filename like 'green_tripdata_2020%'
-- q5
select count(*) from `zoomcamp.yellow_tripdata`
where filename = 'yellow_tripdata_2021-03.csv'
