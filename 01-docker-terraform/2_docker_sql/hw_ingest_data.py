#!/usr/bin/env python
# coding: utf-8

import argparse
import os

from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'   
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if url.endswith('.csv.gz'):
        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime'])
        df = next(df_iter)
    else:
        df = pd.read_csv(csv_name)
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)



# zone_df = pd.read_csv('taxi_zone_lookup.csv')
# zone_df.head(n=0).to_sql(name='taxi_zone', con=engine, if_exists='replace')
# zone_df.to_sql(name='taxi_zone', con=engine, if_exists='append')

# q3 = """
# select distinct Trip_Segmentation, count(*) as count
# from
# (select 
#     case when trip_distance <=1 then 'Up to 1 mile'
#         when trip_distance >1 and trip_distance <= 3 then 'In between 1 and 3 miles'
#         when trip_distance >3 and trip_distance <= 7 then 'In between 3 and 7 miles'
#         when trip_distance >7 and trip_distance <= 10 then 'In between 7 and 10 miles'
#         else 'Over 10 miles'
#     end as Trip_Segmentation, *
# from green_taxi_data 
# where cast(lpep_pickup_datetime as date) >= '2019-10-01'
# and cast(lpep_dropoff_datetime as date) < '2019-11-01') t
# group by Trip_Segmentation
# """
# pd.read_sql(q3, con=engine)

# q4 = """
# select distinct cast(lpep_pickup_datetime as date) as Date, max(trip_distance) as Longest_trip
# from green_taxi_data 
# group by Date
# order by Longest_trip desc limit 1
# """
# pd.read_sql(q4, con=engine)

# q5 = """
# select distinct t."Zone", sum(g."total_amount")
# from green_taxi_data g
# join taxi_zone t
# on g."PULocationID" = t."LocationID"
# where cast(g."lpep_pickup_datetime" as date) = '2019-10-18'
# group by t."Zone"
# having sum(g."total_amount") > 13000
# """
# pd.read_sql(q5, con=engine)

# q6 = """
# select t2."Zone", sum(g."tip_amount")
# from green_taxi_data g
# join taxi_zone t1
# on g."PULocationID" = t1."LocationID"
# join taxi_zone t2
# on g."DOLocationID" = t2."LocationID"
# where cast(g."lpep_pickup_datetime" as date) >= '2019-10-01'
# and cast(g."lpep_pickup_datetime" as date) < '2019-11-01'
# and t1."Zone" = 'East Harlem North'
# group by t2."Zone"
# order by sum(g."tip_amount") desc
# """
# pd.read_sql(q6, con=engine)

# q6 = """
# select t2."Zone", max(g."tip_amount")
# from green_taxi_data g
# join taxi_zone t1
# on g."PULocationID" = t1."LocationID"
# join taxi_zone t2
# on g."DOLocationID" = t2."LocationID"
# where cast(g."lpep_pickup_datetime" as date) >= '2019-10-01'
# and cast(g."lpep_pickup_datetime" as date) < '2019-11-01'
# and t1."Zone" = 'East Harlem North'
# group by t2."Zone"
# order by max(g."tip_amount") desc
# """
# pd.read_sql(q6, con=engine)