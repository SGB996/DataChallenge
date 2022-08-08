## Importing the necessary libraries to perform the ETL

import os
import sys
import pandas as pd
import pandasql as ps
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import matplotlib.pyplot as plt
from tqdm import tqdm

from constants import *

## Reading the CSV with the data sample 
df = pd.read_csv('trips.csv')

## Transformation process, variable casting, field processing and treatment
df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S') ## casting of datetime variable to datetime
df = df.rename(columns={'datetime': 'date'})

## Function to process the coordinates variables and eliminate the characters
def extract_coordinates(df, col):
    coord = []
    for i, row in df.iterrows():
        coord.append(row[col].split('(')[1].split(')')[0])
    df[col] = ""
    df[col] = coord

extract_coordinates(df, 'origin_coord') 
extract_coordinates(df, 'destination_coord')

## To develop the solution using a data model for increasing the scalability of the project, we'll be implementing a Star Schema data model. 
## In this schema in the center we'll have our trips fact table, which have a table_id field which is a unique id for every field of the table, the region_id (region unique identifier), origin_coord,
## destination_coord, datetime, and the datasource_id (datasource unique identifier)
## We'll generate two dimension tables: dim_region (in which we store the regions of each made trips and the trip_id which is a Unique id to identify the region that also helps
## to join this dimension to the fact table), and the dim_datasource (table that stores the datasource name and the datasource_id to identify the field and helps to join this dimension table to the 
## fact table)

## Using a star schema data model we grant that the following data loading to each table and posterior extraction we'll be faster due to the decreasing on the query complexity and because the 
## Table schema changes and has more IDs making easier the reading process of all the fields

df['table_id'] = abs(df[['region', 'origin_coord', 'destination_coord', 'datasource']].sum(axis=1).map(hash)) ## Generate the table id using 4 fields as identifiers
df['region_id'] = abs(df[['region']].sum(axis=1).map(hash)) ## Generate the region_id using the region field
df['datasource_id'] = abs(df[['datasource']].sum(axis=1).map(hash)) ## Generate the datasource_id using the datasource field

## Generation of the dataframes of the fact table and the dimensions tables

trips_ft = df[['table_id', 'region_id', 'origin_coord', 'destination_coord', 'date', 'datasource_id']]
dim_region = df[['region_id', 'region']].drop_duplicates()
dim_datasource = df[['datasource_id', 'datasource']].drop_duplicates()

## After creating our data modelo we can perform the transformation processes before the loading process to the PostgreSQL DB

## Process to group the data by origin and destination, also we group the trips by time of the day
## Trips with similar origin, destination, and time of day should be grouped together

query1 = f"""
select 
b.region,
strftime('%Y-%m-%d', a.date) as date,
strftime('%Y', a.date) as year,
strftime('%m', a.date) as month,
strftime('%d', a.date) as day
from trips_ft a
left join dim_region b
on a.region_id = b.region_id 
group by
b.region, 
strftime('%Y', a.date),
strftime('%m', a.date),
strftime('%d', a.date)
"""
test = ps.sqldf(query1)
print(test.shape)
test['date'] = pd.to_datetime(test['date'], format='%Y-%m-%d %H:%M:%S')
test.head()


## Writing tables on postgresql
#Create the connection string to connect to database
conn = psycopg2.connect(host = Host, database = Db, port = Port, user = User, password = Password)
cur = conn.cursor()

# Table creation on PSQL DB

# Sql query to create the Trips fact table
create_trips_fact_table = (f"""CREATE TABLE IF NOT EXISTS trips_ft
(table_id bigint not null primary key,
region_id bigint not null, 
origin_coord varchar,
destination_coord varchar, 
date timestamp,
datasource_id bigint not null);
""")

# Sql query to create the region dimension table
create_region_dim_table = (f"""CREATE TABLE IF NOT EXISTS dim_region
(region_id bigint not null primary key,
region varchar);
""")

# Sql query to create the datasource dimension table
create_datasource_dim_table = (f"""CREATE TABLE IF NOT EXISTS dim_datasource
(datasource_id bigint not null primary key,
datasource varchar);
""")

# Sql query to create the grouped table of the second requirement
create_trips_group_table = (f"""CREATE TABLE IF NOT EXISTS results_table_1
(region varchar, 
date timestamp,
year varchar,
month varchar,
day varchar);
""")

#Function to create the tables
def create_table(cur, conn, query_name):
    cur.execute(query_name)
    conn.commit()

create_table(cur, conn, create_trips_fact_table)
create_table(cur, conn, create_region_dim_table)
create_table(cur, conn, create_datasource_dim_table)
create_table(cur, conn, create_trips_group_table)

## Loading and inserting the data into the tables in PSQL

def insert_fact_table(cur, table_id, region_id, origin_coord, destination_coord, date, datasource_id):
    
    insert_data = (f"""insert into trips_ft
    (table_id, region_id, origin_coord, destination_coord, date, datasource_id)
    values(%s, %s, %s, %s, %s, %s)""")

    data_to_insert = (table_id, region_id, origin_coord, destination_coord, date, datasource_id)
    cur.execute(insert_data, data_to_insert)

def write_fact_table(cur, data):
    for i, row in tqdm(data.iterrows(), desc="Ingestion process: fact table progress:"):
        insert_fact_table(cur, row['table_id'], row['region_id'], row['origin_coord'], row['destination_coord'], row['date'], row['datasource_id'])

def insert_dim_region(cur, region_id, region):
    
    insert_data = (f"""insert into dim_region
    (region_id, region)
    values(%s, %s)""")

    data_to_insert = (region_id, region)
    cur.execute(insert_data, data_to_insert)

def write_dim_region(cur, data):
    for i, row in tqdm(data.iterrows(), desc="Ingestion process: dim region table progress:"):
        insert_dim_region(cur, row['region_id'], row['region'])

def insert_dim_datasource(cur, datasource_id, datasource):
    
    insert_data = (f"""insert into dim_datasource
    (datasource_id, datasource)
    values(%s, %s)""")

    data_to_insert = (datasource_id, datasource)
    cur.execute(insert_data, data_to_insert)

def write_dim_datasource(cur, data):
    for i, row in tqdm(data.iterrows(), desc="Ingestion process: dim datasource table progress:"):
        insert_dim_datasource(cur, row['datasource_id'], row['datasource'])

def insert_results_1(cur, region, date, year, month, day):
    
    insert_data = (f"""insert into results_table_1
    (region, date, year, month, day)
    values(%s, %s, %s, %s, %s)""")

    data_to_insert = (region, date, year, month, day)
    cur.execute(insert_data, data_to_insert)

def write_results_1(cur, data):
    for i, row in tqdm(data.iterrows(), desc="Ingestion process: results table progress:"):
        insert_results_1(cur, row['region'], row['date'], row['year'], row['month'], row['day'])

write_fact_table(cur, trips_ft)
write_dim_region(cur, dim_region)
write_dim_datasource(cur, dim_datasource)
write_results_1(cur, test)
conn.commit()

## Develop a way to obtain the weekly average number of trips for an area, defined by a bounding box (given by coordinates) or by a region.
## Function to generate an array that contains the dataframes filtered by region

def filter_region(df, col): 
    regions = df[col].unique()
    for i in range(len(regions)):
        regions[i] = df[df[col]==regions[i]]
    return regions

df_f = filter_region(test, 'region')

# Generate a week column by which we'll be grouping our data
for i in tqdm(range(len(df_f)), desc="Transformation process: Weekly frequency generation progress"):
    df_f[i]['week'] = df_f[i]['date'] - timedelta(days=7)

## Generate a dictionary where we save the trips grouped by region and by week, and we count the total trips made weekly.
final = {}
for i in tqdm(range(len(df_f)), desc="Transformation process: Grouping the trips by region in weekly frequency progress"):
    final.update({'df_'+df_f[i]['region'].iloc[0]: df_f[i].groupby([pd.Grouper(key='week', freq='W-MON')]).count().reset_index()})

## Finally, generate the last corrections and changes to the dataframe to present the results
for key in tqdm(final.keys(), desc= "Transformation process: final data preparation progress:"):
    final[key] = final[key].rename(columns={'region': 'count'})
    final[key]['region'] = key.split('_')[1]
    final[key] = final[key][['region', 'week', 'count']]

## Plotting the results of the trips grouped by region and week
for key in tqdm(final.keys(), desc= "Transformation process: Plotting results progress:"):
    fig = plt.figure(figsize = (12, 8))
    plt.bar(final[key]['week'], final[key]['count'], color ='orange', width = 3)
    plt.xticks(rotation=45)
    plt.xlabel('Trips grouped by the region: ' + key.split('_')[1])
    plt.ylabel("Number of trips by region and week")
    plt.title("Result of trips grouped by region and week")
    plt.show()

