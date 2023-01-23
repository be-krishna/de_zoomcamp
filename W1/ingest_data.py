#!/usr/bin/env python
# coding: utf-8

import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


# argument parsing
# parser = argparse.ArgumentParser(description="Ingest data into Postgres")
# parser.add_argument("----host", type=str, default="localhost", help="Postgres host")
# parser.add_argument("----port", type=int, default=5432, help="Postgres port")
# parser.add_argument("----user", type=str, default="root", help="Postgres user")
# parser.add_argument("----password", type=str, default="root", help="Postgres password")
# parser.add_argument("----database", type=str, default="ny_taxi", help="Postgres database")
# parser.add_argument("----table", type=str, default="yellow_taxi_data", help="Postgres table")
# parser.add_argument("----chunksize", type=int, default=100000, help="Pandas chunksize")
# parser.add_argument("----filename", type=str, default="yellow_tripdata_2021-01.csv", help="CSV filename")
# args = parser.parse_args()


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_name = "output.csv"
    url = params.url

    # url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    # download the csv file

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        t_start = time()

        df = next(df_iter)

        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print(f"Inserted another chunk..., took {t_end - t_start:.3f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data into Postgres")

    parser.add_argument("--user", type=str, help="Postgres user")
    parser.add_argument("--password", type=str, help="Postgres password")
    parser.add_argument("--host", type=str, help="Postgres host")
    parser.add_argument("--port", type=int, help="Postgres port")
    parser.add_argument("--db", type=str, help="Postgres database")
    parser.add_argument("--table_name", type=str, help="Postgres table")
    parser.add_argument("--url", type=str, help="CSV file URL")

    args = parser.parse_args()

    main(args)
