import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):

    user = params.user
    pwd = params.pwd
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    file_name = params.file_name

    csv_name = file_name[:-3]

    os.system(f'wget {url} -O {file_name}')
    os.system(f'gzip -d {file_name}')

    # csv_name = "yellow_tripdata_2021-07.csv"

    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, dtype={'store_and_fwd_flag': str}, )

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table, con=engine, if_exists='append')

    df.to_sql(name=table, con=engine, if_exists='append')

    it_count = 0

    while True:

        t_start = time()
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table, con=engine, if_exists='append')

        t_end = time()

        print(f'inserted chunk {it_count}, took {t_end-t_start} seconds.')

        it_count+=1

if __name__ == '__main__':
    

# arguments

    parser = argparse.ArgumentParser(description='Ingest CSV into postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--pwd', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table', help='table name for postgres')
    parser.add_argument('--url', help='url for postgres')
    parser.add_argument('--file_name', help='file name of archived file')

    args = parser.parse_args()

    main(args)