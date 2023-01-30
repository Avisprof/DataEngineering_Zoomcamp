import os
import argparse
from sqlalchemy import create_engine
from time import time
import pandas as pd

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # download the csv
    if url.endswith('csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # ingest data
    for chunk in pd.read_csv(csv_name, 
                      iterator=True, 
                      chunksize=100_000):

        chunk.columns = chunk.columns.str.lower()

        # parse dates
        for col in chunk.columns:
            if col.endswith('datetime'):
                chunk[col] = pd.to_datetime(chunk[col])
        
        # create table
        if not table_exist(table_name, engine):
            chunk.head(0).to_sql(name=table_name, con=engine, if_exists='replace')           
    
        t_start = time()
        chunk.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
    
        print(f'inserted another chunk... took {(time() - t_start):.0f} seconds')

def table_exist(table_name, engine):
    df = pd.read_sql(f"""
                    SELECT 
                        * 
                    FROM pg_catalog.pg_tables
                    WHERE schemaname = 'public'
                        AND tablename = '{table_name}'
                    """, con=engine)
    return len(df) > 0
        


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres', required=True)
    parser.add_argument('--password', help='password for postgres', required=True)
    parser.add_argument('--host', help='host for postgres', required=True)
    parser.add_argument('--port', help='port for postgres', required=True)
    parser.add_argument('--db', help='database name for postgres', required=True)
    parser.add_argument('--table_name', help='name of the table where we will write the results to', required=True)
    parser.add_argument('--url', help='url of the csv file', required=True)

    args = parser.parse_args()
    print(args)

    main(args)
