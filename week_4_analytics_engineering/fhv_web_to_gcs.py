import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from pathlib import Path
import argparse

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_df_by_url(url:str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    print(f'get DataFrame from web: {url}')
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    for col in df.columns:
        if col.endswith('datetime'):
            df[col] = pd.to_datetime(df[col])

    columns_int = ['PUlocationID', 
                   'DOlocationID']
    
    for col in columns_int:
        if col in df.columns:
            df[col] = df[col].astype('Int64')
   
    print(df.head(2))
    print(f"columns: \n {df.dtypes}")

    return df

@task(log_prints=True)
def write_local_to_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame locally as parquet file"""
    print(f'save DataFrame to: {path.resolve()}')
    df.to_parquet(path, compression='gzip')

@task(log_prints=True, retries=1)
def upload_data_to_gcs(local_path, remote_path):
    """Upload data to GCS"""
    print(f'upload file {local_path.resolve()} to GCS {remote_path.resolve()}')

    gcp_bucket = GcsBucket.load("zoom-gcs")
    gcp_bucket.upload_from_path(from_path=local_path,
                                to_path=remote_path)

    print(f'file {local_path.resolve()} uploaded to GCS')

@flow()
def web_to_gcs(year, month):

    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv'
    file_name = f'fhv_tripdata_{year}-{int(month):02}'
    dataset_url = f'{url}/{file_name}.csv.gz'

    local_path = Path(f'fhv_parquet_{file_name}.parquet')
    remote_path = Path(f'fhv_parquet/{file_name}.parquet')

    df = get_df_by_url(dataset_url)
    df_clean = clean(df)
    write_local_to_parquet(df_clean, local_path)
    upload_data_to_gcs(local_path, remote_path)

@flow()
def etl_parent_flow(year: int = 2021,
                    month = None):
    if month is None:
        for m in range(1,13):
            web_to_gcs(year, m)
    else:
        web_to_gcs(year, month)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--year')
    parser.add_argument('--month', nargs='?', const=-1)

    args = parser.parse_args()
    print(args)

    etl_parent_flow(args.year, args.month)
