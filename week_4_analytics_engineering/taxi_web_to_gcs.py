from pathlib import Path
import pandas as pd
import pyarrow
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import argparse

@task(log_prints=True, retries=1)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web in pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    
    for col in df.columns:
        if col.endswith('datetime'):
            df[col] = pd.to_datetime(df[col])

    columns_int = ['VendorID', 
                   'passenger_count', 
                   'RatecodeID', 
                   'PULocationID', 
                   'DOLocationID', 
                   'payment_type',
                   'trip_type']
    
    for col in columns_int:
        if col in df.columns:
            df[col] = df[col].astype('Int64')

    print(df.head(2))
    print(f"columns: \n {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame out locally as parquet file"""
    df.to_parquet(path, compression='gzip', engine='pyarrow')

@task(log_prints=True)
def write_gcs(local_path: Path, remote_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_bucket_block.upload_from_path(
        from_path=local_path,
        to_path=remote_path
    )
    return

@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{int(month):02}"

    remote_path = Path(f"data/{color}/{dataset_file}.parquet")
    local_path = f"{dataset_file}.parquet"

    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    print(f"url: {dataset_url}")
    df = fetch(dataset_url)
    df_clean = clean(df)

    write_local(df_clean, local_path)
    write_gcs(local_path, remote_path)

@flow()
def etl_parent_flow(year: int = 2021,
                    color: str = "yellow",
                    month = None):
    if month is None:
        for m in range(1,13):
            etl_web_to_gcs(year, m, color)
    else:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--year')
    parser.add_argument('--color')
    parser.add_argument('--month', nargs='?', const=-1)

    args = parser.parse_args()
    print(args)

    etl_parent_flow(args.year, args.color, args.month)