import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from pathlib import Path

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
    print(df.head(2))
    print(f"columns: \n {df.dtypes}")
    print(f'rows before drop nan: {len(df)}')
    df.dropna(subset=['PUlocationID','DOlocationID'], inplace=True)
    print(f"rows after drop nan: {len(df)}")

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
    file_name = f'fhv_tripdata_{year}-{month:02}'
    dataset_url = f'{url}/{file_name}.csv.gz'

    local_path = Path(f'../../data/fhv_parquet/{file_name}.parquet')
    remote_path = Path(f'fhv_parquet/{file_name}.parquet')

    df = get_df_by_url(dataset_url)
    df_clean = clean(df)
    write_local_to_parquet(df_clean, local_path)
    upload_data_to_gcs(local_path, remote_path)

if __name__ == '__main__':
    for month in range(1,13):
        web_to_gcs(2019, month)
