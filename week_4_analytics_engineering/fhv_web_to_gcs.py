from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
import os
from pathlib import Path

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_data(url, file_name):
    """Download data from github"""
    file_path = Path(f'fhv_{file_name}')
    print(f'download file {file_name} to path {file_path.resolve()}')
    os.system(f'wget {url}/{file_name} -O {file_path}')
    return file_path


@task(log_prints=True, retries=1)
def upload_data_to_gcs(file_path, file_name):
    """Upload data to GCS"""
    print(f'start upload file {file_name } to GCS')

    gcp_bucket = GcsBucket.load("zoom-gcs")
    gcp_bucket.upload_from_path(from_path=file_path,
                                to_path=f'fhv/{file_name}')

    print(f'finish upload file {file_name} to GCS')

@flow()
def web_to_gcs(year, month):

    file_name = f'fhv_tripdata_{year}-{month:02}.csv.gz'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv'

    file_path = download_data(url, file_name)
    upload_data_to_gcs(file_path, file_name)

if __name__ == '__main__':
    for month in range(1,13):
        web_to_gcs(2019, month)
