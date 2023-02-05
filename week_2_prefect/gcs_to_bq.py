from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trid data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_bucket = GcsBucket.load("zoom-gcs")
    gcp_bucket.get_directory(from_path=gcs_path, local_path=f"../")
    return Path(f"../{gcs_path}")

@task()
def read_data(path: Path) -> pd.DataFrame:
    """Read data frame"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="zoomcamp_bq.ny_taxi",
        project_id="zoomcamp-de-375610",
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow(log_prints=True)
def etl_gcs_to_bq(months: list[int] = [1,2],
                  year: int = 2021,
                  color: str = "yellow"):
    """Main ETL flow to load data into Big Query"""
    total = 0
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = read_data(path)
        total += len(df)
        print(f'{total} rows processed')
        write_bq(df)



if __name__ == '__main__':
    etl_gcs_to_bq()
