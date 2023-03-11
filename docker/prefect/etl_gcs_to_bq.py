from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_dask.task_runners import DaskTaskRunner
import os

"""
@task(log_prints=True)
def extract_from_gcs(file_path: Path, folder_path: str) -> None:
    "Download trip data from GCS"
    gcs_block = GcsBucket.load("boston-blue-bikes")
    "boston_blue_bikes_data_lake/data/tripdata/2015_01-tripdata.parquet"
    gcs_block.get_directory(from_path=file_path, local_path=folder_path)"""


@task()
def transform(file_path: Path) -> Path:
    """Transform into DataFrame"""
    df = pd.read_parquet(file_path)
    return df


@task()
def write_bq(df, file) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("boston-blue-bikes")

    file = file.rstrip("parquet").rstrip(".")

    df.to_gbq(
        destination_table=f"raw_data.{file}",
        project_id="boston-blue-bikes",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=250_000,
        if_exists="replace",
    )


@flow(log_prints=True, task_runner=DaskTaskRunner())
def tripdata_gcs_to_bq(log_prints=True) -> None:
    folder_path = "data/tripdata"
    tripdata_files = os.listdir(Path(folder_path))
    for file in tripdata_files:
        file_path = os.path.join(folder_path, file)

        # extract_from_gcs(file_path, folder_path)

        df = transform.submit(file_path)
        write_bq.submit(df, file)


@flow()
def stationdata_gcs_to_bp() -> None:
    folder_path = "data/stationdata"
    stationdata_files = os.listdir(Path(folder_path))
    for file in stationdata_files:
        file_path = os.path.join(folder_path, file)
        # extract_from_gcs(file_path, file)
        df = transform(file_path)
        write_bq(df, file)


@flow()
def etl_gcs_to_bq() -> None:
    # tripdata_gcs_to_bq()
    stationdata_gcs_to_bp()


if __name__ == "__main__":
    etl_gcs_to_bq()
