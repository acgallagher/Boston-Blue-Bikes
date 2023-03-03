import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
from pathlib import Path
import requests
import json
from datetime import datetime, timedelta


@task()
def extract_station_status() -> pd.DataFrame:
    """Extracts station data from archive and stores the data in a pandas dataframe."""
    url = "https://gbfs.bluebikes.com/gbfs/en/station_status.json"
    r = requests.get(url)
    station_status = r.json()["data"]["stations"]
    df = pd.read_json(json.dumps(station_status))

    return df


@task()
def clean_station_status(df: pd.DataFrame) -> pd.DataFrame:
    """Optimizes the datatypes for each column."""

    # time_prepared
    df.loc[:, "time_prepared"] = datetime.utcnow()

    # last_reported
    df.last_reported = df.last_reported.apply(
        lambda x: datetime(1970, 1, 1) + timedelta(seconds=x)
    )

    # station_id
    df.station_id = df.station_id.astype("int16")

    # legacy_id
    df = df.drop(["legacy_id"], axis=1)

    # station_status
    df.station_status = df.station_status.astype("category")

    # is_installed
    df.is_installed = df.is_installed.astype("bool")

    # is_renting
    df.is_renting = df.is_renting.astype("bool")

    # is_returning
    df.is_returning = df.is_returning.astype("bool")

    # num_docks_available
    df.num_docks_available = df.num_docks_available.astype("int8")

    # num_docks_disabled
    df.num_docks_disabled = df.num_docks_disabled.astype("int8")

    # num_bikes_available
    df.num_bikes_available = df.num_bikes_available.astype("int8")

    # num_bikes_disabled
    df.num_bikes_disabled = df.num_bikes_disabled.astype("int8")

    # num_ebikes_available
    df.num_bikes_available = df.num_bikes_available.astype("int8")

    # eightd_has_available_keys
    df.eightd_has_available_keys = df.eightd_has_available_keys.astype("bool")

    return df


@task()
def write_local_station_status(df: pd.DataFrame) -> Path:
    """Stores the dataframe locally as a parquet file, returning the path where the file is saved."""
    local_path = f"data/stationdata/station_status-{datetime.now().year:4}_{datetime.now().month:02}_{datetime.now().day:02}_{datetime.now().hour:02}-{datetime.now().minute:02}.parquet"
    path = Path(local_path)
    df.to_parquet(path=path, compression="snappy")
    return local_path


@task()
def write_gcs_station_status(local_path: str) -> None:
    """Stores the local parquet file remotely in GCS."""
    gcs_block = GcsBucket.load("boston-blue-bikes")
    path = Path(local_path)
    gcs_block.upload_from_path(from_path=path, to_path=path)


@task()
def write_bq_station_status(df: pd.DataFrame, local_path: str):
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("boston-blue-bikes")

    file = (
        local_path.rstrip("parquet")
        .rstrip(".")
        .lstrip("data/")
        .lstrip("stationdata")
        .lstrip("/")
    )

    df.to_gbq(
        destination_table=f"raw_data.{file}",
        project_id="boston-blue-bikes",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=250_000,
        if_exists="replace",
    )


@flow(log_prints=True)
def web_to_gcp_station_status() -> None:

    df = extract_station_status()
    df = clean_station_status(df)
    local_path = write_local_station_status(df)
    write_gcs_station_status(local_path)
    write_bq_station_status(df, local_path)
