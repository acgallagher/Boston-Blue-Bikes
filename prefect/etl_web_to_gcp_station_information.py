import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
from pathlib import Path
import requests
import json
from datetime import datetime


@task()
def extract_station_information() -> pd.DataFrame:
    """Extracts station data from archive and stores the data in a pandas dataframe."""
    url = "https://gbfs.bluebikes.com/gbfs/en/station_information.json"
    r = requests.get(url)
    station_information = r.json()["data"]["stations"]
    df = pd.read_json(json.dumps(station_information))

    return df


@task()
def clean_station_information(df: pd.DataFrame) -> pd.DataFrame:
    """Optimizes the datatypes for each column."""

    # time_prepared
    df.loc[:, "time_prepared"] = datetime.utcnow()

    # stationname
    df.loc[:, "station_name"] = df.loc[:, "name"].astype("string")
    df.station_name = df.station_name.str.lower()
    df = df.drop(["name"], axis=1)

    # eightd_has_key_dispenser
    df = df.drop(["eightd_has_key_dispenser"], axis=1)

    # rental_methods
    df = df.drop(["rental_methods"], axis=1)

    # region_id
    df.region_id = df.region_id.values.astype("int16")

    # eightd_station_services
    df = df.drop(["eightd_station_services"], axis=1)

    # electric_bike_surcharge_waiver
    df = df.drop(["electric_bike_surcharge_waiver"], axis=1)

    # station_type
    df.station_type = df.station_type.str.lower()
    df.station_type = df.station_type.astype("category")

    # short_name
    df.short_name = df.short_name.astype("string")

    # legacy_id
    df = df.drop(["legacy_id"], axis=1)

    # station_id
    df.station_id = df.station_id.values.astype("int16")

    # has_kiosk
    df = df.drop(["has_kiosk"], axis=1)

    # external_id
    df = df.drop(["external_id"], axis=1)

    # capacity
    df.capacity = df.capacity.values.astype("int8")

    # station_longitude
    df.loc[:, "station_longitude"] = df.loc[:, "lon"].astype("float64")
    df = df.drop(["lon"], axis=1)

    # station_latitude
    df.loc[:, "station_latitude"] = df.loc[:, "lat"].astype("float64")
    df = df.drop(["lat"], axis=1)

    return df


@task()
def write_local_station_information(df: pd.DataFrame) -> str:
    """Stores the dataframe locally as a parquet file, returning the path where the file is saved."""
    local_path = f"data/stationdata/station_information-{datetime.now().year:4}_{datetime.now().month:02}_{datetime.now().day:02}_{datetime.now().hour:02}-{datetime.now().minute:02}.parquet"
    path = Path(local_path)
    df.to_parquet(path=path, compression="snappy")
    return local_path


@task()
def write_gcs_station_information(local_path: str) -> None:
    """Stores the local parquet file remotely in GCS."""
    path = Path(local_path)
    gcs_block = GcsBucket.load("boston-blue-bikes")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@task()
def write_bq_station_information(df: pd.DataFrame, local_path: str):
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
def web_to_gcp_station_information() -> None:

    df = extract_station_information()
    df = clean_station_information(df)
    local_path = write_local_station_information(df)
    write_gcs_station_information(local_path)
    write_bq_station_information(df, local_path)
