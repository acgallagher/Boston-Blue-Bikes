import pandas as pd
import numpy as np
import requests
from prefect import flow, task
from prefect_gcp import GcsBucket
from pathlib import Path


@task()
def extract_stationdata(url: str, filename: str) -> pd.DataFrame:
    """Extracts station data from archive and stores the data in a pandas dataframe."""
    df = pd.read_csv(
        "https://s3.amazonaws.com/hubway-data/current_bluebikes_stations.csv", header=1
    )
    return df


@task()
def clean_stationdata(df: pd.DataFrame) -> pd.DataFrame:
    """Optimizes the datatypes for each column."""

    # station_id
    df["station_id"] = df["Number"].astype("object")
    df = df.drop(["Number"], axis=1)

    # station_name
    df["station_name"] = df["Name"].astype("object").str.lower()
    df = df.drop(["Name"], axis=1)

    # station_latitude
    df["station_latitude"] = df["Latitude"]
    df = df.drop(["Latitude"], axis=1)
    df["station_latitude"] = pd.to_numeric(
        df["station_latitude"], downcast="float", errors="coerce"
    )

    # station_longitude
    df["station_longitude"] = df["Longitude"]
    df = df.drop(["Longitude"], axis=1)
    df["station_longitude"] = pd.to_numeric(
        df["station_longitude"], downcast="float", errors="coerce"
    )

    # district
    df["district"] = df["District"].astype("object")
    df = df.drop(["District"], axis=1)

    # public
    df["public"] = df["Public"].astype("object")
    df = df.drop(["Public"], axis=1)

    # total_docks
    df["total_docks"] = df["Total docks"]
    df = df.drop(["Total docks"], axis=1)
    df["total_docks"] = pd.to_numeric(
        df["total_docks"], downcast="integer", errors="coerce"
    )

    # deployment_year
    df["deployment_year"] = df["Deployment Year"]
    df = df.drop(["Deployment Year"], axis=1)
    df["deployment_year"] = pd.to_numeric(
        df["deployment_year"], downcast="integer", errors="coerce"
    )

    return df


@task()
def write_local_stationdata(df: pd.DataFrame, filename: str) -> Path:
    """Stores the dataframe locally as a parquet file, returning the path where the file is saved."""
    path = Path(f"data/stationdata/{filename}.parquet")
    df.to_parquet(path=path, compression="gzip")
    return path


@task()
def write_gcs_stationdata(path: Path):
    """Stores the local parquet file remotely in GCS."""
    gcs_block = GcsBucket.load("boston-blue-bikes")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@flow()
def web_to_gcs_stationdata() -> None:
    url = "https://s3.amazonaws.com/hubway-data/current_bluebikes_stations.csv"
    filename = "stations"

    df = extract_stationdata(url, filename)
    df = clean_stationdata(df)
    path = write_local_stationdata(df, filename)
    write_gcs_stationdata(path)
