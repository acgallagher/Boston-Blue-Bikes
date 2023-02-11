import pandas as pd
import numpy as np
import requests
from zipfile import ZipFile
import io
from prefect import flow, task
from prefect_gcp import GcsBucket
from prefect_dask.task_runners import DaskTaskRunner
from datetime import date
from pathlib import Path

pd.options.mode.chained_assignment = None


@task()
def extract_tripdata_new(month: int, year: int) -> pd.DataFrame:
    """Extracts newer trip data files"""
    data_date = date(year, month, day=1)
    if data_date < date(2018, 5, 1):
        filename = f"{year}{month:02}-hubway-tripdata"
        df = pd.read_csv(f"https://s3.amazonaws.com/hubway-data/{filename}.zip")
        return df

    else:
        filename = f"{year}{month:02}-bluebikes-tripdata"
        url = f"https://s3.amazonaws.com/hubway-data/{filename}.zip"
        r = requests.get(url)
        with ZipFile(io.BytesIO(r.content)) as file:
            df = pd.read_csv(file.open(f"{filename}.csv"))
        return df


@task()
def clean_tripdata_new(df: pd.DataFrame) -> pd.DataFrame:
    """Columns retyped to increase performance"""

    # tripduration
    df["tripduration"] = pd.to_numeric(df["tripduration"], downcast="integer")
    df = df[df["tripduration"] < 4.32e5]

    # starttime
    df["starttime"] = pd.to_datetime(df["starttime"])

    # stoptime
    df["stoptime"] = pd.to_datetime(df["stoptime"])

    # start_station_id
    df["start_station_id"] = df["start station id"].astype("object")
    df = df.drop(["start station id"], axis=1)

    # start_station_name
    df["start_station_name"] = df["start station name"].str.lower()
    df = df.drop(["start station name"], axis=1)

    # start_station_latitude
    df["start_station_latitude"] = pd.to_numeric(
        df["start station latitude"], downcast="float"
    )
    df = df.drop(["start station latitude"], axis=1)

    # start_station_longitude
    df["start_station_longitude"] = pd.to_numeric(
        df["start station longitude"], downcast="float"
    )
    df = df.drop(["start station longitude"], axis=1)

    # end_station_id
    df["end_station_id"] = df["end station id"].astype("object")
    df = df.drop(["end station id"], axis=1)

    # end_station_name
    df["end_station_name"] = df["end station name"].str.lower()
    df = df.drop(["end station name"], axis=1)

    # end_station_latitude
    df.loc[df["end station latitude"] == r"\N", "end station latitude"] = np.nan
    df["end_station_latitude"] = pd.to_numeric(
        df["end station latitude"], downcast="float"
    )
    df = df.drop(["end station latitude"], axis=1)

    # end_station_longitude
    df.loc[df["end station longitude"] == r"\N", "end station longitude"] = np.nan
    df["end_station_longitude"] = pd.to_numeric(
        df["end station longitude"], downcast="float"
    )
    df = df.drop(["end station longitude"], axis=1)

    # bikeid
    df["bikeid"] = df["bikeid"].astype(str)

    # birth_year
    if "birth year" in df:
        df["birth_year"] = df["birth year"]
        df.loc[df["birth_year"] == r"\N", "birth_year"] = np.nan
        df["birth_year"] = pd.to_numeric(df["birth_year"], downcast="float")
        df = df.drop(["birth year"], axis=1)
    else:
        df["birth_year"] = np.nan

    # gender
    if "gender" in df:
        df["gender"] = pd.to_numeric(df["gender"], downcast="float")
    else:
        df["gender"] = np.nan

    # postal_code
    if "postal code" in df:
        df["postal_code"] = df["postal code"]
        df["postal_code"] = pd.to_numeric(
            df["postal_code"], downcast="float", errors="coerce"
        )
        df = df.drop(["postal code"], axis=1)
    else:
        df["postal_code"] = np.nan

    return df


@task()
def write_local_tripdata_new(month: int, year: int, df: pd.DataFrame) -> Path:
    """Write DataFrame out as a parquet file"""
    path = Path(f"data/tripdata/{year}_{month:02}-tripdata.parquet")
    df.to_parquet(path=path, compression="gzip")
    return path


@task()
def write_gcs_tripdata_new(path: Path) -> None:
    gcs_block = GcsBucket.load("boston-blue-bikes")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@flow(task_runner=DaskTaskRunner)
def web_to_gcs_tripdata_new(months: list[int], years: list[int]) -> None:
    for year in years:
        for month in months:
            df = extract_tripdata_new.submit(month, year)
            df = clean_tripdata_new.submit(df)
            path = write_local_tripdata_new.submit(month, year, df)
            write_gcs_tripdata_new.submit(path)
