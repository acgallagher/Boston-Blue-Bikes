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


@task()
def extract_tripdata_old(year: int) -> pd.DataFrame:
    """Extracts older trip data files"""
    if year != 2014:
        filename = f"hubway_Trips_{year}"
        df = pd.read_csv(f"https://s3.amazonaws.com/hubway-data/{filename}.csv")
        return df
    else:
        filename = f"hubway_Trips_{year}"
        df1 = pd.read_csv(f"https://s3.amazonaws.com/hubway-data/{filename}_1.csv")
        df2 = pd.read_csv(f"https://s3.amazonaws.com/hubway-data/{filename}_2.csv")
        df = pd.concat([df1, df2])
        return df


@task()
def clean_tripdata_old(df: pd.DataFrame) -> pd.DataFrame:
    """All columns need to be renamed and retyped to match newer trips datasets"""

    # tripduration
    df["tripduration"] = df["Duration"]
    df["tripduration"] = pd.to_numeric(df["tripduration"], downcast="integer")
    df = df.drop(["Duration"], axis=1)
    df = df[df["tripduration"] < 4.32e5]

    # starttime
    df["starttime"] = df["Start date"]
    df["starttime"] = pd.to_datetime(df["starttime"])
    df = df.drop(["Start date"], axis=1)

    # stoptime
    df["stoptime"] = df["End date"]
    df["stoptime"] = pd.to_datetime(df["stoptime"])
    df = df.drop(["End date"], axis=1)

    # start_station_id
    df["start_station_id"] = df["Start station number"]
    df = df.drop(["Start station number"], axis=1)

    # start_station_name
    df["start_station_name"] = df["Start station name"].str.lower()
    df = df.drop(["Start station name"], axis=1)

    # start_station_latitude
    df["start_station_latitude"] = np.nan

    # start_station_longitude
    df["start_station_longitude"] = np.nan

    # end_station_id
    df["end_station_id"] = df["End station number"]
    df = df.drop(["End station number"], axis=1)

    # end_station_name
    df["end_station_name"] = df["End station name"].str.lower()
    df = df.drop(["End station name"], axis=1)

    # end_station_latitude
    df["end_station_latitude"] = np.nan

    # end_station_longitude
    df["end_station_longitude"] = np.nan

    # bikeid
    df["bikeid"] = df["Bike number"]
    df = df.drop(["Bike number"], axis=1)

    # usertype
    df["usertype"] = df["Member type"].str.lower()
    df = df.drop(["Member type"], axis=1)

    # postal_code
    df["postal_code"] = df["Zip code"].astype(str)
    df = df.drop(["Zip code"], axis=1)

    df["postal_code"] = pd.to_numeric(
        df["postal_code"], downcast="integer", errors="coerce"
    )

    # birth_year
    df["birth_year"] = np.nan

    # gender
    df["gender"] = df["Gender"]
    df.loc[(df["gender"] != "Male") & (df["gender"] != "Female"), "gender"] = 0
    df.loc[df["gender"] == "Male", "gender"] = 1
    df.loc[df["gender"] == "Female", "gender"] = 2
    df["gender"] = pd.to_numeric(df["gender"], downcast="float")
    df = df.drop(["Gender"], axis=1)

    return df


@task()
def write_local_tripdata_old(year: int, df: pd.DataFrame) -> Path:
    """Write DataFrame out as a parquet file"""
    path = Path(f"data/tripdata/{year}-tripdata.parquet")
    df.to_parquet(path=path, compression="gzip")
    return path


@task()
def write_gcs_tripdata_old(path: Path) -> None:
    gcs_block = GcsBucket.load("boston-blue-bikes")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@flow(task_runner=DaskTaskRunner())
def web_to_gcs_tripdata_old(years: list[int] = [2011, 2012, 2013, 2014]) -> None:
    for year in years:
        df = extract_tripdata_old.submit(year)
        df = clean_tripdata_old.submit(df)
        path = write_local_tripdata_old.submit(year, df)
        write_gcs_tripdata_old.submit(path)
