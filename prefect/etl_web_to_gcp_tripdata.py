import pandas as pd
import numpy as np
import requests
from zipfile import ZipFile
import io
from prefect import flow, task
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_dask.task_runners import DaskTaskRunner
from datetime import date
from pathlib import Path

pd.options.mode.chained_assignment = None


@task()
def extract_tripdata(month: int, year: int) -> pd.DataFrame:
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
def clean_tripdata(df: pd.DataFrame) -> pd.DataFrame:
    """Columns retyped to increase performance"""

    # remove non-overlapping columns
    if "birth year" in df.columns:
        df = df.drop(["birth year"], axis=1)

    if "gender" in df.columns:
        df = df.drop(["gender"], axis=1)

    if "postal code" in df.columns:
        df = df.drop(["postal code"], axis=1)

    # tripduration
    df.tripduration = df.tripduration.astype("int32")
    df = df[df.tripduration.values < 4.32e5]

    # starttime
    df.starttime = pd.to_datetime(df.starttime)

    # stoptime
    df.stoptime = pd.to_datetime(df.stoptime)

    # start_station_id
    df.loc[:, "start_station_id"] = df.loc[:, "start station id"].astype("int16")
    df = df.drop(["start station id"], axis=1)

    # start_station_name
    df.loc[:, "start_station_name"] = df.loc[:, "start station name"].astype("string")
    df.start_station_name = df.start_station_name.str.lower()
    df = df.drop(["start station name"], axis=1)

    # start_station_latitude
    df.loc[:, "start_station_latitude"] = df.loc[:, "start station latitude"].astype(
        "float64"
    )
    df = df.drop(["start station latitude"], axis=1)

    # start_station_longitude
    df.loc[:, "start_station_longitude"] = df.loc[:, "start station longitude"].astype(
        "float64"
    )
    df = df.drop(["start station longitude"], axis=1)

    # end_station_id
    df = df[df.loc[:, "end station id"] != r"\N"]
    df.loc[:, "end_station_id"] = df.loc[:, "end station id"].astype("int16")
    df = df.drop(["end station id"], axis=1)

    # end_station_name
    df.loc[:, "end_station_name"] = df.loc[:, "end station name"].astype("string")
    df.end_station_name = df.end_station_name.str.lower()
    df = df.drop(["end station name"], axis=1)

    # end_station_latitude
    df = df[df.loc[:, "end station latitude"] != r"\N"]
    df.loc[:, "end_station_latitude"] = df.loc[:, "end station latitude"].astype(
        "float64"
    )
    df = df.drop(["end station latitude"], axis=1)

    # end_station_longitude
    df = df[df.loc[:, "end station longitude"] != r"\N"]
    df.loc[:, "end_station_longitude"] = df.loc[:, "end station longitude"].astype(
        "float64"
    )
    df = df.drop(["end station longitude"], axis=1)

    # bikeid
    df.bikeid = df.bikeid.astype("int16")

    # usertype
    df.usertype = df.usertype.str.lower()
    df.usertype = df.usertype.astype("category")

    return df


@task()
def write_local_tripdata(month: int, year: int, df: pd.DataFrame) -> str:
    """Write DataFrame out as a parquet file"""
    local_path = f"data/tripdata/tripdata-{year:4}_{month:02}.parquet"
    path = Path(local_path)
    df.to_parquet(path=path, compression="snappy")
    return local_path


@task()
def write_gcs_tripdata(local_path: str) -> None:
    path = Path(local_path)
    gcs_block = GcsBucket.load("boston-blue-bikes")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@task()
def write_bq_tripdata(df: pd.DataFrame, local_path: str) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("boston-blue-bikes")

    file = (
        local_path.rstrip("parquet")
        .rstrip(".")
        .lstrip("data/")
        .lstrip("tripdata")
        .lstrip("/")
    )

    df.to_gbq(
        destination_table=f"raw_data.{file}",
        project_id="boston-blue-bikes",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=250_000,
        if_exists="replace",
    )


@flow(task_runner=DaskTaskRunner)
def web_to_gcp_tripdata(
    months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    years: list[int] = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022],
) -> None:
    for year in years:
        for month in months:
            df = extract_tripdata.submit(month, year)
            df = clean_tripdata.submit(df)
            local_path = write_local_tripdata.submit(month, year, df)
            write_gcs_tripdata.submit(local_path)
            write_bq_tripdata.submit(df, local_path)
