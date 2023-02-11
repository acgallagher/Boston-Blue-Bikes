from etl_web_to_gcs_tripdata_new import web_to_gcs_tripdata_new
from etl_web_to_gcs_tripdata_old import web_to_gcs_tripdata_old
from etl_web_to_gcs_stationdata import web_to_gcs_stationdata
from etl_gcs_to_bq import etl_gcs_to_bq
from prefect import flow


@flow()
def main_flow(months: list[int], years: list[int]) -> None:
    web_to_gcs_tripdata_old()
    web_to_gcs_tripdata_new(months, years)
    web_to_gcs_stationdata()
    etl_gcs_to_bq()


if __name__ == "__main__":
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    years = [
        2015,
        2015,
        2016,
        2017,
        2018,
        2019,
        2020,
        2021,
        2022,
    ]
    main_flow(months, years)
