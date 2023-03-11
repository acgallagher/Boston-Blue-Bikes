from etl_web_to_gcp_tripdata import web_to_gcp_tripdata
from etl_web_to_gcp_station_information import web_to_gcp_station_information
from etl_web_to_gcp_station_status import web_to_gcp_station_status
from etl_gcs_to_bq import etl_gcs_to_bq
from prefect import flow


@flow()
def main_flow() -> None:
    # web_to_gcp_tripdata()
    web_to_gcp_station_information()
    web_to_gcp_station_status()
    # etl_gcs_to_bq()


if __name__ == "__main__":
    main_flow()
