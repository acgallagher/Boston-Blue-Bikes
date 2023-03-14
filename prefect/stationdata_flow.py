from etl_web_to_gcp_station_information import web_to_gcp_station_information
from etl_web_to_gcp_station_status import web_to_gcp_station_status
from dbt_station_flow import trigger_dbt_flow
from prefect import flow


@flow()
def stationdata_flow() -> None:
    web_to_gcp_station_information()
    web_to_gcp_station_status()
    trigger_dbt_flow()


if __name__ == "__main__":
    stationdata_flow()
