from etl_web_to_gcp_tripdata import web_to_gcp_tripdata
from dbt_flow import trigger_dbt_flow
from prefect import flow


@flow()
def tripdata_flow() -> None:
    web_to_gcp_tripdata()
    trigger_dbt_flow()


if __name__ == "__main__":
    tripdata_flow()
