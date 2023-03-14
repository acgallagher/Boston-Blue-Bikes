from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from stationdata_flow import stationdata_flow
from tripsdata_flow import tripdata_flow

stationdata_dep = Deployment.build_from_flow(
    flow=stationdata_flow,
    name="stationdata-flow",
    schedule=(CronSchedule(cron="*/15 * * * *", timezone="America/New_York")),
)


tripsdata_dep = Deployment.build_from_flow(
    flow=tripdata_flow,
    name="tripsdata-flow",
    schedule=(CronSchedule(cron="0 0 15 * *", timezone="America/New_York")),
)

if __name__ == "__main__":
    tripsdata_dep.apply()
    stationdata_dep.apply()
