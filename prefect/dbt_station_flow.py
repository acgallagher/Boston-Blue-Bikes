from prefect_gcp.credentials import GcpCredentials
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile

credentials = GcpCredentials.load("boston-blue-bikes")
production_configs = BigQueryTargetConfigs(
    schema="production",  # also known as dataset
    credentials=credentials,
)
production_configs.save("production_configs")

dbt_cli_profile = DbtCliProfile(
    name="production_profile",
    target="production",
    target_configs=production_configs,
)
dbt_cli_profile.save("production_profile")
