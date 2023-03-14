from prefect import task, flow
from prefect_gcp.credentials import GcpCredentials
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile
from prefect_dbt.cli.commands import DbtCoreOperation

"""
@task()
def build_schema_blocks(schema: str) -> str:

    credentials = GcpCredentials.load("boston-blue-bikes")
    target_configs = BigQueryTargetConfigs(
        schema=f"{schema}",  # also known as dataset
        credentials=credentials,
    )
    target_configs.save(f"{schema}_configs")

    dbt_cli_profile = DbtCliProfile(
        name=f"{schema}_profile",
        target=f"{schema}",
        target_configs=target_configs,
    )
    dbt_cli_profile.save(f"{schema}_profile")

    return f"{schema}_profile"


@task()
def build_operation_block(schema: str, commands: list[str], command_desc: str) -> str:

    dbt_cli_profile = DbtCliProfile.load(f"{schema}_profile")
    dbt_core_operation = DbtCoreOperation(
        commands=commands,
        dbt_cli_profile=dbt_cli_profile,
        overwrite_profiles=True,
    )
    dbt_core_operation.save(f"{schema}_{command_desc}")

    return f"{schema}_{command_desc}"
"""


@flow()
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=[
            'dbt run-operation stage_external_sources --vars "ext_full_refresh: true"',
            "dbt run",
        ],
        project_dir="./dbt/boston_blue_bikes",
        profiles_path="$HOME/.dbt",
    ).run()
    return result


if __name__ == "__main__":
    trigger_dbt_flow()
