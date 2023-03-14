from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation


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
