"""Standard imports"""

from datetime import datetime

# The DAG object
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_powerbi_plugin.operators.powerbi import PowerBIDatasetRefreshOperator


with DAG(
    dag_id="refresh_dataset_powerbi",
    schedule_interval=None,
    start_date=datetime(2023, 8, 7),
    catchup=False,
    concurrency=20,
    tags=["powerbi", "dataset", "refresh"],
) as dag:

    start_dataset_refresh = BashOperator(
        task_id="Start_PowerBI_Dataset_Refresh", bash_command="echo Starting Refresh"
    )

    refresh_in_given_workspace = PowerBIDatasetRefreshOperator(
        task_id="refresh_in_given_workspace",
        group_id="<group_id>",
        dataset_id="<dataset_id>",
        force_refresh=False,
        wait_for_termination=True,
    )

    refresh_in_given_workspace
