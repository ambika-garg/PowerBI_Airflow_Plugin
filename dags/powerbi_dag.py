"""Standard imports"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from operators.powerbi_refresh_dataset_operator import PowerBIDatasetRefreshOperator


with DAG(
        dag_id='refresh_dataset_powerbi',
        schedule_interval=None,
        start_date=datetime(2023, 8, 7),
        catchup=False,
        concurrency=20,
        tags=['powerbi', 'dataset', 'refresh']
) as dag:

    start_dataset_refresh = BashOperator(
        task_id="Start_PowerBI_Dataset_Refresh",
        bash_command="echo Starting Refresh"
    )

    refresh_in_given_workspace = PowerBIDatasetRefreshOperator(
        task_id="refresh_in_given_workspace",
        dataset_id="5bd3c9f2-4bda-45e6-be0a-f6106b7ae38b",
        group_id="b7f916f7-b7bf-41ca-846d-f133ec6d2a46",
        force_refresh = True,
        wait_for_termination = False
    )

    start_dataset_refresh >> refresh_in_given_workspace
