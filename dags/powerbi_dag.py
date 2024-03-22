"""Standard imports"""
from datetime import datetime

# The DAG object
from airflow import DAG
from airflow.operators.bash import BashOperator

# Operators
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
        dataset_id="372d46ba-e761-4c9e-b306-5d7d89676b13",
        group_id="effb3465-0270-42ec-857a-0b2c9aafce46",
        force_refresh = False,
        # wait_for_completion = False
    )

    # refresh_in_my_workspace = PowerBIDatasetRefreshOperator(
    #     task_id="refresh_in_my_workspace",
    #     dataset_id="372d46ba-e761-4c9e-b306-5d7d89676b13",
    #     group_id="effb3465-0270-42ec-857a-0b2c9aafce46"
    # )

    start_dataset_refresh >> refresh_in_given_workspace #ignore
    # >> refresh_in_my_workspace
