"""Standard imports"""
from datetime import datetime

# The DAG object
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

    # refresh_in_given_workspace = PowerBIDatasetRefreshOperator(
    #     task_id="refresh_in_given_workspace",
    #     dataset_id="2cb8be46-5870-4e93-a936-7a7e36690da7",
    #     group_id="b7f916f7-b7bf-41ca-846d-f133ec6d2a46",
    #     force_refresh = True,
    #     # wait_for_termination= False
    # )
    # [START howto_operator_powerbi_refresh_dataset]
    dataset_refresh = PowerBIDatasetRefreshOperator(
        powerbi_conn_id= "powerbi_default",
        task_id="dataset_refresh",
        dataset_id="2cb8be46-5870-4e93-a936-7a7e36690da7",
        group_id="b7f916f7-b7bf-41ca-846d-f133ec6d2a46",
    )
    # [END howto_operator_powerbi_refresh_dataset]

    # [START howto_operator_powerbi_refresh_dataset_async]
    dataset_refresh2 = PowerBIDatasetRefreshOperator(
        powerbi_conn_id= "powerbi_default",
        task_id="dataset_refresh2",
        dataset_id="2cb8be46-5870-4e93-a936-7a7e36690da7",
        group_id="b7f916f7-b7bf-41ca-846d-f133ec6d2a46",
        wait_for_termination=False,
    )
    # [END howto_operator_powerbi_refresh_dataset_async]

    # [START howto_operator_powerbi_refresh_dataset_force_refresh]
    dataset_refresh3 = PowerBIDatasetRefreshOperator(
        powerbi_conn_id= "powerbi_default",
        task_id="dataset_refresh3",
        dataset_id="2cb8be46-5870-4e93-a936-7a7e36690da7",
        group_id="b7f916f7-b7bf-41ca-846d-f133ec6d2a46",
        force_refresh=True,
    )
    # [END howto_operator_powerbi_refresh_dataset_force_refresh]

    start_dataset_refresh >> dataset_refresh >> dataset_refresh2 >> dataset_refresh3
