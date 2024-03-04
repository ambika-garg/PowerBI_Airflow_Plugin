
# The DAG object
from airflow import DAG

# Operators
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Format date
from datetime import datetime, timedelta
from airflow.models.variable import Variable
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

    list_files = BashOperator(
        task_id = "List_files",
        bash_command = "ls /opt/airflow/git/powerbi-dataset-refresh.git"
    )

    powerbi_dataset_refresh = PowerBIDatasetRefreshOperator(
        task_id = "powerbi_dataset_refresh_task",
        client_id=Variable.get("client_id", default_var=None),
        group_id=Variable.get("group_id", default_var=None),
        dataset_id=Variable.get("dataset_id", default_var=None)
    )

    list_files >> powerbi_dataset_refresh