
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
        task_id="power_bi_dataset_refresh",
        dataset_id="9f91d322-1201-461d-b76b-78e0aef0670a",
        group_id="00d1112d-45fd-4af6-aa0c-65b9602772ff"
    )

    list_files >> powerbi_dataset_refresh