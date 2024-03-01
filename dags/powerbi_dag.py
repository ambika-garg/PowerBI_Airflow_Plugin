
# The DAG object
from airflow import DAG

# Operators
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Format date
from datetime import datetime, timedelta
from airflow.models.variable import Variable

from operators.PowerBIRefreshDatasetOperator import PowerBIDatasetRefreshOperator


with DAG(
        dag_id='refresh_dataset_powerbi',
        schedule_interval=None,
        start_date=datetime(2023, 8, 7),
        catchup=False,
        concurrency=20,
        tags=['powerbi', 'dataset', 'refresh']
) as dag:

    powerbi_dataset_refresh = PowerBIDatasetRefreshOperator(
        client_id=Variable.get("client_id", default_var=None),
        group_id=Variable.get("group_id", default_var=None),
        dataset_id=Variable.get("dataset_id", default_var=None)
    )

    powerbi_dataset_refresh