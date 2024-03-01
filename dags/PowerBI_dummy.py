
# The DAG object
from airflow import DAG

# Operators
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Format date
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from operators.powerbi_dataflow_refresh_operator import PowerBIDataflowRefreshOperator
from hooks.powerbi_client_credentials_hook import PowerBIClientCredentialsHook



def get_token_microsoft(**context):
    pbHook = PowerBIClientCredentialsHook(
        client_id=Variable.get('analytics_powerbi_client_id'),  # Replace with your actual Client ID (Application ID)
        client_secret=Variable.get('analytics_powerbi_client_secret'),  # Replace with your actual Client Secret (Application Secret)
        tenant_id=Variable.get('analytics_powerbi_tenant_id'),  # Replace with your actual Tenant ID (if not 'common')
        resource='https://analysis.windows.net/powerbi/api'
    )

    access_token = pbHook.get_access_token()
    print(f"access_token: {access_token}")

    context['ti'].xcom_push(key='analytics_ms_powerbi_token', value=access_token)


default_args = {
    'owner': 'herculano.cunha',
    'email': ['herculanocm@outlook.com'],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'on_failure_callback': None,
    'on_success_callback': None,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=3)
}


with DAG(
        dag_id='datalake_refresh_dataflow_powerbi',
        schedule_interval=None,
        start_date=datetime(2023, 8, 7),
        default_args=default_args,
        catchup=False,
        concurrency=20,
        tags=['datalake','powerbi', 'dataflow', 'refresh']
) as dag:

    taskIniEmptySeq01 = EmptyOperator(
        task_id='taskIniEmptySeq01',
        dag=dag
    )

    taskGetTokenMSPowerBi = PythonOperator(
            task_id='taskGetTokenMSPowerBi',
            provide_context=True,
            python_callable=get_token_microsoft,
            dag=dag
    )

    # taskRefreshDataflowPowerBI = PowerBIDataflowRefreshOperator(
    #     task_id='taskRefreshDataflowPowerBI',
    #     dataflow_id='155a2f76-9107-*************',  # Replace with your actual Power BI dataflow ID
    #     workspace_id='7e7b31a6-d2e3-************',  # Replace with your actual Power BI workspace ID
    #     token='''{{ ti.xcom_pull(task_ids="taskGetTokenMSPowerBi", key='analytics_ms_powerbi_token') }}''',
    #     dag=dag,
    # )

    taskEndEmptySeq01 = EmptyOperator(
        task_id='taskEndEmptySeq01',
        dag=dag
    )

taskIniEmptySeq01 >> taskGetTokenMSPowerBi >> taskRefreshDataflowPowerBI >> taskEndEmptySeq01