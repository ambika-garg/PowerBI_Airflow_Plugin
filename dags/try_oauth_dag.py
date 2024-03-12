
# # The DAG object
# from airflow import DAG

# # Operators
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator


# # Format date
# from datetime import datetime, timedelta
# from airflow.models.variable import Variable
# from airflow.operators.bash import BashOperator

# from operators.try_openauth import TryOpenAuthOperator

# with DAG(
#         dag_id='try_open_auth',
#         schedule_interval=None,
#         start_date=datetime(2023, 8, 7),
#         catchup=False,
#         concurrency=20,
#         tags=['OpenAuth flow']
# ) as dag:

#     list_files = BashOperator(
#         task_id = "List_files",
#         bash_command = "ls /opt/airflow/git/powerbi-dataset-refresh.git"
#     )

#     try_open_auth = TryOpenAuthOperator(
#         task_id="try_open_auth",
#     )

#     list_files >> try_open_auth