from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    task_get_stfp = PythonOperator(
        task_id="task_get_stfp",
        python_callable=get_sftp
    )
    