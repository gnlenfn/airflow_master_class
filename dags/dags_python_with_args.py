from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id="dags_python_with_args",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    regist_t1 = PythonOperator(
        task_id="dags_python_with_args",
        python_callable=regist,
        op_args=['kim', 'man', 'kr', 'seoul']
    )

    regist_t1
