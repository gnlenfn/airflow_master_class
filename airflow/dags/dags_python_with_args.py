from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from common.common_func import register, register2

with DAG(
    dag_id="dags_python_with_args",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    register_t1 = PythonOperator(
        task_id="dags_python_with_args",
        python_callable=register,
        op_args=['kim', 'man', 'kr', 'seoul']
    )

    register_t2 = PythonOperator(
        task_id="dags_python_with_kwargs",
        python_callable=register2,
        op_args=['kim', 'man', 'kr', 'seoul'],
        op_kwargs={
            "email": "test@test.com",
            "phone": "010-2020-1123"
        }
    )

    register_t1 >> register_t2
