from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id="first_group")
    def group_1():
        """task_group 데코레이터를 이용한 첫 번째 그룹"""

        @task(task_id="inner_function1")
        def inner_func1(**kwargs):
            print("The first task in Group 1")

        inner_func2 = PythonOperator(
            task_id="inner_function2",
            python_callable=inner_func,
            op_kwargs={"msg": "The second task in Group 1"}
        )

        inner_func1() >> inner_func2

    with TaskGroup(group_id="second_group", tooltip="두 번째 그룹") as group_2:
        """docstring docstring"""
        @task(task_id="inner_function1")
        def inner_func1(**kwargs):
            print("The first task in Group 2")

        inner_func2 = PythonOperator(
            task_id="inner_function2",
            python_callable=inner_func,
            op_kwargs={"msg": "The second task in Group 2"}
        )

        inner_func1() >> inner_func2

    group_1() >> group_2
