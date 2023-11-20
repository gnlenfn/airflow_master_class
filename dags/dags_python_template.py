from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id="python_t1",
        python_callable=python_function1,
        op_kwargs={
            "start_date": "{{data_interval_start | ds}}",
            "end_date": "{{data_interval_end | ds}}"
        }
    )

    @task(task_id="python_t2")
    def python_function2(**kwargs):
        print(kwargs)
        print(f"ds: {kwargs['ds']}")
        print(f"ts: {kwargs['ts']}")
        print(f"data_interval_start: {str(kwargs['data_interval_start'])}")
        print(f"data_interval_end: {str(kwargs['data_interval_end'])}")
        print(f"task_instance: {str(kwargs['ti'])}")
