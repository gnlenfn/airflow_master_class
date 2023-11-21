from airflow import DAG
import pendulum

from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task(task_id="python_xcom_push_by_return")
    def xcom_push_result(**kwargs):
        return "Seccess"

    @task(task_id="python_xcom_pull_1")
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print(f"xcom_pull로 직접 땡겨옴 {value1}")

    @task(task_id="python_xcom_pull_2")
    def xcom_pull_2(status, **kwargs):
        print(f"파라미터로 받아옴 {status}")

    xcom_pull_2(xcom_push_result())
    xcom_push_result() >> xcom_pull_1()
