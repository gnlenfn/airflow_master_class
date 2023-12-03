from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023,11,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    def insert_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = "insert 수행"
                sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    def hook_postgres_insert(postgres_conn_id, **kwargs):
        from contextlib import closing
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = "hook insert 수행"
                sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insert_task = PythonOperator(
        task_id="insert_task",
        python_callable=insert_postgres,
        op_args=['172.28.0.3', '5432', 'gnlenfn', 'gnlenfn', 'gnlenfn']
    )

    hook_insert_task = PythonOperator(
        task_id="hook_insert_task",
        python_callable=hook_postgres_insert,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom'}
    )

    insert_task >> hook_insert_task
