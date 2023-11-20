from airflow import DAG
import pendulum

from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task(task_id="task_using_macros",
          templates_dict={
              'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + '
                            'macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
              'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + '
                          'macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
          }
          )
    def get_datetime_marco(**kwargs):
        template_dict = kwargs.get('template_dict') or {}
        if template_dict:
            start_date = template_dict.get('start_date') or "start_date 없음"
            end_date = template_dict.get('end_date') or 'end_date 없음'

            print(start_date)
            print(end_date)


    @task(task_id="task_direct_calc")
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta
        data_interval_end = kwargs.get('data_interval_end')

        prev_month_first_day = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)
        prev_month_last_day = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)

        print(prev_month_first_day)
        print(prev_month_last_day)

    get_datetime_marco() >> get_datetime_calc()
