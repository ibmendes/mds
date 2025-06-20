import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

now = pendulum.now(tz="UTC")
now_to_the_hour = (now - datetime.timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
START_DATE = now_to_the_hour
DAG_NAME = "test_dag_v1"

dag = DAG(
    DAG_NAME,
    schedule="*/10 * * * *",
    default_args={"depends_on_past": True},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

run_this_1 = EmptyOperator(task_id="run_this_1", dag=dag)
run_this_2 = EmptyOperator(task_id="run_this_2", dag=dag)
run_this_2.set_upstream(run_this_1)
run_this_3 = EmptyOperator(task_id="run_this_3", dag=dag)
run_this_3.set_upstream(run_this_2)

def print_hello_world():
    print("Hello, World!")

hello_world_task = EmptyOperator(task_id="hello_world_task", dag=dag)
hello_world_task.set_upstream(run_this_3)
hello_world_task.execute = print_hello_world