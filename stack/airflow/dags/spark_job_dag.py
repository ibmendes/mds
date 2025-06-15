from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta
import os


# Caminho para o seu notebook Jupyter na pasta utils
NOTEBOOK_PATH = '/opt/airflow/utils/notebooks/paxpe.ipynb'
OUTPUT_PATH = '/opt/airflow/utils/notebooks/logs/paxpe.ipynb'



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='chained_spark_jobs',
    default_args=default_args,
    description='Chain Spark jobs sequentially',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['spark'],
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='spark-submit --master local /opt/airflow/sparktest.py',
    )

    run_secondary_spark_job = SparkSubmitOperator(
        task_id='run_secondary_spark_job',
        application='/opt/airflow/sparktest.py',
        conn_id='spark_default',
    )

    run_notebook_spark_job = PapermillOperator(
        task_id='run_notebook_spark_job',
        input_nb=NOTEBOOK_PATH,
        output_nb=OUTPUT_PATH
    )

    run_spark_job >> run_secondary_spark_job >> run_notebook_spark_job
