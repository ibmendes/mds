from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta
import os

# Caminho para o seu notebook Jupyter na pasta utils
NOTEBOOK_PATH = os.path.join(os.path.dirname(__file__), 'utils', 'notebooks', 'paxpe.ipynb')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), 'utils', 'notebooks', 'logs', 'paxpe_log.ipynb')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 26, 0, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'paxpe_dag',
    default_args=default_args,
    description='A DAG to run the PAXPE notebook daily at midnight UTC-3',
    schedule='0 3 * * *',  # 0h UTC-3 é 3h UTC
    catchup=False,
)

run_notebook_task = PapermillOperator(
    task_id='run_paxpe_notebook',
    input_nb=NOTEBOOK_PATH,
    output_nb=OUTPUT_PATH,
    dag=dag,
)

run_notebook_task
