from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

import os
import subprocess
import sys



py_utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "utils", "py"))
requirements_path = os.path.join(py_utils_path, "bacen", "requirements_pip.txt")

# Instala automaticamente os pacotes se o arquivo existir
if os.path.exists(requirements_path):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_path])


with DAG(
    dag_id="bacen_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_bacen = SparkSubmitOperator(
        task_id="run_bacen_spark",
        application=os.path.join(py_utils_path, "bacen", "main.py"),
        name="bacen_spark_job",
        conn_id="spark_default",
        verbose=False,
        application_args=[],
        # conf={
        #     "spark.driver.memory": "2g",
        #     "spark.executor.memory": "2g",
        # },
        jars="/opt/airflow/jars/postgresql-42.6.0.jar",
    )

run_bacen