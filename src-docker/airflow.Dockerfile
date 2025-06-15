# Base na imagem oficial do Airflow 3.0.0 (Debian Bookworm)
FROM apache/airflow:3.0.0-python3.10
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Instalar dependências
USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY src-docker/build/requirements.txt  /requirements.txt
COPY src-docker/build/airflow/sparktest.py /opt/airflow/sparktest.py
COPY src-docker/build/airflow/init_connections.py /opt/airflow/init_connections.py


USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64


# spark submit para execs locais, papermill para notebooks, openlineage para rastreamento
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
&& pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-papermill \
apache-airflow-providers-openlineage>=1.8.0

