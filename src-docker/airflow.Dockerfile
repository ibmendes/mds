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

###############################
# 8) JARs adicionais necessários
###############################

COPY src-docker/build/spark/jars/ambiente/*.jar /opt/airflow/jars/

# RUN wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/4.0.0/hive-metastore-4.0.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-exec/4.0.0/hive-exec-4.0.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.0/hadoop-common-3.4.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.4.0/hadoop-hdfs-client-3.4.0.jar

###############################
# 9) Copia JARs customizados do projeto
###############################
COPY src-docker/build/spark/jars/*.jar /opt/airflow/jars/

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64


# spark submit para execs locais, papermill para notebooks, openlineage para rastreamento
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
&& pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-papermill \
apache-airflow-providers-openlineage>=1.8.0

