###############################
# 1) Imagem base oficial Spark
###############################
FROM spark:4.0.0-python3

    
###############################
# 2) Variáveis de ambiente
###############################
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV SPARK_WORKER_WEBUI_PORT=8082

###############################
# 3) Dependências do sistema
###############################
USER root
RUN apt-get update && \
    apt-get install -y wget curl unzip build-essential libssl-dev zlib1g-dev libbz2-dev \
                       libreadline-dev libsqlite3-dev libffi-dev libncursesw5-dev xz-utils tk-dev \
                       openjdk-21-jre-headless
                       
###############################
# 4) Instala dependências Python
###############################
COPY src-docker/build/requirements.txt /tmp/requirements.txt
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir --upgrade -r /tmp/requirements.txt

###############################
# 5) Configuração HDFS / Hive
###############################
COPY src-docker/build/config/hive-site.xml    $SPARK_HOME/conf/hive-site.xml
COPY src-docker/build/config/core-site.xml    $SPARK_HOME/conf/core-site.xml
COPY src-docker/build/config/hdfs-site.xml    $SPARK_HOME/conf/hdfs-site.xml

# >>> Adiciona log4j.properties para evitar warning
RUN echo '\
log4j.rootLogger=INFO, console\n\
log4j.appender.console=org.apache.log4j.ConsoleAppender\n\
log4j.appender.console.layout=org.apache.log4j.PatternLayout\n\
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n' \
> $SPARK_HOME/conf/log4j.properties

###############################
# 6) Remoção de JARs conflitantes e configuração nativa do Hadoop
###############################
RUN rm -f ${SPARK_HOME}/jars/hive-*

ENV HADOOP_VERSION=3.4.0
COPY src-docker/build/downloads/hadoop-${HADOOP_VERSION}.tar.gz /tmp/hadoop-${HADOOP_VERSION}.tar.gz

RUN apt-get update && \
    tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt && \
    rm /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    ln -s /opt/hadoop-${HADOOP_VERSION}/bin/hdfs /usr/local/bin/hdfs && \
    \
    cp -r /opt/hadoop-${HADOOP_VERSION}/lib/native /usr/local/lib/hadoop-native && \
    \
    apt-get install -y libsnappy1v5 libzstd1 liblzo2-2 libssl3 && \
    \
    echo "export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/local/lib/hadoop-native" >> /etc/profile.d/hadoop-native.sh && \
    echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/usr/local/lib/hadoop-native" >> /etc/profile.d/hadoop-native.sh

ENV PATH="/opt/hadoop-${HADOOP_VERSION}/bin:${PATH}" \
    HADOOP_COMMON_LIB_NATIVE_DIR=/usr/local/lib/hadoop-native \
    LD_LIBRARY_PATH=/usr/local/lib/hadoop-native

# Instala Hive 4
ENV HIVE_VERSION=4.0.0
COPY src-docker/build/downloads/apache-hive-${HIVE_VERSION}-bin.tar.gz /tmp/apache-hive-${HIVE_VERSION}-bin.tar.gz
RUN tar -xzf /tmp/apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt && \
    rm /tmp/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    ln -s /opt/apache-hive-${HIVE_VERSION}-bin/bin/hive /usr/local/bin/hive

ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV PATH="${HIVE_HOME}/bin:${PATH}"
ENV HADOOP_CONF_DIR=$SPARK_HOME/conf
ENV HADOOP_USER_NAME=hdfs

###############################
# 8) JARs adicionais necessários
###############################
WORKDIR ${SPARK_HOME}/jars

COPY src-docker/build/spark/jars/ambiente/*.jar ${SPARK_HOME}/jars/

# RUN wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/4.0.0/hive-metastore-4.0.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/hive/hive-exec/4.0.0/hive-exec-4.0.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.0/hadoop-common-3.4.0.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.4.0/hadoop-hdfs-client-3.4.0.jar

###############################
# 9) Copia JARs customizados do projeto
###############################
COPY src-docker/build/spark/jars/*.jar ${SPARK_HOME}/jars/

# ENV SPARK_CLASSPATH=${SPARK_HOME}/jars-extras/*

###############################
# 10) Usuário e permissões
###############################
ARG USERNAME=sparkuser
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME && \
    echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/event_logs && \
    chown -R $USERNAME:$USERNAME ${SPARK_HOME}

###############################
# 11) Configuração Spark
###############################
RUN echo "spark.eventLog.enabled true"                             >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.eventLog.dir file://${SPARK_HOME}/event_logs"       >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.history.fs.logDirectory file://${SPARK_HOME}/event_logs" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.master.host 0.0.0.0" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.master.ui.port 7078" >> $SPARK_HOME/conf/spark-defaults.conf

###############################
# 12) Ambiente de notebooks
###############################
USER $USERNAME
WORKDIR /home/$USERNAME/app
ENV PYSPARK_DRIVER_PYTHON_OPTS="lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --notebook-dir=/home/sparkuser/app \
    --NotebookApp.token='' \
    --NotebookApp.password='' \
    --NotebookApp.disable_check_xsrf=True"
EXPOSE 4040 4041 18080 8888 5555 7077 7078 8082

###############################
# 13) CMD com Jupyter + Spark Master + Worker
###############################
CMD ["bash", "-c", "/opt/spark/sbin/start-master.sh && \
                    /opt/spark/sbin/start-worker.sh spark://$(hostname):7077 && \
                    jupyter lab \
                      --ip=0.0.0.0 \
                      --port=8888 \
                      --no-browser \
                      --allow-root \
                      --notebook-dir=/home/sparkuser/app \
                      --NotebookApp.token='' \
                      --NotebookApp.password='' \
                      --NotebookApp.disable_check_xsrf=True"]
