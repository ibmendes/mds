FROM debian:bullseye-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    vim \
    netcat \
    procps \
    postgresql-client-13\
    unzip \
    sudo \
    ssh

## Download and install Hive
#RUN wget https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
#RUN tar -xzvf apache-hive-3.1.3-bin.tar.gz
#RUN mv apache-hive-3.1.3 /usr/local/hive

# Copy Hadoop and Hive tarballs
COPY src-docker/build/downloads/hadoop-3.4.0.tar.gz /tmp/hadoop-3.4.0.tar.gz
RUN mkdir -p /home/hadoop
RUN tar -xf /tmp/hadoop-3.4.0.tar.gz -C /home/hadoop --strip-components=1
RUN rm /tmp/hadoop-3.4.0.tar.gz


COPY src-docker/build/downloads/apache-hive-4.0.0-bin.tar.gz /tmp/apache-hive-4.0.0-bin.tar.gz
RUN mkdir -p /home/hive
RUN tar -xf /tmp/apache-hive-4.0.0-bin.tar.gz -C /home/hive/ --strip-components=1
RUN rm /tmp/apache-hive-4.0.0-bin.tar.gz

# Set up environment variables
ENV HIVE_HOME /home/hive
ENV PATH $HIVE_HOME/bin:$PATH
ENV HADOOP_HOME /home/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV HIVE_CONF_DIR /home/hive/conf
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# PostgreSQL JDBC Driver
# Download PostgreSQL JDBC driver compatible with PostgreSQL 13
# Use the latest stable 42.x driver for PostgreSQL 13 compatibility
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P $HIVE_HOME/lib/

RUN mkdir -p $HIVE_HOME/lib/jdbc && \
    cp $HIVE_HOME/lib/postgresql-42.7.3.jar $HIVE_HOME/lib/jdbc/


# Copy configuration files
COPY src-docker/build/config/hive-site.xml /home/hive/conf/hive-site.xml
COPY src-docker/build/config/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh
COPY src-docker/build/config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY src-docker/build/config/core-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml


# Expose ports for Hive services
EXPOSE 10000 10002

# Create hdfs user and set permissions
RUN useradd -m hdfs
RUN chown -R hdfs:hdfs /home/hadoop
RUN chown -R hdfs:hdfs /home/hive

# spark user 

RUN useradd -m sparkuser && usermod -aG hdfs sparkuser
RUN chown -R sparkuser:hdfs /home/hive /home/hadoop


#### ao invés de passar o jdbc driver no classpath do beeline, vamos criar um wrapper que passa o driver como argumento
# Renomeia o beeline original
RUN mv /home/hive/bin/beeline /home/hive/bin/beeline.orig

# Cria wrapper global no /usr/local/bin
RUN printf '#!/bin/bash\nexec /home/hive/bin/beeline.orig -u jdbc:hive2://localhost:10000 "$@"\n' > /usr/local/bin/beeline && \
    chmod +x /usr/local/bin/beeline


ENV HADOOP_USER_NAME hdfs
ENV HDFS_NAMENODE_USER=hdfs
ENV HDFS_DATANODE_USER=hdfs
ENV HDFS_SECONDARYNAMENODE_USER=hdfs



# Entrypoint script to start Hive services
COPY src-docker/build/scripts/hive_entrypoint.sh /hive_entrypoint.sh
RUN chmod +x /hive_entrypoint.sh

# Command to run Hive
ENTRYPOINT ["/hive_entrypoint.sh"]