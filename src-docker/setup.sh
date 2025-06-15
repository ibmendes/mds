#!/bin/bash

# Diretórios onde os arquivos serão armazenados
DOWNLOAD_DIR="build/downloads"
JARS_DIR="build/spark/jars/ambiente"

# Cria os diretórios se não existirem
mkdir -p "$DOWNLOAD_DIR"
mkdir -p "$JARS_DIR"

# URLs das distribuições
HIVE_URL="https://archive.apache.org/dist/hive/hive-4.0.0/apache-hive-4.0.0-bin.tar.gz"
HADOOP_URL="https://archive.apache.org/dist/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz"
DELTA_URL="https://codeload.github.com/delta-io/delta/tar.gz/refs/tags/v4.0.0"
# URLs dos jars do Delta Lake
ANTLR4_JAR_URL="https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.13.1/antlr4-runtime-4.13.1.jar"
DELTA_CORE_JAR_URL="https://repo1.maven.org/maven2/io/delta/delta-core_2.13/2.4.0/delta-core_2.13-2.4.0.jar"
DELTA_SPARK_JAR_URL="https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar"
DELTA_STORAGE_JAR_URL="https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar"

# URLs dos jars adicionais do Hive, Hadoop e PostgreSQL
HIVE_METASTORE_JAR_URL="https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/4.0.0/hive-metastore-4.0.0.jar"
HIVE_EXEC_JAR_URL="https://repo1.maven.org/maven2/org/apache/hive/hive-exec/4.0.0/hive-exec-4.0.0.jar"
POSTGRESQL_JAR_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"
HADOOP_COMMON_JAR_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.0/hadoop-common-3.4.0.jar"
HADOOP_HDFS_CLIENT_JAR_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.4.0/hadoop-hdfs-client-3.4.0.jar"

# Função para baixar um arquivo, se ele não existir
baixar() {
  local url="$1"
  local dest="$2"

  if [ -f "$dest" ]; then
    echo "✔ Arquivo já existe: $(basename "$dest")"
  else
    echo "⬇ Baixando: $(basename "$dest")..."
    curl -L "$url" -o "$dest"
    if [ $? -eq 0 ]; then
      echo "✅ Download concluído: $(basename "$dest")"
    else
      echo "❌ Erro ao baixar $(basename "$dest")"
      exit 1
    fi
  fi
}

# Baixa os tarballs principais
baixar "$HIVE_URL" "$DOWNLOAD_DIR/apache-hive-4.0.0-bin.tar.gz"
baixar "$HADOOP_URL" "$DOWNLOAD_DIR/hadoop-3.4.0.tar.gz"
baixar "$DELTA_URL" "$DOWNLOAD_DIR/delta-spark-4.0.0.tar.gz"

# Baixa os jars do Delta Lake
baixar "$ANTLR4_JAR_URL" "$JARS_DIR/antlr4-runtime-4.13.1.jar"
baixar "$DELTA_CORE_JAR_URL" "$JARS_DIR/delta-core_2.13-2.4.0.jar"
baixar "$DELTA_SPARK_JAR_URL" "$JARS_DIR/delta-spark_2.13-4.0.0.jar"
baixar "$DELTA_STORAGE_JAR_URL" "$JARS_DIR/delta-storage-4.0.0.jar"

# Baixa os jars do Hive, Hadoop e PostgreSQL
baixar "$HIVE_METASTORE_JAR_URL" "$JARS_DIR/hive-metastore-4.0.0.jar"
baixar "$HIVE_EXEC_JAR_URL" "$JARS_DIR/hive-exec-4.0.0.jar"
baixar "$POSTGRESQL_JAR_URL" "$JARS_DIR/postgresql-42.6.0.jar"
baixar "$HADOOP_COMMON_JAR_URL" "$JARS_DIR/hadoop-common-3.4.0.jar"
baixar "$HADOOP_HDFS_CLIENT_JAR_URL" "$JARS_DIR/hadoop-hdfs-client-3.4.0.jar"
