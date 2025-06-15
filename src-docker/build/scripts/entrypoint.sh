#!/bin/bash
set -e

if [[ $HOSTNAME == "namenode" ]]; then
  if [ ! -d "/home/hadoop/dfs/name/current" ] || [ -z "$(ls -A /home/hadoop/dfs/name/current)" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force -nonInteractive
  else
    echo "NameNode já formatado – pulando."
  fi

  echo "Starting HDFS services..."
  $HADOOP_HOME/sbin/start-dfs.sh

  (
    echo "Aguardando NameNode iniciar..."
    sleep 10
    echo "Forçando saída do Safe Mode..."
    hdfs dfsadmin -safemode leave || echo "Safe Mode já pode ter sido desligado"
  ) &

  echo "Iniciando NameNode em foreground (mantém o container vivo)..."
  exec $HADOOP_HOME/bin/hdfs namenode

elif [[ $HOSTNAME == "datanode" ]]; then
  if [ ! -d "/hadoop/dfs/data/current" ]; then
    echo "Diretório de dados do DataNode não existe, criando..."
    mkdir -p /hadoop/dfs/data
    chown -R hdfs:hdfs /hadoop/dfs/data
  else
    echo "Diretório de dados do DataNode já existe – pulando criação."
  fi

  echo "Iniciando DataNode em foreground..."
  exec $HADOOP_HOME/bin/hdfs datanode

else
  echo "Hostname não reconhecido: $HOSTNAME. Nenhuma ação executada."
  exec "$@"
fi
