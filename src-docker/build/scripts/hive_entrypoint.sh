#!/bin/bash
set -e

echo "Iniciando SSH para HDFS (start-dfs.sh via SSH)..."
$HADOOP_HOME/sbin/start-dfs.sh

echo "Aguardando o HDFS ficar disponível..."
max_attempts=10
attempt=1
while ! hdfs dfs -ls / >/dev/null 2>&1; do
  echo "Tentativa $attempt de $max_attempts: HDFS ainda não está acessível..."
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "❌ ERRO: HDFS não está disponível após $max_attempts tentativas."

    echo "🔍 DEBUG:"
    echo ">> JPS (processos Java):"
    jps || true

    echo ">> Portas em uso (netstat):"
    netstat -tulnp || true

    echo ">> Testando acesso HTTP ao NameNode WebUI (curl):"
    curl -s http://localhost:9870 || echo "NameNode WebUI inacessível"

    echo ">> Permissões no diretório do HDFS local:"
    ls -l $HADOOP_HOME/hdfs || true

    echo ">> Verificando status do NameNode:"
    $HADOOP_HOME/bin/hdfs dfsadmin -report || true

    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 3
done

echo "✅ HDFS está disponível."

echo "Verificando se o warehouse do Hive existe em: /user/hive/warehouse ..."
if ! hdfs dfs -test -d /user/hive/warehouse; then
  echo "📁 Diretório /user/hive/warehouse não existe. Criando..."
  hdfs dfs -mkdir -p /user/hive/warehouse
  hdfs dfs -chmod 1777 /user/hive/warehouse
  echo "✅ Diretório /user/hive/warehouse criado com sucesso."
else
  echo "✅ Diretório /user/hive/warehouse já existe."
fi

echo "Aguardando PostgreSQL Metastore..."
until pg_isready -h "$HIVE_METASTORE_HOST" -p 5432 -U "$HIVE_METASTORE_USER" >/dev/null 2>&1; do
  echo "⏳ PostgreSQL ainda não disponível..."
  sleep 3
done

echo "✅ PostgreSQL está disponível."

# Inicializa o schema do Hive se necessário
if ! schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "⚙️ Inicializando schema do Hive..."
  schematool -dbType postgres -initSchema
else
  echo "✅ Schema do Hive já está inicializado."
fi

# Inicia serviços Hive
echo "🚀 Iniciando Hive Metastore..."
nohup hive --service metastore &
sleep 10

echo "🚀 Iniciando HiveServer2..."
exec hive --service hiveserver2


