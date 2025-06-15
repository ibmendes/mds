#!/usr/bin/env python

from pyspark.sql import SparkSession

# Criar a sessão do Spark
spark = SparkSession.builder \
    .appName("SparkTestJob") \
    .getOrCreate()

# Criando um DataFrame simples
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["Name", "Value"]
df = spark.createDataFrame(data, columns)

# Exibir o DataFrame
df.show()

# Realizar uma transformação: filtrar onde o valor é maior que 1
filtered_df = df.filter(df["Value"] > 1)

# Exibir o DataFrame filtrado
filtered_df.show()

# # Salvar o DataFrame filtrado em formato Parquet
# filtered_df.write.parquet("/tmp/spark_test_output")

# Fechar a sessão do Spark
spark.stop()
