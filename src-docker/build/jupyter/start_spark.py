# start_spark.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Persistent Spark") \
    .master("spark://jupyter-spark:7077") \
    .getOrCreate()

# Mantém o contexto ativo
spark
