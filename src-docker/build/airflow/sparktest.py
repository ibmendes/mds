from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Cria a sessão Spark
spark = SparkSession.builder \
    .appName("TesteSimplesSpark") \
    .getOrCreate()

# Dados de exemplo
dados = [
    ("Alice", 30),
    ("Bob", 25),
    ("Carol", 40),
    ("David", 22)
]

# Define as colunas
colunas = ["nome", "idade"]

# Cria o DataFrame
df = spark.createDataFrame(dados, colunas)

# Filtra pessoas com idade >= 30
df_filtrado = df.filter(col("idade") >= 30)

# Exibe os dados
print("=== Todos os dados ===")
df.show()

print("=== Pessoas com idade >= 30 ===")
df_filtrado.show()

# Encerra a sessão
spark.stop()
