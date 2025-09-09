# sgs_ipca.py

import pandas as pd
from datetime import date, timedelta
from bcb import sgs  # precisa instalar: pip install bcb

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from datetime import * 

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from connect.postgres import PostgresConnection

import findspark
findspark.init()  # inicializa o Spark no Python


# --- Inicializa Spark ---
spark = SparkSession.builder.appName("IPCA_SGS").master("local[*]").getOrCreate()


def get_referencia():
    """Retorna a data de referência mais recente no banco de dados"""
    pg = PostgresConnection()
    conn = pg.connect("sgs_bacen")
    cur = conn.cursor()
    cur.execute("SELECT MAX(data) FROM ipca;")
    max_data = cur.fetchone()[0]  # pega o valor do tuple
    print("Maior data:", max_data)

    cur.close()
    conn.close()

    return max_data

def get_ipca(start):
    """
    Busca dados históricos do IPCA usando sgs.get.
    Data final = último dia do mês passado.
    433 = código do IPCA no SGS.
    """
    hoje = date.today()
    primeiro_deste_mes = hoje.replace(day=1)
    ultimo_mes = primeiro_deste_mes - timedelta(days=1)
    end = ultimo_mes.strftime("%Y-%m-%d")

    df = sgs.get(433, start=start, end=end)
    df = df.rename(columns={433: "ipca"})
    df = df.reset_index().rename(columns={"index": "data"})
    return df

def pandas_to_structured(df):
    # Converte todos os nomes de colunas para string
    df.columns = df.columns.astype(str)

    # Renomeia colunas
    df = df.rename(columns={
        "Date": "data",
        "433": "valor"   # agora é string
    })

    # Converte tipos
    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["valor"] = df["valor"].astype(float)
    df["cod_sgs"] = 433
    df["dt_ingestao"] = datetime.now()

    # Mantém apenas as colunas desejadas
    df_structured = df[["cod_sgs", "data", "valor", "dt_ingestao"]]

    return df_structured


def insert_postgres (df_ipca_spark, database="sgs_bacen"): 
    pg_conn = PostgresConnection()
    jdbc_url = pg_conn.jdbc_url(database)

    df_ipca_spark.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "ipca") \
        .option("user", pg_conn.user) \
        .option("password", pg_conn.password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
def run_job():
    """Função principal para coletar, processar e salvar os dados do IPCA."""
    max_data = get_referencia()

    df_ipca = get_ipca(max_data)

    df_structured = pandas_to_structured(df_ipca)

    df_ipca_spark = spark.createDataFrame(df_structured)

    df_ipca_spark = df_ipca_spark.filter(col("data") > lit(max_data))  # filtra só os novos

    df_ipca_spark = df_ipca_spark.orderBy(col("data").desc())
    
    if df_ipca_spark.count() > 0:
        insert_postgres(df_ipca_spark)
        
    spark.stop()