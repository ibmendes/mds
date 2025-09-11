import pandas as pd
from datetime import date, timedelta, datetime
from bcb import sgs

from pyspark.sql import DataFrame

def get_referencia(pg_conn, schema: str = "raw"):
    """
    Retorna a data de referência mais recente no banco de dados.
    pg_conn deve ser uma instância do seu conector (PostgresConnector / PostgresConnection)
    que exponha um método .connect(database) retornando uma conexão DB-API.
    """
    conn = None
    try:
        conn = pg_conn.connect("sgs_bacen")
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(data) FROM {schema}.cdi;")
        max_data = cur.fetchone()[0]  # pode ser date ou None
        cur.close()
    except Exception:
        # Se ocorrer erro, devolve None
        max_data = None
    finally:
        if conn:
            conn.close()

    # Se max_data for None, define data default para início da série
    if max_data is None:
        max_data = date(2016, 1, 1)

    return max_data

def get_cdi(start):
    """
    Busca dados históricos do CDI usando sgs.get.
    Data final = ontem
    12 = código do CDI diário no SGS.
    """
    hoje = date.today()
    primeiro_deste_mes = hoje.replace(day=1)
    ontem = hoje - timedelta(days=1)
    end = ontem.strftime("%Y-%m-%d")

    df = sgs.get(12, start=start, end=end)  # código 12 = CDI
    df = df.rename(columns={12: "cdi"})
    df = df.reset_index().rename(columns={"index": "data"})
    return df

def pandas_to_structured(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estrutura e tipa corretamente o DataFrame do IPCA (pandas).
    """

    # Converte todos os nomes de colunas para string
    df.columns = df.columns.astype(str)

    # Renomeia colunas
    df = df.rename(columns={
        "Date": "data",
        "12": "valor"   # agora é string
    })
    

    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["valor"] = df["valor"].astype(float)
    df["cod_sgs"] = 12
    df["dt_ingestao"] = datetime.now()
    df["nm_indice"] = "CDI"

    return df[["cod_sgs", "nm_indice", "data", "valor", "dt_ingestao"]]

def get_final_cdi(spark, pg_conn, schema="raw") -> DataFrame:
    """
    Função final chamada no main.py.
    Retorna o DataFrame Spark com novos registros do CDI prontos para escrita.
    """
    max_data = get_referencia(pg_conn, schema=schema)
    df_cdi = get_cdi(max_data)
    df_structured = pandas_to_structured(df_cdi)

    df_cdi_spark = spark.createDataFrame(df_structured)
    df_cdi_spark = df_cdi_spark.filter(df_cdi_spark["data"] > max_data)
    df_cdi_spark = df_cdi_spark.orderBy(df_cdi_spark["data"].desc())

    return df_cdi_spark