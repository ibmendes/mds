import pandas as pd
from datetime import date, timedelta, datetime
from bcb import sgs

from pyspark.sql import DataFrame

from datetime import date

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
        cur.execute(f"SELECT MAX(data) FROM {schema}.ipca;")
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
        max_data = date(1991, 1, 1)

    return max_data


def get_ipca(start):
    """
    Busca dados históricos do IPCA via SGS.
    433 = código do IPCA no SGS.
    """
    hoje = date.today()
    primeiro_deste_mes = hoje.replace(day=1)
    ultimo_mes = primeiro_deste_mes - timedelta(days=1)
    end = ultimo_mes.strftime("%Y-%m-%d")

    df = sgs.get(433, start=start, end=end)
    df = df.rename(columns={433: "valor"})
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
        "433": "valor"   # agora é string
    })

    df["data"] = pd.to_datetime(df["data"]).dt.date
    df["valor"] = df["valor"].astype(float)
    df["cod_sgs"] = 433
    df["dt_ingestao"] = datetime.now()
    df["nm_indice"] = "IPCA"

    return df[["cod_sgs", "nm_indice", "data", "valor", "dt_ingestao"]]


def get_final_ipca(spark, pg_conn, schema="raw") -> DataFrame:
    """
    Função final chamada no main.py.
    Retorna o DataFrame Spark com novos registros do IPCA prontos para escrita.
    """
    max_data = get_referencia(pg_conn, schema=schema)
    df_ipca = get_ipca(max_data)
    df_structured = pandas_to_structured(df_ipca)

    df_ipca_spark = spark.createDataFrame(df_structured)
    df_ipca_spark = df_ipca_spark.filter(df_ipca_spark["data"] > max_data)
    df_ipca_spark = df_ipca_spark.orderBy(df_ipca_spark["data"].desc())

    return df_ipca_spark