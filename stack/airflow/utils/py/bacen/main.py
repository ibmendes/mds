from utils.connect import *
from utils.sgs import *

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from utils.connect import *

import logging 

class Main:
    """
    Classe central para inicializar Spark e fornecer JDBC URL para Postgres.
    """

    def __init__(self, app_name="BacenETL", host="postgres", port=5432, database="sgs_bacen"):
        """
        Inicializa Spark com configuração local otimizada e conexão Postgres.
        """
        # Configura logger dentro da classe
        self.BacenMainLogger = logging.getLogger("BacenMainLogger")
        self.BacenMainLogger.setLevel(logging.INFO)

        if not self.BacenMainLogger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s [%(name)s] [%(levelname)s] - %(message)s"
            )
            handler.setFormatter(formatter)
            self.BacenMainLogger.addHandler(handler)

        self.BacenMainLogger.info("Inicializando sessão Spark...")

        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("spark://jupyter-spark:7077") \
            .config(conf=SparkConf()
                    .set("spark.sql.shuffle.partitions", "4")
                    .set("spark.driver.memory", "2g")
                    .set("spark.executor.memory", "2g")
                    .set("spark.ui.port", "4040")
                    .set("spark.ui.enabled", "true")
                    .set("spark.sql.adaptive.enabled", "true")
                    ).enableHiveSupport().getOrCreate()

        self.BacenMainLogger.info("Sessão Spark inicializada ✅")
        # Conector Postgres
        self.connector = PostgresConnection(host=host, port=port)
        self.database = database

    def jdbc_url(self, schema=None):
        schema = schema or "public"
        return self.connector.jdbc_url(database=self.database, schema=schema)

    def write_df(self, df, table_name, schema=None, mode="overwrite", user="postgres", password="postgres"):
        schema = schema or "public"
        self.BacenMainLogger.info(f"Escrevendo DataFrame em {schema}.{table_name}...")

        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url(schema=schema)) \
            .option("dbtable", f"{schema}.{table_name}") \
            .option("user", self.connector.user) \
            .option("password", self.connector.password) \
            .mode(mode) \
            .save()

        self.BacenMainLogger.info(f"DataFrame salvo em {schema}.{table_name} ✅")
    
    def run(self):
        """
        Executa o pipeline de ETL.
        cada script em sua def final gera um df, que aqui é escrito no Postgres.
        """
        self.BacenMainLogger.info("Iniciando ipca.py")
        df_ipca = get_final_ipca(self.spark, self.connector, schema="raw")

        if df_ipca.count() > 0:
            self.write_df(df_ipca, table_name="ipca", schema="raw",mode="append")
        else:
            self.BacenMainLogger.warning("DataFrame IPCA vazio, nada foi escrito.")

        self.BacenMainLogger.info("Iniciando CDI.py")
        df_cdi = get_final_cdi(self.spark, self.connector, schema="raw")

        if df_cdi.count() > 0:
            self.write_df(df_cdi, table_name="cdi", schema="raw",mode="append")
        else:
            self.BacenMainLogger.warning("DataFrame CDI vazio, nada foi escrito.")
            

        self.BacenMainLogger.info("Pipeline ETL finalizado ✅")

if __name__ == "__main__":
    main = Main()
    main.BacenMainLogger.info("Iniciando pipeline...")
    main.run()
