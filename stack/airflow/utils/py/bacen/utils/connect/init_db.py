# query_n 
## cria as tabelas if not exists para a camada raw

from .postgres import PostgresConnection

pg = PostgresConnection()
conn = pg.connect("sgs_bacen")  # conecta no DB
cur = conn.cursor()

# schema
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

#### n queries para parses 

ipca = """
CREATE TABLE IF NOT EXISTS raw.ipca (
    cod_sgs BIGINT NOT NULL,
    nm_indice VARCHAR(50) NOT NULL,
    data DATE NOT NULL,
    valor DOUBLE PRECISION,
    dt_ingestao TIMESTAMP,
    PRIMARY KEY (cod_sgs, data)
);
"""

cdi = """
CREATE TABLE IF NOT EXISTS raw.cdi (
    cod_sgs BIGINT NOT NULL,
    nm_indice VARCHAR(50) NOT NULL,
    data DATE NOT NULL,
    valor DOUBLE PRECISION,
    dt_ingestao TIMESTAMP,
    PRIMARY KEY (cod_sgs, data)
);
"""

# cria a tabela se não existir
pg.create_table(ipca, table_name="ipca")
pg.create_table(cdi, table_name="cdi")

