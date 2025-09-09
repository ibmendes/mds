from .postgres import PostgresConnection

pg = PostgresConnection()

# crio o db se não existir
pg.create_database_if_not_exists("sgs_bacen")


# Queries de criação de tabelas
ipca_query = """
CREATE TABLE IF NOT EXISTS ipca (
    cod_sgs BIGINT NOT NULL,
    data DATE NOT NULL,
    valor DOUBLE PRECISION,
    dt_ingestao TIMESTAMP,
    PRIMARY KEY (cod_sgs, data)
);
"""

# creates if not exists
pg.create_table(ipca_query, table_name="ipca")
