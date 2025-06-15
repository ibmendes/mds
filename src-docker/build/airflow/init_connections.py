#!/usr/bin/env python

from airflow.models import Connection
from airflow import settings
from airflow.exceptions import AirflowException

def create_connection_if_not_exists(conn_id, conn_type, host=None, login=None,
                                    password=None, schema=None, port=None,
                                    extra=None):
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing_conn:
        print(f"[INFO] Conexão '{conn_id}' já existe. Nada será feito.")
        return

    try:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            schema=schema,
            port=port,
            extra=extra
        )
        session.add(new_conn)
        session.commit()
        print(f"[OK] Conexão '{conn_id}' criada com sucesso.")
    except Exception as e:
        session.rollback()
        raise AirflowException(f"[ERRO] Falha ao criar conexão '{conn_id}': {e}")
    finally:
        session.close()

# === Definições das conexões que deseja criar ===

connections = [
    {
        "conn_id": "postgres_default",
        "conn_type": "postgres",
        "host": "postgres",
        "schema": "metastore",
        "login": "hiveuser",
        "password": "hivepassword",
        "port": 5432
    },
    {
        "conn_id": "postgres_airflow",
        "conn_type": "postgres",
        "host": "postgres",
        "schema": "airflow",
        "login": "airflow",
        "password": "airflow",
        "port": 5432
    },
    {
        "conn_id": "spark_default",
        "conn_type": "spark",
        "host": "spark://jupyter-spark",
        "port": 7077
    },
]

# === Criação em lote ===
for conn_cfg in connections:
    create_connection_if_not_exists(**conn_cfg)

