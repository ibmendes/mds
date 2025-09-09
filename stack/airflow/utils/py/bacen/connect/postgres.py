# connection/postgres.py

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PostgresConnection:
    def __init__(self):
        # valores principais (pode vir do docker-compose/env)
        self.host = os.getenv("POSTGRES_HOST", "postgres")  # tenta primeiro 'postgres' (docker)
        self.port = int(os.getenv("POSTGRES_PORT", 5432))
        self.user = os.getenv("POSTGRES_USER", "hiveuser")
        self.password = os.getenv("POSTGRES_PASSWORD", "hivepassword")

    def connect(self, database="postgres"):
        """
        Conecta ao PostgreSQL.
        Se não conseguir no host definido, tenta localhost.
        """
        try:
            return psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database
            )
        except Exception as e:
            print(f"[WARN] Falha ao conectar em {self.host}:{self.port} → {e}")
            print("[INFO] Tentando conectar via localhost...")

            return psycopg2.connect(
                host="localhost",
                port=self.port,
                user=self.user,
                password=self.password,
                database=database
            )

    def create_database_if_not_exists(self, dbname="sgs_bacen"):
        """Cria o banco se não existir e define owner"""
        conn = self.connect("postgres")  # conecta ao banco padrão
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f'CREATE DATABASE "{dbname}" OWNER "{self.user}";')
            print(f"[OK] Banco {dbname} criado com sucesso com owner {self.user}!")
        else:
            print(f"[INFO] Banco {dbname} já existe.")

        cur.close()
        conn.close()
        
    def jdbc_url(self, database="sgs_bacen"):
            # Tenta usar o host definido (docker), se não, usa localhost
            host = self.host
            try:
                import socket
                socket.gethostbyname(host)  # testa resolução
            except Exception:
                host = "localhost"
            return f"jdbc:postgresql://{host}:{self.port}/{database}"
    
    def create_table(self, create_table_query, table_name=None, database="sgs_bacen", owner=None):
        """
        Cria uma tabela a partir de uma query passada como argumento.
        
        Parâmetros:
        - create_table_query: string com a query SQL de criação (CREATE TABLE IF NOT EXISTS ...)
        - table_name: nome da tabela (opcional, usado apenas para alterar owner)
        - database: nome do banco
        - owner: usuário dono da tabela (opcional, default = self.user)
        """
        owner = owner or self.user

        conn = self.connect(database)
        cur = conn.cursor()

        # Executa a query de criação
        cur.execute(create_table_query)

        # Define o owner se o nome da tabela foi passado
        if table_name:
            cur.execute(f'ALTER TABLE {table_name} OWNER TO "{owner}";')

        conn.commit()
        cur.close()
        conn.close()
        print(f"[OK] Tabela {table_name or 'desconhecida'} criada ou já existe no banco {database}, owner={owner}")
