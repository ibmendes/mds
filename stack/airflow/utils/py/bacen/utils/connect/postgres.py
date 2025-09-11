import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class PostgresConnection:
    def __init__(self, host=None, port=None, user=None, password=None):
        """
        Inicializa a conexão com o PostgreSQL.
        Valores padrão vêm do Docker/env, mas podem ser sobrescritos.
        """
        self.host = host or os.getenv("POSTGRES_HOST", "postgres")
        self.port = port or int(os.getenv("POSTGRES_PORT", 5432))
        self.user = user or os.getenv("POSTGRES_USER", "hiveuser")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "hivepassword")

    def connect(self, database="postgres"):
        """Conecta ao PostgreSQL; tenta localhost se falhar no host principal."""
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
            self.host = "localhost"  # atualiza o host
            return psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database
            )
        
    def create_database_if_not_exists(self, dbname="sgs_bacen"):
        """Cria o banco se não existir e define owner."""
        conn = self.connect("postgres")
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

    def create_table(self, create_table_query, table_name=None, database="sgs_bacen", owner=None):
        """
        Cria uma tabela a partir de uma query SQL.
        Define owner se table_name for passado.
        """
        owner = owner or self.user
        conn = self.connect(database)
        cur = conn.cursor()

        cur.execute(create_table_query)
        if table_name:
            cur.execute(f'ALTER TABLE {table_name} OWNER TO "{owner}";')

        conn.commit()
        cur.close()
        conn.close()
        print(f"[OK] Tabela {table_name or 'desconhecida'} criada ou já existe no banco {database}, owner={owner}")

    def jdbc_url(self, database="sgs_bacen", schema="public"):
        """Retorna URL JDBC compatível com Spark."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{database}?currentSchema={schema}"
