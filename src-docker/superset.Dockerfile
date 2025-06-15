FROM apache/superset:latest-py310

USER root

# Criar pasta esperada por Superset para configurações personalizadas
ENV PYTHONPATH="/app/pythonpath"

RUN mkdir -p /app/pythonpath

# Instale ferramentas de compilação antes dos pacotes Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Agora instale os pacotes Python
RUN pip install --no-cache-dir \
    sqlalchemy-trino \
    Flask-SQLAlchemy \
    PyHive[hive] \
    thrift \
    thrift-sasl

# Cria o superset_config.py no local correto
COPY ./src-docker/build/superset/superset_config.py /app/pythonpath/superset_config.py

COPY /src-docker/build/superset/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


USER superset

# ENTRYPOINT ["/entrypoint.sh"]
