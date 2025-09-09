#!/usr/bin/env python
# coding: utf-8

# # PAXPE - Ingestão de dados para banco de dados SQL usando Airflow
# 
# ## Visão geral
# 
# Este notebook demonstra como agendar um script Python para ingerir dados em um Banco de Dados SQL do Postgres, orquestrado pelo airflow.
# 
# # Documentação do Processo de Criação de Tabelas com Dados do Yahoo Finance
# 
# ## Objetivo
# O objetivo deste processo é obter dados financeiros, de mercado, dividendos, valuation e informações gerais de empresas listadas na bolsa, utilizando a API do Yahoo Finance. Os dados são coletados para um ou mais tickers e organizados em DataFrames utilizando PySpark para garantir performance e escalabilidade, especialmente ao lidar com uma grande quantidade de tickers.
# 
# 1. **Coleta de Dados**: 
#    - Para cada ticker fornecido, as informações relevantes foram extraídas da API do Yahoo Finance utilizando a biblioteca `yfinance`. Os dados foram organizados em dicionários para posterior conversão em DataFrames.
# 
# 2. **Criação dos DataFrames**:
#    - **Tabela Geral** (`df_geral`): Contém informações gerais da empresa, como setor, indústria, número de empregados, localização e resumo das atividades.
#    - **Tabela Financeira** (`df_financeira`): Contém dados financeiros da empresa, como capitalização de mercado, receita, lucro líquido, EBITDA, dívida total, entre outros.
#    - **Tabela de Mercado** (`df_mercado`): Inclui dados relacionados ao mercado, como preço atual, preço de abertura, volume de negociação, beta, entre outros.
#    - **Tabela de Dividendos** (`df_dividendos`): Contém informações sobre dividendos, incluindo taxa de dividendos, data ex-dividendo e índice de distribuição.
#    - **Tabela de Valuation** (`df_valuation`): Inclui dados de valuation da empresa, como índices P/E (Price to Earnings), P/B (Price to Book) e PEG (Price/Earnings to Growth).
#    - **Tabela de Retorno Mensal** (`df_retorno_mensal`): Retorno mensal da ação com base em preço da ação, dividendos e percentual
# 
# ## Considerações Finais
# Este processo permite a coleta eficiente e escalável de dados financeiros de várias empresas, facilitando análises complexas em grandes volumes de dados. O uso do PySpark garante que mesmo listas extensas de tickers possam ser processadas rapidamente, gerando tabelas estruturadas e prontas para análise.
# 
# 
# # Changelog
# 
# | Responsável | Data       | Change Log                                                                                      |
# |-------------|------------|--------------------------------------------------------------------------------------------------|
# | IGOR MENDES | 10-08-24 | Criação do script em spark                   |
# | IGOR MENDES | 28-08-24 | Criação da logica de upsert com o postgresSQL                |
# | IGOR MENDES | 20-10-24 | Sprint 3 - adicionando indices a um dataframe               |

# In[11]:


#fontes - yahoo finance api
get_ipython().system('pip install yahoofinance')
get_ipython().system('pip install yahooquery')
get_ipython().system('pip install --upgrade yfinance')
get_ipython().system('pip install psycopg2-binary')
get_ipython().system('pip install yfinance')


# In[12]:


import findspark
findspark.init()  # Inicializa o Spark
findspark.find()  # Verifica se o Spark está corretamente configurado

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("PAXPE")
    .config("spark.sql.session.timeZone", "America/Sao_Paulo")  # Define o fuso horário para São Paulo
    .config("spark.driver.memory", "16g")  # Memória do driver
    .config("spark.executor.memory", "12g")  # Memória para cada executor (ajuste conforme a carga)
    .config("spark.executor.cores", "8")  # Núcleos por executor
    .config("spark.cores.max", "24")  # Total de núcleos disponíveis
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", "2")
    .config("spark.dynamicAllocation.maxExecutors", "10")
    .config("spark.dynamicAllocation.initialExecutors", "4")
    .config("spark.default.parallelism", "24")  # Nível de paralelismo
    .config("spark.memory.fraction", "0.8")  # Memória usada para armazenamento e execução
    .config("spark.memory.storageFraction", "0.5")  # Memória usada para armazenamento
    # .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.4.jar")
    .getOrCreate()
)


spark


# In[13]:


get_ipython().run_line_magic('pip', 'install yfinance')

import pandas as pd

import yfinance as yf
from yfinance import Ticker
#api yahoo
from yahooquery import Screener, Ticker


#criar timestamps e automatizar a safra de tempo da análise
# apagar depois que tiver usando a api do spark sql
from datetime import datetime, timedelta

from pyspark.sql.functions import col, lit, when, lag, current_timestamp , date_format, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, DateType, DoubleType,IntegerType
from pyspark.sql.window import Window


import psycopg2
from psycopg2 import OperationalError
import sys


# In[14]:


def obter_empresas_ativas():
    screener = Screener()
    dados = screener.get_screeners('most_actives', count=200)
    empresas = dados['most_actives']['quotes']
    
    # Selecionar apenas as colunas desejadas para evitar problemas de inferência de tipo
    colunas = [
        'symbol', 'shortName', 'displayName', 'regularMarketPrice', 'regularMarketChange', 
        'regularMarketChangePercent', 'regularMarketVolume', 'marketCap', 
        'fullExchangeName', 'quoteSourceName'
    ]
    empresas_filtradas = [
        {k: v for k, v in empresa.items() if k in colunas}
        for empresa in empresas
    ]
    
    df = spark.createDataFrame(empresas_filtradas)
    
    # Renomear colunas para português
    df = df.withColumnRenamed('symbol', 'ticker') \
           .withColumnRenamed('shortName', 'nome_curto') \
           .withColumnRenamed('displayName', 'nome_exibicao') \
           .withColumnRenamed('regularMarketPrice', 'preco_mercado_regular') \
           .withColumnRenamed('regularMarketChange', 'mudanca_mercado_regular') \
           .withColumnRenamed('regularMarketChangePercent', 'mudanca_percentual_mercado_regular') \
           .withColumnRenamed('regularMarketVolume', 'volume_mercado_regular') \
           .withColumnRenamed('marketCap', 'capitalizacao_mercado') \
           .withColumnRenamed('fullExchangeName', 'nome_exchange_completa') \
           .withColumnRenamed('quoteSourceName', 'nome_fonte_cotacao')
    
    # Adicionar coluna com data e hora atual
    df = df.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
    df = df.withColumn('dthr_igtao', current_timestamp())
    
    # Garantir que 'ticker' não tenha valores nulos
    df = df.withColumn('ticker', col('ticker').cast('string'))
    df = df.dropna(subset=['ticker'])

    # Ordenar por capitalizacao_mercado
    df = df.orderBy(col('capitalizacao_mercado').desc())
    
    return df


# In[15]:


def obter_dados_historicos(symbols, start_date, end_date):
    dados = {}
    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        historico = ticker.history(start=start_date, end=end_date, interval='1mo')
        dados[symbol] = historico
    
    return dados


# In[16]:


from concurrent.futures import ThreadPoolExecutor, as_completed

def obter_dados_historicos_ticker(symbol, start_date, end_date):
    ticker = yf.Ticker(symbol)
    historico = ticker.history(start=start_date, end=end_date, interval='1mo')
    return (symbol, historico)

def obter_dados_historicos(symbols, start_date, end_date):
    dados = {}
    
    # Usar ThreadPoolExecutor para executar as solicitações em paralelo
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit tarefas para o executor
        futuros = [executor.submit(obter_dados_historicos_ticker, symbol, start_date, end_date) for symbol in symbols]
        
        # Coletar os resultados conforme as tarefas são concluídas
        for futuro in as_completed(futuros):
            symbol, historico = futuro.result()
            dados[symbol] = historico
    
    return dados


# In[17]:


def retorno_mensal(dados):
    # Inicializar uma lista vazia para armazenar dados estruturados
    dados_estruturados = []
    
    # Iterar sobre os dados históricos de cada símbolo
    for symbol, df in dados.items():
        # Converter DataFrame do Pandas para PySpark
        df['symbol'] = symbol

        df_spark = spark.createDataFrame(df.reset_index())
        
        
        # Renomear colunas para português
        df_spark = df_spark.withColumnRenamed('symbol', 'ticker') \
                        .withColumnRenamed('Date', 'data') \
                        .withColumnRenamed('Open', 'abertura') \
                        .withColumnRenamed('High', 'alta') \
                        .withColumnRenamed('Low', 'baixa') \
                        .withColumnRenamed('Close', 'fechamento') \
                        .withColumnRenamed('Volume', 'volume') \
                        .withColumnRenamed('Dividends', 'dividendos') \
                        .withColumnRenamed('Stock Splits', 'desdobramentos')

        janela = Window.partitionBy('ticker').orderBy('Data')


        # Calcular preço de fechamento do mês anterior (deslocar uma linha para cima)
        df_spark = df_spark.withColumn('fechamento_mes_anterior', lag('fechamento').over(janela))

        # Calcular Retorno em valor (diferença absoluta)
        df_spark = df_spark.withColumn('valor_retorno', col('fechamento') - col('fechamento_mes_anterior'))

        # Calcular Retorno em porcentagem
        df_spark = df_spark.withColumn('porcentagem_retorno', (col('valor_retorno') / col('fechamento_mes_anterior')) * 100)

        # Adicionar coluna com data atual no formato desejado
        df_spark = df_spark.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
        df_spark = df_spark.withColumn('dthr_igtao', current_timestamp())

        # Reordenar colunas
        df_spark = df_spark.select(
            'ticker',               # 'symbol' traduzido para 'ticker'
            'data',                 # 'date' traduzido para 'Data'
            'abertura',             # 'open' traduzido para 'Abertura'
            'alta',                 # 'high' traduzido para 'Alta'
            'baixa',                # 'low' traduzido para 'Baixa'
            'fechamento',           # 'close' traduzido para 'Fechamento'
            'volume',               # 'volume' mantido como 'Volume'
            'dividendos',           # 'dividends' traduzido para 'Dividendos'
            'desdobramentos',       # 'splits' traduzido para 'Desdobramentos'
            'fechamento_mes_anterior', # 'Close_Last_Month' traduzido para 'Fechamento_Mes_Anterior'
            'valor_retorno',        # 'Return_Value' traduzido para 'Valor_Retorno'
            'porcentagem_retorno',  # 'Return_Percentage' traduzido para 'Porcentagem_Retorno'
            'dt_ptcao',             # 'dt_ptcao' mantido como está
            'dthr_igtao'            # 'DTHR_IGTAO' mantido como está
        )
        
        # Adicionar o DataFrame à lista de dados estruturados
        dados_estruturados.append(df_spark)

    # Unir todos os DataFrames em um único DataFrame
    df_final = dados_estruturados[0]
    for df in dados_estruturados[1:]:
        df_final = df_final.union(df)
    
    return df_final


# # Tabela fato -  maiores empresas segundo a api do yahoo finance

# In[18]:


df_ativas = obter_empresas_ativas()

df_ativas.show()
df_ativas.printSchema()


# # Dimensão - Retornos mensais 10 anos

# In[19]:


# 10 anos passados
start_date = datetime.today() - timedelta(days=10*365)

# hoje
end_date = datetime.today()

# 'YYYY-MM-DD'
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

print(f"start_date: {start_date_str}")
print(f"end_date: {end_date_str}")


# In[20]:


# Selecionar a coluna 'symbol' e coletar os valores como uma lista
#100 maiores para dimensão das 100 maiores e retornos
symbol_list = df_ativas.select('ticker').rdd.flatMap(lambda x: x).collect()

# Converter a lista para uma tupla
df_tickers = tuple(symbol_list)

print(df_tickers)


# In[ ]:


historical_data = obter_dados_historicos(df_tickers, start_date_str, end_date_str)
df_retorno_mensal = retorno_mensal(historical_data)

# Mostrar o DataFrame final
df_retorno_mensal.show()


# # dimensões gerais 
# 
# 2. **Criação dos DataFrames**:
#    - **Tabela Geral** (`df_financeira`): Contém informações gerais da empresa, como setor, indústria, número de empregados, localização e resumo das atividades.
#    - **Tabela Financeira** (`df_financeira`): Contém dados financeiros da empresa, como capitalização de mercado, receita, lucro líquido, EBITDA, dívida total, entre outros.
#    - **Tabela de Mercado** (`df_mercado`): Inclui dados relacionados ao mercado, como preço atual, preço de abertura, volume de negociação, beta, entre outros.
#    - **Tabela de Dividendos** (`df_dividendos`): Contém informações sobre dividendos, incluindo taxa de dividendos, data ex-dividendo e índice de distribuição.
#    - **Tabela de Valuation** (`df_valuation`): Inclui dados de valuation da empresa, como índices P/E (Price to Earnings), P/B (Price to Book) e PEG (Price/Earnings to Growth).
#    - **Tabela de Retorno Mensal** (`df_retorno_mensal`): Retorno mensal da ação com base em preço da ação, dividendos e percentual

# In[ ]:


from pyspark.sql.functions import from_unixtime, col

def criar_tabelas_spark(tickers):
    if isinstance(tickers, str):
        tickers = (tickers,)
    
    # Esquema para a tabela geral
    schema_geral = StructType([
        StructField('ticker', StringType(), False),
        StructField('setor', StringType(), True),
        StructField('industria', StringType(), True),
        StructField('funcionarios', IntegerType(), True),
        StructField('cidade', StringType(), True),
        StructField('estado', StringType(), True),
        StructField('pais', StringType(), True),
        StructField('website', StringType(), True),
        StructField('resumo_negocios', StringType(), True),
        StructField('exchange', StringType(), True)
    ])
    
    # Esquema para a tabela financeira
    schema_financeira = StructType([
        StructField('ticker', StringType(), False),
        StructField('capitalizacao_mercado', LongType(), True),
        StructField('valor_empresa', LongType(), True),
        StructField('receita', LongType(), True),
        StructField('lucros_brutos', LongType(), True),
        StructField('lucro_liquido', LongType(), True),
        StructField('ebitda', LongType(), True),
        StructField('divida_total', LongType(), True),
        StructField('caixa_total', LongType(), True),
        StructField('dividend_yield', DoubleType(), True)
    ])
    
    # Esquema para a tabela de mercado
    schema_mercado = StructType([
        StructField('ticker', StringType(), False),
        StructField('preco_atual', DoubleType(), True),
        StructField('fechamento_anterior', DoubleType(), True),
        StructField('abertura', DoubleType(), True),
        StructField('minimo_dia', DoubleType(), True),
        StructField('maximo_dia', DoubleType(), True),
        StructField('minimo_52_semanas', DoubleType(), True),
        StructField('maximo_52_semanas', DoubleType(), True),
        StructField('volume', LongType(), True),
        StructField('volume_medio', LongType(), True),
        StructField('beta', DoubleType(), True)
    ])
    
    # Esquema para a tabela de dividendos
    schema_dividendos = StructType([
        StructField('ticker', StringType(), False),
        StructField('taxa_dividendo', DoubleType(), True),
        StructField('data_exdividendo', StringType(), True),  # Temporariamente como StringType
        StructField('indice_distribuicao', DoubleType(), True)
    ])
    
    # Esquema para a tabela de valuation
    schema_valuation = StructType([
        StructField('ticker', StringType(), False),
        StructField('pl_futuro', DoubleType(), True),
        StructField('pl_retroativo', DoubleType(), True),
        StructField('preco_booking', DoubleType(), True),
        StructField('indice_preco_lucro_cresc', DoubleType(), True)
    ])
    
    # Inicializa as listas de dicionários para cada tabela
    geral = []
    financeira = []
    mercado = []
    dividendos = []
    valuation = []
    
    for ticker in tickers:
        try:
            empresa = yf.Ticker(ticker)
            info = empresa.info
            
            # Filtra e trata valores infinitos
            def safe_get(key, default=None):
                value = info.get(key)
                if isinstance(value, str) and value in ('Infinity', '-Infinity'):
                    return default
                return value
            
            # Preencher dados da tabela geral
            geral.append({
                'ticker': ticker,
                'setor': info.get('sector'),
                'industria': info.get('industry'),
                'funcionarios': info.get('fullTimeEmployees'),
                'cidade': info.get('city'),
                'estado': info.get('state'),
                'pais': info.get('country'),
                'website': info.get('website'),
                'resumo_negocios': info.get('longBusinessSummary'),
                'exchange': info.get('exchange')
            })
            
            # Preencher dados da tabela financeira
            financeira.append({
                'ticker': ticker,
                'capitalizacao_mercado': safe_get('marketCap', 0),
                'valor_empresa': safe_get('enterpriseValue', 0),
                'receita': safe_get('revenue', 0),
                'lucros_brutos': safe_get('grossProfits', 0),
                'lucro_liquido': safe_get('netIncome', 0),
                'ebitda': safe_get('ebitda', 0),
                'divida_total': safe_get('totalDebt', 0),
                'caixa_total': safe_get('totalCash', 0),
                'dividend_yield': safe_get('dividendYield', 0.0)
            })
            
            # Preencher dados da tabela de mercado
            mercado.append({
                'ticker': ticker,
                'preco_atual': safe_get('currentPrice', 0.0),
                'fechamento_anterior': safe_get('previousClose', 0.0),
                'abertura': safe_get('open', 0.0),
                'minimo_dia': safe_get('dayLow', 0.0),
                'maximo_dia': safe_get('dayHigh', 0.0),
                'minimo_52_semanas': safe_get('fiftyTwoWeekLow', 0.0),
                'maximo_52_semanas': safe_get('fiftyTwoWeekHigh', 0.0),
                'volume': safe_get('volume', 0),
                'volume_medio': safe_get('averageVolume', 0),
                'beta': safe_get('beta', 0.0)
            })
            
            # Preencher dados da tabela de dividendos
            dividendos.append({
                'ticker': ticker,
                'taxa_dividendo': safe_get('dividendRate', 0.0),
                'data_exdividendo': safe_get('exDividendDate'),  # Unix timestamp
                'indice_distribuicao': safe_get('payoutRatio', 0.0)
            })
            
            # Preencher dados da tabela de valuation
            valuation.append({
                'ticker': ticker,
                'pl_futuro': safe_get('forwardPE', 0.0),
                'pl_retroativo': safe_get('trailingPE', 0.0),
                'preco_booking': safe_get('priceToBook', 0.0),
                'indice_preco_lucro_cresc': safe_get('pegRatio', 0.0)
            })
            
        except Exception as e:
            print(f"Erro ao processar o ticker {ticker}: {e}")
    
    # Criar DataFrames Spark com esquema definido
    df_geral = spark.createDataFrame(geral, schema=schema_geral)
    df_financeira = spark.createDataFrame(financeira, schema=schema_financeira)
    df_mercado = spark.createDataFrame(mercado, schema=schema_mercado)
    df_dividendos = spark.createDataFrame(dividendos, schema=schema_dividendos)
    df_valuation = spark.createDataFrame(valuation, schema=schema_valuation)

    # Converter Unix timestamp para data legível
    df_dividendos = df_dividendos.withColumn('data_exdividendo', from_unixtime(col('data_exdividendo').cast('bigint')))
    
    # Adicionar colunas de timestamp
    df_geral = df_geral.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
    df_geral = df_geral.withColumn('dthr_igtao', current_timestamp())

    df_financeira = df_financeira.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
    df_financeira = df_financeira.withColumn('dthr_igtao', current_timestamp())

    df_mercado = df_mercado.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
    df_mercado = df_mercado.withColumn('dthr_igtao', current_timestamp())

    df_dividendos = df_dividendos.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
    df_dividendos = df_dividendos.withColumn('dthr_igtao', current_timestamp())

    df_valuation = df_valuation.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
    df_valuation = df_valuation.withColumn('dthr_igtao', current_timestamp())
    
    return df_geral, df_financeira, df_mercado, df_dividendos, df_valuation


# In[ ]:


df_geral, df_financeira, df_mercado, df_dividendos, df_valuation = criar_tabelas_spark(df_tickers)
# Exibir os DataFrames
df_geral.show()
df_financeira.show()
df_mercado.show()
df_dividendos.show()
df_valuation.show()


# In[ ]:


df_retorno_mensal.printSchema()
df_ativas.printSchema()
df_geral.printSchema()
df_financeira.printSchema()
df_mercado.printSchema()
df_dividendos.printSchema()
df_valuation.printSchema()


# # Sprint 3 - Indices

# In[ ]:


indices = {
    '^BVSP': 'Ibovespa',
    'BOVA11.SA': 'BOVA11',
    '^GSPC': 'S&P 500',
    '^DJI': 'Dow Jones',
    '^IXIC': 'NASDAQ',
    '^FTSE': 'FTSE 100',
    '^GDAXI': 'DAX 30',
    '^N225': 'Nikkei 225'
}

schema = StructType([
    StructField("indice", StringType(), True),
    StructField("abertura", DoubleType(), True),
    StructField("alta", DoubleType(), True),
    StructField("baixa", DoubleType(), True),
    StructField("fechamento", DoubleType(), True),
    StructField("fechamento_ajustado", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("nome_indice", StringType(), True),
    StructField("data", DateType(), True)
])

data_list = []

for ticker, name in indices.items():
    try:
        data = yf.download(ticker, start=start_date_str, end=end_date_str)
        if not data.empty:
            for index, row in data.iterrows():
                # Verifica se 'Adj Close' existe, senão usa 'Close'
                adj_close = row['Adj Close'] if 'Adj Close' in row else row['Close']
                try:
                    data_list.append((
                        ticker,
                        float(row['Open'].item()) if hasattr(row['Open'], 'item') else float(row['Open']),
                        float(row['High'].item()) if hasattr(row['High'], 'item') else float(row['High']),
                        float(row['Low'].item()) if hasattr(row['Low'], 'item') else float(row['Low']),
                        float(row['Close'].item()) if hasattr(row['Close'], 'item') else float(row['Close']),
                        float(adj_close.item()) if hasattr(adj_close, 'item') else float(adj_close),
                        int(row['Volume'].item()) if hasattr(row['Volume'], 'item') else int(row['Volume']),
                        name,
                        index.date()
                    ))
                except Exception as e:
                    print(f"Erro ao processar linha para {ticker}: {e}")
        else:
            print(f"Nenhum dado para {ticker}")
    except Exception as e:
        print(f"Falha ao baixar dados para {ticker}: {e}")

df_indices = spark.createDataFrame(data_list, schema)

janela = Window.partitionBy('indice').orderBy('data')
df_indices = df_indices.withColumn('fechamento_mes_anterior', lag('fechamento').over(janela))
df_indices = df_indices.withColumn('valor_retorno', col('fechamento') - col('fechamento_mes_anterior'))
df_indices = df_indices.withColumn('porcentagem_retorno', (col('valor_retorno') / col('fechamento_mes_anterior')) * 100)
df_indices = df_indices.withColumn('dt_ptcao', date_format(current_timestamp(), 'yyyy-MM-dd'))
df_indices = df_indices.withColumn('dthr_igtao', current_timestamp())
df_indices = df_indices.orderBy('indice', 'data')
df_indices.show()


# # Postgres upsert

# In[ ]:


# SRVNAME = "postgres"
# USER = "airflow"
# PASSWORD = "airflow"
# HOST = "postgres"
# PORT = "5432"
# DBNAME = "paxpedb"

# # Parâmetros de conexão usando as variáveis
# conn_params = {
#     'dbname': DBNAME,
#     'user': USER,
#     'password': PASSWORD,
#     'host': HOST,
#     'port': PORT
# }


# In[3]:


def test_connection():
    try:
        # Connect to the PostgreSQL server using variables
        connection = psycopg2.connect(
            dbname=SRVNAME,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        print("Conexão com postgres sucedida")
        return True
    except OperationalError as e:
        print(f"Error: {e}")
        return False
    finally:
        if connection:
            connection.close()
# if not test_connection():
#     sys.exit(1)


# ### para inserir no postgres é necessário
# > 1. alimentar as tabelas em stage --- temp_nometabela
# > 2. realizar o upsert comparando as tabelas

# In[ ]:


# def write_to_postgres(df, table_name, schema_name="paxpestg"):
  
#     df.write \
#         .format("jdbc") \
#         .option("url", f"jdbc:postgresql://{HOST}:{PORT}/{DBNAME}") \
#         .option("dbtable", f"{schema_name}.{table_name}") \
#         .option("user", USER) \
#         .option("password", PASSWORD) \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("overwrite") \
#         .save()


# In[ ]:


# # escrita das tabelas em stage
# write_to_postgres(df_retorno_mensal, "temp_retorno_mensal")
# write_to_postgres(df_ativas, "temp_captacao_mercado")
# write_to_postgres(df_geral, "temp_cadastro")
# write_to_postgres(df_financeira, "temp_financas")
# write_to_postgres(df_mercado, "temp_mercado")
# write_to_postgres(df_dividendos, "temp_dividendos")
# write_to_postgres(df_valuation, "temp_valuation")
# write_to_postgres(df_indices, "temp_indices")


# In[4]:


def upsert_data(table_name, temp_table_name, key_columns):
    try:
        # Conectar ao PostgreSQL
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()
        
        # Definir o fuso horário da sessão para UTC-3
        cursor.execute("SET TIME ZONE 'America/Sao_Paulo';")

        # Definir esquemas
        schema_fact = "paxpe"
        schema_stg = "paxpestg"
        
        # Obter todas as colunas da tabela
        cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema_fact}' AND table_name = '{table_name}'
        """)
        all_columns = [row[0] for row in cursor.fetchall()]

        # Identificar colunas não chave e colunas de referência
        non_key_columns = [col for col in all_columns if col not in key_columns and col not in ['dt_ptcao', 'dthr_igtao']]
        reference_columns = ['dthr_igtao']

        # Construir a declaração SQL de atualização col1, col2
        key_columns_str = ', '.join(key_columns)
        update_set_non_key = ', '.join([
            f"{col} = EXCLUDED.{col}" 
            for col in non_key_columns
        ])
        
        # Atualizar apenas as colunas não-chave se houver uma mudança
        where_condition = ' OR '.join([
            f"target.{col} IS DISTINCT FROM EXCLUDED.{col}" 
            for col in non_key_columns
        ])
        
        # Para atualizar as colunas de referência se houver uma atualização nas colunas não-chave
        update_set_reference = ', '.join([
            f"{col} = EXCLUDED.{col}" 
            for col in reference_columns
        ])
        
        sql = f"""
        INSERT INTO {schema_fact}.{table_name} 
        (SELECT * FROM {schema_stg}.{temp_table_name})
        ON CONFLICT ({key_columns_str}) 
        DO UPDATE SET
        {update_set_non_key},
        {update_set_reference}
        WHERE EXISTS (
            SELECT 1
            FROM {schema_fact}.{table_name} AS target
            WHERE { ' AND '.join([f"target.{key} = EXCLUDED.{key}" for key in key_columns]) }
            AND ({where_condition})
        );
        """
        
        # Executar a declaração SQL de upsert
        cursor.execute(sql)
        connection.commit()
        print(f"Upsert de {schema_stg}.{temp_table_name} realizado para {schema_fact}.{table_name}.")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if connection:
            cursor.close()
            connection.close()


# In[ ]:


# # colunas chave para cada tabela
# key_columns_retorno_mensal = ["ticker", "data"]
# key_columns_cadastro = ["ticker"]
# key_columns_cap_mercado = ["ticker"]
# key_columns_financas = ["ticker"]
# key_columns_mercado = ["ticker"]
# key_columns_dividendos = ["ticker","data_exdividendo"]
# key_columns_valuation = ["ticker"]
# key_columns_indices = ["indice","data"]


# In[ ]:


# # realiza o upsert
# upsert_data("retorno_mensal", "temp_retorno_mensal", key_columns_retorno_mensal)
# upsert_data("cadastro", "temp_cadastro", key_columns_cadastro)
# upsert_data("captacao_mercado", "temp_captacao_mercado", key_columns_cap_mercado)
# upsert_data("financas", "temp_financas", key_columns_financas)
# upsert_data("mercado", "temp_mercado", key_columns_mercado)
# upsert_data("dividendos", "temp_dividendos", key_columns_dividendos)
# upsert_data("valuation", "temp_valuation", key_columns_valuation)
# upsert_data("indices", "temp_indices", key_columns_indices)

