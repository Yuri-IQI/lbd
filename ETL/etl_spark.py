from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, udf, row_number
import os
from dotenv import load_dotenv
import requests
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

load_dotenv()

spark = SparkSession.builder \
    .appName("LBD") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.16") \
    .getOrCreate()

db_url = os.getenv("DB_URL")

db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Funções básicas para o ETL

def extract_from_principal(query):
    return spark.read \
        .format("jdbc") \
        .option("url", db_url + "/star_comex_principal") \
        .option("query", query) \
        .options(**db_properties) \
        .load()

def transform_text_to_uppercase(df, columns):
    for column in columns:
        df = df.withColumn(column, upper(col(column)))
    return df

def load_to_data_mart(df, table_name):
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", db_url + "/star_comex_data_mart") \
        .option("dbtable", table_name) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .save()

# ETL de produtos

products_query = """select p.id as id_produto, p.descricao, cp.descricao as ds_categoria, codigo_ncm from produtos p 
    inner join categoria_produtos cp on cp.id = p.categoria_id"""

# Extração das tabelas de produtos e categoria_produtos
products_extract = extract_from_principal(products_query)

# Transformação
products_transformed = transform_text_to_uppercase(products_extract, ["descricao", "codigo_ncm", "ds_categoria"])

# Adiciona surrogate key
window = Window.orderBy("id_produto")
products_with_sk = products_transformed.withColumn("sk_produto", row_number().over(window))

# Reorganiza colunas
dm_produtos = products_with_sk.select(
    "sk_produto", "id_produto", "descricao", "codigo_ncm", "ds_categoria"
)

# Carregamento
load_to_data_mart(dm_produtos, "dm_produtos")


# ETL de transportes

transports_query = "select id as id_transporte, descricao as ds_transporte from transportes"
transports_extract = extract_from_principal(transports_query)
transports_transformed = transform_text_to_uppercase(transports_extract, ["ds_transporte"])

window = Window.orderBy("id_transporte")
transports_with_sk = transports_transformed.withColumn("sk_transporte", row_number().over(window))

dm_transporte = transports_with_sk.select("sk_transporte", "id_transporte", "ds_transporte")
load_to_data_mart(dm_transporte, "dm_transporte")

# ETL de países

countries_query = """select p.id as id_pais, p.nome as pais, p.codigo_iso, b.nome as nm_bloco
  from paises p inner join blocos_economicos b on b.id = p.bloco_id"""
countries_extract = extract_from_principal(countries_query)
countries_transformed = transform_text_to_uppercase(countries_extract, ["pais", "codigo_iso", "nm_bloco"])

window = Window.orderBy("id_pais")
countries_with_sk = countries_transformed.withColumn("sk_pais", row_number().over(window))

dm_pais = countries_with_sk.select("sk_pais", "id_pais", "pais", "codigo_iso", "nm_bloco")
load_to_data_mart(dm_pais, "dm_pais")


# ETL de tempos

@udf(StringType())
def extract_time(date):
    if date is not None:
        date_str = str(date)
        if len(date_str) >= 10:
            year = date_str[0:4]
            month = date_str[5:7]
            day = date_str[8:10]
            print(f"Ano: {year}, Mês: {month}, Dia: {day}")
            do_time_etl(year, month, day)

            return f"{year}{month}{day}"
    return None

def do_time_etl(year, month, day):
    tima_data = [{}]

    spark.createDataFrame

# ETL de cambios

exchange_query = """
    select c.id as id_cambio, c.data,
           m1.descricao as ds_moeda_origem, m1.pais as pais_moeda_origem,
           m2.descricao as ds_destino, m2.pais as pais_moeda_destino,
           c.taxa_cambio
    from cambios c
    inner join moedas m1 on m1.id = c.moeda_origem
    inner join moedas m2 on m2.id = c.moeda_destino
"""

# Atualizar para pegar cambios por range. ex: https://api.frankfurter.dev/v1/2023-12-01..2023-12-10?amount=100&from=GBP&to=USD
@udf(StringType())
def update_exchange_rate(date, currency_from, currency_to):
    try:
        res = requests.get(
            f"https://api.frankfurter.dev/v1/{date}?from={currency_from}&to={currency_to}"
        ).json()
        return str(res["rates"][currency_to])
    except Exception as e:
        print(f"Erro na API: {e}")
        return None

exchange_extract = extract_from_principal(exchange_query)

exchange_transformed = transform_text_to_uppercase(
    exchange_extract,
    ["ds_moeda_origem", "pais_moeda_origem", "ds_destino", "pais_moeda_destino"]
)

exchange_transformed = exchange_transformed.withColumn("data", extract_time(col("data")))

exchange_transformed = exchange_transformed.withColumn(
    "taxa_cambio",
    update_exchange_rate(col("data"), col("ds_moeda_origem"), col("ds_destino"))
)

window = Window.orderBy("id_cambio")
exchange_with_sk = exchange_transformed.withColumn("sk_cambio", row_number().over(window))

dm_cambio = exchange_with_sk.select(
    "sk_cambio", "id_cambio", "data", "ds_moeda_origem", "pais_moeda_origem",
    "ds_destino", "pais_moeda_destino", "taxa_cambio"
)

load_to_data_mart(dm_cambio, "dm_cambios")

spark.stop()