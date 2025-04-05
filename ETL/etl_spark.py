from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    upper, col, udf, row_number,
    year, month, dayofmonth
)
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.window import Window
from dotenv import load_dotenv
import requests
import os
import pycountry

# Load environment variables
load_dotenv()
db_url = os.getenv("DB_URL")

# Spark session config
spark = SparkSession.builder \
    .appName("LBD Dimensional ETL") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.16") \
    .getOrCreate()

# DB connection properties
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ========== Helper Functions ==========

def extract_from_principal(query: str):
    return spark.read \
        .format("jdbc") \
        .option("url", f"{db_url}/star_comex_principal") \
        .option("query", query) \
        .options(**db_properties) \
        .load()

def transform_text_to_uppercase(df, columns: list):
    for column in columns:
        df = df.withColumn(column, upper(col(column)))
    return df

def load_to_data_mart(df, table_name: str):
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"{db_url}/star_comex_data_mart") \
        .option("dbtable", table_name) \
        .options(**db_properties) \
        .save()

def add_surrogate_key(df, key_column: str, sk_name: str = None):
    if sk_name is None:
        sk_name = f"sk_{key_column.replace('id_', '')}"
    window = Window.orderBy(key_column)
    return df.withColumn(sk_name, row_number().over(window))

def get_currency_from_country_code(country_code):
    country = pycountry.countries.get(alpha_3=country_code.upper())
    if country:
        currency = pycountry.currencies.get(numeric=country.numeric)
        return currency.alpha_3 if currency else None
    return None

# ========== ETLs ==========

def etl_products():
    query = """
        SELECT p.id AS id_produto, p.descricao, cp.descricao AS ds_categoria, codigo_ncm
        FROM produtos p
        INNER JOIN categoria_produtos cp ON cp.id = p.categoria_id
    """
    df = extract_from_principal(query)
    df = transform_text_to_uppercase(df, ["descricao", "codigo_ncm", "ds_categoria"])
    df = add_surrogate_key(df, "id_produto", "sk_produto")
    df = df.select("sk_produto", "id_produto", "descricao", "codigo_ncm", "ds_categoria")
    load_to_data_mart(df, "dm_produtos")

def etl_transports():
    query = "SELECT id AS id_transporte, descricao AS ds_transporte FROM transportes"
    df = extract_from_principal(query)
    df = transform_text_to_uppercase(df, ["ds_transporte"])
    df = add_surrogate_key(df, "id_transporte", "sk_transporte")
    df = df.select("sk_transporte", "id_transporte", "ds_transporte")
    load_to_data_mart(df, "dm_transporte")

def etl_countries():
    query = """
        SELECT p.id AS id_pais, p.nome AS pais, p.codigo_iso, b.nome AS nm_bloco
        FROM paises p
        INNER JOIN blocos_economicos b ON b.id = p.bloco_id
    """
    df = extract_from_principal(query)
    df = transform_text_to_uppercase(df, ["pais", "codigo_iso", "nm_bloco"])
    df = add_surrogate_key(df, "id_pais", "sk_pais")
    df = df.select("sk_pais", "id_pais", "pais", "codigo_iso", "nm_bloco")
    load_to_data_mart(df, "dm_pais")

# ========== Exchange Rates ==========

@udf(StringType())
def fetch_exchange_rate(date, from_currency, to_currency):
    try:
        origin = get_currency_from_country_code(from_currency)
        destination = get_currency_from_country_code(to_currency)

        res = requests.get(f"https://api.frankfurter.dev/v1/latest?from={origin}&to={destination}")
        res.raise_for_status()
        return 1
    except Exception as e:
        print(f"API error: {e}")
        return 2

def etl_exchange_rates():
    query = """
        SELECT c.id AS id_cambio, c.data,
               m1.descricao AS ds_moeda_origem, m1.pais AS pais_moeda_origem,
               m2.descricao AS ds_moeda_destino, m2.pais AS pais_moeda_destino,
               c.taxa_cambio
        FROM cambios c
        INNER JOIN moedas m1 ON m1.id = c.moeda_origem
        INNER JOIN moedas m2 ON m2.id = c.moeda_destino
    """
    df = extract_from_principal(query)
    df = transform_text_to_uppercase(df, ["ds_moeda_origem", "pais_moeda_origem", "ds_moeda_destino", "pais_moeda_destino"])
    df = df.withColumn("taxa_cambio", fetch_exchange_rate(col("data"), col("ds_moeda_origem"), col("ds_moeda_destino")).cast(FloatType()))
    df = add_surrogate_key(df, "id_cambio", "sk_cambio")

    df = df.select(
        "sk_cambio", "id_cambio", "data",
        "ds_moeda_origem", "pais_moeda_origem",
        "ds_moeda_destino", "pais_moeda_destino", "taxa_cambio"
    )
    load_to_data_mart(df, "dm_cambios")
    return df

def etl_dim_tempo_from_exchange(exchange_df):
    dates_df = exchange_df.select("data").distinct()
    dates_df = dates_df.withColumn("ano", year(col("data"))) \
                       .withColumn("mes", month(col("data"))) \
                       .withColumn("dia", dayofmonth(col("data")))
    dates_df = dates_df.withColumn("sk_tempo", row_number().over(Window.orderBy("data")))
    dm_tempo = dates_df.select("sk_tempo", col("data").alias("data_completa"), "ano", "mes", "dia")
    load_to_data_mart(dm_tempo, "dm_tempo")

# ========== Run All ==========

etl_products()
etl_transports()
etl_countries()
exchange_df = etl_exchange_rates()
etl_dim_tempo_from_exchange(exchange_df)

spark.stop()