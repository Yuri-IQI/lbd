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
from babel.numbers import get_territory_currencies

load_dotenv()
db_url = os.getenv("DB_URL")

spark = SparkSession.builder \
    .appName("LBD Dimensional ETL") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.16") \
    .getOrCreate()

db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


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

def get_currency_from_country_code(alpha_3_code):
    try:
        country = pycountry.countries.get(alpha_3=alpha_3_code.upper())
        if not country:
            return None

        alpha_2 = country.alpha_2
        currencies = get_territory_currencies(alpha_2)
        if currencies:
            return currencies[0]
    except Exception as e:
        print(f"Error getting currency for {alpha_3_code}: {e}")
    return None


# ETL de Produtos, Transportes e Países
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

    return df

def etl_transports():
    query = "SELECT id AS id_transporte, descricao AS ds_transporte FROM transportes"
    df = extract_from_principal(query)
    df = transform_text_to_uppercase(df, ["ds_transporte"])
    df = add_surrogate_key(df, "id_transporte", "sk_transporte")
    df = df.select("sk_transporte", "id_transporte", "ds_transporte")
    load_to_data_mart(df, "dm_transporte")

    return df

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

    return df

# ETL de Câmbio
@udf(StringType())
def fetch_exchange_rate(date, from_country_code, to_country_code):
    if from_country_code == 'EUR':
        origin = 'EUR'
        destination = get_currency_from_country_code(to_country_code)

    elif to_country_code == 'EUR':
        origin = get_currency_from_country_code(from_country_code)
        destination = 'EUR'
    else:
        origin = get_currency_from_country_code(from_country_code)
        destination = get_currency_from_country_code(to_country_code)

    try:
        if not origin or not destination:
            return None

        res = requests.get(f"https://api.frankfurter.dev/v1/{date}?from={origin}&to={destination}")
        res.raise_for_status()
        rate = res.json().get("rates", {}).get(destination)
        return str(rate) if rate else None
    except Exception as e:
        return None

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
    df = df.withColumn("taxa_cambio", fetch_exchange_rate(col("data"), col("pais_moeda_origem"), col("pais_moeda_destino")).cast(FloatType()))
    df = add_surrogate_key(df, "id_cambio", "sk_cambio")

    df = df.select(
        "sk_cambio", "id_cambio", "data",
        "ds_moeda_origem", "pais_moeda_origem",
        "ds_moeda_destino", "pais_moeda_destino", "taxa_cambio"
    )
    load_to_data_mart(df, "dm_cambios")
    return df

# ETL de Tempo
def etl_time_from_exchange(exchange_df):
    dates_df = exchange_df.select("data").distinct()

    dates_df = dates_df.withColumn("ano", year(col("data"))) \
                       .withColumn("mes", month(col("data"))) \
                       .withColumn("dia", dayofmonth(col("data")))
    
    dates_df = dates_df.withColumn("sk_tempo", row_number().over(Window.orderBy("data")))

    dm_time = dates_df.select("sk_tempo", col("data").alias("data_completa"), "ano", "mes", "dia")
    load_to_data_mart(dm_time, "dm_tempo")

    return dm_time

# ETL da Tabela Fatos
def etl_facts():
    query = """ 
        SELECT
            t.id AS id_transacao,
            t.quantidade,
            t.valor_monetario,
            t.produto_id,
            t.transporte_id,
            t.pais_origem,
            t.pais_destino,
            t.cambio_id,
            tt.descricao AS tp_transacao,
            c.data AS data_transacao
        FROM transacoes t
        INNER JOIN tipos_transacoes tt ON tt.id = t.tipo_id
        INNER JOIN cambios c ON c.id = t.cambio_id
    """

    df = extract_from_principal(query)
    df = transform_text_to_uppercase(df, ["tp_transacao"])
    df = add_surrogate_key(df, "id_transacao", "sk_transacao")

    df = df \
        .join(products_df.select("id_produto", "sk_produto"), df["produto_id"] == products_df["id_produto"], "inner") \
        .join(transports_df.select("id_transporte", "sk_transporte"), df["transporte_id"] == transports_df["id_transporte"], "inner") \
        .join(countries_df.select(col("id_pais").alias("pais_origem"), col("sk_pais").alias("sk_pais_origem")),
              on="pais_origem", how="inner") \
        .join(countries_df.select(col("id_pais").alias("pais_destino"), col("sk_pais").alias("sk_pais_destino")),
              on="pais_destino", how="inner") \
        .join(exchange_df.select("id_cambio", "sk_cambio"), df["cambio_id"] == exchange_df["id_cambio"], "inner") \
        .join(time_df.select(col("data_completa").alias("data_transacao"), "sk_tempo"), on="data_transacao", how="inner")

    df = df.select(
        "sk_transacao",
        "id_transacao",
        "sk_transporte",
        "sk_pais_origem",
        "sk_pais_destino",
        "sk_produto",
        "sk_cambio",
        "sk_tempo",
        "valor_monetario",
        "quantidade",
        "tp_transacao"
    )

    load_to_data_mart(df, "ft_transacoes")
    return df

products_df = etl_products()
transports_df = etl_transports()
countries_df = etl_countries()
exchange_df = etl_exchange_rates()
time_df = etl_time_from_exchange(exchange_df)
facts_df = etl_facts()

spark.stop()