from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col
import os
from dotenv import load_dotenv

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

products_query = "select p.id as id_produto, p.descricao, cp.descricao as ds_categoria, codigo_ncm from produtos p " \
    "inner join categoria_produtos cp on cp.id = p.categoria_id"

# Extração das tabelas de produtos e categoria_produtos
products_extract = extract_from_principal(products_query)

# Transformação dos campos de texto para uppercase
products_transformed = transform_text_to_uppercase(products_extract, ["descricao", "codigo_ncm", "ds_categoria"])

products_transformed.show()

# Carregamento dos dados transformados em uma nova tabela
load_to_data_mart(products_transformed, "dm_produtos")

# ETL de transportes

transports_query = "select id as id_transporte, descricao as ds_transporte from transportes"
transports_extract = extract_from_principal(transports_query)

transports_transformed = transform_text_to_uppercase(transports_extract, ["ds_transporte"])

transports_transformed.show()

load_to_data_mart(transports_transformed, "dm_transporte")

# ETL de países

countries_query = "select p.id as id_pais, p.nome as pais, p.codigo_iso, b.nome as nm_bloco" \
    "from paises p inner join blocos_economicos b on b.id = p.bloco_id"
countries_extract = extract_from_principal(countries_query)

countries_transformed = transform_text_to_uppercase(countries_extract, ["pais", "codigo_iso", "nm_bloco"])

countries_transformed.show()

load_to_data_mart(countries_transformed, "dm_paises")

spark.stop()