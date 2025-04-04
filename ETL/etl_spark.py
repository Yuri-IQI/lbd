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

# ETL de produtos

# Extração das tabelas de produtos e categoria_produtos
products_extract = spark.read \
    .format("jdbc") \
    .option("url", db_url + "/space_comex_principal") \
    .option("query", "select p.id as id_produto, p.descricao, cp.descricao as ds_categoria, codigo_ncm from produtos p " \
    "inner join categoria_produtos cp on cp.id = p.categoria_id") \
    .options(**db_properties) \
    .load()

# Transformação dos campos de texto para uppercase
products_transformed = products_extract \
    .withColumn("descricao", upper(col("descricao"))) \
    .withColumn("codigo_ncm", upper(col("codigo_ncm"))) \
    .withColumn("ds_categoria", upper(col("ds_categoria")))

products_transformed.show()

# Carregamento dos dados transformados em uma nova tabela
products_transformed.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", db_url + "/space_comex_data_mart") \
    .option("dbtable", "dm_produtos") \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .save()

spark.stop()