"""
Glue Job: NYC TLC Bronze → Silver (Iceberg)
Transformação, limpeza, deduplicação e criação de tabela Iceberg
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Configuração
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_BUCKET",
        "SILVER_BUCKET",
        "SILVER_DATABASE",
        "SILVER_TABLE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Parâmetros
bronze_bucket = args["BRONZE_BUCKET"]
silver_bucket = args["SILVER_BUCKET"]
silver_database = args["SILVER_DATABASE"]
silver_table = args["SILVER_TABLE"]

print(f"Lendo dados Bronze: s3://{bronze_bucket}/bronze/nyc_tlc/")
df = spark.read.format("parquet").option("recursiveFileLookup", "true").load(
    f"s3://{bronze_bucket}/bronze/nyc_tlc/"
)

# Se não for Parquet, tenta CSV
if df.count() == 0:
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        f"s3://{bronze_bucket}/bronze/nyc_tlc/"
    )

print(f"Registros lidos: {df.count()}")

# 1. Limpeza e padronização
df_clean = df.select(
    F.col("vendor_id").alias("vendor_id"),
    F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss").alias("pickup_datetime"),
    F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss").alias("dropoff_datetime"),
    F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
    F.col("trip_distance").cast(DoubleType()).alias("trip_distance"),
    F.col("pulocationid").cast(IntegerType()).alias("pickup_location_id"),
    F.col("dolocationid").cast(IntegerType()).alias("dropoff_location_id"),
    F.col("payment_type").cast(IntegerType()).alias("payment_type"),
    F.col("fare_amount").cast(DoubleType()).alias("fare_amount"),
    F.col("tip_amount").cast(DoubleType()).alias("tip_amount"),
    F.col("total_amount").cast(DoubleType()).alias("total_amount"),
    F.col("tpep_pickup_datetime").cast("date").alias("pickup_date"),
).filter(
    # Qualidade: duração entre 1 min e 3h
    (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
    (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime") >= 60) &
    (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime") <= 10800) &
    # trip_distance > 0 e < 100
    (F.col("trip_distance") > 0) &
    (F.col("trip_distance") < 100) &
    # fare_amount >= 0
    (F.col("fare_amount") >= 0)
)

print(f"Registros após limpeza: {df_clean.count()}")

# 2. Deduplicação
window_spec = Window.partitionBy(
    "vendor_id", "pickup_datetime", "dropoff_datetime"
).orderBy(F.col("total_amount").desc())

df_dedup = df_clean.withColumn(
    "row_num", F.row_number().over(window_spec)
).filter(F.col("row_num") == 1).drop("row_num")

print(f"Registros após deduplicação: {df_dedup.count()}")

# 3. Adicionar colunas de particionamento
df_final = df_dedup.withColumn(
    "pickup_year", F.year("pickup_date")
).withColumn(
    "pickup_month", F.month("pickup_date")
).withColumn(
    "pickup_day", F.dayofmonth("pickup_date")
)

# 4. Escrever como Iceberg
table_path = f"s3://{silver_bucket}/silver/nyc_trips/"

try:
    df_final.write.format("iceberg").option("path", table_path).option(
        "write-mode", "append"
    ).mode("append").saveAsTable(f"{silver_database}.{silver_table}")
    print(f"✓ Tabela Iceberg criada: {silver_database}.{silver_table}")
except Exception as e:
    # Se tabela não existe, criar primeiro
    print(f"Criando tabela Iceberg...")
    df_final.write.format("iceberg").option("path", table_path).mode(
        "overwrite"
    ).saveAsTable(f"{silver_database}.{silver_table}")
    print(f"✓ Tabela Iceberg criada: {silver_database}.{silver_table}")

job.commit()
