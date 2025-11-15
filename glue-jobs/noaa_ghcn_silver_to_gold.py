"""
Glue Job: NOAA GHCN Silver → Gold
Modelagem dimensional: fato climático e dimensões
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_DATABASE",
        "SILVER_TABLE",
        "GOLD_BUCKET",
        "GOLD_DATABASE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Iceberg é configurado via --conf no Terraform antes da sessão Spark ser criada

silver_database = args["SILVER_DATABASE"]
silver_table = args["SILVER_TABLE"]
gold_bucket = args["GOLD_BUCKET"]
gold_database = args["GOLD_DATABASE"]

print("Lendo tabela Silver...")

# Ler tabela Silver
observations_df = spark.table(f"{silver_database}.{silver_table}")

# Ler dimensões
try:
    dim_stations = spark.table(f"{silver_database}.dim_stations")
    dim_countries = spark.table(f"{silver_database}.dim_countries")
    dim_states = spark.table(f"{silver_database}.dim_states")
except Exception as e:
    print(f"⚠️ Aviso: Algumas dimensões não encontradas: {e}")
    print("Continuando sem dimensões...")
    dim_stations = None
    dim_countries = None
    dim_states = None

# Dimensão: Element Type
dim_element = observations_df.select(
    F.col("element_type").alias("element_code"),
    F.when(F.col("element_type") == "TMAX", "Maximum Temperature")
    .when(F.col("element_type") == "TMIN", "Minimum Temperature")
    .when(F.col("element_type") == "PRCP", "Precipitation")
    .when(F.col("element_type") == "SNOW", "Snowfall")
    .when(F.col("element_type") == "SNWD", "Snow Depth")
    .when(F.col("element_type") == "AWND", "Average Wind Speed")
    .when(F.col("element_type") == "WT11", "Fog")
    .otherwise(F.col("element_type"))
    .alias("element_name"),
).distinct()

# Dimensão: Date (calendário)
dim_date = observations_df.select(
    F.col("observation_date").alias("date"),
    F.year("observation_date").alias("year"),
    F.month("observation_date").alias("month"),
    F.dayofmonth("observation_date").alias("day"),
    F.dayofweek("observation_date").alias("day_of_week"),
    F.dayofyear("observation_date").alias("day_of_year"),
    F.quarter("observation_date").alias("quarter"),
    F.weekofyear("observation_date").alias("week_of_year"),
).distinct()

# Fato: Climate Observations
fact_climate = observations_df.select(
    F.col("station_id"),
    F.col("observation_date"),
    F.col("element_type"),
    F.col("value"),
    F.col("year"),
    F.col("month"),
    F.col("day"),
    # Métricas calculadas
    F.when(F.col("element_type").isin(["TMAX", "TMIN"]), F.col("value") / 10.0)
    .otherwise(F.col("value"))
    .alias("normalized_value"),  # Temperaturas em décimos de grau
)

# Escrever dimensões
dim_element_path = f"s3://{gold_bucket}/gold/dim_element/"
try:
    dim_element.write.format("iceberg").option("path", dim_element_path).mode(
        "overwrite"
    ).saveAsTable(f"{gold_database}.dim_element")
    print(f"✓ dim_element criada")
except Exception as e:
    print(f"⚠️ Erro ao criar dim_element: {e}")

dim_date_path = f"s3://{gold_bucket}/gold/dim_date/"
try:
    dim_date.write.format("iceberg").option("path", dim_date_path).mode(
        "overwrite"
    ).saveAsTable(f"{gold_database}.dim_date")
    print(f"✓ dim_date criada")
except Exception as e:
    print(f"⚠️ Erro ao criar dim_date: {e}")

# Escrever fato
fact_climate_path = f"s3://{gold_bucket}/gold/fact_climate/"
try:
    fact_climate.write.format("iceberg").option("path", fact_climate_path).mode(
        "append"
    ).saveAsTable(f"{gold_database}.fact_climate")
    print(f"✓ fact_climate atualizada")
except Exception as e:
    print(f"⚠️ Erro ao criar fact_climate: {e}")

print(f"\n✓ Tabelas Gold criadas:")
print(f"  - {gold_database}.dim_element")
print(f"  - {gold_database}.dim_date")
print(f"  - {gold_database}.fact_climate")

job.commit()

