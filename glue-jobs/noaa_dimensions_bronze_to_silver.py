"""
Glue Job: NOAA GHCN Dimensions Bronze → Silver (Iceberg)
Processa tabelas de dimensão (stations, countries, states, inventory)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_BUCKET",
        "SILVER_BUCKET",
        "SILVER_DATABASE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Iceberg é configurado via --conf no Terraform antes da sessão Spark ser criada

bronze_bucket = args["BRONZE_BUCKET"]
silver_bucket = args["SILVER_BUCKET"]
silver_database = args["SILVER_DATABASE"]

print("Processando tabelas de dimensão NOAA GHCN...")

# 1. Stations (ghcnd-stations.txt)
# Formato fixo: ID (11), LATITUDE (9), LONGITUDE (10), ELEVATION (7), STATE (2), NAME (31), GSN FLAG (3), HCN/CRN FLAG (3), WMO ID (5)
stations_path = f"s3://{bronze_bucket}/bronze/noaa_ghcn/dimension/ghcnd-stations.txt"
print(f"Lendo stations: {stations_path}")

try:
    # Ler como texto e fazer parsing usando split por espaços múltiplos
    stations_raw = spark.read.text(stations_path)
    
    # Split por espaços múltiplos e extrair campos
    # Formato: ID LATITUDE LONGITUDE ELEVATION STATE NAME [GSN_FLAG] [HCN_CRN_FLAG] [WMO_ID]
    # Usar regex para extrair campos de forma mais robusta
    stations_df = stations_raw.select(
        F.regexp_extract(F.col("value"), r"^(\S+)", 1).alias("station_id"),
        F.regexp_extract(F.col("value"), r"^\S+\s+(-?\d+\.?\d*)", 1).cast(DoubleType()).alias("latitude"),
        F.regexp_extract(F.col("value"), r"^\S+\s+-?\d+\.?\d*\s+(-?\d+\.?\d*)", 1).cast(DoubleType()).alias("longitude"),
        F.regexp_extract(F.col("value"), r"^\S+\s+-?\d+\.?\d*\s+-?\d+\.?\d*\s+(-?\d+\.?\d*)", 1).cast(DoubleType()).alias("elevation"),
        F.regexp_extract(F.col("value"), r"^\S+\s+-?\d+\.?\d*\s+-?\d+\.?\d*\s+-?\d+\.?\d*\s+(\S{2})", 1).alias("state"),
        # Nome: remover os primeiros 5 campos e as flags finais, pegar o resto
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.col("value"),
                        r"^\S+\s+-?\d+\.?\d*\s+-?\d+\.?\d*\s+-?\d+\.?\d*\s+\S{2}\s+",
                        ""
                    ),
                    r"\s+(GSN|HCN|CRN).*$",
                    ""
                ),
                r"\s+\d{5}$",
                ""
            )
        ).alias("station_name"),
        # Flags opcionais - extrair do final da linha
        F.when(
            F.col("value").rlike(r"\sGSN\s"),
            "GSN"
        ).otherwise(None).alias("gsn_flag"),
        F.when(
            F.col("value").rlike(r"\s(HCN|CRN)\s"),
            F.regexp_extract(F.col("value"), r"\s(HCN|CRN)\s", 1)
        ).otherwise(None).alias("hcn_crn_flag"),
        F.when(
            F.col("value").rlike(r"\s\d{5}$"),
            F.regexp_extract(F.col("value"), r"(\d{5})$", 1)
        ).otherwise(None).alias("wmo_id"),
    ).filter(
        F.col("station_id").isNotNull() & (F.col("station_id") != "")
    )
    
    stations_path_silver = f"s3://{silver_bucket}/silver/noaa_dimensions/dim_stations/"
    try:
        stations_df.write.format("iceberg").option(
            "path", stations_path_silver
        ).mode("overwrite").saveAsTable(f"{silver_database}.dim_stations")
        print(f"✓ dim_stations criada (Iceberg): {stations_df.count()} registros")
    except Exception as iceberg_error:
        print(f"⚠️ Iceberg não disponível, usando Parquet: {iceberg_error}")
        stations_df.write.format("parquet").mode("overwrite").option(
            "path", stations_path_silver
        ).saveAsTable(f"{silver_database}.dim_stations")
        print(f"✓ dim_stations criada (Parquet): {stations_df.count()} registros")
except Exception as e:
    print(f"⚠️ Erro ao processar stations: {e}")
    import traceback
    traceback.print_exc()

# 2. Countries (ghcnd-countries.txt)
# Formato: CODE (2 chars) + espaço + NAME (resto)
countries_path = f"s3://{bronze_bucket}/bronze/noaa_ghcn/dimension/ghcnd-countries.txt"
print(f"Lendo countries: {countries_path}")

try:
    # Ler como texto e fazer parsing
    countries_raw = spark.read.text(countries_path)
    
    # Extrair código (2 primeiros chars) e nome (resto após espaço)
    # Formato: CODE (2 chars) + espaço + NAME (resto da linha)
    countries_df = countries_raw.select(
        F.trim(F.substring(F.col("value"), 1, 2)).alias("country_code"),
        F.trim(F.substring(F.col("value"), 4, 200)).alias("country_name"),  # 200 chars deve ser suficiente
    ).filter(
        F.col("country_code").isNotNull() & (F.col("country_code") != "")
    )
    
    countries_path_silver = f"s3://{silver_bucket}/silver/noaa_dimensions/dim_countries/"
    try:
        countries_df.write.format("iceberg").option(
            "path", countries_path_silver
        ).mode("overwrite").saveAsTable(f"{silver_database}.dim_countries")
        print(f"✓ dim_countries criada (Iceberg): {countries_df.count()} registros")
    except Exception as iceberg_error:
        print(f"⚠️ Iceberg não disponível, usando Parquet: {iceberg_error}")
        countries_df.write.format("parquet").mode("overwrite").option(
            "path", countries_path_silver
        ).saveAsTable(f"{silver_database}.dim_countries")
        print(f"✓ dim_countries criada (Parquet): {countries_df.count()} registros")
except Exception as e:
    print(f"⚠️ Erro ao processar countries: {e}")
    import traceback
    traceback.print_exc()

# 3. States (ghcnd-states.txt)
# Formato: CODE (2 chars) + espaços + NAME (resto)
states_path = f"s3://{bronze_bucket}/bronze/noaa_ghcn/dimension/ghcnd-states.txt"
print(f"Lendo states: {states_path}")

try:
    # Ler como texto e fazer parsing
    states_raw = spark.read.text(states_path)
    
    # Extrair código (2 primeiros chars) e nome (resto após espaços)
    states_df = states_raw.select(
        F.trim(F.substring(F.col("value"), 1, 2)).alias("state_code"),
        F.trim(F.regexp_extract(F.col("value"), r"^[A-Z]{2}\s+(.+)", 1)).alias("state_name"),
    ).filter(
        F.col("state_code").isNotNull() & (F.col("state_code") != "")
    )
    
    states_path_silver = f"s3://{silver_bucket}/silver/noaa_dimensions/dim_states/"
    try:
        states_df.write.format("iceberg").option(
            "path", states_path_silver
        ).mode("overwrite").saveAsTable(f"{silver_database}.dim_states")
        print(f"✓ dim_states criada (Iceberg): {states_df.count()} registros")
    except Exception as iceberg_error:
        print(f"⚠️ Iceberg não disponível, usando Parquet: {iceberg_error}")
        states_df.write.format("parquet").mode("overwrite").option(
            "path", states_path_silver
        ).saveAsTable(f"{silver_database}.dim_states")
        print(f"✓ dim_states criada (Parquet): {states_df.count()} registros")
except Exception as e:
    print(f"⚠️ Erro ao processar states: {e}")
    import traceback
    traceback.print_exc()

# 4. Inventory (ghcnd-inventory.txt)
# Formato fixo: ID (11), LATITUDE (9), LONGITUDE (10), ELEMENT (4), FIRSTYEAR (4), LASTYEAR (4)
inventory_path = f"s3://{bronze_bucket}/bronze/noaa_ghcn/dimension/ghcnd-inventory.txt"
print(f"Lendo inventory: {inventory_path}")

try:
    # Ler como texto e fazer parsing manual
    inventory_raw = spark.read.text(inventory_path)
    
    # Extrair campos usando split por espaços múltiplos
    # Formato: ID LATITUDE LONGITUDE ELEMENT FIRSTYEAR LASTYEAR
    inventory_df = inventory_raw.select(
        F.split(F.col("value"), r"\s+")[0].alias("station_id"),
        F.split(F.col("value"), r"\s+")[1].cast(DoubleType()).alias("latitude"),
        F.split(F.col("value"), r"\s+")[2].cast(DoubleType()).alias("longitude"),
        F.split(F.col("value"), r"\s+")[3].alias("element"),
        F.split(F.col("value"), r"\s+")[4].cast(IntegerType()).alias("first_year"),
        F.split(F.col("value"), r"\s+")[5].cast(IntegerType()).alias("last_year"),
    ).filter(
        F.col("station_id").isNotNull() & (F.col("station_id") != "")
    )
    
    inventory_path_silver = f"s3://{silver_bucket}/silver/noaa_dimensions/dim_inventory/"
    try:
        inventory_df.write.format("iceberg").option(
            "path", inventory_path_silver
        ).mode("overwrite").saveAsTable(f"{silver_database}.dim_inventory")
        print(f"✓ dim_inventory criada (Iceberg): {inventory_df.count()} registros")
    except Exception as iceberg_error:
        print(f"⚠️ Iceberg não disponível, usando Parquet: {iceberg_error}")
        inventory_df.write.format("parquet").mode("overwrite").option(
            "path", inventory_path_silver
        ).saveAsTable(f"{silver_database}.dim_inventory")
        print(f"✓ dim_inventory criada (Parquet): {inventory_df.count()} registros")
except Exception as e:
    print(f"⚠️ Erro ao processar inventory: {e}")
    import traceback
    traceback.print_exc()

print("\n✓ Processamento de dimensões concluído")

job.commit()

