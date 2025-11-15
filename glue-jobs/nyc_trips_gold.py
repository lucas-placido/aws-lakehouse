"""
Glue Job: Silver → Gold (NYC Trips)
Modelagem dimensional: fato e dimensões
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

silver_database = args["SILVER_DATABASE"]
silver_table = args["SILVER_TABLE"]
gold_bucket = args["GOLD_BUCKET"]
gold_database = args["GOLD_DATABASE"]

# Ler tabela Silver
trips_df = spark.table(f"{silver_database}.{silver_table}")

# Dimensão: Vendor
dim_vendor = trips_df.select(
    F.col("vendor_id").alias("vendor_id"),
    F.when(F.col("vendor_id") == "1", "Creative Mobile Technologies")
    .when(F.col("vendor_id") == "2", "VeriFone Inc")
    .otherwise("Unknown")
    .alias("vendor_name"),
).distinct()

# Dimensão: Taxi Zone (simplificado)
dim_taxi_zone = trips_df.select(
    F.col("pickup_location_id").alias("zone_id"),
    F.concat(F.lit("Zone "), F.col("pickup_location_id")).alias("zone_name"),
).distinct().union(
    trips_df.select(
        F.col("dropoff_location_id").alias("zone_id"),
        F.concat(F.lit("Zone "), F.col("dropoff_location_id")).alias("zone_name"),
    ).distinct()
).distinct()

# Fato: Trips
fact_trips = trips_df.select(
    F.col("vendor_id"),
    F.col("pickup_location_id").alias("pickup_zone_id"),
    F.col("dropoff_location_id").alias("dropoff_zone_id"),
    F.col("pickup_datetime"),
    F.col("dropoff_datetime"),
    F.col("pickup_date"),
    F.col("trip_distance"),
    F.col("fare_amount"),
    F.col("tip_amount"),
    F.col("total_amount"),
    F.col("passenger_count"),
    F.col("payment_type"),
    # Métricas calculadas
    (
        F.unix_timestamp("dropoff_datetime")
        - F.unix_timestamp("pickup_datetime")
    ).alias("trip_duration_seconds"),
    (F.col("fare_amount") / F.greatest(F.col("trip_distance"), 0.01)).alias("fare_per_mile"),
)

# Escrever dimensões
dim_vendor_path = f"s3://{gold_bucket}/gold/dim_vendor/"
try:
    dim_vendor.write.format("iceberg").option("path", dim_vendor_path).mode(
        "overwrite"
    ).saveAsTable(f"{gold_database}.dim_vendor")
except:
    dim_vendor.write.format("iceberg").option("path", dim_vendor_path).mode(
        "overwrite"
    ).saveAsTable(f"{gold_database}.dim_vendor")

dim_taxi_zone_path = f"s3://{gold_bucket}/gold/dim_taxi_zone/"
try:
    dim_taxi_zone.write.format("iceberg").option("path", dim_taxi_zone_path).mode(
        "overwrite"
    ).saveAsTable(f"{gold_database}.dim_taxi_zone")
except:
    dim_taxi_zone.write.format("iceberg").option("path", dim_taxi_zone_path).mode(
        "overwrite"
    ).saveAsTable(f"{gold_database}.dim_taxi_zone")

# Escrever fato
fact_trips_path = f"s3://{gold_bucket}/gold/fact_trips/"
fact_trips.write.format("iceberg").option("path", fact_trips_path).mode(
    "append"
).saveAsTable(f"{gold_database}.fact_trips")

print(f"✓ Tabelas Gold criadas:")
print(f"  - {gold_database}.dim_vendor")
print(f"  - {gold_database}.dim_taxi_zone")
print(f"  - {gold_database}.fact_trips")

job.commit()
