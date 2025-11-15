"""
Glue Job: NOAA GHCN Bronze → Silver (Iceberg)
Transforma dados climáticos brutos em tabela Iceberg particionada
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
        "SILVER_TABLE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Iceberg é configurado via --conf no Terraform antes da sessão Spark ser criada
# Não podemos modificar spark.sql.extensions após a sessão ser criada

bronze_bucket = args["BRONZE_BUCKET"]
silver_bucket = args["SILVER_BUCKET"]
silver_database = args["SILVER_DATABASE"]
silver_table = args["SILVER_TABLE"]

print(f"Lendo dados Bronze: s3://{bronze_bucket}/bronze/noaa_ghcn/")
print("Lendo arquivos Parquet organizados por YEAR e ELEMENT...")

# Ler todos os arquivos Parquet do NOAA GHCN
# Estrutura: bronze/noaa_ghcn/YEAR=2025/ELEMENT=WT11/arquivo.parquet
# Colunas: ID, DATE, DATA_VALUE, M_FLAG, Q_FLAG, S_FLAG, OBS_TIME
df = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(f"s3://{bronze_bucket}/bronze/noaa_ghcn/YEAR=*")
)

print(f"Registros lidos: {df.count()}")
print(f"Colunas: {df.columns}")

# Verificar se há dados
if df.count() == 0:
    print("⚠️ Nenhum dado encontrado no Bronze")
    job.commit()
    sys.exit(0)

# Mostrar schema para debug
df.printSchema()

# Extrair ELEMENT do caminho do arquivo
# O caminho contém ELEMENT=XXX, precisamos extrair isso
# Usar input_file_name() para obter o caminho completo
df_with_path = df.withColumn("file_path", F.input_file_name())

# Extrair ELEMENT do caminho (ex: .../ELEMENT=WT11/...)
df_with_element = df_with_path.withColumn(
    "element_type", F.regexp_extract(F.col("file_path"), r"ELEMENT=([^/]+)", 1)
)

# Limpeza e padronização
# Colunas: ID, DATE (YYYYMMDD), DATA_VALUE, M_FLAG, Q_FLAG, S_FLAG, OBS_TIME
df_clean = df_with_element.select(
    F.col("ID").alias("station_id"),
    # Converter DATE de YYYYMMDD para date
    F.to_date(F.col("DATE").cast("string"), "yyyyMMdd").alias("observation_date"),
    F.col("element_type"),
    F.col("DATA_VALUE").cast(DoubleType()).alias("value"),
    F.col("M_FLAG").alias("measurement_flag"),
    F.col("Q_FLAG").alias("quality_flag"),
    F.col("S_FLAG").alias("source_flag"),
    F.col("OBS_TIME").alias("observation_time"),
).filter(
    # Qualidade: valor não nulo e flags válidas
    F.col("value").isNotNull()
    & (F.col("quality_flag").isNull() | (F.col("quality_flag") == ""))
)

print(f"Registros após limpeza: {df_clean.count()}")

# Adicionar colunas de particionamento
df_final = (
    df_clean.withColumn("year", F.year("observation_date"))
    .withColumn("month", F.month("observation_date"))
    .withColumn("day", F.dayofmonth("observation_date"))
)

# Escrever como Iceberg usando Glue Catalog
table_path = f"s3://{silver_bucket}/silver/noaa_ghcn/"

# No Glue 4.0, usar o catálogo do Glue diretamente com Iceberg
# A configuração já foi feita via --conf no Terraform
try:
    # Criar ou atualizar tabela Iceberg
    df_final.write.format("iceberg").option("path", table_path).mode(
        "append"
    ).saveAsTable(f"{silver_database}.{silver_table}")
    print(f"✓ Tabela Iceberg atualizada: {silver_database}.{silver_table}")
except Exception as e:
    # Se tabela não existe, criar primeiro via SQL
    print(f"Criando tabela Iceberg... Erro: {e}")
    try:
        # Criar tabela Iceberg via SQL usando o catálogo do Glue
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {silver_database}.{silver_table} (
                station_id string,
                observation_date date,
                element_type string,
                value double,
                measurement_flag string,
                quality_flag string,
                source_flag string,
                observation_time string,
                year int,
                month int,
                day int
            ) USING iceberg
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        )

        # Inserir dados
        df_final.write.format("iceberg").mode("append").saveAsTable(
            f"{silver_database}.{silver_table}"
        )
        print(f"✓ Tabela Iceberg criada: {silver_database}.{silver_table}")
    except Exception as e2:
        print(f"Erro ao criar tabela Iceberg: {e2}")
        # Fallback: usar Parquet simples (sem Iceberg)
        print("⚠️ Usando Parquet como fallback (sem recursos Iceberg)...")
        df_final.write.format("parquet").mode("overwrite").option(
            "path", table_path
        ).saveAsTable(f"{silver_database}.{silver_table}")
        print(f"✓ Tabela Parquet criada: {silver_database}.{silver_table}")

job.commit()
