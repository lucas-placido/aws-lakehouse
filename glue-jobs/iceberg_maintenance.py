"""
Glue Job: Manutenção Iceberg
Compaction e expire snapshots
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATABASE",
        "TABLES",
        "SNAPSHOT_RETENTION_DAYS",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

database = args["DATABASE"]
tables = args["TABLES"].split(",")
snapshot_retention_days = int(args.get("SNAPSHOT_RETENTION_DAYS", "7"))

print(f"Manutenção Iceberg para database: {database}")
print(f"Tabelas: {tables}")
print(f"Retenção snapshots: {snapshot_retention_days} dias\n")

for table_name in tables:
    table_name = table_name.strip()
    full_table_name = f"{database}.{table_name}"
    print(f"Processando: {full_table_name}")

    try:
        # 1. Expire snapshots antigos
        spark.sql(
            f"""
            CALL {database}.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{snapshot_retention_days} days ago'
            )
            """
        )
        print(f"  ✓ Snapshots expirados")

        # 2. Compactar arquivos pequenos
        spark.sql(
            f"""
            CALL {database}.system.rewrite_data_files(
                table => '{table_name}',
                strategy => 'binpack',
                options => map(
                    'target-file-size-bytes', '268435456'  -- 256MB
                )
            )
            """
        )
        print(f"  ✓ Compaction executada")

        # 3. Remover manifestos órfãos
        spark.sql(
            f"""
            CALL {database}.system.remove_orphan_files(
                table => '{table_name}',
                older_than => TIMESTAMP '{snapshot_retention_days} days ago'
            )
            """
        )
        print(f"  ✓ Arquivos órfãos removidos")

    except Exception as e:
        print(f"  ✗ Erro ao processar {full_table_name}: {e}")

print("\n✓ Manutenção concluída")

job.commit()
