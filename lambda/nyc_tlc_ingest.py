"""
Lambda Function: NOAA GHCN Data Ingestion (Parquet)
Copia dados do AWS Open Data Registry (NOAA GHCN)
em formato Parquet para S3 Bronze
"""

import boto3
import os
from typing import List
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config

# Configuração
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
# AWS_REGION será obtida do contexto Lambda
SOURCE_BUCKET = "noaa-ghcn-pds"
SOURCE_PREFIX = "parquet/by_year/"
YEAR = 2025

# Arquivos de dimensão (tabelas de referência)
DIMENSION_FILES = [
    "ghcnd-countries.txt",
    "ghcnd-inventory.txt",
    "ghcnd-states.txt",
    "ghcnd-stations.txt",
]


def get_dest_key(source_key: str) -> str:
    """Gera S3 key de destino mantendo estrutura relativa"""
    # Remove o prefixo e mantém a estrutura
    # YEAR=2025/ELEMENT=.../arquivo.parquet
    # Exemplo: parquet/by_year/YEAR=2025/ELEMENT=WT11/file.parquet
    # -> bronze/noaa_ghcn/YEAR=2025/ELEMENT=WT11/file.parquet
    relative_path = source_key.replace(SOURCE_PREFIX, "")
    return f"bronze/noaa_ghcn/{relative_path}"


def get_dimension_dest_key(filename: str) -> str:
    """Gera S3 key de destino para arquivos de dimensão"""
    return f"bronze/noaa_ghcn/dimension/{filename}"


def file_exists_in_s3(s3_client: boto3.client, bucket: str, key: str) -> bool:
    """Verifica se arquivo já existe no S3"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def get_public_s3_client():
    """Cria cliente S3 sem credenciais (acesso anônimo)"""
    public_s3_config = Config(
        signature_version=UNSIGNED,
        region_name="us-east-1",
    )
    return boto3.client("s3", config=public_s3_config)


def list_parquet_files(public_s3_client, year: int) -> List[str]:
    """Lista todos os arquivos parquet para um ano específico"""
    prefix = f"{SOURCE_PREFIX}YEAR={year}/"
    parquet_files = []

    print(f"Listando arquivos em: s3://{SOURCE_BUCKET}/{prefix}")

    paginator = public_s3_client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if key.endswith(".parquet"):
                        parquet_files.append(key)
                        print(f"Encontrado: {key}")
    except ClientError as e:
        print(f"Erro ao listar arquivos: {e}")
        raise

    print(f"Total de arquivos parquet encontrados: {len(parquet_files)}")
    return parquet_files


def copy_from_registry_to_bronze(
    s3_client: boto3.client,
    public_s3_client: boto3.client,
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
) -> bool:
    """Copia arquivo de bucket público usando cliente anônimo"""
    try:
        # Primeiro, tenta usar copy_object (mais eficiente)
        try:
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key,
            )
            print(f"Copied via copy_object: {dest_key}")
            return True
        except ClientError as copy_error:
            # Se copy_object falhar, usa get_object com cliente anônimo
            error_code = copy_error.response.get("Error", {}).get("Code", "")
            if error_code == "AccessDenied":
                print(
                    f"copy_object failed ({error_code}), "
                    f"trying get_object with anonymous client..."
                )
            else:
                raise

            # Download do bucket público usando cliente anônimo
            print(f"Downloading: s3://{source_bucket}/{source_key}")
            response = public_s3_client.get_object(
                Bucket=source_bucket,
                Key=source_key,
            )
            file_content = response["Body"].read()

            # Upload para o bucket Bronze
            s3_client.put_object(
                Bucket=dest_bucket,
                Key=dest_key,
                Body=file_content,
            )

            print(f"Uploaded: {dest_key} ({len(file_content)} bytes)")
            return True

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ["NoSuchKey", "404"]:
            return False
        print(f"S3 Error {error_code}: {e}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise


def lambda_handler(event, context):
    """Lambda handler"""
    if not BRONZE_BUCKET:
        raise ValueError("BRONZE_BUCKET não configurado")

    # Obtém região do contexto Lambda ou usa padrão
    if context:
        region_from_arn = context.invoked_function_arn.split(":")[3]
        aws_region = os.getenv("AWS_REGION") or region_from_arn
    else:
        aws_region = os.getenv("AWS_REGION") or "us-east-1"

    s3_client = boto3.client("s3", region_name=aws_region)
    public_s3_client = get_public_s3_client()

    total_copied = 0
    total_skipped = 0
    total_errors = 0

    print(f"Iniciando ingestão NOAA GHCN Parquet para bucket: {BRONZE_BUCKET}")
    print(f"Região: {aws_region}")
    print(f"Ano: {YEAR}")

    # Copia arquivos de dimensão primeiro
    dimension_copied = 0
    dimension_skipped = 0
    dimension_errors = 0

    print("\n=== Iniciando ingestão de arquivos de dimensão ===")
    for dimension_file in DIMENSION_FILES:
        source_key = dimension_file
        dest_key = get_dimension_dest_key(dimension_file)

        # Verifica se já existe
        if file_exists_in_s3(s3_client, BRONZE_BUCKET, dest_key):
            print(f"Já existe: {dest_key}")
            dimension_skipped += 1
            continue

        # Tenta copiar
        try:
            if copy_from_registry_to_bronze(
                s3_client,
                public_s3_client,
                SOURCE_BUCKET,
                source_key,
                BRONZE_BUCKET,
                dest_key,
            ):
                print(f"Copiado: {source_key} -> {dest_key}")
                dimension_copied += 1
            else:
                print(f"Falha ao copiar: {source_key}")
                dimension_errors += 1
        except Exception as e:
            print(f"Erro ao copiar {source_key}: {e}")
            dimension_errors += 1

    print(
        f"Arquivos de dimensão: {dimension_copied} copiados, "
        f"{dimension_skipped} ignorados, {dimension_errors} erros"
    )

    # Lista todos os arquivos parquet para o ano especificado
    print("\n=== Iniciando ingestão de arquivos Parquet ===")
    parquet_files = list_parquet_files(public_s3_client, YEAR)

    if not parquet_files:
        print(f"Nenhum arquivo parquet encontrado para YEAR={YEAR}")
        return {
            "statusCode": 200,
            "body": {
                "message": "Ingestão concluída (sem arquivos parquet)",
                "parquet": {
                    "copiados": 0,
                    "ignorados": 0,
                    "erros": 0,
                    "total_arquivos": 0,
                },
                "dimension": {
                    "copiados": dimension_copied,
                    "ignorados": dimension_skipped,
                    "erros": dimension_errors,
                    "total_arquivos": len(DIMENSION_FILES),
                },
            },
        }

    # Copia cada arquivo
    for source_key in parquet_files:
        dest_key = get_dest_key(source_key)

        # Verifica se já existe
        if file_exists_in_s3(s3_client, BRONZE_BUCKET, dest_key):
            print(f"Já existe: {dest_key}")
            total_skipped += 1
            continue

        # Tenta copiar
        try:
            if copy_from_registry_to_bronze(
                s3_client,
                public_s3_client,
                SOURCE_BUCKET,
                source_key,
                BRONZE_BUCKET,
                dest_key,
            ):
                print(f"Copiado: {source_key} -> {dest_key}")
                total_copied += 1
            else:
                print(f"Falha ao copiar: {source_key}")
                total_errors += 1
        except Exception as e:
            print(f"Erro ao copiar {source_key}: {e}")
            total_errors += 1

    return {
        "statusCode": 200,
        "body": {
            "message": "Ingestão concluída",
            "parquet": {
                "copiados": total_copied,
                "ignorados": total_skipped,
                "erros": total_errors,
                "total_arquivos": len(parquet_files),
            },
            "dimension": {
                "copiados": dimension_copied,
                "ignorados": dimension_skipped,
                "erros": dimension_errors,
                "total_arquivos": len(DIMENSION_FILES),
            },
        },
    }
