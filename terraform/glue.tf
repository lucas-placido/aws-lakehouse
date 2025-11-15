# Glue Databases
resource "aws_glue_catalog_database" "bronze" {
  name        = "bronze"
  description = "Bronze layer - raw data"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "silver"
  description = "Silver layer - curated Iceberg tables"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "gold"
  description = "Gold layer - analytics ready"
}

# Glue IAM Role
resource "aws_iam_role" "glue_role" {
  name = "lakehouse-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*",
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_catalog" {
  name = "glue-catalog-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/bronze",
          "arn:aws:glue:${local.region}:${local.account_id}:database/silver",
          "arn:aws:glue:${local.region}:${local.account_id}:database/gold",
          "arn:aws:glue:${local.region}:${local.account_id}:table/bronze/*",
          "arn:aws:glue:${local.region}:${local.account_id}:table/silver/*",
          "arn:aws:glue:${local.region}:${local.account_id}:table/gold/*"
        ]
      }
    ]
  })
}

# Glue Jobs
resource "aws_glue_job" "nyc_tlc_bronze_to_silver" {
  name     = "nyc-tlc-bronze-to-silver"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue-jobs/nyc_tlc_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-spark-ui"         = "false"  # Desabilitado para reduzir custos
    "--enable-metrics"          = "true"
    "--enable-glue-datacatalog" = "true"
    "--BRONZE_BUCKET"           = aws_s3_bucket.bronze.bucket
    "--SILVER_BUCKET"           = aws_s3_bucket.silver.bucket
    "--SILVER_DATABASE"         = "silver"
    "--SILVER_TABLE"            = "nyc_trips"
    "--job-language"            = "python"
    "--job-bookmark-option"     = "job-bookmark-enable"
  }

  glue_version      = "4.0"
  number_of_workers = 2  # Mínimo necessário para performance
  worker_type       = "G.1X"

  tags = local.common_tags
}

resource "aws_glue_job" "nyc_trips_silver_to_gold" {
  name     = "nyc-trips-silver-to-gold"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue-jobs/nyc_trips_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-spark-ui"         = "false"  # Desabilitado para reduzir custos
    "--enable-metrics"          = "true"
    "--enable-glue-datacatalog" = "true"
    "--SILVER_DATABASE"         = "silver"
    "--SILVER_TABLE"            = "nyc_trips"
    "--GOLD_BUCKET"             = aws_s3_bucket.gold.bucket
    "--GOLD_DATABASE"           = "gold"
    "--job-language"            = "python"
  }

  glue_version      = "4.0"
  number_of_workers = 2  # Mínimo necessário para performance
  worker_type       = "G.1X"

  tags = local.common_tags
}

resource "aws_glue_job" "iceberg_maintenance" {
  name     = "iceberg-maintenance"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue-jobs/iceberg_maintenance.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-spark-ui"         = "false"  # Desabilitado para reduzir custos
    "--enable-metrics"          = "true"
    "--enable-glue-datacatalog" = "true"
    "--DATABASE"                = "silver"
    "--TABLES"                  = "nyc_trips"
    "--SNAPSHOT_RETENTION_DAYS" = "7"
    "--job-language"            = "python"
  }

  glue_version      = "4.0"
  number_of_workers = 2  # Mínimo para G.1X
  worker_type       = "G.1X"

  tags = local.common_tags
}
