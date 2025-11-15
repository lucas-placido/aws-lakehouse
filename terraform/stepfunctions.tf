# Step Functions IAM Role
resource "aws_iam_role" "stepfunctions_role" {
  name = "lakehouse-stepfunctions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "stepfunctions_glue" {
  name = "stepfunctions-glue-access"
  role = aws_iam_role.stepfunctions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          aws_glue_job.noaa_dimensions_bronze_to_silver.arn,
          aws_glue_job.noaa_ghcn_bronze_to_silver.arn,
          aws_glue_job.noaa_ghcn_silver_to_gold.arn,
          aws_glue_job.iceberg_maintenance.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "stepfunctions_lambda" {
  name = "stepfunctions-lambda-access"
  role = aws_iam_role.stepfunctions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.noaa_ghcn_ingest.arn
        ]
      }
    ]
  })
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "lakehouse_pipeline" {
  name     = "lakehouse-pipeline"
  role_arn = aws_iam_role.stepfunctions_role.arn

  definition = jsonencode({
    Comment = "Lakehouse Pipeline - Bronze to Gold"
    StartAt = "IngestBronze"
    States = {
      IngestBronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.noaa_ghcn_ingest.function_name
        }
        Next = "ProcessDimensions"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts     = 2
            BackoffRate     = 2.0
          }
        ]
      }
      ProcessDimensions = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.noaa_dimensions_bronze_to_silver.name
          Arguments = {
            "--BRONZE_BUCKET"   = aws_s3_bucket.bronze.bucket
            "--SILVER_BUCKET"   = aws_s3_bucket.silver.bucket
            "--SILVER_DATABASE" = "silver"
          }
        }
        Next = "BronzeToSilver"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 2
            BackoffRate     = 2.0
          }
        ]
      }
      BronzeToSilver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.noaa_ghcn_bronze_to_silver.name
          Arguments = {
            "--BRONZE_BUCKET"   = aws_s3_bucket.bronze.bucket
            "--SILVER_BUCKET"   = aws_s3_bucket.silver.bucket
            "--SILVER_DATABASE" = "silver"
            "--SILVER_TABLE"    = "noaa_ghcn"
          }
        }
        Next = "SilverToGold"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 2
            BackoffRate     = 2.0
          }
        ]
      }
      SilverToGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.noaa_ghcn_silver_to_gold.name
          Arguments = {
            "--SILVER_DATABASE" = "silver"
            "--SILVER_TABLE"    = "noaa_ghcn"
            "--GOLD_BUCKET"     = aws_s3_bucket.gold.bucket
            "--GOLD_DATABASE"   = "gold"
          }
        }
        Next = "Maintenance"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 2
            BackoffRate     = 2.0
          }
        ]
      }
      Maintenance = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.iceberg_maintenance.name
          Arguments = {
            "--DATABASE"                = "silver"
            "--TABLES"                  = "noaa_ghcn,dim_stations,dim_countries,dim_states,dim_inventory"
            "--SNAPSHOT_RETENTION_DAYS" = "7"
          }
        }
        End = true
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed"]
            IntervalSeconds = 60
            MaxAttempts     = 1
            BackoffRate     = 2.0
          }
        ]
      }
    }
  })

  tags = local.common_tags
}

# EventBridge Rule para pipeline diário
resource "aws_cloudwatch_event_rule" "daily_pipeline" {
  name                = "lakehouse-daily-pipeline"
  description         = "Trigger daily pipeline"
  schedule_expression = "cron(0 3 * * ? *)" # 3 AM UTC daily

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "daily_pipeline" {
  rule      = aws_cloudwatch_event_rule.daily_pipeline.name
  target_id = "TriggerPipeline"
  arn       = aws_sfn_state_machine.lakehouse_pipeline.arn
  role_arn  = aws_iam_role.stepfunctions_role.arn
}
