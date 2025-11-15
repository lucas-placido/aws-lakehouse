# Lambda IAM Role
resource "aws_iam_role" "lambda_role" {
  name = "lakehouse-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  name = "lambda-s3-access"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:CopyObject",
          "s3:HeadObject"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          "arn:aws:s3:::noaa-ghcn-pds/*",
          "arn:aws:s3:::noaa-ghcn-pds"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::noaa-ghcn-pds"
        ]
      }
    ]
  })
}

# Lambda Functions
data "archive_file" "nyc_tlc_ingest" {
  type        = "zip"
  source_file = "${path.module}/../lambda/nyc_tlc_ingest.py"
  output_path = "${path.module}/nyc_tlc_ingest.zip"
}

resource "aws_lambda_function" "nyc_tlc_ingest" {
  filename         = data.archive_file.nyc_tlc_ingest.output_path
  function_name    = "lakehouse-nyc-tlc-ingest"
  role             = aws_iam_role.lambda_role.arn
  handler          = "nyc_tlc_ingest.lambda_handler"
  source_code_hash = data.archive_file.nyc_tlc_ingest.output_base64sha256
  runtime          = "python3.11"
  timeout          = 300 # Reduzido de 900s para 300s (5min) - suficiente para cópia
  memory_size      = 256 # Reduzido de 512MB para 256MB - otimização de custo

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.bronze.bucket
      # AWS_REGION é reservada e não pode ser definida manualmente
      # O Lambda já tem acesso à região via contexto
    }
  }

  tags = local.common_tags
}

# EventBridge Rule para ingestão diária
resource "aws_cloudwatch_event_rule" "daily_ingest" {
  name                = "lakehouse-daily-ingest"
  description         = "Trigger daily data ingestion"
  schedule_expression = "cron(0 2 * * ? *)" # 2 AM UTC daily

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "daily_ingest" {
  rule      = aws_cloudwatch_event_rule.daily_ingest.name
  target_id = "TriggerNYCTLCIngest"
  arn       = aws_lambda_function.nyc_tlc_ingest.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.nyc_tlc_ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_ingest.arn
}
