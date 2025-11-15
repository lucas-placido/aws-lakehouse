output "bronze_bucket" {
  description = "Bronze bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  description = "Silver bucket name"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  description = "Gold bucket name"
  value       = aws_s3_bucket.gold.bucket
}

output "scripts_bucket" {
  description = "Scripts bucket name"
  value       = aws_s3_bucket.scripts.bucket
}

output "glue_role_arn" {
  description = "Glue role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "stepfunctions_state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.lakehouse_pipeline.arn
}

output "lambda_function_arn" {
  description = "Lambda function ARN (NOAA GHCN ingestion)"
  value       = aws_lambda_function.noaa_ghcn_ingest.arn
}

output "resource_group_arn" {
  description = "Resource Group ARN"
  value       = aws_resourcegroups_group.lakehouse.arn
}

output "resource_group_name" {
  description = "Resource Group name"
  value       = aws_resourcegroups_group.lakehouse.name
}
