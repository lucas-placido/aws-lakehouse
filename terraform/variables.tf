variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS profile to use"
  type        = string
  default     = "gudy"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "lakehouse"
}

variable "environment" {
  description = "Environment"
  type        = string
  default     = "dev"
}
