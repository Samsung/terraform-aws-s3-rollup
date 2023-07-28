variable "slug" {
  description = "Used for naming resources."
  type        = string
}

variable "subnet_ids" {
  description = "IDs of subnets to run Lambda function in. If null, Lambda runs in Amazon-managed VPC."
  type        = list(string)
  default     = null
}

variable "security_group_ids" {
  description = "IDs of security groups to attach to Lambda function. Only valid if var.subnet_ids is not null."
  type        = list(string)
  default     = null
}

variable "s3_access_roles" {
  description = "ARNs of IAM roles that the function assume to read/write S3 buckets. These child roles can only be created after the parent role is created."
  type        = list(string)
}

variable "runtime" {
  description = "Labmda function runtime. See: https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html"
  type        = string
  default     = "python3.8"
}

variable "memory_size" {
  description = "Lambda function memory size in MiB."
  type        = number
  default     = 1024
  validation {
    condition     = var.memory_size <= 1024 * 10
    error_message = "Lambda allows max memory size of 10 GiB."
  }
}

variable "ephemeral_storage_size" {
  description = "Lambda function ephemeral storage size in MiB."
  type        = number
  default     = 1024 * 10
  validation {
    condition     = var.ephemeral_storage_size <= 1024 * 10
    error_message = "Lambda allows max ephemeral storage size of 10 GiB."
  }
}

variable "maximum_concurrency" {
  description = "How many Lambda function instances can be launched concurrently by SQS."
  type        = number
  default     = 10
}

variable "timeout" {
  description = "Lambda function timeout in seconds."
  type        = number
  default     = 60 * 10
  validation {
    condition     = var.timeout <= 60 * 15
    error_message = "Lambda allows max execution timeout of 15 minutes."
  }
}

variable "sqs_visibility_timeout" {
  description = "How long the message stays invisible when it has been received. Must be greater than Lambda timeout."
  type        = number
  default     = 60 * 15
}

variable "sqs_message_retention_seconds" {
  description = "How long the message stays in queue before being purged."
  type        = number
  default     = 86400 * 14 # 14 days
  validation {
    condition     = var.sqs_message_retention_seconds <= 86400 * 14
    error_message = "Lambda allows max retention time of 14 days."
  }
}

variable "enable_eventbridge_schedule" {
  description = "If true, producer Lambda scans S3 prefixes every day to make tasks."
  type        = bool
  default     = true
}

variable "eventbridge_invocation_payload" {
  description = "Map of account alias => JSON payload to pass to Lambda function by EventBridge."
  type        = map(string)
  default     = {}
}
