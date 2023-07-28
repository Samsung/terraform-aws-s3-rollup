# SQS
resource "aws_sqs_queue" "main" {
  name                       = "s3-rollup-${var.slug}"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  message_retention_seconds  = var.sqs_message_retention_seconds
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 1
  })
}

resource "aws_sqs_queue" "dlq" {
  name                       = "s3-rollup-${var.slug}-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  message_retention_seconds  = var.sqs_message_retention_seconds
}

# IAM
resource "aws_iam_role" "main" {
  # This name is referenced in AssumeRole policy in other accounts
  name               = "s3-rollup-${var.slug}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
  ]
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      identifiers = [
        "lambda.amazonaws.com",
        "scheduler.amazonaws.com",
      ]
    }
  }
}

resource "aws_iam_role_policy" "main" {
  role   = aws_iam_role.main.id
  policy = data.aws_iam_policy_document.main.json
}

data "aws_iam_policy_document" "main" {
  statement {
    sid     = "SQS"
    actions = ["sqs:*"]
    resources = [
      aws_sqs_queue.main.arn,
      aws_sqs_queue.dlq.arn,
    ]
  }
  statement {
    sid       = "Lambda"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.main.arn]
  }
  statement {
    sid       = "AssumeRole"
    actions   = ["sts:AssumeRole"]
    resources = var.s3_access_roles
  }
}

# Lambda
resource "aws_lambda_function" "main" {
  function_name = "s3-rollup-${var.slug}"
  role          = aws_iam_role.main.arn

  architectures    = ["arm64"]
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  handler          = "main.lambda_handler"
  runtime          = var.runtime
  memory_size      = var.memory_size
  timeout          = var.timeout

  layers = [
    # Lambda Insights
    local.lambda_insights_layer_arns[data.aws_region.current.name]
  ]

  ephemeral_storage {
    size = var.ephemeral_storage_size
  }

  dynamic "vpc_config" {
    for_each = var.subnet_ids != null ? toset([true]) : toset([])
    content {
      subnet_ids         = var.subnet_ids
      security_group_ids = var.security_group_ids
    }
  }

  environment {
    variables = {
      ROLLUP_QUEUE_NAME = aws_sqs_queue.main.name
    }
  }
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/python/main.py"
  output_path = "lambda.zip"
}

resource "aws_lambda_event_source_mapping" "main" {
  batch_size       = 1
  function_name    = aws_lambda_function.main.arn
  event_source_arn = aws_sqs_queue.main.arn

  scaling_config {
    maximum_concurrency = var.maximum_concurrency
  }
}

# EventBridge
resource "aws_scheduler_schedule" "main" {
  for_each = var.eventbridge_invocation_payload

  name = "s3-rollup-${var.slug}-${each.key}"

  state = var.enable_eventbridge_schedule ? "ENABLED" : "DISABLED"

  # https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html#cron-based
  # Run this every day at UTC noon
  schedule_expression = "cron(0 12 * * ? *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.main.arn
    role_arn = aws_iam_role.main.arn
    input    = each.value
  }
}
