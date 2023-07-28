output "iam_role_arn" {
  description = "IAM role ARN of the app. This is needed for setting up bucket access roles."
  value       = aws_iam_role.main.arn
}
