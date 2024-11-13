output "db_instance_id" {
  value = aws_db_instance.default.identifier
  description = "The ID of the RDS database instance"
}

output "iam_role_arn" {
  value = aws_iam_role.rds_s3_role.arn
  description = "The ARN of the IAM role"
}