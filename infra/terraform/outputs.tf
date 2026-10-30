output "bucket" {
  description = "Lake bucket name."
  value       = aws_s3_bucket.lake.bucket
}

output "warehouse" {
  description = "Value for SETTLELAKE_WAREHOUSE when SETTLELAKE_CATALOG=glue."
  value       = "s3://${aws_s3_bucket.lake.bucket}/${local.warehouse_prefix}"
}

output "athena_workgroup" {
  value = aws_athena_workgroup.settlelake.name
}

output "spark_job_role_arn" {
  value = aws_iam_role.spark_jobs.arn
}
