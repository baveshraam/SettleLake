data "aws_caller_identity" "current" {}

locals {
  bucket_name = "${var.name_prefix}-lake-${data.aws_caller_identity.current.account_id}-${var.region}"
  # Iceberg namespaces used by the Spark jobs map 1:1 to Glue databases.
  databases        = toset(["bronze", "silver", "gold", "ops"])
  trusted          = var.trusted_principal_arns
  warehouse_prefix = "warehouse"
}

# ---------------------------------------------------------------- storage
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 14
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {
      prefix = ""
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ---------------------------------------------------------------- catalog
resource "aws_glue_catalog_database" "layer" {
  for_each     = local.databases
  name         = each.key
  description  = "SettleLake ${each.key} Iceberg namespace (synthetic data)"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/${local.warehouse_prefix}/${each.key}.db/"
}

# ---------------------------------------------------------------- query
resource "aws_athena_workgroup" "settlelake" {
  name          = "${var.name_prefix}-analytics"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_athena_named_query" "match_rate_by_provider" {
  name      = "match-rate-by-provider"
  workgroup = aws_athena_workgroup.settlelake.id
  database  = aws_glue_catalog_database.layer["gold"].name
  query     = <<-SQL
    SELECT provider,
           count(*) AS reconciled_keys,
           count_if(classification = 'MATCHED') AS matched,
           round(100.0 * count_if(classification = 'MATCHED') / count(*), 2) AS match_rate_pct
    FROM reconciliation
    GROUP BY provider
    ORDER BY provider
  SQL
}

resource "aws_athena_named_query" "exceptions_by_class" {
  name      = "exceptions-by-classification"
  workgroup = aws_athena_workgroup.settlelake.id
  database  = aws_glue_catalog_database.layer["gold"].name
  query     = <<-SQL
    SELECT classification, internal_currency AS currency, count(*) AS records,
           sum(internal_amount_minor) AS internal_total_minor
    FROM exceptions
    GROUP BY classification, internal_currency
    ORDER BY records DESC
  SQL
}

resource "aws_athena_named_query" "volume_by_currency" {
  name      = "payment-volume-by-currency"
  workgroup = aws_athena_workgroup.settlelake.id
  database  = aws_glue_catalog_database.layer["gold"].name
  query     = <<-SQL
    SELECT source_currency AS currency, count(*) AS payments, sum(source_amount_minor) AS total_minor
    FROM fact_payments
    GROUP BY source_currency
    ORDER BY payments DESC
  SQL
}

# ---------------------------------------------------------------- access
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = local.trusted
    }
  }
}

data "aws_iam_policy_document" "spark_jobs" {
  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid       = "LakeBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions",
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog"],
      [for d in aws_glue_catalog_database.layer : d.arn],
      [for d in aws_glue_catalog_database.layer : "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${d.name}/*"],
    )
  }
  statement {
    sid       = "AthenaQueries"
    actions   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"]
    resources = [aws_athena_workgroup.settlelake.arn]
  }
}

resource "aws_iam_role" "spark_jobs" {
  name               = "${var.name_prefix}-spark-jobs"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "spark_jobs" {
  name   = "lake-access"
  role   = aws_iam_role.spark_jobs.id
  policy = data.aws_iam_policy_document.spark_jobs.json
}
