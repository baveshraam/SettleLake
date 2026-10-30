variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for named resources (the S3 bucket name also gets the account id and region)."
  type        = string
  default     = "settlelake"
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty lake bucket (synthetic data only)."
  type        = bool
  default     = false
}

variable "trusted_principal_arns" {
  description = "IAM principals (user/role ARNs) allowed to assume the Spark job role. Required: no account-wide default."
  type        = list(string)

  validation {
    condition     = length(var.trusted_principal_arns) > 0
    error_message = "Provide at least one trusted principal ARN."
  }
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query data-scanned limit for the Athena workgroup in bytes (minimum 10 MB)."
  type        = number
  default     = 10737418240 # 10 GiB
}
