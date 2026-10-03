variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix for resource names and the value of the Project cost-allocation tag."
  type        = string
  default     = "streaming-lakehouse"
}

variable "sdk_pandas_layer_arn" {
  description = <<-EOT
    ARN of the AWS SDK for pandas (awswrangler) managed Lambda layer for your region
    and Python 3.12. Look it up in the AWS SDK for pandas docs ("Lambda Managed Layers")
    and paste it here. Example shape: arn:aws:lambda:<region>:336392948345:layer:AWSSDKPandas-Python312:<version>
  EOT
  type        = string
}

variable "shard_count" {
  description = "Number of Kinesis shards (1 shard ~ 1 MB/s or 1,000 records/s in)."
  type        = number
  default     = 1
}

variable "batch_size" {
  description = "Max records per Lambda invocation. Larger batches mean fewer Athena INSERTs."
  type        = number
  default     = 5000
}

variable "batching_window_seconds" {
  description = "How long Lambda waits to fill a batch before invoking."
  type        = number
  default     = 60
}

variable "athena_scan_limit_bytes" {
  description = "Per-query data-scanned cutoff for the Athena workgroup (cost guardrail)."
  type        = number
  default     = 10737418240 # 10 GiB
}
