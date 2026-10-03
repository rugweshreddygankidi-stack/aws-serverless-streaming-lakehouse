data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  name        = var.project_name
  bucket_name = "${var.project_name}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  db_name     = replace("${var.project_name}_db", "-", "_")
  table_name  = "events"
}

# ---------------------------------------------------------------- Storage
resource "aws_s3_bucket" "lakehouse" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-athena-scratch"
    status = "Enabled"
    filter { prefix = "athena-temp/" }
    expiration { days = 1 }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }

  rule {
    id     = "expire-dead-letter"
    status = "Enabled"
    filter { prefix = "dead-letter/" }
    expiration { days = 30 }
  }
}

# ---------------------------------------------------------------- Catalog + query engine
resource "aws_glue_catalog_database" "lakehouse" {
  name = local.db_name
}

resource "aws_athena_workgroup" "lakehouse" {
  name          = "${local.name}-wg"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_bytes

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
    }
  }
}

# ---------------------------------------------------------------- Ingestion
resource "aws_kinesis_stream" "events" {
  name             = "${local.name}-events"
  shard_count      = var.shard_count
  retention_period = 24
}

resource "aws_sqs_queue" "failed_batches" {
  name                      = "${local.name}-failed-batches"
  message_retention_seconds = 1209600
}

# ---------------------------------------------------------------- Lambda
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/build/lambda.zip"
  excludes    = ["lakehouse/__pycache__", "lakehouse/producer.py"]
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name}-ingest"
  retention_in_days = 14
}

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.name}-ingest-role"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

resource "aws_iam_role_policy_attachment" "kinesis_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole"
}

data "aws_iam_policy_document" "lambda_access" {
  statement {
    sid       = "BucketObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }
  statement {
    sid       = "BucketList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }
  statement {
    sid = "Athena"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup"
    ]
    resources = [aws_athena_workgroup.lakehouse.arn]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition", "glue:CreatePartition"
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${local.db_name}",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${local.db_name}/*",
    ]
  }
  statement {
    sid       = "FailedBatchQueue"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.failed_batches.arn]
  }
}

resource "aws_iam_role_policy" "lambda_access" {
  name   = "${local.name}-ingest-access"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_access.json
}

resource "aws_lambda_function" "ingest" {
  function_name    = "${local.name}-ingest"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lakehouse.handler.lambda_handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  layers           = [var.sdk_pandas_layer_arn]
  memory_size      = 1024
  timeout          = 300

  environment {
    variables = {
      LAKEHOUSE_BUCKET = aws_s3_bucket.lakehouse.bucket
      ICEBERG_DATABASE = aws_glue_catalog_database.lakehouse.name
      ICEBERG_TABLE    = local.table_name
      ATHENA_WORKGROUP = aws_athena_workgroup.lakehouse.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy.lambda_access]
}

resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn                   = aws_kinesis_stream.events.arn
  function_name                      = aws_lambda_function.ingest.arn
  starting_position                  = "LATEST"
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.batching_window_seconds
  parallelization_factor             = 1
  bisect_batch_on_function_error     = true
  maximum_retry_attempts             = 3
  maximum_record_age_in_seconds      = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.failed_batches.arn
    }
  }
}
