output "kinesis_stream_name" { value = aws_kinesis_stream.events.name }
output "bucket" { value = aws_s3_bucket.lakehouse.bucket }
output "athena_workgroup" { value = aws_athena_workgroup.lakehouse.name }
output "glue_database" { value = aws_glue_catalog_database.lakehouse.name }
output "iceberg_table" { value = local.table_name }
output "failed_batches_queue" { value = aws_sqs_queue.failed_batches.url }
