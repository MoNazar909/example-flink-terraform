# ============================================================
# Outputs — copy these values into stage2/terraform.tfvars
# ============================================================

output "flink_compute_pool_id" {
  description = "Copy this into stage2/terraform.tfvars as flink_compute_pool_id"
  value       = confluent_flink_compute_pool.standard_poc.id
}

output "flink_runner_sa_id" {
  description = "Copy this into stage2/terraform.tfvars as flink_principal_id"
  value       = confluent_service_account.flink_runner.id
}

output "flink_runner_api_key_id" {
  description = "Copy this into stage2/terraform.tfvars as flink_api_key"
  value       = confluent_api_key.flink_key.id
}

output "flink_runner_api_key_secret" {
  description = "Copy this into stage2/terraform.tfvars as flink_api_secret"
  value       = confluent_api_key.flink_key.secret
  sensitive   = true
}

output "kafka_topics_created" {
  description = "List of created Kafka topic names"
  value = [
    confluent_kafka_topic.eoi_source.topic_name,
    confluent_kafka_topic.flink_common.topic_name,
    confluent_kafka_topic.flink_workday.topic_name,
    confluent_kafka_topic.flink_workday_dlq.topic_name,
    confluent_kafka_topic.workday_sink.topic_name,
    confluent_kafka_topic.workday_response.topic_name,
    confluent_kafka_topic.workday_error.topic_name,
    confluent_kafka_topic.workday_connector_dlq.topic_name,
    confluent_kafka_topic.connect_configs.topic_name,
    confluent_kafka_topic.connect_offsets.topic_name,
    confluent_kafka_topic.connect_status.topic_name,
  ]
}

output "schemas_created" {
  description = "List of created schema subjects"
  value = [
    confluent_schema.eoi_source_value.subject_name,
    confluent_schema.eoi_source_key.subject_name,
    confluent_schema.flink_common_value.subject_name,
    confluent_schema.flink_common_key.subject_name,
    confluent_schema.flink_workday_value.subject_name,
    confluent_schema.flink_workday_key.subject_name,
    confluent_schema.flink_workday_dlq_value.subject_name,
    confluent_schema.flink_workday_dlq_key.subject_name,
    confluent_schema.workday_sink_value.subject_name,
    confluent_schema.workday_sink_key.subject_name,
    confluent_schema.workday_response_value.subject_name,
    confluent_schema.workday_error_value.subject_name,
    confluent_schema.workday_connector_dlq_value.subject_name,
  ]
}
