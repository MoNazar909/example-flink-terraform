# ============================================================
# Outputs — copy these values into stage2/terraform.tfvars
# ============================================================

output "flink_compute_pool_id" {
  description = "Copy this into stage2/terraform.tfvars as flink_compute_pool_id"
  value       = confluent_flink_compute_pool.standard_poc.id
}

output "kafka_topics_created" {
  description = "List of created Kafka topic names"
  value = [
    confluent_kafka_topic.eoi_source.topic_name,
    confluent_kafka_topic.flink_common.topic_name,
    confluent_kafka_topic.flink_common_dlq.topic_name,
    confluent_kafka_topic.flink_workday.topic_name,
    confluent_kafka_topic.flink_workday_dlq.topic_name,
    confluent_kafka_topic.flink_routing_dlq.topic_name,
    confluent_kafka_topic.workday_sink.topic_name,
    confluent_kafka_topic.flink_hcm2.topic_name,
  ]
}

output "schemas_created" {
  description = "List of created schema subjects (value + key)"
  value = [
    confluent_schema.eoi_source_value.subject_name,
    confluent_schema.eoi_source_key.subject_name,
    confluent_schema.flink_common_value.subject_name,
    confluent_schema.flink_common_key.subject_name,
    confluent_schema.flink_common_dlq_value.subject_name,
    confluent_schema.flink_common_dlq_key.subject_name,
    confluent_schema.flink_workday_value.subject_name,
    confluent_schema.flink_workday_key.subject_name,
    confluent_schema.flink_workday_dlq_value.subject_name,
    confluent_schema.flink_workday_dlq_key.subject_name,
    confluent_schema.flink_routing_dlq_value.subject_name,
    confluent_schema.flink_routing_dlq_key.subject_name,
    confluent_schema.workday_sink_value.subject_name,
    confluent_schema.workday_sink_key.subject_name,
    confluent_schema.flink_hcm2_value.subject_name,
    confluent_schema.flink_hcm2_key.subject_name,
  ]
}
