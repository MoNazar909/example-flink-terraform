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
    confluent_kafka_topic.flink_workday.topic_name,
    confluent_kafka_topic.flink_workday_dlq.topic_name,
    confluent_kafka_topic.workday_sink.topic_name,
    confluent_kafka_topic.flink_hcm2.topic_name,
  ]
}

output "schemas_created" {
  description = "List of created schema subjects"
  value = [
    confluent_schema.eoi_source.subject_name,
    confluent_schema.flink_common.subject_name,
    confluent_schema.flink_workday.subject_name,
    confluent_schema.flink_workday_dlq.subject_name,
    confluent_schema.workday_sink.subject_name,
    confluent_schema.flink_hcm2.subject_name,
  ]
}
