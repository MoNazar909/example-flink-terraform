# ============================================================
# Outputs — copy these values into stage1 and stage2 terraform.tfvars
# ============================================================

# ============================================================
# standard-cluster-admin-sa
# API key used by stage1 to create Kafka topics
# ============================================================

output "cluster_admin_kafka_key_id" {
  description = "Copy into stage1/terraform.tfvars as cluster_admin_kafka_key_id"
  value       = confluent_api_key.cluster_admin_kafka_key.id
}

output "cluster_admin_kafka_key_secret" {
  description = "Copy into stage1/terraform.tfvars as cluster_admin_kafka_key_secret"
  value       = confluent_api_key.cluster_admin_kafka_key.secret
  sensitive   = true
}

# ============================================================
# standard-sr-admin-sa
# API key used by stage1 to register schemas
# ============================================================

output "sr_admin_sr_key_id" {
  description = "Copy into stage1/terraform.tfvars as sr_admin_key_id"
  value       = confluent_api_key.sr_admin_sr_key.id
}

output "sr_admin_sr_key_secret" {
  description = "Copy into stage1/terraform.tfvars as sr_admin_key_secret"
  value       = confluent_api_key.sr_admin_sr_key.secret
  sensitive   = true
}

# ============================================================
# standard-eda-source-sa
# Handed to the upstream EDA team
# ============================================================

output "eda_source_kafka_key_id" {
  description = "Kafka API key for standard-eda-source-sa — hand to EDA team"
  value       = confluent_api_key.eda_source_kafka_key.id
}

output "eda_source_kafka_key_secret" {
  description = "Kafka API secret for standard-eda-source-sa — hand to EDA team"
  value       = confluent_api_key.eda_source_kafka_key.secret
  sensitive   = true
}

output "eda_source_sr_key_id" {
  description = "SR API key for standard-eda-source-sa — hand to EDA team"
  value       = confluent_api_key.eda_source_sr_key.id
}

output "eda_source_sr_key_secret" {
  description = "SR API secret for standard-eda-source-sa — hand to EDA team"
  value       = confluent_api_key.eda_source_sr_key.secret
  sensitive   = true
}

# ============================================================
# standard-flink-bentech-sa
# SA ID and Flink API key used by stage2 to submit statements
# ============================================================

output "flink_bentech_sa_id" {
  description = "Copy into stage2/terraform.tfvars as flink_principal_id"
  value       = confluent_service_account.flink_bentech.id
}

output "flink_bentech_flink_key_id" {
  description = "Copy into stage2/terraform.tfvars as flink_api_key"
  value       = confluent_api_key.flink_bentech_flink_key.id
}

output "flink_bentech_flink_key_secret" {
  description = "Copy into stage2/terraform.tfvars as flink_api_secret"
  value       = confluent_api_key.flink_bentech_flink_key.secret
  sensitive   = true
}

# ============================================================
# standard-connect-sa
# Mounted into the CFK Connect worker pod
# ============================================================

output "connect_kafka_key_id" {
  description = "Kafka API key for standard-connect-sa — mount into Connect worker pod"
  value       = confluent_api_key.connect_kafka_key.id
}

output "connect_kafka_key_secret" {
  description = "Kafka API secret for standard-connect-sa — mount into Connect worker pod"
  value       = confluent_api_key.connect_kafka_key.secret
  sensitive   = true
}

output "connect_sr_key_id" {
  description = "SR API key for standard-connect-sa — mount into Connect worker pod"
  value       = confluent_api_key.connect_sr_key.id
}

output "connect_sr_key_secret" {
  description = "SR API secret for standard-connect-sa — mount into Connect worker pod"
  value       = confluent_api_key.connect_sr_key.secret
  sensitive   = true
}

# ============================================================
# standard-workday-connector-sa
# Set as kafka.service.account.id on the Workday HTTP Sink connector
# ============================================================

output "workday_connector_kafka_key_id" {
  description = "Kafka API key for standard-workday-connector-sa — set as connector principal"
  value       = confluent_api_key.workday_connector_kafka_key.id
}

output "workday_connector_kafka_key_secret" {
  description = "Kafka API secret for standard-workday-connector-sa — set as connector principal"
  value       = confluent_api_key.workday_connector_kafka_key.secret
  sensitive   = true
}

output "workday_connector_sr_key_id" {
  description = "SR API key for standard-workday-connector-sa"
  value       = confluent_api_key.workday_connector_sr_key.id
}

output "workday_connector_sr_key_secret" {
  description = "SR API secret for standard-workday-connector-sa"
  value       = confluent_api_key.workday_connector_sr_key.secret
  sensitive   = true
}
