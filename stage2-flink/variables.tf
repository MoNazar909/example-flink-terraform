# ============================================================
# Confluent Cloud API Credentials
# ============================================================

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud resource management scope)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret (Cloud resource management scope)"
  type        = string
  sensitive   = true
}

# ============================================================
# Environment and Cluster
# ============================================================

variable "environment_id" {
  description = "Confluent Cloud environment ID (e.g. env-xxxxxx)"
  type        = string
}

variable "organization_id" {
  description = "Confluent Cloud organization ID (UUID format)"
  type        = string
}

variable "kafka_cluster_id" {
  description = "Kafka cluster ID — used as the Flink database name"
  type        = string
}

# ============================================================
# Flink Credentials
# ============================================================

variable "flink_rest_endpoint" {
  description = "Flink REST endpoint URL"
  type        = string
}

variable "flink_api_key" {
  description = "Flink API key (Flink region scope)"
  type        = string
  sensitive   = true
}

variable "flink_api_secret" {
  description = "Flink API secret (Flink region scope)"
  type        = string
  sensitive   = true
}

variable "flink_compute_pool_id" {
  description = "Flink compute pool ID — from stage1 output flink_compute_pool_id"
  type        = string
}

variable "flink_principal_id" {
  description = "Principal ID for Flink statements — your Confluent Cloud user ID (e.g. u-xxxxxx)"
  type        = string
}

# ============================================================
# UDF
# ============================================================

variable "udf_artifact_id" {
  description = "Confluent Cloud artifact ID for the Workday UDF JAR (e.g. cfa-xxxxxx). Must be uploaded manually before terraform apply."
  type        = string
}
