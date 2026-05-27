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
  description = "Kafka cluster ID (e.g. lkc-xxxxxx)"
  type        = string
}

variable "flink_catalog" {
  description = "Flink SQL catalog name — shown in top-right of Flink SQL editor (e.g. default)"
  type        = string
}

variable "flink_database" {
  description = "Flink SQL database name — Kafka cluster display name shown in Flink SQL editor (e.g. cluster_0)"
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
  description = "Service account ID for Flink statements — from stage1 output flink_runner_sa_id (e.g. sa-xxxxxx)"
  type        = string
}

# ============================================================
# UDF
# ============================================================

variable "udf_artifact_id" {
  description = "Confluent Cloud artifact ID for the Workday UDF JAR (e.g. cfa-xxxxxx). Must be uploaded manually before terraform apply."
  type        = string
}
