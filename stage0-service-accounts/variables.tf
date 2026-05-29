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

variable "kafka_cluster_id" {
  description = "Confluent Cloud Kafka cluster ID (e.g. lkc-xxxxxx)"
  type        = string
}

# ============================================================
# Schema Registry
# ============================================================

variable "schema_registry_id" {
  description = "Schema Registry cluster ID (e.g. lsrc-xxxxxx)"
  type        = string
}
