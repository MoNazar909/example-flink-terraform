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
# Kafka Credentials
# ============================================================

variable "kafka_api_key" {
  description = "Kafka cluster API key"
  type        = string
  sensitive   = true
}

variable "kafka_api_secret" {
  description = "Kafka cluster API secret"
  type        = string
  sensitive   = true
}

# ============================================================
# Schema Registry Credentials
# ============================================================

variable "schema_registry_id" {
  description = "Schema Registry ID (e.g. lsrc-xxxxxx)"
  type        = string
}

variable "schema_registry_url" {
  description = "Schema Registry REST endpoint URL"
  type        = string
}

variable "schema_registry_api_key" {
  description = "Schema Registry API key"
  type        = string
  sensitive   = true
}

variable "schema_registry_api_secret" {
  description = "Schema Registry API secret"
  type        = string
  sensitive   = true
}
