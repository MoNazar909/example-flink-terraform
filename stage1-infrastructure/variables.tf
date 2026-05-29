# ============================================================
# Kafka Cluster
# ============================================================

variable "kafka_cluster_id" {
  description = "Confluent Cloud Kafka cluster ID (e.g. lkc-xxxxxx)"
  type        = string
}

variable "kafka_rest_endpoint" {
  description = "Kafka cluster REST endpoint URL (e.g. https://pkc-xxxxx.<region>.aws.confluent.cloud:443)"
  type        = string
}

# ============================================================
# Schema Registry
# ============================================================

variable "schema_registry_id" {
  description = "Schema Registry ID (e.g. lsrc-xxxxxx)"
  type        = string
}

variable "schema_registry_url" {
  description = "Schema Registry REST endpoint URL"
  type        = string
}

# ============================================================
# Admin API Keys — from stage0 outputs
# ============================================================

variable "cluster_admin_kafka_key_id" {
  description = "Kafka API key ID for standard-cluster-admin-sa — from stage0 output cluster_admin_kafka_key_id"
  type        = string
  sensitive   = true
}

variable "cluster_admin_kafka_key_secret" {
  description = "Kafka API key secret for standard-cluster-admin-sa — from stage0 output cluster_admin_kafka_key_secret"
  type        = string
  sensitive   = true
}

variable "sr_admin_key_id" {
  description = "SR API key ID for standard-sr-admin-sa — from stage0 output sr_admin_sr_key_id"
  type        = string
  sensitive   = true
}

variable "sr_admin_key_secret" {
  description = "SR API key secret for standard-sr-admin-sa — from stage0 output sr_admin_sr_key_secret"
  type        = string
  sensitive   = true
}
