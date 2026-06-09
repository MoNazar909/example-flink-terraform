terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.10.0"
    }
  }
}

# ============================================================
# Provider Configuration
# ============================================================

provider "confluent" {}

# ============================================================
# Kafka Topics
# ============================================================

resource "confluent_kafka_topic" "eoi_source" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-eoi"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "604800000"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "flink_common" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-flink-common"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "604800000"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "flink_workday" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-flink-workday"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "604800000"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "flink_workday_dlq" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-flink-workday-dlq"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "-1"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "workday_sink" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-workday"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "604800000"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "workday_response" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-workday-response"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "604800000"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "workday_error" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-workday-error"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "604800000"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "workday_connector_dlq" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-eda-bentechdata-workday-dlq"
  partitions_count = 6
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "retention.ms"        = "-1"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

# ============================================================
# Kafka Connect Internal Topics
# No schemas — Connect manages serialization internally
# ============================================================

resource "confluent_kafka_topic" "connect_configs" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-connect-configs"
  partitions_count = 1
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "cleanup.policy"      = "compact"
    "min.insync.replicas" = "2"
    "retention.ms"        = "-1"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "connect_offsets" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-connect-offsets"
  partitions_count = 25
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "cleanup.policy"      = "compact"
    "min.insync.replicas" = "2"
    "retention.ms"        = "-1"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

resource "confluent_kafka_topic" "connect_status" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }
  topic_name       = "standard-connect-status"
  partitions_count = 5
  rest_endpoint    = var.kafka_rest_endpoint
  config = {
    "cleanup.policy"      = "compact"
    "min.insync.replicas" = "2"
    "retention.ms"        = "-1"
  }
  credentials {
    key    = var.cluster_admin_kafka_key_id
    secret = var.cluster_admin_kafka_key_secret
  }
}

# ============================================================
# Schema Registry — Value Schemas
# ============================================================

resource "confluent_schema" "eoi_source_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.eoi_source.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/evidence_of_insurability.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.eoi_source]
}

resource "confluent_schema" "flink_common_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_common.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.flink_common]
}

resource "confluent_schema" "flink_workday_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_workday.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.flink_workday]
}

resource "confluent_schema" "flink_workday_dlq_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_workday_dlq.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.flink_workday_dlq]
}

resource "confluent_schema" "workday_sink_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_sink.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_bentechsink_workday.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.workday_sink]
}

resource "confluent_schema" "workday_response_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_response.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/bentechsink_response.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.workday_response]
}

resource "confluent_schema" "workday_error_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_error.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/bentechsink_response.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.workday_error]
}

resource "confluent_schema" "workday_connector_dlq_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_connector_dlq.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/bentechsink_response.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.workday_connector_dlq]
}

# ============================================================
# Schema Registry — Key Schemas
# Registering a key schema causes Confluent Cloud Flink to
# auto-discover kafka_key as the PRIMARY KEY on each table.
# ============================================================

resource "confluent_schema" "eoi_source_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.eoi_source.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.eoi_source]
}

resource "confluent_schema" "flink_common_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_common.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.flink_common]
}

resource "confluent_schema" "flink_workday_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_workday.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.flink_workday]
}

resource "confluent_schema" "flink_workday_dlq_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_workday_dlq.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.flink_workday_dlq]
}

resource "confluent_schema" "workday_sink_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_sink.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")
  credentials {
    key    = var.sr_admin_sr_key_id
    secret = var.sr_admin_sr_key_secret
  }
  depends_on = [confluent_kafka_topic.workday_sink]
}

