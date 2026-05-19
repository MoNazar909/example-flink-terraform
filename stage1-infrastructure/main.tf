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

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# ============================================================
# Data Sources — reference existing environment and cluster
# ============================================================

data "confluent_environment" "standard_poc" {
  id = var.environment_id
}

data "confluent_kafka_cluster" "standard_poc" {
  id = var.kafka_cluster_id
  environment {
    id = var.environment_id
  }
}

# ============================================================
# Kafka Topics
# ============================================================

resource "confluent_kafka_topic" "eoi_source" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.hcmdata.eoi"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "flink_common" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.hcmdata.flink.common"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "flink_workday" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.hcmdata.flink.workday"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "flink_workday_dlq" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.hcmdata.flink.workday.dlq"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "workday_sink" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.hcmdata.workday"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

resource "confluent_kafka_topic" "flink_hcm2" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.hcmdata.flink.hcm2"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

# ============================================================
# Schema Registry Schemas
# ============================================================

resource "confluent_schema" "eoi_source" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.eoi_source.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/eoi_source.avsc")

  credentials {
    key    = var.schema_registry_api_key
    secret = var.schema_registry_api_secret
  }

  depends_on = [confluent_kafka_topic.eoi_source]
}

resource "confluent_schema" "flink_common" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_common.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")

  credentials {
    key    = var.schema_registry_api_key
    secret = var.schema_registry_api_secret
  }

  depends_on = [confluent_kafka_topic.flink_common]
}

resource "confluent_schema" "flink_workday" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_workday.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")

  credentials {
    key    = var.schema_registry_api_key
    secret = var.schema_registry_api_secret
  }

  depends_on = [confluent_kafka_topic.flink_workday]
}

resource "confluent_schema" "flink_workday_dlq" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_workday_dlq.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")

  credentials {
    key    = var.schema_registry_api_key
    secret = var.schema_registry_api_secret
  }

  depends_on = [confluent_kafka_topic.flink_workday_dlq]
}

resource "confluent_schema" "workday_sink" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_sink.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/hcm_sink_workday.avsc")

  credentials {
    key    = var.schema_registry_api_key
    secret = var.schema_registry_api_secret
  }

  depends_on = [confluent_kafka_topic.workday_sink]
}

resource "confluent_schema" "flink_hcm2" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_hcm2.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")

  credentials {
    key    = var.schema_registry_api_key
    secret = var.schema_registry_api_secret
  }

  depends_on = [confluent_kafka_topic.flink_hcm2]
}

# ============================================================
# Flink Compute Pool
# ============================================================

resource "confluent_flink_compute_pool" "standard_poc" {
  display_name = "standard-insurance-flink-poc"
  cloud        = "AWS"
  region       = "us-east-2"
  max_cfu      = 10

  environment {
    id = var.environment_id
  }
}
