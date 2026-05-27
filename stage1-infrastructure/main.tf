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

data "confluent_schema_registry_cluster" "standard_poc" {
  id = var.schema_registry_id
  environment {
    id = var.environment_id
  }
}

data "confluent_flink_region" "standard_poc" {
  cloud  = "AWS"
  region = "us-east-2"
}

# ============================================================
# Service Accounts
# ============================================================

resource "confluent_service_account" "terraform_manager" {
  display_name = "standard-poc-terraform-manager"
  description  = "Creates and manages Kafka topics and Schema Registry schemas"
}

resource "confluent_service_account" "flink_runner" {
  display_name = "standard-poc-flink-runner"
  description  = "Submits Flink SQL statements and reads/writes Kafka topics"
}

# ============================================================
# Role Bindings
# ============================================================

resource "confluent_role_binding" "terraform_manager_kafka_admin" {
  principal   = "User:${confluent_service_account.terraform_manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = data.confluent_kafka_cluster.standard_poc.rbac_crn
}

resource "confluent_role_binding" "terraform_manager_sr_owner" {
  principal   = "User:${confluent_service_account.terraform_manager.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=*"
}

resource "confluent_role_binding" "flink_runner_developer" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_environment.standard_poc.resource_name
}

resource "confluent_role_binding" "flink_runner_topic_read" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=*"
}

resource "confluent_role_binding" "flink_runner_topic_write" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=*"
}

resource "confluent_role_binding" "flink_runner_sr_read" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=*"
}

resource "confluent_role_binding" "flink_runner_transactional_id" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/transactional-id=*"
}

resource "confluent_role_binding" "flink_runner_consumer_group" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/group=*"
}

# ============================================================
# API Keys
# ============================================================

resource "confluent_api_key" "terraform_kafka_key" {
  display_name = "standard-poc-terraform-kafka-key"
  description  = "Kafka API key used by Terraform to manage topics"
  owner {
    id          = confluent_service_account.terraform_manager.id
    api_version = confluent_service_account.terraform_manager.api_version
    kind        = confluent_service_account.terraform_manager.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.standard_poc.id
    api_version = data.confluent_kafka_cluster.standard_poc.api_version
    kind        = data.confluent_kafka_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.terraform_manager_kafka_admin]
}

resource "confluent_api_key" "terraform_sr_key" {
  display_name = "standard-poc-terraform-sr-key"
  description  = "Schema Registry API key used by Terraform to register schemas"
  owner {
    id          = confluent_service_account.terraform_manager.id
    api_version = confluent_service_account.terraform_manager.api_version
    kind        = confluent_service_account.terraform_manager.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.standard_poc.id
    api_version = data.confluent_schema_registry_cluster.standard_poc.api_version
    kind        = data.confluent_schema_registry_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.terraform_manager_sr_owner]
}

resource "confluent_api_key" "flink_kafka_key" {
  display_name = "standard-poc-flink-kafka-key"
  description  = "Kafka API key used by the flink-runner SA to read/write topics"
  owner {
    id          = confluent_service_account.flink_runner.id
    api_version = confluent_service_account.flink_runner.api_version
    kind        = confluent_service_account.flink_runner.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.standard_poc.id
    api_version = data.confluent_kafka_cluster.standard_poc.api_version
    kind        = data.confluent_kafka_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.flink_runner_developer]
}

resource "confluent_api_key" "flink_key" {
  display_name = "standard-poc-flink-key"
  description  = "Flink API key used by the flink-runner SA to submit statements"
  owner {
    id          = confluent_service_account.flink_runner.id
    api_version = confluent_service_account.flink_runner.api_version
    kind        = confluent_service_account.flink_runner.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.standard_poc.id
    api_version = data.confluent_flink_region.standard_poc.api_version
    kind        = data.confluent_flink_region.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.flink_runner_developer]
}

# ============================================================
# Kafka Topics
# ============================================================

resource "confluent_kafka_topic" "eoi_source" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.eoi"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "flink_common" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.flink.common"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "eoi_dlq" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.eoi.dlq"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "2592000000" # 30 days — longer retention for DLQ investigation
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "flink_workday" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.flink.workday"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "flink_workday_dlq" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.flink.workday.dlq"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "2592000000" # 30 days
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "flink_common_dlq" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.flink.common.dlq"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "2592000000" # 30 days
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "workday_sink" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.workday"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "flink_bentech2" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.flink.bentech2"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
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
  schema        = file("${path.module}/schemas/eoi_source.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_common]
}

resource "confluent_schema" "eoi_dlq_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.eoi_dlq.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.eoi_dlq]
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_workday_dlq]
}

resource "confluent_schema" "flink_common_dlq_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_common_dlq.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_common_dlq]
}

resource "confluent_schema" "workday_sink_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_sink.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/bentech_sink_workday.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_sink]
}

resource "confluent_schema" "flink_bentech2_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_bentech2.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/flink_common.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_bentech2]
}

# ============================================================
# Schema Registry — Key Schemas (all topics)
# Registering a key schema causes Confluent Cloud Flink to
# auto-discover kafka_key as the PRIMARY KEY on each table,
# so no ALTER TABLE statements are needed in Stage 2.
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_common]
}

resource "confluent_schema" "eoi_dlq_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.eoi_dlq.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.eoi_dlq]
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_workday_dlq]
}

resource "confluent_schema" "flink_common_dlq_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_common_dlq.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_common_dlq]
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
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_sink]
}

resource "confluent_schema" "flink_bentech2_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.flink_bentech2.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.flink_bentech2]
}

# ============================================================
# Connector Response Topics
# These are written to by the HTTP Sink Connector after it
# posts to Workday. Not managed by Flink.
# ============================================================

resource "confluent_kafka_topic" "workday_response" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.workday.response"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "604800000"
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "workday_connector_dlq" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.workday.dlq"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "2592000000" # 30 days
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

resource "confluent_kafka_topic" "workday_error" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.standard_poc.id
  }
  topic_name       = "standard.eda.bentechdata.workday.error"
  partitions_count = 6
  rest_endpoint    = data.confluent_kafka_cluster.standard_poc.rest_endpoint

  config = {
    "retention.ms" = "2592000000" # 30 days
  }

  credentials {
    key    = confluent_api_key.terraform_kafka_key.id
    secret = confluent_api_key.terraform_kafka_key.secret
  }
}

# Value schemas — connector response topics

resource "confluent_schema" "workday_response_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_response.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/bentech_sink_response.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_response]
}

resource "confluent_schema" "workday_connector_dlq_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_connector_dlq.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/bentech_sink_response.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_connector_dlq]
}

resource "confluent_schema" "workday_error_value" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_error.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/bentech_sink_response.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_error]
}

# Key schemas — connector response topics

resource "confluent_schema" "workday_response_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_response.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_response]
}

resource "confluent_schema" "workday_connector_dlq_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_connector_dlq.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_connector_dlq]
}

resource "confluent_schema" "workday_error_key" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }
  rest_endpoint = var.schema_registry_url
  subject_name  = "${confluent_kafka_topic.workday_error.topic_name}-key"
  format        = "AVRO"
  schema        = file("${path.module}/schemas/kafka_key.avsc")

  credentials {
    key    = confluent_api_key.terraform_sr_key.id
    secret = confluent_api_key.terraform_sr_key.secret
  }

  depends_on = [confluent_kafka_topic.workday_error]
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
