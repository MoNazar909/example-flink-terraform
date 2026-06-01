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
# Data Sources
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
# 1. standard-cluster-admin-sa — Platform Administration
# CloudClusterAdmin on the Kafka cluster
# ============================================================

resource "confluent_service_account" "cluster_admin" {
  display_name = "standard-cluster-admin-sa"
  description  = "Used by Standard Insurance Platform Team for Kafka cluster administration activities"
}

resource "confluent_role_binding" "cluster_admin_kafka_admin" {
  principal   = "User:${confluent_service_account.cluster_admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = data.confluent_kafka_cluster.standard_poc.rbac_crn
}

resource "confluent_api_key" "cluster_admin_kafka_key" {
  display_name = "standard-cluster-admin-sa-api-key"
  description  = "Issued to: standard-cluster-admin-sa, Owner: Standard Insurance Platform Team"
  owner {
    id          = confluent_service_account.cluster_admin.id
    api_version = confluent_service_account.cluster_admin.api_version
    kind        = confluent_service_account.cluster_admin.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.standard_poc.id
    api_version = data.confluent_kafka_cluster.standard_poc.api_version
    kind        = data.confluent_kafka_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.cluster_admin_kafka_admin]
}

# ============================================================
# 2. standard-sr-admin-sa — Schema Registry Administration
# ResourceOwner on all SR subjects
# ============================================================

resource "confluent_service_account" "sr_admin" {
  display_name = "standard-sr-admin-sa"
  description  = "Used by Standard Insurance Platform Team for Schema Registry administration activities"
}

resource "confluent_role_binding" "sr_admin_resource_owner" {
  principal   = "User:${confluent_service_account.sr_admin.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=*"
}

resource "confluent_api_key" "sr_admin_sr_key" {
  display_name = "standard-sr-admin-sa-api-key"
  description  = "Issued to: standard-sr-admin-sa, Owner: Standard Insurance Platform Team"
  owner {
    id          = confluent_service_account.sr_admin.id
    api_version = confluent_service_account.sr_admin.api_version
    kind        = confluent_service_account.sr_admin.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.standard_poc.id
    api_version = data.confluent_schema_registry_cluster.standard_poc.api_version
    kind        = data.confluent_schema_registry_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.sr_admin_resource_owner]
}

# ============================================================
# 3. standard-eda-source-sa — EOI Event Producer
# DeveloperWrite on source topic; DeveloperRead+Write on its SR subjects
# ============================================================

resource "confluent_service_account" "eda_source" {
  display_name = "standard-eda-source-sa"
  description  = "Used by EDA source system to publish EOI events to standard-eda-bentechdata-eoi"
}

resource "confluent_role_binding" "eda_source_topic_write" {
  principal   = "User:${confluent_service_account.eda_source.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-eoi"
}

resource "confluent_role_binding" "eda_source_sr_read" {
  principal   = "User:${confluent_service_account.eda_source.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-eoi-*"
}

resource "confluent_role_binding" "eda_source_sr_write" {
  principal   = "User:${confluent_service_account.eda_source.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-eoi-*"
}

resource "confluent_api_key" "eda_source_kafka_key" {
  display_name = "standard-eda-source-sa-kafka-api-key"
  description  = "Issued to: standard-eda-source-sa, Owner: Standard Insurance EDA Team"
  owner {
    id          = confluent_service_account.eda_source.id
    api_version = confluent_service_account.eda_source.api_version
    kind        = confluent_service_account.eda_source.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.standard_poc.id
    api_version = data.confluent_kafka_cluster.standard_poc.api_version
    kind        = data.confluent_kafka_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.eda_source_topic_write]
}

resource "confluent_api_key" "eda_source_sr_key" {
  display_name = "standard-eda-source-sa-sr-api-key"
  description  = "Issued to: standard-eda-source-sa, Owner: Standard Insurance EDA Team"
  owner {
    id          = confluent_service_account.eda_source.id
    api_version = confluent_service_account.eda_source.api_version
    kind        = confluent_service_account.eda_source.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.standard_poc.id
    api_version = data.confluent_schema_registry_cluster.standard_poc.api_version
    kind        = data.confluent_schema_registry_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.eda_source_sr_read, confluent_role_binding.eda_source_sr_write]
}

# ============================================================
# 4. standard-flink-bentech-sa — Flink Stream Processor
# FlinkDeveloper on environment; scoped topic+SR access for all
# Flink intermediate topics and the Workday sink topic
# ============================================================

resource "confluent_service_account" "flink_bentech" {
  display_name = "standard-flink-bentech-sa"
  description  = "Flink processor for EOI events: reads eoi topic, reads/writes flink-* intermediate topics, writes to workday sink topic"
}

resource "confluent_role_binding" "flink_bentech_flink_developer" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_environment.standard_poc.resource_name
}

resource "confluent_role_binding" "flink_bentech_eoi_topic_read" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-eoi"
}

resource "confluent_role_binding" "flink_bentech_flink_topics_read" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-flink-*"
}

resource "confluent_role_binding" "flink_bentech_flink_topics_write" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-flink-*"
}

# Flink Step 3 (04_transform_workday.sql) writes its success output to this topic
resource "confluent_role_binding" "flink_bentech_workday_sink_topic_write" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-workday"
}

resource "confluent_role_binding" "flink_bentech_eoi_sr_read" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-eoi-*"
}

resource "confluent_role_binding" "flink_bentech_flink_sr_read" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-flink-*"
}

resource "confluent_role_binding" "flink_bentech_flink_sr_write" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-flink-*"
}

# Avro serializer calls POST /subjects/standard-eda-bentechdata-workday-{key,value}/versions at startup
resource "confluent_role_binding" "flink_bentech_workday_sink_sr_read" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-*"
}

resource "confluent_role_binding" "flink_bentech_workday_sink_sr_write" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-*"
}

resource "confluent_role_binding" "flink_bentech_consumer_group" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/group=confluent-flink*"
}

resource "confluent_role_binding" "flink_bentech_transactional_id_read" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/transactional-id=_confluent-flink_*"
}

resource "confluent_role_binding" "flink_bentech_transactional_id_write" {
  principal   = "User:${confluent_service_account.flink_bentech.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/transactional-id=_confluent-flink_*"
}

resource "confluent_api_key" "flink_bentech_flink_key" {
  display_name = "standard-flink-bentech-sa-api-key"
  description  = "Issued to: standard-flink-bentech-sa for EOI Flink statement execution, Owner: Standard Insurance Platform Team"
  owner {
    id          = confluent_service_account.flink_bentech.id
    api_version = confluent_service_account.flink_bentech.api_version
    kind        = confluent_service_account.flink_bentech.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.standard_poc.id
    api_version = data.confluent_flink_region.standard_poc.api_version
    kind        = data.confluent_flink_region.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.flink_bentech_flink_developer]
}

# ============================================================
# 5. standard-connect-sa — Kafka Connect Worker
# Scoped to Connect internal housekeeping topics only
# ============================================================

resource "confluent_service_account" "connect" {
  display_name = "standard-connect-sa"
  description  = "CFK Connect worker SA for internal topics (configs, offsets, status). Key mounted into Connect worker pod."
}

resource "confluent_role_binding" "connect_configs_read" {
  principal   = "User:${confluent_service_account.connect.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-connect-configs"
}

resource "confluent_role_binding" "connect_configs_write" {
  principal   = "User:${confluent_service_account.connect.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-connect-configs"
}

resource "confluent_role_binding" "connect_offsets_read" {
  principal   = "User:${confluent_service_account.connect.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-connect-offsets"
}

resource "confluent_role_binding" "connect_offsets_write" {
  principal   = "User:${confluent_service_account.connect.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-connect-offsets"
}

resource "confluent_role_binding" "connect_status_read" {
  principal   = "User:${confluent_service_account.connect.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-connect-status"
}

resource "confluent_role_binding" "connect_status_write" {
  principal   = "User:${confluent_service_account.connect.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-connect-status"
}

resource "confluent_role_binding" "connect_consumer_group" {
  principal   = "User:${confluent_service_account.connect.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/group=standard-connect"
}

resource "confluent_api_key" "connect_kafka_key" {
  display_name = "standard-connect-sa-kafka-api-key"
  description  = "Issued to: standard-connect-sa, Owner: Standard Insurance Platform Team"
  owner {
    id          = confluent_service_account.connect.id
    api_version = confluent_service_account.connect.api_version
    kind        = confluent_service_account.connect.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.standard_poc.id
    api_version = data.confluent_kafka_cluster.standard_poc.api_version
    kind        = data.confluent_kafka_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.connect_configs_read]
}

resource "confluent_api_key" "connect_sr_key" {
  display_name = "standard-connect-sa-sr-api-key"
  description  = "Issued to: standard-connect-sa, Owner: Standard Insurance Platform Team"
  owner {
    id          = confluent_service_account.connect.id
    api_version = confluent_service_account.connect.api_version
    kind        = confluent_service_account.connect.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.standard_poc.id
    api_version = data.confluent_schema_registry_cluster.standard_poc.api_version
    kind        = data.confluent_schema_registry_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
}

# ============================================================
# 6. standard-workday-connector-sa — Workday Connector Principal
# Reads from the Workday sink topic; writes all outcome topics
# ============================================================

resource "confluent_service_account" "workday_connector" {
  display_name = "standard-workday-connector-sa"
  description  = "Workday HTTP Sink connector principal: reads workday topic, writes response/error/dlq outcome topics."
}

resource "confluent_role_binding" "workday_connector_sink_topic_read" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-workday"
}

resource "confluent_role_binding" "workday_connector_response_topic_write" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-workday-response"
}

resource "confluent_role_binding" "workday_connector_error_topic_write" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-workday-error"
}

resource "confluent_role_binding" "workday_connector_dlq_topic_write" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/topic=standard-eda-bentechdata-workday-dlq"
}

resource "confluent_role_binding" "workday_connector_sink_sr_read" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-*"
}

resource "confluent_role_binding" "workday_connector_response_sr_read" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-response-*"
}

resource "confluent_role_binding" "workday_connector_response_sr_write" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-response-*"
}

resource "confluent_role_binding" "workday_connector_error_sr_read" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-error-*"
}

resource "confluent_role_binding" "workday_connector_error_sr_write" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-error-*"
}

resource "confluent_role_binding" "workday_connector_dlq_sr_read" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-dlq-*"
}

resource "confluent_role_binding" "workday_connector_dlq_sr_write" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.standard_poc.resource_name}/subject=standard-eda-bentechdata-workday-dlq-*"
}

resource "confluent_role_binding" "workday_connector_consumer_group" {
  principal   = "User:${confluent_service_account.workday_connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.standard_poc.rbac_crn}/kafka=${data.confluent_kafka_cluster.standard_poc.id}/group=connect-workday-eoi-sink*"
}

resource "confluent_api_key" "workday_connector_kafka_key" {
  display_name = "standard-workday-connector-sa-kafka-api-key"
  description  = "Issued to: standard-workday-connector-sa for Workday HTTP Sink connector principal auth, Owner: Standard Insurance Platform Team"
  owner {
    id          = confluent_service_account.workday_connector.id
    api_version = confluent_service_account.workday_connector.api_version
    kind        = confluent_service_account.workday_connector.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.standard_poc.id
    api_version = data.confluent_kafka_cluster.standard_poc.api_version
    kind        = data.confluent_kafka_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.workday_connector_sink_topic_read]
}

resource "confluent_api_key" "workday_connector_sr_key" {
  display_name = "standard-workday-connector-sa-sr-api-key"
  description  = "Issued to: standard-workday-connector-sa for Schema Registry read access, Owner: Standard Insurance Platform Team"
  owner {
    id          = confluent_service_account.workday_connector.id
    api_version = confluent_service_account.workday_connector.api_version
    kind        = confluent_service_account.workday_connector.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.standard_poc.id
    api_version = data.confluent_schema_registry_cluster.standard_poc.api_version
    kind        = data.confluent_schema_registry_cluster.standard_poc.kind
    environment {
      id = var.environment_id
    }
  }
  depends_on = [confluent_role_binding.workday_connector_sink_sr_read]
}
