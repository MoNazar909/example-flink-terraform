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
# All 7 Flink attributes must be set together
# ============================================================

provider "confluent" {
  cloud_api_key         = var.confluent_cloud_api_key
  cloud_api_secret      = var.confluent_cloud_api_secret
  flink_rest_endpoint   = var.flink_rest_endpoint
  flink_api_key         = var.flink_api_key
  flink_api_secret      = var.flink_api_secret
  organization_id       = var.organization_id
  environment_id        = var.environment_id
  flink_compute_pool_id = var.flink_compute_pool_id
  flink_principal_id    = var.flink_principal_id
}


# ============================================================
# Step 0: Register the UDF function
# Pre-requisite: Upload workday-data-conversion-udf-1.0.0-shaded.jar
# to Confluent Cloud Artifacts BEFORE running terraform apply.
# Set var.udf_artifact_id to the uploaded artifact ID.
# ============================================================

resource "confluent_flink_statement" "register_udf" {
  organization {
    id = var.organization_id
  }
  environment {
    id = var.environment_id
  }
  compute_pool {
    id = var.flink_compute_pool_id
  }
  principal {
    id = var.flink_principal_id
  }

  statement_name = "std-ins-register-udf"

  statement = templatefile("${path.module}/flink-sql/01_register_udf.sql", {
    udf_artifact_id = var.udf_artifact_id
  })

  properties = {
    "sql.current-catalog"  = var.flink_catalog
    "sql.current-database" = var.flink_database
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }
}

# ============================================================
# Step 1: Flatten EOI source messages into common topic
# Reads from: standard.eda.bentechdata.eoi
# Success  →  standard.eda.bentechdata.flink.common
# DLQ      →  standard.eda.bentechdata.eoi.dlq
#             (when event_MetaData or event_Data is null)
#
# kafka_key is built here from payload fields and carried
# through every downstream topic as the Kafka record key.
# Key schemas registered in Stage 1 mean Flink auto-discovers
# kafka_key as the PRIMARY KEY — no ALTER TABLE needed.
# ============================================================

resource "confluent_flink_statement" "flatten_eoi" {
  organization {
    id = var.organization_id
  }
  environment {
    id = var.environment_id
  }
  compute_pool {
    id = var.flink_compute_pool_id
  }
  principal {
    id = var.flink_principal_id
  }

  statement_name = "std-ins-flatten-eoi"

  statement = file("${path.module}/flink-sql/02_flatten_eoi.sql")

  properties = {
    "sql.current-catalog"  = var.flink_catalog
    "sql.current-database" = var.flink_database
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [confluent_flink_statement.register_udf]
}

# ============================================================
# Step 2: Route messages from common topic to BENTECH-specific topics
# Reads from: standard.eda.bentechdata.flink.common
# Workday  →  standard.eda.bentechdata.flink.workday
# BENTECH2 →  standard.eda.bentechdata.flink.bentech2
# DLQ      →  standard.eda.bentechdata.flink.common.dlq
#             (when target_BENTECH does not match any known system)
# ============================================================

resource "confluent_flink_statement" "route_bentech" {
  organization {
    id = var.organization_id
  }
  environment {
    id = var.environment_id
  }
  compute_pool {
    id = var.flink_compute_pool_id
  }
  principal {
    id = var.flink_principal_id
  }

  statement_name = "std-ins-route-bentech"

  statement = file("${path.module}/flink-sql/03_route_bentech.sql")

  properties = {
    "sql.current-catalog"  = var.flink_catalog
    "sql.current-database" = var.flink_database
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [confluent_flink_statement.flatten_eoi]
}

# ============================================================
# Step 3: Transform Workday messages to SOAP XML via UDF
# Reads from: standard.eda.bentechdata.flink.workday
# Success  →  standard.eda.bentechdata.workday (HTTP Sink Connector reads from here)
# DLQ      →  standard.eda.bentechdata.flink.workday.dlq
# ============================================================

resource "confluent_flink_statement" "transform_workday" {
  organization {
    id = var.organization_id
  }
  environment {
    id = var.environment_id
  }
  compute_pool {
    id = var.flink_compute_pool_id
  }
  principal {
    id = var.flink_principal_id
  }

  statement_name = "std-ins-transform-workday"

  statement = file("${path.module}/flink-sql/04_transform_workday.sql")

  properties = {
    "sql.current-catalog"  = var.flink_catalog
    "sql.current-database" = var.flink_database
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [confluent_flink_statement.route_bentech]
}
