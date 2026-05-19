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

  statement = <<-EOT
    CREATE FUNCTION IF NOT EXISTS workdayeoidataconversion
    AS 'com.standardinsurance.eoi.flink.udf.WorkdayEOIDataConversionUDF'
    LANGUAGE JAVA
    USING JAR 'confluent-artifact://${var.udf_artifact_id}';
  EOT

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = var.kafka_cluster_id
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }
}

# ============================================================
# Step 1: Flatten EOI source messages into common firehose topic
# Reads from: standard.eda.hcmdata.eoi
# Writes to:  standard.eda.hcmdata.flink.common
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

  statement = <<-EOT
    INSERT INTO `standard.eda.hcmdata.flink.common`
    (flattened_event, exception, target_HCM, group_id, event_type, correlation_id)
    SELECT
      JSON_OBJECT(
        KEY 'msg_source'              VALUE event_MetaData.msg_source,
        KEY 'message_version'         VALUE event_MetaData.message_version,
        KEY 'correlation_id'          VALUE event_MetaData.correlation_id,
        KEY 'group_id'                VALUE event_MetaData.group_id,
        KEY 'target_hcm'              VALUE event_MetaData.target_hcm,
        KEY 'target_api_url'          VALUE event_MetaData.target_api_url,
        KEY 'target_api_token_url'    VALUE event_MetaData.target_api_token_url,
        KEY 'event_type'              VALUE event_MetaData.event_type,
        KEY 'content_type'            VALUE event_MetaData.content_type,
        KEY 'worker_id'               VALUE event_Data.worker_id,
        KEY 'worker_id_type'          VALUE event_Data.worker_id_type,
        KEY 'worker_descriptor'       VALUE event_Data.worker_descriptor,
        KEY 'benefit_plan_id'         VALUE event_Data.benefit_plan_id,
        KEY 'benefit_plan_id_type'    VALUE event_Data.benefit_plan_id_type,
        KEY 'benefit_plan_descriptor' VALUE event_Data.benefit_plan_descriptor,
        KEY 'approve_for_selected'    VALUE CAST(event_Data.approve_for_selected AS STRING),
        KEY 'deny_for_selected'       VALUE CAST(event_Data.deny_for_selected AS STRING),
        KEY 'eoi_decision_date'       VALUE event_Data.eoi_decision_date
      ),
      CAST(NULL AS STRING),
      event_MetaData.target_hcm,
      event_MetaData.group_id,
      event_MetaData.event_type,
      event_MetaData.correlation_id
    FROM `standard.eda.hcmdata.eoi`;
  EOT

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = var.kafka_cluster_id
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [confluent_flink_statement.register_udf]
}

# ============================================================
# Step 2: Route messages from common topic to HCM-specific topics
# Reads from: standard.eda.hcmdata.flink.common
# Writes to:  standard.eda.hcmdata.flink.workday
#             standard.eda.hcmdata.flink.hcm2
#             (add more HCM topics here for production)
# ============================================================

resource "confluent_flink_statement" "route_hcm" {
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

  statement = <<-EOT
    EXECUTE STATEMENT SET
    BEGIN

      -- Workday routing: filters messages where target_HCM = 'Workday'
      -- In production, duplicate this block for each of the 30 HCM systems
      INSERT INTO `standard.eda.hcmdata.flink.workday`
      (flattened_event, exception, target_HCM, group_id, event_type, correlation_id)
      SELECT
        flattened_event,
        exception,
        target_HCM,
        group_id,
        event_type,
        correlation_id
      FROM `standard.eda.hcmdata.flink.common`
      WHERE target_HCM = 'Workday';

      -- HCM2 routing: filters messages where target_HCM = 'HCM2'
      -- Replace 'HCM2' with the actual HCM system name (e.g. 'SAP', 'Oracle', etc.)
      INSERT INTO `standard.eda.hcmdata.flink.hcm2`
      (flattened_event, exception, target_HCM, group_id, event_type, correlation_id)
      SELECT
        flattened_event,
        exception,
        target_HCM,
        group_id,
        event_type,
        correlation_id
      FROM `standard.eda.hcmdata.flink.common`
      WHERE target_HCM = 'HCM2';

      -- TODO: Add routing blocks here for remaining HCM systems
      -- Example:
      -- INSERT INTO `standard.eda.hcmdata.flink.sap` (...) SELECT ... FROM `standard.eda.hcmdata.flink.common` WHERE target_HCM = 'SAP';
      -- INSERT INTO `standard.eda.hcmdata.flink.oracle` (...) SELECT ... FROM `standard.eda.hcmdata.flink.common` WHERE target_HCM = 'Oracle';

    END;
  EOT

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = var.kafka_cluster_id
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [confluent_flink_statement.flatten_eoi]
}

# ============================================================
# Step 3: Transform Workday messages to SOAP XML
# Reads from: standard.eda.hcmdata.flink.workday
# Success → standard.eda.hcmdata.workday (connector reads from here)
# Error   → standard.eda.hcmdata.flink.workday.dlq
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

  statement = <<-EOT
    EXECUTE STATEMENT SET
    BEGIN

      -- Success path: UDF returned non-null result with no exception
      -- Writes transformed SOAP XML payload to Workday sink topic
      -- for the HTTP Sink Connector to pick up and deliver to Workday
      INSERT INTO `standard.eda.hcmdata.workday`
      (flattened_event, exception, target_HCM_payload, target_HCM, group_id, event_type, correlation_id)
      SELECT
        fws.flattened_event,
        CAST(NULL AS STRING),
        JSON_VALUE(fws.conversion_result, '$.target_HCM_payload'),
        fws.target_HCM,
        fws.group_id,
        fws.event_type,
        fws.correlation_id
      FROM (
        SELECT
          flattened_event,
          exception,
          target_HCM,
          group_id,
          event_type,
          correlation_id,
          workdayeoidataconversion(flattened_event) AS conversion_result
        FROM `standard.eda.hcmdata.flink.workday`
      ) fws
      WHERE fws.conversion_result IS NOT NULL
      AND JSON_VALUE(fws.conversion_result, '$.exception') IS NULL;

      -- Error path: UDF returned null (catastrophic failure) OR
      -- UDF returned JSON but exception field is populated (transformation error)
      -- Writes to DLQ topic for manual investigation
      INSERT INTO `standard.eda.hcmdata.flink.workday.dlq`
      (flattened_event, exception, target_HCM, group_id, event_type, correlation_id)
      SELECT
        fws.flattened_event,
        JSON_VALUE(fws.conversion_result, '$.exception'),
        fws.target_HCM,
        fws.group_id,
        fws.event_type,
        fws.correlation_id
      FROM (
        SELECT
          flattened_event,
          exception,
          target_HCM,
          group_id,
          event_type,
          correlation_id,
          workdayeoidataconversion(flattened_event) AS conversion_result
        FROM `standard.eda.hcmdata.flink.workday`
      ) fws
      WHERE fws.conversion_result IS NULL
      OR JSON_VALUE(fws.conversion_result, '$.exception') IS NOT NULL;

    END;
  EOT

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = var.kafka_cluster_id
  }

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  depends_on = [confluent_flink_statement.route_hcm]
}
