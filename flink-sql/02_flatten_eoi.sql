-- Step 1: Flatten EOI source messages into common topic
-- Reads from:  standard.eda.bentechdata.eoi
-- Success  →   standard.eda.bentechdata.flink.common
-- DLQ      →   standard.eda.bentechdata.eoi.dlq  (when event_MetaData or event_Data is null)

EXECUTE STATEMENT SET
BEGIN

  -- Success path: both metadata and data are present
  INSERT INTO `standard.eda.bentechdata.flink.common`
  (kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id)
  SELECT
    CONCAT(event_MetaData.group_id, '-', event_Data.worker_id, '-', event_MetaData.target_bentech, '-', event_MetaData.event_type),
    JSON_OBJECT(
      KEY 'msg_source'              VALUE event_MetaData.msg_source,
      KEY 'message_version'         VALUE event_MetaData.message_version,
      KEY 'correlation_id'          VALUE event_MetaData.correlation_id,
      KEY 'group_id'                VALUE event_MetaData.group_id,
      KEY 'target_bentech'          VALUE event_MetaData.target_bentech,
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
    event_MetaData.target_bentech,
    event_MetaData.group_id,
    event_MetaData.event_type,
    event_MetaData.correlation_id
  FROM `standard.eda.bentechdata.eoi`
  WHERE event_MetaData IS NOT NULL AND event_Data IS NOT NULL;

  -- DLQ path: event_MetaData or event_Data is null
  INSERT INTO `standard.eda.bentechdata.eoi.dlq`
  (kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id)
  SELECT
    CONCAT(
      COALESCE(event_MetaData.group_id,       'unknown'), '-',
      COALESCE(event_Data.worker_id,           'unknown'), '-',
      COALESCE(event_MetaData.target_bentech,  'unknown'), '-',
      COALESCE(event_MetaData.event_type,      'unknown')
    ),
    CAST(NULL AS STRING),
    'flatten_eoi: event_MetaData or event_Data is null',
    event_MetaData.target_bentech,
    event_MetaData.group_id,
    event_MetaData.event_type,
    event_MetaData.correlation_id
  FROM `standard.eda.bentechdata.eoi`
  WHERE event_MetaData IS NULL OR event_Data IS NULL;

END;
