-- Step 2: Route messages from common topic to BENTECH-specific topics
-- Reads from:  standard.eda.bentechdata.flink.common
-- Workday  →   standard.eda.bentechdata.flink.workday
-- BENTECH2 →   standard.eda.bentechdata.flink.bentech2
-- DLQ      →   standard.eda.bentechdata.flink.common.dlq  (when target_BENTECH has no routing rule)

EXECUTE STATEMENT SET
BEGIN

  -- Workday routing
  INSERT INTO `standard.eda.bentechdata.flink.workday`
  (kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id)
  SELECT kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id
  FROM `standard.eda.bentechdata.flink.common`
  WHERE target_BENTECH = 'Workday';

  -- BENTECH2 routing
  INSERT INTO `standard.eda.bentechdata.flink.bentech2`
  (kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id)
  SELECT kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id
  FROM `standard.eda.bentechdata.flink.common`
  WHERE target_BENTECH = 'BENTECH2';

  -- TODO: Add routing blocks here for additional BENTECH systems
  -- Example:
  -- INSERT INTO `standard.eda.bentechdata.flink.sap` (...) SELECT ... WHERE target_BENTECH = 'SAP';

  -- DLQ: target_BENTECH does not match any configured system
  INSERT INTO `standard.eda.bentechdata.flink.common.dlq`
  (kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id)
  SELECT
    kafka_key,
    flattened_event,
    CONCAT('route_bentech: no routing rule for target_BENTECH=', COALESCE(target_BENTECH, 'null')),
    target_BENTECH,
    group_id,
    event_type,
    correlation_id
  FROM `standard.eda.bentechdata.flink.common`
  WHERE target_BENTECH NOT IN ('Workday', 'BENTECH2') OR target_BENTECH IS NULL;

END;
