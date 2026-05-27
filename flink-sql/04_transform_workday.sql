-- Step 3: Transform Workday messages to SOAP XML via UDF
-- Reads from:  standard.eda.bentechdata.flink.workday
-- Success  →   standard.eda.bentechdata.workday  (HTTP Sink Connector reads from here)
-- DLQ      →   standard.eda.bentechdata.flink.workday.dlq  (when UDF returns null or an exception)

EXECUTE STATEMENT SET
BEGIN

  -- Success path: UDF returned a result with no exception
  INSERT INTO `standard.eda.bentechdata.workday`
  (kafka_key, flattened_event, exception, target_BENTECH_payload, target_BENTECH, group_id, event_type, correlation_id)
  SELECT
    fws.kafka_key,
    fws.flattened_event,
    CAST(NULL AS STRING),
    JSON_VALUE(fws.conversion_result, '$.target_BENTECH_payload'),
    fws.target_BENTECH,
    fws.group_id,
    fws.event_type,
    fws.correlation_id
  FROM (
    SELECT
      kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id,
      workdayeoidataconversion(flattened_event) AS conversion_result
    FROM `standard.eda.bentechdata.flink.workday`
  ) fws
  WHERE fws.conversion_result IS NOT NULL
    AND JSON_VALUE(fws.conversion_result, '$.exception') IS NULL;

  -- DLQ path: UDF returned null or returned an exception
  INSERT INTO `standard.eda.bentechdata.flink.workday.dlq`
  (kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id)
  SELECT
    fws.kafka_key,
    fws.flattened_event,
    JSON_VALUE(fws.conversion_result, '$.exception'),
    fws.target_BENTECH,
    fws.group_id,
    fws.event_type,
    fws.correlation_id
  FROM (
    SELECT
      kafka_key, flattened_event, exception, target_BENTECH, group_id, event_type, correlation_id,
      workdayeoidataconversion(flattened_event) AS conversion_result
    FROM `standard.eda.bentechdata.flink.workday`
  ) fws
  WHERE fws.conversion_result IS NULL
    OR JSON_VALUE(fws.conversion_result, '$.exception') IS NOT NULL;

END;
