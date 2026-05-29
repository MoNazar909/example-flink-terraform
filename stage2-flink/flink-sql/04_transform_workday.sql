-- Step 3: Transform Workday messages to SOAP XML via UDF
-- Reads from:  standard-eda-bentechdata-flink-workday      (flink_common.avsc — camelCase)
-- Success  →   standard-eda-bentech-workday                (flink_bentechsink_workday.avsc — snake_case, HTTP Sink Connector reads from here)
-- DLQ      →   standard-eda-bentechdata-flink-workday-dlq  (flink_common.avsc — camelCase, UDF returned null or exception)

EXECUTE STATEMENT SET
BEGIN

  -- Success path: UDF returned a result with no exception
  INSERT INTO `standard-eda-bentech-workday`
  (kafka_key, flattenedEvent, exception, targetBentechPayload, targetBentech, groupId, eventType, correlationId)
  SELECT
    fws.kafka_key,
    fws.flattenedEvent,
    CAST(NULL AS STRING),
    JSON_VALUE(fws.conversion_result, '$.targetBentechPayload'),
    fws.targetBentech,
    fws.groupId,
    fws.eventType,
    fws.correlationId
  FROM (
    SELECT
      kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId,
      workdayeoidataconversion(flattenedEvent) AS conversion_result
    FROM `standard-eda-bentechdata-flink-workday`
  ) fws
  WHERE fws.conversion_result IS NOT NULL
    AND JSON_VALUE(fws.conversion_result, '$.exception') IS NULL
    AND JSON_VALUE(fws.conversion_result, '$.targetBentechPayload') IS NOT NULL;

  -- DLQ path: UDF returned null, returned an exception, or returned a null payload
  INSERT INTO `standard-eda-bentechdata-flink-workday-dlq`
  (kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId)
  SELECT
    fws.kafka_key,
    fws.flattenedEvent,
    JSON_VALUE(fws.conversion_result, '$.exception'),
    fws.targetBentech,
    fws.groupId,
    fws.eventType,
    fws.correlationId
  FROM (
    SELECT
      kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId,
      workdayeoidataconversion(flattenedEvent) AS conversion_result
    FROM `standard-eda-bentechdata-flink-workday`
  ) fws
  WHERE fws.conversion_result IS NULL
    OR JSON_VALUE(fws.conversion_result, '$.exception') IS NOT NULL
    OR JSON_VALUE(fws.conversion_result, '$.targetBentechPayload') IS NULL;

END;
