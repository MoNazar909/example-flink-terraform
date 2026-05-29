-- Step 3: Transform Workday messages to SOAP XML via UDF
-- Reads from:  standard-eda-bentechdata-flink-workday      (flink_common.avsc — camelCase)
-- Success  →   standard-eda-bentech-workday                (flink_bentechsink_workday.avsc)
-- DLQ      →   standard-eda-bentechdata-flink-workday-dlq  (flink_common.avsc — camelCase)
-- Note: one input message produces N output messages (one per employer/employee/coverage combo)

EXECUTE STATEMENT SET
BEGIN

  -- Success path: UDF returned a result with no exception
  INSERT INTO `standard-eda-bentech-workday`
  (kafka_key, flattenedEvent, exception, targetBentechPayload, targetBentech, groupId, eventType, correlationId)
  SELECT
    src.kafka_key,
    src.flattenedEvent,
    CAST(NULL AS STRING),
    JSON_VALUE(T.conversion_result, '$.targetBentechPayload'),
    src.targetBentech,
    src.groupId,
    src.eventType,
    src.correlationId
  FROM `standard-eda-bentechdata-flink-workday` src
  CROSS JOIN UNNEST(workdayeoidataconversion(src.flattenedEvent)) AS T(conversion_result)
  WHERE JSON_VALUE(T.conversion_result, '$.exception') IS NULL
    AND JSON_VALUE(T.conversion_result, '$.targetBentechPayload') IS NOT NULL;

  -- DLQ path: UDF returned an exception or null payload
  INSERT INTO `standard-eda-bentechdata-flink-workday-dlq`
  (kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId)
  SELECT
    src.kafka_key,
    src.flattenedEvent,
    JSON_VALUE(T.conversion_result, '$.exception'),
    src.targetBentech,
    src.groupId,
    src.eventType,
    src.correlationId
  FROM `standard-eda-bentechdata-flink-workday` src
  CROSS JOIN UNNEST(workdayeoidataconversion(src.flattenedEvent)) AS T(conversion_result)
  WHERE JSON_VALUE(T.conversion_result, '$.exception') IS NOT NULL
     OR JSON_VALUE(T.conversion_result, '$.targetBentechPayload') IS NULL;

END;
