-- Step 1: Flatten EOI source messages into common topic
-- Reads from:  standard-eda-bentechdata-eoi
-- Success  →   standard-eda-bentechdata-flink-common
-- Messages where eventMetaData or eventData is null are dropped (no DLQ for source topic)

INSERT INTO `standard-eda-bentechdata-flink-common`
(kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId)
SELECT
  CONCAT(
    eventMetaData.groupId, '-',
    eventMetaData.workerId, '-',
    eventMetaData.targetBentech, '-',
    eventMetaData.eventType
  ),
  JSON_OBJECT(
    KEY 'eventMetaData' VALUE eventMetaData,
    KEY 'eventData'     VALUE eventData
  ),
  CAST(NULL AS STRING),
  eventMetaData.targetBentech,
  eventMetaData.groupId,
  eventMetaData.eventType,
  eventMetaData.correlationId
FROM `standard-eda-bentechdata-eoi`
WHERE eventMetaData IS NOT NULL AND eventData IS NOT NULL;
