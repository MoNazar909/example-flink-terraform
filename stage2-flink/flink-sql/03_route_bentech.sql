-- Step 2: Route messages from common topic to BENTECH-specific topics
-- Reads from:  standard-eda-bentechdata-flink-common   (flink_common.avsc)
-- Workday  →   standard-eda-bentechdata-flink-workday  (flink_common.avsc — same schema, same message)
-- Unknown targetBentech values are dropped (no DLQ for common topic)

INSERT INTO `standard-eda-bentechdata-flink-workday`
(kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId)
SELECT kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId
FROM `standard-eda-bentechdata-flink-common`
WHERE targetBentech = 'Workday';
