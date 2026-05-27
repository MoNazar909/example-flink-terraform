-- Step 0: Register the Java UDF
-- Replace <cfa-xxxxxx> with the artifact ID from Confluent Cloud → Flink → Artifacts

CREATE FUNCTION IF NOT EXISTS workdayeoidataconversion
AS 'com.standardinsurance.eoi.flink.udf.WorkdayEOIDataConversionUDF'
LANGUAGE JAVA
USING JAR 'confluent-artifact://<cfa-xxxxxx>';
