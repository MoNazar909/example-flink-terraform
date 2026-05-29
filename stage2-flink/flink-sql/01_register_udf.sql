-- Step 0: Register the Java UDF

CREATE FUNCTION IF NOT EXISTS workdayeoidataconversion
AS 'com.standardinsurance.eoi.flink.udf.WorkdayEOIDataConversionUDF'
LANGUAGE JAVA
USING JAR 'confluent-artifact://${udf_artifact_id}';
