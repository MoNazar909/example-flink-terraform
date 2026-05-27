# Manual Setup Guide

This guide covers how to set up the full pipeline manually through the Confluent Cloud UI — no Terraform required. You need a Confluent Cloud user account with **EnvironmentAdmin** (or OrganizationAdmin) on the target environment.

Schema files are in [stage1-infrastructure/schemas/](stage1-infrastructure/schemas/).  
Flink SQL files are in [flink-sql/](flink-sql/).

---

## Step 1 — Create Kafka Topics

Go to **Confluent Cloud → your environment → Kafka cluster → Topics → + Add topic**.

Create all 11 topics below. Leave all settings as default except partitions and retention.

**7-day retention, 6 partitions each:**
- `standard.eda.bentechdata.eoi`
- `standard.eda.bentechdata.flink.common`
- `standard.eda.bentechdata.flink.workday`
- `standard.eda.bentechdata.flink.bentech2`
- `standard.eda.bentechdata.workday`
- `standard.eda.bentechdata.workday.response`

**30-day retention, 6 partitions each** (DLQ topics — longer retention for investigation):
- `standard.eda.bentechdata.eoi.dlq`
- `standard.eda.bentechdata.flink.common.dlq`
- `standard.eda.bentechdata.flink.workday.dlq`
- `standard.eda.bentechdata.workday.dlq`
- `standard.eda.bentechdata.workday.error`

---

## Step 2 — Register Schemas

Go to **Confluent Cloud → your environment → Schema Registry → + Add schema**.

Each topic needs two schemas registered: a **value schema** (the message payload) and a **key schema** (the Kafka record key). Do not skip the key schema — registering it is what makes Flink automatically discover `kafka_key` as a column. Without it, Flink SQL will fail with "Unknown target column 'kafka_key'".

**How to register a schema:**
1. Click **+ Add schema**
2. Enter the subject name exactly as shown below
3. Select format **AVRO**
4. Paste the contents of the schema file
5. Click **Create**

**The key schema file is `kafka_key.avsc` for every topic.** The subject name for the key is always `<topic-name>-key`.

Register value then key for each topic:

---

**`standard.eda.bentechdata.eoi`**
- Value subject: `standard.eda.bentechdata.eoi-value` → use `eoi_source.avsc`
- Key subject: `standard.eda.bentechdata.eoi-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.flink.common`**
- Value subject: `standard.eda.bentechdata.flink.common-value` → use `flink_common.avsc`
- Key subject: `standard.eda.bentechdata.flink.common-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.flink.workday`**
- Value subject: `standard.eda.bentechdata.flink.workday-value` → use `flink_common.avsc`
- Key subject: `standard.eda.bentechdata.flink.workday-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.flink.bentech2`**
- Value subject: `standard.eda.bentechdata.flink.bentech2-value` → use `flink_common.avsc`
- Key subject: `standard.eda.bentechdata.flink.bentech2-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.workday`**
- Value subject: `standard.eda.bentechdata.workday-value` → use `bentech_sink_workday.avsc`
- Key subject: `standard.eda.bentechdata.workday-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.workday.response`**
- Value subject: `standard.eda.bentechdata.workday.response-value` → use `bentech_sink_response.avsc`
- Key subject: `standard.eda.bentechdata.workday.response-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.eoi.dlq`**
- Value subject: `standard.eda.bentechdata.eoi.dlq-value` → use `flink_common.avsc`
- Key subject: `standard.eda.bentechdata.eoi.dlq-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.flink.common.dlq`**
- Value subject: `standard.eda.bentechdata.flink.common.dlq-value` → use `flink_common.avsc`
- Key subject: `standard.eda.bentechdata.flink.common.dlq-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.flink.workday.dlq`**
- Value subject: `standard.eda.bentechdata.flink.workday.dlq-value` → use `flink_common.avsc`
- Key subject: `standard.eda.bentechdata.flink.workday.dlq-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.workday.dlq`**
- Value subject: `standard.eda.bentechdata.workday.dlq-value` → use `bentech_sink_response.avsc`
- Key subject: `standard.eda.bentechdata.workday.dlq-key` → use `kafka_key.avsc`

**`standard.eda.bentechdata.workday.error`**
- Value subject: `standard.eda.bentechdata.workday.error-value` → use `bentech_sink_response.avsc`
- Key subject: `standard.eda.bentechdata.workday.error-key` → use `kafka_key.avsc`

---

## Step 3 — Create a Flink Compute Pool

Go to **Confluent Cloud → your environment → Flink → + Add compute pool**.

- Cloud: **AWS**
- Region: **us-east-2** (or match your cluster's region)
- Max CFU: **10**
- Name: `standard-insurance-flink-poc` (or any name)

---

## Step 4 — Upload the UDF JAR

Go to **Confluent Cloud → your environment → Flink → Artifacts → Upload artifact**.

Upload `workday-data-conversion-udf-1.0.0-shaded.jar`. After upload, copy the artifact ID (format: `cfa-xxxxxx`) — you'll need it in Step 5.

---

## Step 5 — Run Flink SQL Statements

Go to **Confluent Cloud → your environment → Flink → + New statement**.

Before running any statement, set the **catalog** and **database** dropdowns in the top-right of the editor:
- Catalog: `default`
- Database: the display name of your Kafka cluster (e.g. `cluster_0`)

Run the statements **in order**. Each one must be in `Running` or `Completed` state before starting the next.

### 5.1 — Register UDF

File: [flink-sql/01_register_udf.sql](flink-sql/01_register_udf.sql)

Open the file, replace `<cfa-xxxxxx>` with the artifact ID from Step 4, paste into the editor, and run.

Wait for the statement to reach **Completed** status before continuing.

### 5.2 — Flatten EOI

File: [flink-sql/02_flatten_eoi.sql](flink-sql/02_flatten_eoi.sql)

Paste and run. This statement runs continuously — wait for it to reach **Running** status.

### 5.3 — Route BENTECH

File: [flink-sql/03_route_bentech.sql](flink-sql/03_route_bentech.sql)

Paste and run. Wait for **Running** status.

### 5.4 — Transform Workday

File: [flink-sql/04_transform_workday.sql](flink-sql/04_transform_workday.sql)

Paste and run. Wait for **Running** status.

---

## Verify

- **Topics:** All 11 topics exist and are visible under your cluster
- **Schema Registry:** 22 subjects registered (2 per topic)
- **Flink:** Compute pool is `Running`; all 4 statements are in `Running` or `Completed` state

---

## Notes

- The HTTP Sink Connector (to send `standard.eda.bentechdata.workday` messages to the Workday SOAP API) must be configured separately.
- To add routing for a new BENTECH system, add a new `INSERT INTO` block in [flink-sql/03_route_bentech.sql](flink-sql/03_route_bentech.sql) following the same pattern as the Workday and BENTECH2 entries. You will also need a new topic and schemas for it.
- The three `workday.response`, `workday.dlq`, and `workday.error` topics are written to by the HTTP Sink Connector after it posts to Workday — they are not written to by Flink.
