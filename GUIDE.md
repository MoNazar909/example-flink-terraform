# Confluent Cloud Terraform POC — Deployment Guide

This guide walks through deploying the two-stage pipeline from scratch: Stage 1 creates Kafka topics, schemas, and a Flink compute pool; Stage 2 deploys Flink SQL jobs including a Java UDF.

---

## Prerequisites

- Terraform >= 1.0 installed
- Access to a Confluent Cloud account with an existing environment and Kafka cluster
- The UDF JAR file: `workday-data-conversion-udf-1.0.0-shaded.jar`

---

## What to Collect from Confluent Cloud

Before filling in any `terraform.tfvars` file, gather the following from the Confluent Cloud UI or CLI. All values are found under **confluent.cloud → your environment**.

### 1. Environment & Cluster IDs

| What | Where to find it | Variable name |
|---|---|---|
| Environment ID | Home → Environments → click your env → URL contains `env-xxxxxx` | `environment_id` |
| Kafka Cluster ID | Environments → your env → Kafka Clusters → click cluster → URL contains `lkc-xxxxxx` | `kafka_cluster_id` |
| Schema Registry ID | Environments → your env → Schema Registry → Settings → `lsrc-xxxxxx` | `schema_registry_id` |
| Schema Registry URL | Same page → Endpoint URL (e.g. `https://psrc-xxxxx.us-east-2.aws.confluent.cloud`) | `schema_registry_url` |
| Organization ID | Top-right hamburger → *Your Organization Name* → Organization ID (UUID format) | `organization_id` (Stage 2 only) |

### 2. Bootstrap Cloud API Key

Create one API key with broad scope — Terraform uses this to create service accounts and all other resources.

1. Go to **Administration → Cloud API Keys → Add key**
2. Choose **Org Admin** scope (or Environment Admin)
3. Save the **Key** and **Secret**
4. Variables: `confluent_cloud_api_key` / `confluent_cloud_api_secret`

> All other credentials (Kafka, Schema Registry, Flink) are created automatically by Terraform in Stage 1 via service accounts. You do not need to create them manually.

### 3. Flink Credentials (Stage 2 only — fill after Stage 1 runs)

After Stage 1 applies, run `terraform output` to get the service account credentials for Stage 2:

```
terraform output flink_compute_pool_id       → flink_compute_pool_id
terraform output flink_runner_sa_id          → flink_principal_id
terraform output flink_runner_api_key_id     → flink_api_key
terraform output -raw flink_runner_api_key_secret → flink_api_secret
```

Also collect the Flink REST endpoint from the UI:
- **Environments → your env → Flink → Endpoints**
- Example: `https://flink.us-east-2.aws.confluent.cloud`

---

## Stage 1 — Infrastructure

### Fill in `stage1-infrastructure/terraform.tfvars`

```hcl
confluent_cloud_api_key    = "<your-cloud-api-key>"
confluent_cloud_api_secret = "<your-cloud-api-secret>"

environment_id   = "<env-xxxxxx>"
kafka_cluster_id = "<lkc-xxxxxx>"

schema_registry_id  = "<lsrc-xxxxxx>"
schema_registry_url = "https://psrc-xxxxx.<region>.aws.confluent.cloud"
```

### Run Stage 1

```bash
cd stage1-infrastructure
terraform init
terraform plan
terraform apply
```

**What gets created:**
- 2 service accounts (`standard-poc-terraform-manager`, `standard-poc-flink-runner`)
- 3 role bindings (CloudClusterAdmin, ResourceOwner on SR, FlinkDeveloper)
- 4 API keys (Kafka + SR for terraform-manager; Kafka + Flink for flink-runner)
- 11 Kafka topics (`standard.eda.bentechdata.*`)
- 22 Avro schemas in Schema Registry (value + key for each topic)
- 1 Flink compute pool (`standard-insurance-flink-poc`, AWS us-east-2, 10 CFU max)

### Capture Stage 1 Outputs

```bash
terraform output flink_compute_pool_id
terraform output flink_runner_sa_id
terraform output flink_runner_api_key_id
terraform output -raw flink_runner_api_key_secret
```

---

## Upload the UDF JAR (Required Before Stage 2)

The Flink SQL job calls a Java UDF for Workday transformation. The JAR must exist in Confluent Cloud Artifacts **before** Stage 2 runs.

### Upload Steps

1. Go to **confluent.cloud → Environments → your env → Flink → Artifacts**
2. Click **Upload artifact**
3. Upload `workday-data-conversion-udf-1.0.0-shaded.jar`
4. After upload, copy the artifact ID in the format `cfa-xxxxxx`

### Update Stage 2 Config

Open [stage2-flink/terraform.tfvars](stage2-flink/terraform.tfvars) and set:

```hcl
udf_artifact_id = "<cfa-xxxxxx>"
```

---

## Stage 2 — Flink SQL Jobs

### Fill in `stage2-flink/terraform.tfvars`

```hcl
confluent_cloud_api_key    = "<your-cloud-api-key>"
confluent_cloud_api_secret = "<your-cloud-api-secret>"

environment_id   = "<env-xxxxxx>"
kafka_cluster_id = "<lkc-xxxxxx>"
organization_id  = "<xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx>"

flink_rest_endpoint   = "https://flink.<region>.aws.confluent.cloud"
flink_api_key         = "<from: terraform output flink_runner_api_key_id>"
flink_api_secret      = "<from: terraform output -raw flink_runner_api_key_secret>"
flink_principal_id    = "<from: terraform output flink_runner_sa_id>"
flink_compute_pool_id = "<from: terraform output flink_compute_pool_id>"

udf_artifact_id = "<cfa-xxxxxx>"
```

### Run Stage 2

```bash
cd stage2-flink
terraform init
terraform plan
terraform apply
```

**What gets created (4 Flink SQL statements):**

| Statement | What it does |
|---|---|
| `register_udf` | Registers the Java UDF function `workdayeoidataconversion` |
| `flatten_eoi` | Reads source EOI events, flattens nested Avro records into a JSON string, writes to common topic. DLQ → `eoi.dlq` |
| `route_bentech` | Reads common topic, routes to per-system topics (`flink.workday`, `flink.bentech2`) based on `target_BENTECH` field. DLQ → `flink.common.dlq` |
| `transform_workday` | Calls the UDF on each Workday event; successes go to `workday` sink topic, failures go to `flink.workday.dlq` |

---

## Data Flow

```
Upstream EOI System
        ↓
  [standard.eda.bentechdata.eoi]  ← produce Avro events here
        ↓  (Flink: flatten_eoi)
        ├──→ SUCCESS → [standard.eda.bentechdata.flink.common]
        └──→ DLQ     → [standard.eda.bentechdata.eoi.dlq]
                ↓  (Flink: route_bentech)
                ├──→ Workday  → [standard.eda.bentechdata.flink.workday]
                │                    ↓  (Flink: transform_workday + UDF)
                │                    ├──→ SUCCESS → [standard.eda.bentechdata.workday] → HTTP Sink Connector → Workday SOAP API
                │                    └──→ DLQ     → [standard.eda.bentechdata.flink.workday.dlq]
                ├──→ BENTECH2 → [standard.eda.bentechdata.flink.bentech2]
                └──→ DLQ     → [standard.eda.bentechdata.flink.common.dlq]
```

---

## Kafka Key Format

Every message produced to `standard.eda.bentechdata.eoi` must use the following key format:

```
{GroupId}-{WorkerId}-{Target_BENTECH}-{EventType}
```

| Field | Source in payload |
|---|---|
| `GroupId` | `event_MetaData.group_id` |
| `WorkerId` | `event_Data.worker_id` |
| `Target_BENTECH` | `event_MetaData.target_bentech` |
| `EventType` | `event_MetaData.event_type` |

**Example:** `WMT-12345-100319TS-Workday-BenefitsEOI`

---

## Verify Deployment

After both stages apply successfully, confirm in the Confluent Cloud UI:

- **Topics:** 11 topics exist under your Kafka cluster
- **Schema Registry:** 22 subjects registered
- **Service Accounts:** `standard-poc-terraform-manager` and `standard-poc-flink-runner` visible
- **Flink:** Compute pool is `Running`; all 4 statements are in `Completed` or `Running` state
- **Artifacts:** UDF JAR appears under Flink → Artifacts

---

## Destroy / Teardown

To tear down in reverse order:

```bash
cd stage2-flink && terraform destroy
cd ../stage1-infrastructure && terraform destroy
```

Note: Manually delete the UDF artifact from the Confluent Cloud UI — it is not managed by Terraform.

---

## Notes

- `terraform.tfvars` files contain credentials and are excluded from git via `.gitignore` — never commit them.
- `terraform.tfstate` files are also excluded — store state remotely (e.g. Terraform Cloud or S3) for team environments.
- The HTTP Sink Connector (to deliver `workday` topic events to the Workday SOAP API) is not deployed by Terraform and must be configured separately in Confluent Cloud.
- The routing statement (`route_bentech`) can be extended to support additional systems by adding `INSERT INTO` blocks following the same pattern as the Workday and BENTECH2 entries.
