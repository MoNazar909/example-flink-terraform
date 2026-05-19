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
| Organization ID | Top-right hamburger → *Your Organization Name*→ Organization ID (UUID format) | `organization_id` (Stage 2 only) |

### 2. API Keys

You need three sets of API keys. Create them under **Data Integration → API Keys** (or within each resource's settings page).

#### Cloud API Key (resource management)
1. Go to **Administration → Cloud API Keys → Add key**
2. Choose **Global access** (or scope to the environment)
3. Save the **Key** and **Secret**
4. Variables: `confluent_cloud_api_key` / `confluent_cloud_api_secret`

#### Kafka API Key (topic access)
1. Go to **Environments → your env → your Kafka cluster → API Keys → Add key**
2. Save the **Key** and **Secret**
3. Variables: `kafka_api_key` / `kafka_api_secret`

#### Schema Registry API Key
1. Go to **Environments → your env → Schema Registry → API Keys → Add key**
2. Save the **Key** and **Secret**
3. Variables: `schema_registry_api_key` / `schema_registry_api_secret`

### 3. Flink Credentials (Stage 2 only — after Stage 1 runs)

#### Flink Compute Pool ID
After Stage 1 runs, get this from its output:
```
terraform output flink_compute_pool_id
```
Or in the UI: **Environments → your env → Flink → Compute Pools → click pool → URL contains `lfcp-xxxxxx`**

Variable: `flink_compute_pool_id`

#### Flink REST Endpoint
Go to **Environments → your env → Flink → Endpoints*
The endpoint looks like: `https://flink.us-east-2.aws.confluent.cloud`

Variable: `flink_rest_endpoint`

#### Flink API Key
1. Go to **Environments → your env → Flink → API Keys → Add key**
2. Save the **Key** and **Secret**
3. Variables: `flink_api_key` / `flink_api_secret`

#### Flink Principal ID
This is the user or service account that runs the Flink SQL statements.
1. Go to **Administration → Users** (or **Service Accounts**)
2. Click your user/service account → the ID in the URL is `u-xxxxxxx`

Variable: `flink_principal_id`

---

## Stage 1 — Infrastructure

### Fill in `stage1-infrastructure/terraform.tfvars`

```hcl
# Cloud API credentials
confluent_cloud_api_key    = "<your-cloud-api-key>"
confluent_cloud_api_secret = "<your-cloud-api-secret>"

# Environment and cluster
environment_id   = "<env-xxxxxx>"
kafka_cluster_id = "<lkc-xxxxxx>"

# Kafka cluster credentials
kafka_api_key    = "<your-kafka-api-key>"
kafka_api_secret = "<your-kafka-api-secret>"

# Schema Registry
schema_registry_id         = "<lsrc-xxxxxx>"
schema_registry_url        = "https://psrc-xxxxx.<region>.aws.confluent.cloud"
schema_registry_api_key    = "<your-sr-api-key>"
schema_registry_api_secret = "<your-sr-api-secret>"
```

### Run Stage 1

```bash
cd stage1-infrastructure
terraform init
terraform plan
terraform apply
```

**What gets created:**
- 6 Kafka topics (`standard.eda.hcmdata.*`)
- 6 Avro schemas in Schema Registry
- 1 Flink compute pool (`standard-insurance-flink-poc`, AWS us-east-2, 10 CFU max)

### Capture Stage 1 Outputs

After apply completes, note these for Stage 2:

```bash
terraform output flink_compute_pool_id
```

---

## Upload the UDF JAR (Required Before Stage 2)

The Flink SQL job calls a Java UDF for Workday transformation. The JAR must exist in Confluent Cloud Artifacts **before** Stage 2 runs, because Stage 2 references it by artifact ID.

### Upload Steps

1. Go to **confluent.cloud → Environments → your env → Flink → Artifacts**
2. Click **Upload artifact**
3. Upload `workday-data-conversion-udf-1.0.0-shaded.jar`
4. After upload, the UI will show an artifact ID in the format `cfa-xxxxxx`
5. Copy that artifact ID

### Update Stage 2 Config

Open [stage2-flink/terraform.tfvars](stage2-flink/terraform.tfvars) and set:

```hcl
udf_artifact_id = "<cfa-xxxxxx>"
```

---

## Stage 2 — Flink SQL Jobs

### Fill in `stage2-flink/terraform.tfvars`

```hcl
# Cloud API credentials (same as Stage 1)
confluent_cloud_api_key    = "<your-cloud-api-key>"
confluent_cloud_api_secret = "<your-cloud-api-secret>"

# Environment and cluster
environment_id   = "<env-xxxxxx>"
kafka_cluster_id = "<lkc-xxxxxx>"

# Organization ID (UUID, from org settings)
organization_id = "<xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx>"

# Flink compute pool (from Stage 1 output)
flink_compute_pool_id = "<lfcp-xxxxxx>"

# Flink principal (user or service account running the statements)
flink_principal_id = "<u-xxxxxxx>"

# Flink API credentials
flink_rest_endpoint = "https://flink.<region>.aws.confluent.cloud"
flink_api_key       = "<your-flink-api-key>"
flink_api_secret    = "<your-flink-api-secret>"

# UDF artifact (from upload step above)
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
| `flatten_eoi` | Reads source EOI events, flattens nested Avro records into a JSON string, writes to common topic |
| `route_hcm` | Reads common topic, fans out to per-HCM routing topics (`flink.workday`, `flink.hcm2`) based on `target_HCM` field |
| `transform_workday` | Calls the UDF on each Workday event; successes go to `workday` sink topic, failures go to `flink.workday.dlq` |

---

## Data Flow

```
Upstream EOI System
        ↓
  [standard.eda.hcmdata.eoi]  ← produce Avro events here
        ↓  (Flink: flatten_eoi)
  [standard.eda.hcmdata.flink.common]
        ↓  (Flink: route_hcm)
        ├──→ [standard.eda.hcmdata.flink.workday]
        │           ↓  (Flink: transform_workday + UDF)
        │           ├──→ SUCCESS → [standard.eda.hcmdata.workday]  → HTTP Sink Connector → Workday SOAP API
        │           └──→ ERROR   → [standard.eda.hcmdata.flink.workday.dlq]
        └──→ [standard.eda.hcmdata.flink.hcm2]  (future: add transform statement for HCM2)
```

---

## Verify Deployment

After both stages apply successfully, confirm in the Confluent Cloud UI:

- **Topics:** 6 topics exist under your Kafka cluster
- **Schema Registry:** 6 subjects registered
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
- The routing statement (`route_hcm`) can be extended to support additional HCM systems by adding `INSERT INTO` blocks following the same pattern as the Workday and HCM2 entries.
