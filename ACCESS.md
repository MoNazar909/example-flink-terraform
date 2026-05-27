# Access & Permissions Design

This document describes every service account, role binding, and API key in this POC, and explains why each permission is needed.

---

## Overview

There are three service accounts. Only one (`terraform-bootstrap`) is created manually — it is the bootstrap credential Terraform uses to create everything else. The other two are created by Terraform in Stage 1.

```
terraform-bootstrap        → manually created, OrganizationAdmin, Global API key
    └── creates (via Terraform):
        ├── standard-poc-terraform-manager   → manages topics and schemas
        └── standard-poc-flink-runner        → runs Flink SQL jobs
```

---

## 1. terraform-bootstrap

**Created:** Manually, once, before running Terraform.  
**Managed by:** You (not Terraform).

### Role

**OrganizationAdmin** — scoped to the entire organization.

Allows Terraform to create service accounts, assign role bindings, create API keys, and manage environment-level resources. Without this, Terraform cannot set up any of the other access.

### API Key

One API key, with **Global** scope.

The key must reach the Confluent Cloud management API (not just a specific Kafka cluster). Global scope is required for management-plane operations like creating service accounts and role bindings.

> This is the only credential you manage manually. Everything else is created by Terraform.

---

## 2. standard-poc-terraform-manager

**Created by:** Stage 1 Terraform.  
**Purpose:** Used by Terraform to create Kafka topics and register Avro schemas.

### Role Bindings

**CloudClusterAdmin** — scoped to the Kafka cluster.

Required to create, delete, and configure Kafka topics. This is the minimum role that includes topic management. A lesser role like `DeveloperWrite` only allows producing to topics that already exist — it cannot create them.

**ResourceOwner** — scoped to Schema Registry, all subjects (`subject=*`).

Required to register, update, and delete Avro schemas. A "subject" in Schema Registry is a named slot where one schema lives — each topic has two subjects, one for its key schema and one for its value schema. `subject=*` means this role applies to all subjects across the cluster. `ResourceOwner` is the minimum role that includes schema registration. `DeveloperWrite` on Schema Registry only allows producing schema-encoded messages; it does not grant the ability to register schemas.

### API Keys

**standard-poc-terraform-kafka-key** — scoped to the Kafka cluster.

This is the key Terraform uses in the `credentials` block of every `confluent_kafka_topic` resource when creating and configuring topics.

**standard-poc-terraform-sr-key** — scoped to the Schema Registry cluster.

This is the key Terraform uses in the `credentials` block of every `confluent_schema` resource when registering schemas.

---

## 3. standard-poc-flink-runner

**Created by:** Stage 1 Terraform.  
**Purpose:** The identity under which all Flink SQL statements run. Every Flink statement is submitted under this SA, and it reads from source topics and writes to sink topics at runtime.

### Role Bindings

**FlinkDeveloper** — scoped to the environment.

Allows the SA to submit Flink SQL statements, list them, and stop/start them. Without this, Terraform cannot create any Flink statement resources under this SA.

**DeveloperRead** — scoped to all topics in the Kafka cluster (`topic=*`).

Flink reads from source topics (`eoi`, `flink.common`, `flink.workday`) as a Kafka consumer. Topic-level read access is required for every partition Flink consumes.

**DeveloperWrite** — scoped to all topics in the Kafka cluster (`topic=*`).

Flink writes to sink topics (`flink.common`, `flink.workday`, `workday`, and all DLQ topics) as a Kafka producer. Topic-level write access is required for every partition Flink produces to.

**DeveloperRead** — scoped to Schema Registry, all subjects (`subject=*`).

Flink reads Avro key and value schemas from Schema Registry to build its internal table structure. Without this, Flink cannot see the key schema, which means it cannot discover the `kafka_key` column on any topic table, and SQL validation fails with "Unknown target column 'kafka_key'".

**DeveloperWrite** — scoped to all transactional IDs in the Kafka cluster (`transactional-id=*`).

Flink uses Kafka transactions internally to achieve exactly-once delivery. When a Flink checkpoint completes, it atomically commits all buffered output records as a transaction. Flink auto-generates the transactional ID names so the `*` wildcard is necessary — you cannot predict the names ahead of time.

**DeveloperRead** — scoped to all consumer groups in the Kafka cluster (`group=*`).

Flink creates and manages Kafka consumer groups to track its read position (offset) on source topics. When a Flink job restarts, it resumes from the stored offset in the consumer group. Flink auto-generates the consumer group names so the `*` wildcard is required for the same reason as transactional IDs.

### API Keys

**standard-poc-flink-kafka-key** — scoped to the Kafka cluster.

Used by Flink at runtime to read from source topics and write to sink topics.

**standard-poc-flink-key** — scoped to the Flink region.

Used to submit Flink SQL statements. This key appears in the `credentials` block of every `confluent_flink_statement` resource and in the Stage 2 provider configuration.

Two separate keys are needed because Kafka access and Flink statement submission are separate API surfaces — a single key cannot cover both.

---

## Why Not Just Give Everything OrganizationAdmin?

It would be simpler, but:

- **Blast radius:** A leaked flink-runner key can only read and write topics in this cluster. A leaked OrganizationAdmin key can delete your entire organization.
- **Auditability:** Confluent Cloud audit logs show which SA did what. If all SAs share one credential, you lose the ability to distinguish Flink activity from schema changes from infrastructure changes.
- **Least privilege:** Each SA can only do what its job requires. terraform-manager cannot submit Flink statements. flink-runner cannot create topics or register schemas.
