---
title: 'Tableflow × Snowflake: Kafka as Iceberg SQL'
description: 'Terraform that wires Confluent Cloud Tableflow, ADLS Gen2, and Snowflake Horizon Catalog so Kafka topics become Iceberg tables you can SELECT.'
pubDate: 2026-08-18
tags: ['Terraform', 'Snowflake', 'Kafka', 'Iceberg', 'Confluent', 'Azure']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/SnowflakeConfluentTableFlowIceberg'
image: '/snowflake-tableflow-iceberg.svg'
---

![Kafka Avro topic to Tableflow Iceberg on ADLS to Snowflake Horizon SQL](/snowflake-tableflow-iceberg.svg)

## Overview

You would not hand a warehouse analyst a Kafka consumer and say "read the topic."
You would give them a table.

That gap is the whole point of this project. **Kafka** is a conveyor of Avro
events. **Snowflake** wants a cataloged table it can `SELECT`. **Tableflow** is
the kitchen in between: it materializes the topic as Apache Iceberg (Parquet plus
metadata) on **ADLS Gen2**, publishes that table through Tableflow's Iceberg REST
Catalog (IRC), and Snowflake Horizon Catalog points at both. After two Terraform
applies and one Azure admin consent, the Datagen `stocks` topic is a table in
`TABLEFLOW_DB.CONFLUENT`.

The Terraform lives in
[SnowflakeConfluentTableFlowIceberg](https://github.com/MosheBlumbergX/SnowflakeConfluentTableFlowIceberg).
It follows Confluent's
[Horizon Catalog integration guide](https://github.com/jeremyber/Integrate-Tableflow-with-Snowflake-Horizon-Catalog),
turned into something you can actually `apply`.

## How it works

Four pieces have to agree, or Snowflake has a catalog with nothing to read:

- **Produce:** a Datagen connector writes Avro `stock_trades` to a Kafka topic
  (default `stocks`) with Schema Registry in the loop.
- **Materialize:** Tableflow on that topic writes Iceberg files into an ADLS
  Gen2 container, using Confluent's Azure provider integration (multi-tenant app
  + `Storage Blob Data Contributor`).
- **Catalog:** Tableflow IRC is the metadata plane. Snowflake's catalog
  integration talks to it with the Tableflow API key as OAuth client credentials
  (`oauth_allowed_scopes = ["catalog"]`). The Kafka cluster id is the catalog
  namespace; the topic name is `CATALOG_TABLE_NAME`.
- **Query:** a Snowflake external volume uses the **blob** endpoint (not `dfs`)
  so Horizon can download Parquet. `CREATE ICEBERG TABLE` maps the topic, with
  `AUTO_REFRESH = TRUE` and a 30s IRC poll.

```
Kafka topic (Avro) ──► Tableflow ──► ADLS Gen2 (Parquet + metadata)
                            │                    │
                       IRC metadata         External volume
                            └────────┬───────────┘
                                     ▼
                       Snowflake catalog integration
                                     ▼
                       Iceberg table in Horizon ──► SELECT
```

## What it demonstrates

- **Managed Iceberg, not a Connect sink.** Tableflow is a Confluent Cloud
  service on the topic. You are not running
  `org.apache.iceberg.connect.IcebergSinkConnector`, a control topic, or your own
  compaction. The repo's [Tableflow vs Iceberg sink](https://github.com/MosheBlumbergX/SnowflakeConfluentTableFlowIceberg/blob/main/TABLEFLOW_VS_ICEBERG_SINK.md)
  note spells out the trade: 1:1 topic-to-table, Schema Registry as the evolution
  contract, compaction included, Connect workers not required.
- **Horizon as a query engine, not a second writer.** Snowflake is a consumer of
  Tableflow IRC. Compatible schema adds show up as new Iceberg snapshots; Horizon
  picks them up on auto-refresh. No Snowflake DDL for a backward-compatible
  optional field.
- **Azure consent is part of the architecture.** Snowflake's multi-tenant app
  does not exist in your tenant until someone opens the external-volume consent
  URL. Terraform models that as two applies, not a comment in a README.

## Deploy (two applies)

Copy `terraform.tfvars.example` → `terraform.tfvars`. You need a Confluent Cloud
API key, an Azure tenant + subscription, and a Snowflake user that can create
integrations and volumes (`ACCOUNTADMIN` in the demo). Prefer
`SNOWFLAKE_PASSWORD` in the environment over committing it.

**Phase 1** creates the Confluent environment, Kafka cluster, Schema Registry,
Datagen topic, ADLS Gen2 storage, Tableflow on the topic, and the Snowflake
database / schema / external volume.

```bash
terraform init
terraform apply
terraform output snowflake_azure_consent_url
```

Open that URL, Accept as a tenant admin, then set
`snowflake_azure_consent_completed = true`.

**Phase 2** grants Snowflake `Storage Blob Data Contributor` on the storage
account, creates `TABLEFLOW_CATALOG_INTEGRATION`, waits two minutes for IRC to
publish the topic, and runs `CREATE OR REPLACE ICEBERG TABLE` with
`AUTO_REFRESH`.

```bash
terraform apply
terraform output snowflake_query_example
```

```sql
SELECT *
FROM TABLEFLOW_DB.CONFLUENT.STOCKS
LIMIT 10;
```

Zero rows on the first `SELECT` usually means Tableflow has not written a
snapshot yet, or auto-refresh has not ticked. Wait a minute, or
`ALTER ICEBERG TABLE … REFRESH`.

## Technical details

- **Stack:** Terraform with `confluentinc/confluent` 2.57, `azurerm` ~3.80,
  `azuread`, `snowflakedb/snowflake` ~2.0, and `hashicorp/time` for the IRC wait.
  Kafka cluster and storage share an Azure region (example: `eastus2`).
- **Identity split:** `app-manager` owns topics and ACLs; a connector SA produces;
  a Tableflow SA gets `CloudClusterAdmin`, `EnvironmentAdmin`, and Schema Registry
  `DeveloperRead`. The Tableflow API key is also the Snowflake OAuth client.
- **Table naming:** Snowflake unquoted identifiers are uppercased. Terraform
  always `UPPER(replace(topic, "-", "_"))` so you never get a second quoted
  mixed-case table.
- **Why `snowflake_execute`:** `snowflake_iceberg_table_from_rest` fails
  `DESCRIBE` on Avro `MAP` fields from the `stock_trades` schema. The execute
  resource runs the `CREATE ICEBERG TABLE` SQL and a matching `DROP` on revert.
- **Snowflake provider:** Azure locators like `FAB10230.east-us-2.azure` still
  need the experimental `account` fallback. Optional
  `snowflake_organization_name` + `snowflake_account_name` use the v2 stable
  fields instead.
- **Working role grants:** Terraform creates objects as `ACCOUNTADMIN`, then
  grants `USAGE` and `CREATE ICEBERG TABLE` to `SYSADMIN` (configurable).

## Challenges & lessons

1. **Azure consent cannot be skipped.** Phase 1 stops at the external volume.
   Until someone Accepts the consent URL, Snowflake's app is not in Azure AD, and
   the blob role assignment cannot exist. The boolean
   `snowflake_azure_consent_completed` is the circuit breaker.
2. **IRC lags the Tableflow toggle.** Creating the Iceberg table immediately
   after enablement returns `Catalog table lkc-….topic does not exist in catalog
   integration`. That is Tableflow publishing into IRC, not a bad URI. A
   `time_sleep` of 120 seconds (configurable) sits between the catalog
   integration and `CREATE ICEBERG TABLE`. If you applied before that wait
   existed, apply again.
3. **Use the blob endpoint, not `dfs`.** With HNS and POSIX ACLs, `dfs` reads
   often fail with `Unable to download parquet` even when RBAC is correct. The
   volume URL is `azure://<account>.blob.core.windows.net/<container>/`.
4. **Cross-tenant Graph calls 403.** Registering Confluent's multi-tenant app as
   an SP via the `azuread` provider often fails. A `local-exec` of
   `az ad sp create --id …` (or `az ad sp show`) is what actually works.
5. **Destroy can 409 on the Azure provider integration.** After the Kafka
   cluster is gone, Confluent still reports the integration `ACTIVE` and refuses
   delete. Drop
   `confluent_provider_integration_setup.azure` (and authorization if present)
   from state, destroy the rest, then import on the next apply if Confluent kept
   the old `ACTIVE` integration.
6. **Secrets stay out of git.** `terraform.tfvars` is gitignored. The Tableflow
   API key secret is the catalog OAuth client secret: treat it like a password,
   not a display name.
