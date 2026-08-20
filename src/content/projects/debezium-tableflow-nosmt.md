---
title: 'Debezium × Tableflow: Iceberg Before the Columns Exist'
description: 'Local MySQL CDC into a Confluent Cloud Tableflow topic, materialized as Iceberg on ADLS Gen2 and queried in Snowflake Horizon, with no local Kafka brokers.'
pubDate: 2026-08-20
tags: ['Debezium', 'MySQL', 'Tableflow', 'Snowflake', 'Iceberg', 'Avro']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/DebziumConfluentTableFlowIcebergSnowflakeNoSMT'
image: '/debezium-tableflow-nosmt.svg'
---

![MySQL CDC through Debezium to a Tableflow Iceberg topic queried in Snowflake Horizon](/debezium-tableflow-nosmt.svg)

## Overview

You want a warehouse table of live MySQL rows, and Tableflow has to be **on**
before Debezium has seen a single column. That is the wrong order for ordinary
CDC, and it is exactly the constraint this demo is built around.

**Tableflow** cannot enable on a schemaless topic. **Debezium** does not know
the `mydb.team` columns until the connector starts. So Terraform plants a
generic value-only Avro schema (`tableflow.Value` with optional `tf_bootstrap`),
sets that subject to **FORWARD**, and turns Tableflow on. Then a local Connect
worker, talking SASL_SSL to Confluent Cloud, unwraps CDC rows, forces every
field optional, and auto-registers the live columns onto that same value
subject. Snowflake Horizon reads the Iceberg table. There are no local Kafka
brokers.

The Terraform lives in
[DebziumConfluentTableFlowIcebergSnowflakeNoSMT](https://github.com/MosheBlumbergX/DebziumConfluentTableFlowIcebergSnowflakeNoSMT).
It is a sibling of the Datagen-to-Horizon wiring in
[Tableflow × Snowflake](/projects/snowflake-tableflow-iceberg): same catalog
path, different producer, and a schema-evolution problem Datagen never has.

## How it works

Three decisions keep Tableflow RUNNING instead of suspended:

- **Bootstrap first.** Terraform registers `nosmt_mydb_team-value` as Avro
  `tableflow.Value` with one optional field, `tf_bootstrap`. No key schema.
  Then it enables Tableflow Iceberg on ADLS Gen2.
- **FORWARD, subject-only.** The registry's global default stays BACKWARD. Only
  this value subject is overridden so a later producer can *add* fields while
  Tableflow's existing table keeps reading.
- **Every CDC field optional.** Stock Connect transforms unwrap the envelope,
  drop deletes, flatten so ancestor optionality lands on every leaf (including
  MySQL `NOT NULL` columns), and re-insert `tf_bootstrap` forever. Keys stay
  JSON, out of Schema Registry.

```
MySQL (Docker)
      │  binlog CDC
      ▼
Kafka Connect + Debezium  ──SASL_SSL──►  Confluent Cloud  nosmt_mydb_team
                                         Tableflow already enabled
                                                │
                                                ▼
                                         Iceberg on ADLS Gen2
                                                │
                                                ▼
                         Snowflake  NOSMT_TABLEFLOW_DB.CONFLUENT.NOSMT_MYDB_TEAM
```

## What it demonstrates

- **Tableflow before the table schema.** Version 1 is a placeholder, not a copy
  of `mydb.team`. The first CDC record becomes v2 (`id`, `name`, `email`,
  `last_modified`, all optional, plus `tf_bootstrap`). An
  `ALTER TABLE … ADD COLUMN` becomes v3 the same way.
- **Registry vs Tableflow.** FORWARD lets the extra optional fields through
  Schema Registry. Optionality is what keeps Iceberg alive: a required-field
  Avro version suspends Tableflow even if the subject is `NONE` and the schema
  stored. Deleting `tf_bootstrap` suspends it too.
- **Why not ExtractNewRecordState.** Debezium's usual unwrap SMT rebuilds the
  value schema through `copySchemaBasics()`, which drops `optional`. Flatten
  then has nothing to propagate, MySQL `NOT NULL` columns register as required,
  and Tableflow suspends with `a required field was added at 'id'`.
  `ExtractField$Value` on `after` keeps the flag.
- **Cloud cluster, local worker.** Connect bootstraps the Confluent Cloud
  cluster. Compose runs MySQL 8.1 and `cp-kafka-connect` 7.6.5 with Debezium
  MySQL 2.5.4. `topic.delimiter=_` publishes straight to `nosmt_mydb_team`, so
  there is no `RegexRouter`.

## Technical details

- **Stack:** Terraform (`confluentinc/confluent` 2.57, `azurerm` ~3.80,
  `azuread`, `snowflakedb/snowflake` ~2.0). Docker Compose for MySQL + Connect.
  Iceberg on ADLS Gen2. Snowflake Horizon catalog integration against Tableflow
  IRC.
- **Value-only Avro:** `key.converter` is JSON with schemas disabled. A required
  Avro key becomes Iceberg `key_*` columns and suspends Tableflow the same way
  a required value field does.
- **Transform chain:** `unwrap` (`ExtractField$Value` / `after`) → `dropdeletes`
  (tombstones: Tableflow APPEND suspends on null values) → `flatten`
  (optionality, not nested paths) → `tfb` (`InsertField` of `tf_bootstrap` with
  an empty `static.value`). All of these are on Confluent Cloud's allowed list.
- **Snowflake query:** `NOSMT_TABLEFLOW_DB.CONFLUENT.NOSMT_MYDB_TEAM`.
  `AUTO_REFRESH` polls IRC every 30s; Tableflow's commit cadence is the real
  wait, typically 3–6 minutes from insert to visible row.
- **Honest constraint:** this FORWARD + all-optional + placeholder pattern is
  for when you cannot register the table schema before Tableflow. If the columns
  are known, or CDC can start first, use the usual BACKWARD + required first
  schema, then optional adds.

## Challenges & lessons

1. **Required fields suspend Tableflow, not just the Registry.** Adding a
   required Avro field is at most forward compatible in Schema Registry, and it
   is the change that takes the Iceberg table down. `NONE` is not a workaround:
   Tableflow applies its own rule on top.

2. **Debezium's unwrap SMT is the trap.** `ExtractNewRecordState` looks like
   the right tool and is the one that breaks this path. Measured on Debezium
   2.5.4 / Kafka 3.6: after that SMT, leaf fields are required. After
   `ExtractField$Value`, they stay optional with `default: null`.

3. **`tf_bootstrap` is not scaffolding.** Dropping it suspends the table
   (`the field 'tf_bootstrap' was deleted`). Schema Registry under FORWARD would
   allow the drop. Tableflow will not. The `tfb` transform has to re-add the
   column on every record, forever, and it must run after `flatten`.

4. **Deletes must never reach the topic.** A delete envelope has `after ==
   null`, so unwrap emits a null value. Tableflow in APPEND mode suspends on
   those. `dropdeletes` plus `include.schema.changes=false` keep non-row
   records off the topic.

5. **Do not swap the storage account in place.** Terraform will plan it, then
   fail with both halves destroyed and a one-hour grace period before Tableflow
   can re-enable. Destroy and apply a new environment. Pick a globally unique
   name you have not used recently, or Snowflake resolves metadata and then
   fails to download Parquet.

6. **Destroy can 409 on the Azure provider integration.** Once the cluster is
   gone, Confluent still reports the integration in use. `terraform state rm
   confluent_provider_integration_setup.azure` then destroy again; deleting the
   environment removes anything still inside it.
