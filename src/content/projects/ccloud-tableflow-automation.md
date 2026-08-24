---
title: 'Tableflow Watcher: Iceberg Turns On When the Schema Arrives'
description: 'A Kubernetes watcher that enables Confluent Cloud Tableflow on labelled Kafka topics the moment Schema Registry has a TopicNameStrategy subject.'
pubDate: 2026-08-24
tags: ['Kubernetes', 'Tableflow', 'Python', 'Terraform', 'Confluent', 'Azure']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/ccloud-tableflow-automation'
image: '/ccloud-tableflow-automation.svg'
---

![CFK KafkaTopic with an opt-in label, a Python watcher waiting on Schema Registry, then Tableflow Iceberg on ADLS Gen2](/ccloud-tableflow-automation.svg)

## Overview

You would not turn a Kafka topic into a warehouse table before anyone had agreed
what the rows look like. Tableflow agrees: it **refuses schemaless topics**.

That refusal is the whole trigger. You hang a label on a `KafkaTopic` that
*wants* Iceberg. A small in-cluster watcher polls Schema Registry for
`<topic>-value`. The moment that subject exists, it POSTs the Tableflow API and
writes Iceberg into your own ADLS Gen2 container. Until then the topic sits in
`PendingSchema`, which is a status you can see with `kubectl`, not a mystery in
the Confluent UI.

The code lives in
[ccloud-tableflow-automation](https://github.com/MosheBlumbergX/ccloud-tableflow-automation).
Terraform follows the same Azure provider-integration pattern as
[Debezium × Tableflow](/projects/debezium-tableflow-nosmt), including the
operational workarounds that stack only taught the hard way. This repo is the
other half of that story: not "how do I query Iceberg in Snowflake," but "how
does Tableflow get turned on at all, without a human clicking Enable."

## How it works

Three layers, each owning one job:

- **Terraform** provisions the Confluent Cloud environment and Standard cluster
  on Azure, the Azure provider integration, ADLS Gen2 storage, service accounts,
  and three API keys (Kafka, Schema Registry, Tableflow).
- **Confluent for Kubernetes** declares topics and Avro schemas as custom
  resources. The opt-in is a label on the `KafkaTopic`:
  `tableflow.confluent.io/enable=true`.
- **The watcher** is one standard-library Python file, mounted from a ConfigMap
  into `python:3.13-alpine`. No image to build, no packages to install.

```
labelled KafkaTopic CR          Schema CR (<topic>-value)
        │                                │
        │  CFK creates the topic         │  CFK registers Avro
        ▼                                ▼
 tableflow-watcher  ──polls──►  Schema Registry
        │
        │  subject exists, Tableflow not on yet
        ▼
 POST /tableflow/v1/tableflow-topics
        │
        ▼
 Iceberg in your ADLS Gen2 container
        │
        ▼
 annotation: tableflow.confluent.io/status=Enabled
```

Every 30 seconds the watcher lists labelled topics, looks up
`<topic>-value` (Tableflow only supports TopicNameStrategy), enables Tableflow
against the storage Terraform already wired, and patches status annotations back
onto the CR.

The reconcile is **one-way**. Removing the label takes the topic out of the
selector. It does not turn Tableflow off, stop writes to ADLS, or stop billing.

## What it demonstrates

The example deploys three topics so you can see each path without guessing:

| Topic | Labelled | Schema | What you see |
|---|---|---|---|
| `<prefix>-payments` | yes | yes | Tableflow enabled on the first pass |
| `<prefix>-clickstream` | yes | no | stays `PendingSchema` until you register a subject |
| `<prefix>-audit-log` | no | yes | ignored, even with a schema |

Register an Avro schema for clickstream and, within one poll interval, the
annotation flips to `Enabled`. That is the demo: the schema is the gate, the
label is the intent, and Kubernetes is where you read the decision.

It also shows a GitOps-friendly split. The label is an **input your pipeline
owns**. The watcher never writes labels, so `kubectl label` is reverted on the
next sync, which is the point. Status (`status`, `message`,
`last-transition-time`) is an **output the watcher owns**, patched with
`application/merge-patch+json`. `kubectl apply`, Argo CD, and Flux leave those
annotations alone because they are a different field manager (`Python-urllib`).
A replace-style sync drops them cosmetically; the next pass restores them from
live Confluent Cloud state without re-calling enable.

## Technical details

- **Stack:** Terraform (`confluentinc/confluent` ~> 2.57, `azurerm` ~> 3.80,
  `azuread`, `random`). Helm for CFK. `deploy.sh` turns `terraform output -json`
  into Kubernetes secrets and renders envsubst templates. Watcher: Python 3
  stdlib only (`urllib`, `ssl`, in-cluster service account).
- **Why a Tableflow API key exists.** The Tableflow API rejects ordinary Cloud
  API keys. Terraform creates a key whose `managed_resource.id` is `"tableflow"`.
  The watcher mounts that secret separately from Schema Registry credentials.
- **Per-topic knobs.** Optional annotations override table formats (default
  `ICEBERG`) and snapshot `retention-ms`. Do not set the status annotations
  yourself; the watcher owns them.
- **Storage.** Default is BYOS ADLS Gen2 plus the Confluent Azure provider
  integration. The watcher also understands Confluent Managed Storage and AWS
  BYOB if you point `TABLEFLOW_STORAGE_KIND` at them.
- **Honest constraint:** the watcher only sees `KafkaTopic` custom resources. A
  topic created by a connector in Confluent Cloud is invisible until you write a
  matching CR, and adopting it transfers lifecycle ownership to CFK.
- **This costs money.** A Standard cluster and every enabled Tableflow topic
  bill until you tear them down. Tableflow is not available on Google Cloud, and
  only some Azure/AWS regions support it.

## Challenges & lessons

1. **Opt-out is not disable.** Dropping `tableflow.confluent.io/enable` only
   removes the topic from the label selector. Tableflow keeps writing to ADLS
   Gen2 and keeps costing money. Disabling is a separate, deliberate API call,
   and you have to scale the watcher to zero first or the next pass turns it
   back on. `teardown.sh` follows that order for the same reason: watcher off,
   Tableflow deleted, then Terraform. Making missing labels mean "disable" would
   let a deleted line in git drop a table someone is querying.

2. **The Tableflow key is not your Cloud key.** Enable calls fail until the key
   is scoped to `managed_resource.id = "tableflow"`. That is why Terraform
   creates a dedicated service account and why `deploy.sh` writes a third
   secret.

3. **Do not ignore the whole annotation prefix.** Inputs (`table-formats`,
   `retention-ms`) and outputs (`status`, `message`, `last-transition-time`)
   share `tableflow.confluent.io/`. An Argo CD `ignoreDifferences` rule on the
   prefix would hide your own declared knobs. Ignore the three status keys
   individually.

4. **Never reuse a storage account name.** After destroy, the default generates
   `<prefix>tf<random>` again. Reusing a name shortly after produces a stack
   where Tableflow writes correctly but readers cannot fetch Parquet
   (`Unable to download parquet file`), even when the file is there and RBAC is
   right. Rebuilding with a fresh name is the reliable fix. Do not swap the
   account on a live Tableflow topic either: Confluent enforces a grace period
   of up to an hour.

5. **A rejected schema change suspends the topic, and it stays suspended.**
   Adding a required field or deleting a field leaves Tableflow down even after
   you register a corrected version. You register the fix, then PATCH
   `suspended: false`. The watcher will not unsuspend it for you.

6. **Destroy 409s on the Azure provider integration.** Tableflow cleanup is
   asynchronous, and the integration cannot be destroyed while a topic still
   references it. `teardown.sh` waits, then if Terraform still 409s it drops the
   integration from state and retries. Deleting the environment removes it
   server-side, so nothing is orphaned. The same `az ad sp create --id` loop
   from the sibling Tableflow repos is how the multi-tenant Confluent app
   becomes a service principal in your tenant in the first place.
