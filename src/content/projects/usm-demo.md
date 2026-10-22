---
title: 'USM Demo: Self-Managed Kafka in the Cloud UI'
description: 'Confluent Platform 8.3 on EKS with CFK 3.3, registered in Confluent Cloud Unified Stream Manager over PrivateLink, plus a source-initiated cluster link and Schema Linking into an Enterprise cluster.'
pubDate: 2026-10-22
tags: ['Kafka', 'CFK', 'EKS', 'Terraform', 'Confluent Cloud', 'Schema Registry']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/usmDemo'
image: '/usm-demo.svg'
---

![CP on EKS ships USM telemetry, a source-initiated cluster link, and Schema Linking over PrivateLink into Confluent Cloud](/usm-demo.svg)

## Overview

You open Confluent Cloud and the clusters you expect to see are the ones Cloud itself is running. The brokers in your VPC are a different login, a different UI, and usually a port-forward.

This demo puts both on one screen. **Confluent Platform 8.3** (KRaft) runs on **EKS** under **Confluent for Kubernetes 3.3**. A **Unified Stream Manager** agent dials out over **PrivateLink** and the CP cluster shows up in the same Cloud environment as a private **Enterprise** cluster: brokers, topics, Connect, consumer groups, and a Kafka Streams app.

Picture a locked basement and a monitor at the front desk. The basement gear calls the desk over a private line, paints its own cameras onto the lobby monitor, and ships copies of two shelves upstairs. Here that call is PrivateLink: USM telemetry, the cluster link, and schema export all leave the VPC on it.

Terraform, CFK custom resources, and a handful of shell scripts are the whole thing. They live in [usmDemo](https://github.com/MosheBlumbergX/usmDemo).

## How it works

`make up` is five steps: infra, CP, USM registration, workloads, then the hybrid Cloud path. Three pipes share one PrivateLink.

- **Telemetry.** CFK turns on `dependencies.usmAgentClient.clientMetricsEnabled` for Kafka and KRaft, which configures the telemetry exporter and the KIP-714 / KIP-1076 client-metrics subscription (including `org.apache.kafka.stream.*`). The USM Agent (`cp-usm-agent:1.2.1`) posts that to Cloud. `make register` then registers the CP Kafka cluster and the Connect cluster through the `/usm/v1` REST API.
- **Data.** A destination `ClusterLink` is applied first and declares mirror topics `test1` and `test2`. A source `ClusterLink` on the CP brokers dials the Enterprise cluster. A fully managed Datagen connector on Cloud writes `orders`. Consumers on EKS read both the mirrors and `orders` back through the same access point.
- **Schemas.** Datagen writes Avro pageviews to `test2` against the CP Schema Registry. A `SchemaExporter` copies subject `test2-value` into Cloud Schema Registry with `contextType: NONE`, in IMPORT mode, so the schema ID in the mirrored bytes still resolves.

```
EKS  namespace usm
  KRaft x3 · Kafka x3 · Connect x2 · Schema Registry
  datagen-users → test1 · datagen-pageviews → test2 · s3-sink → S3
  ks-validate (WordCount) · persistent-consumer
        │
        ├─ USM Agent ──────── PrivateLink ─► USM view
        ├─ cluster link (source) ──────────► Enterprise: test1, test2 mirrors
        └─ schema exporter (NONE) ─────────► Cloud SR: test2-value, same ID

  same PrivateLink, other direction for reads:
        managed Datagen → orders
        ccloud-consumer + ccloud-avro-consumer (on EKS) read the Enterprise cluster
```

Workloads on the CP side are small on purpose. JSON users, Avro pageviews, an S3 sink that uses EKS Pod Identity (no AWS keys on the connector), a SASL consumer, and a stateful Kafka Streams WordCount so the Streams page has RocksDB and processor metrics to show.

Optional `make flink` adds Confluent Manager for Apache Flink and a `drop-gender` SQL job from `test1` to `flink-test`. The core walkthrough does not need it.

## What it demonstrates

The walkthrough is the Confluent Cloud UI for environment `usm-demo`, then the Enterprise cluster beside it:

1. **The CP cluster sits next to cloud clusters**, with broker, partition, and throughput metrics.
2. **Topics** from `test1` through `streams-wordcount-output` show live throughput.
3. **Connectors** `datagen-users`, `datagen-pageviews`, and `s3-sink-users` run on self-managed Connect and still show task status. The S3 bucket is a Terraform output.
4. **Consumer groups** include `test1-persistent-consumer`, `connect-s3-sink-users`, and `ks-validate`, with lag.
5. **Kafka Streams** for `ks-validate`: thread and state, RocksDB, end-to-end latency, processor metrics.
6. **Hybrid data.** On cluster `usm-demo-cloud`, `test1` and `test2` are mirror topics from the `cp-to-cloud` link. `orders` is produced in Cloud. `ccloud-demo-consumer` on EKS reads both.
7. **Schema Linking.** `test2-value` appears in the environment Schema Registry in IMPORT mode with the same schema ID as CP, so the message browser decodes the mirrored Avro. `ccloud-avro-consumer` decodes the same records through the Cloud SR.

## Technical details

- **AWS:** Terraform builds a 3-AZ VPC with a single NAT, EKS 1.35, a managed node group (default 4× `m5.xlarge`), the EBS CSI driver, Pod Identity, and an S3 bucket plus IAM role for the sink. Default region is `us-east-1`. Prefix is `usm-demo`.
- **PrivateLink:** a Confluent Cloud ingress PrivateLink gateway, an AWS interface VPC endpoint, an access point, and a Route53 private zone for `*.<access-point-domain>`. The agent front door is `https://api-<ap-id>.<region>.aws.accesspoint.glb.confluent.cloud`. The Enterprise bootstrap hostname is `lkc-<id>-<ap-id>.<region>.aws.accesspoint.glb.confluent.cloud`.
- **Confluent Cloud:** an environment with Advanced Stream Governance, a USM service account (`UsmAgent` and `DataSteward`) plus a Cloud resource-management API key, and a private Enterprise cluster with a `cloud-app` service account. That account is `CloudClusterAdmin` here to keep the demo short.
- **CFK:** namespace `confluent` for the operator, namespace `usm` for KRaftController ×3, Kafka ×3 (`cp-server:8.3.0`), Connect ×2, Schema Registry, the USMAgent, and topics. Connectors and apps are their own manifests under `k8s/connectors` and `k8s/apps`.
- **Registration:** `confluent usm kafka|connect register` wants an interactive login. `scripts/register-usm.sh` calls the same API with the Cloud API key.
- **Secrets stay out of git.** `.env`, tfvars, state, and the generated kubeconfig are ignored. SASL passwords come from Terraform `random_password` and are pushed into Kubernetes by `scripts/deploy-cp.sh`. Local Terraform state holds API keys. A remote encrypted backend is the step past a personal demo.

## Challenges & lessons

1. **PrivateLink is the only front door.** The USM Agent reaches Confluent Cloud on a private connection. If the agent is dark, resolve the front-door host from a pod in the cluster. Route53 should answer with VPC-endpoint addresses, not public ones.
2. **Register Connect before you create connectors.** USM learns a connector from the metadata event emitted when the connector is created. A connector that already exists is rejected with `USM resource not found (Connector)`. `make workloads` runs after `make register`. Fixing an existing cluster means deleting the `Connector` CRs and applying them again.
3. **The Cloud credential secret has its own name.** CFK generates `<agent-name>-ccloud-credential` for the agent Envoy config and will overwrite a secret you created with that name. The CR then fails because `basic.txt` is missing. This repo uses `ccloud-usm-agent-credential`.
4. **The Streams page needs CP 8.3, DEBUG metrics, and a stateful topology.** 8.2.x skips the string gauges `stream.state`, `thread.state`, and `application.id`. `ks-validate` sets `metrics.recording.level=DEBUG` and uses `count()` so RocksDB shows up. It runs on a `cp-kafka` image because RocksDB needs `libstdc++`, which `apache/kafka` images do not ship. Connect 8.1.2 through 8.3.0 also needs the documented `confluent.telemetry.exporter._usm.metrics.include` override in `k8s/cfk/30-connect.yaml`.
5. **Schema Linking keeps the ID only in the default context.** Mirrored `test2` records carry the CP schema ID in the header bytes. `contextType: NONE` exports the subject as-is into the Cloud SR default context and sets IMPORT mode, so that ID still resolves. `AUTO` would park it under a `.lsrc-...` context. The CP Schema Registry needs `enableSchemaExporter: true` and a `passwordEncoder` for the stored destination credentials.
6. **The link is source-initiated, and its truststore is tiny.** Cloud cannot dial the brokers on EKS, so CP opens the connection. Create the destination `ClusterLink` first (it declares the mirrors), then the source CR, whose Cloud credentials the Kafka `passwordEncoder` encrypts. The TLS truststore is stored in the link config, which caps at 32 KB, so the script ships only the ISRG roots Confluent Cloud chains to. Terraform itself runs outside the VPC, so the Enterprise cluster sets `disable_wait_for_ready = true`. Cloud topics and the link are applied by CFK from inside the cluster.
7. **An authorizer changes the KRaft restart story.** This demo leaves authorization off. If you turn on the `simple` authorizer with `allow.everyone.if.no.acl.found=true`, add `User:ANONYMOUS` to `superUsers` on both Kafka and KRaft. Controllers need that to re-form quorum after a cold restart.
