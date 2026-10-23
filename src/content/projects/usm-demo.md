---
title: 'USM Demo: Self-Managed Kafka in the Cloud UI'
description: 'Confluent Platform 8.3 on EKS with CFK 3.3, registered in Confluent Cloud Unified Stream Manager over PrivateLink, with a source-initiated link of users and pageviews, Schema Linking, Control Center, and a laptop proxy into the private Cloud Console.'
pubDate: 2026-10-22
updatedDate: 2026-10-23
tags: ['Kafka', 'CFK', 'EKS', 'Terraform', 'Confluent Cloud', 'Control Center']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/usmDemo'
image: '/usm-demo.svg'
---

![CP on EKS sends USM telemetry, users, and pageviews over PrivateLink, with Control Center and a Cloud Console proxy on the laptop](/usm-demo.svg)

## Overview

You open Confluent Cloud and the clusters on the screen are the ones Cloud is running. The brokers in your VPC sit behind another login.

Think of a locked basement and a monitor at the front desk. The basement calls the desk on a private line, paints its own cameras onto that monitor, and sends copies of two shelves upstairs. You also get a service window into the basement, and a slot in the lobby glass that looks back at those upstairs shelves.

That private line is **PrivateLink**. **Confluent Platform 8.3** (KRaft) runs on **EKS** under **Confluent for Kubernetes 3.3**. A **Unified Stream Manager** agent dials out, and the CP cluster shows up in the same environment as a private **Enterprise** cluster: brokers, topics, Connect, consumer groups, and a Kafka Streams app. The two shelves are `users` (JSON) and `pageviews` (Avro), mirrored by a source-initiated cluster link, with Schema Linking keeping `pageviews-value` on the same schema ID. The service window is **Control Center** next-gen 2.6 at `http://localhost:9021`. The lobby slot is a TLS passthrough proxy so the Cloud Console in your browser can reach the private Enterprise endpoints.

Terraform, CFK custom resources, and a handful of shell scripts are the whole thing. They live in [usmDemo](https://github.com/MosheBlumbergX/usmDemo).

## How it works

`make up` is seven steps: infra, CP (including Control Center), USM registration, workloads, the hybrid Cloud path, then `make c3` and `make console`. Three pipes share one PrivateLink. Two local paths let the laptop see the private clusters.

- **Telemetry.** CFK turns on `dependencies.usmAgentClient.clientMetricsEnabled` for Kafka and KRaft, which configures the telemetry exporter and the KIP-714 / KIP-1076 client-metrics subscription (including `org.apache.kafka.stream.*`). The USM Agent (`cp-usm-agent:1.2.1`) posts that to Cloud. `make register` then registers the CP Kafka cluster and the Connect cluster through the `/usm/v1` REST API. `dependencies.metricsClient` adds a second exporter, `_c3`, aimed at Control Center's Prometheus on port 9090.
- **Data.** A destination `ClusterLink` is applied first and declares mirror topics `users` and `pageviews`. A source `ClusterLink` on the CP brokers dials the Enterprise cluster. A fully managed Datagen connector on Cloud writes `orders`. `ccloud-demo-consumer` on EKS reads `users` and `orders` back through the same access point. `ccloud-avro-consumer` reads the `pageviews` mirror.
- **Schemas.** Datagen writes Avro pageviews to `pageviews` against the CP Schema Registry. A `SchemaExporter` copies subject `pageviews-value` into Cloud Schema Registry with `contextType: NONE`, in IMPORT mode, so the schema ID in the mirrored bytes still resolves.
- **Local UIs.** `make c3` port-forwards Control Center to `localhost:9021`. `make console` is the browser path into the private cluster: `/etc/hosts` sends the `lkc-` and `lsrc-` names to `127.0.0.1`, pf redirects 443 to 18443, kubectl port-forwards into the `console-proxy` pod, and NGINX `ssl_preread` forwards the TLS stream by SNI. TLS ends in the browser.

```
EKS  namespace usm
  KRaft x3 · Kafka x3 · Connect x2 · Schema Registry · Control Center 2.6
  datagen-users → users · datagen-pageviews → pageviews · s3-sink → S3
  ks-validate (WordCount) · persistent-consumer
        │
        ├─ USM Agent ──────── PrivateLink ─► USM view
        ├─ cluster link (source) ──────────► Enterprise: users, pageviews mirrors
        ├─ schema exporter (NONE) ─────────► Cloud SR: pageviews-value, same ID
        └─ metricsClient (_c3) ────────────► Control Center Prometheus :9090

  laptop
        ├─ make c3 ── port-forward ────────► http://localhost:9021
        └─ make console
              /etc/hosts + pf :443 → :18443 → port-forward → console-proxy
              NGINX ssl_preread ── PrivateLink ─► Enterprise REST / Cloud SR

  same PrivateLink, reads from EKS:
        managed Datagen → orders
        ccloud-demo-consumer reads users + orders
        ccloud-avro-consumer reads pageviews via Cloud SR
```

Workloads on the CP side are small on purpose. JSON users, Avro pageviews, an S3 sink that uses EKS Pod Identity for its AWS credentials, a SASL consumer on `users`, and a stateful Kafka Streams WordCount so the Streams page has RocksDB and processor metrics to show.

Optional `make flink` adds Confluent Manager for Apache Flink and a `drop-gender` SQL job from `users` to `users-redacted`. The core walkthrough stands without it.

## What it demonstrates

The walkthrough is the Confluent Cloud UI for environment `usm-demo`, the Enterprise cluster beside it, and the two local UIs:

1. **The CP cluster sits next to cloud clusters**, with broker, partition, and throughput metrics.
2. **Topics** from `users` through `streams-wordcount-output` show live throughput.
3. **Connectors** `datagen-users`, `datagen-pageviews`, and `s3-sink-users` run on self-managed Connect and still show task status. The S3 bucket is a Terraform output.
4. **Consumer groups** include `users-persistent-consumer`, `connect-s3-sink-users`, and `ks-validate`, with lag.
5. **Kafka Streams** for `ks-validate`: thread and state, RocksDB, end-to-end latency, processor metrics.
6. **Hybrid data.** On cluster `usm-demo-cloud`, `users` and `pageviews` are mirror topics from the `cp-to-cloud` link. `orders` is produced in Cloud. `ccloud-demo-consumer` on EKS reads `users` and `orders`.
7. **Schema Linking.** `pageviews-value` appears in the environment Schema Registry in IMPORT mode with the same schema ID as CP. `ccloud-avro-consumer` decodes the mirrored Avro through the Cloud SR.
8. **Control Center** on `http://localhost:9021` is the CP-side UI. Kafka and KRaft push broker metrics to its Prometheus through the `_c3` exporter. With no license it runs as a 30-day trial.
9. **Cloud Console on the private cluster.** After `make console`, mirror topics show the link icon and the message browser loads, because those pages call the private REST endpoint from your browser.

## Technical details

- **AWS:** Terraform builds a 3-AZ VPC with a single NAT, EKS 1.35, a managed node group (default 4× `m5.xlarge`), the EBS CSI driver, Pod Identity, and an S3 bucket plus IAM role for the sink. Default region is `us-east-1`. Prefix is `usm-demo`.
- **PrivateLink:** a Confluent Cloud ingress PrivateLink gateway, an AWS interface VPC endpoint, an access point, and a Route53 private zone for `*.<access-point-domain>`. The agent front door is `https://api-<ap-id>.<region>.aws.accesspoint.glb.confluent.cloud`. The Enterprise bootstrap hostname is `lkc-<id>-<ap-id>.<region>.aws.accesspoint.glb.confluent.cloud`.
- **Confluent Cloud:** an environment with Advanced Stream Governance, a USM service account (`UsmAgent` and `DataSteward`) plus a Cloud resource-management API key, and a private Enterprise cluster with a `cloud-app` service account. That account is `CloudClusterAdmin` here to keep the demo short.
- **CFK:** namespace `confluent` for the operator, namespace `usm` for KRaftController ×3, Kafka ×3 (`cp-server:8.3.0`), Connect ×2, Schema Registry, the USMAgent, topics, and Control Center (`cp-enterprise-control-center-next-gen:2.6.1`) with its Prometheus and Alertmanager. Connectors, apps, and the console proxy live under `k8s/connectors`, `k8s/apps`, and `k8s/ccloud`.
- **Console proxy:** `k8s/ccloud/60-console-proxy.yaml` is an NGINX pod with no Service. It forwards only to `*.aws.accesspoint.glb.confluent.cloud`, resolved through VPC DNS and the Route53 private zone. `make ccloud` deploys it. `scripts/console-proxy-local.sh` adds the hosts entries and the pf rule, and sudo runs only when the private endpoint is still unreachable. `make console` is idempotent. `make console-stop` and `make down` remove the forward, the hosts lines, and the pf rule.
- **Registration:** `confluent usm kafka|connect register` wants an interactive login. `scripts/register-usm.sh` calls the same API with the Cloud API key.
- **Secrets stay out of git.** `.env`, tfvars, state, and the generated kubeconfig are ignored. SASL passwords come from Terraform `random_password` and are pushed into Kubernetes by `scripts/deploy-cp.sh`. Local Terraform state holds API keys. A remote encrypted backend is the step past a personal demo.

## Challenges & lessons

1. **PrivateLink is the only front door.** The USM Agent reaches Confluent Cloud on a private connection. If the agent is dark, resolve the front-door host from a pod in the cluster. Route53 should answer with VPC-endpoint addresses.
2. **Register Connect before you create connectors.** USM learns a connector from the metadata event emitted when the connector is created. A connector that already exists is rejected with `USM resource not found (Connector)`. `make workloads` runs after `make register`. On a live cluster, delete the `Connector` CRs and apply them again.
3. **The Cloud credential secret has its own name.** CFK writes `<agent-name>-ccloud-credential` for the agent Envoy config and will overwrite a secret you placed at that name. The CR then fails because `basic.txt` is missing. This repo uses `ccloud-usm-agent-credential`.
4. **The Streams page needs CP 8.3, DEBUG metrics, and a stateful topology.** 8.2.x skips the string gauges `stream.state`, `thread.state`, and `application.id`. `ks-validate` sets `metrics.recording.level=DEBUG` and uses `count()` so RocksDB shows up. It runs on a `cp-kafka` image because RocksDB needs `libstdc++`. Connect 8.1.2 through 8.3.0 also needs the `confluent.telemetry.exporter._usm.metrics.include` override in `k8s/cfk/30-connect.yaml`.
5. **Schema Linking keeps the ID in the default context.** Mirrored `pageviews` records carry the CP schema ID in the header bytes. `contextType: NONE` exports `pageviews-value` as-is into the Cloud SR default context and sets IMPORT mode, so that ID still resolves. `AUTO` would place the subject under a `.lsrc-...` context. The CP Schema Registry needs `enableSchemaExporter: true` and a `passwordEncoder` for the stored destination credentials.
6. **The link is source-initiated, and its truststore is tiny.** Cloud has no route to the brokers on EKS, so the CP brokers dial out. Create the destination `ClusterLink` first (it declares the mirrors), then the source CR. The Kafka `passwordEncoder` encrypts the Cloud credentials stored on that link. The TLS truststore lives in the link config, capped at 32 KB, so the script ships only the ISRG roots. Terraform runs outside the VPC, which is why the Enterprise cluster sets `disable_wait_for_ready = true`. CFK applies the cloud topics and the link from inside the cluster.
7. **The Cloud Console calls the private REST endpoint from the browser.** Mirror state, messages, and schemas load from that hostname. `make console` maps the `lkc-` and `lsrc-` names to `127.0.0.1`, redirects 443 to 18443 with pf, and port-forwards to `console-proxy`. macOS blocks a normal user from binding 443, and kubectl has to stay in your SSO session, so sudo covers only the hosts file and the pf rule. Re-run `make console` after a reboot, because pf rules are gone, or after an AWS SSO refresh. If the endpoint is unreachable, read `.console-proxy.log`.
8. **Control Center rides a second exporter.** Next-gen C3 ships its own Prometheus and Alertmanager. `dependencies.metricsClient` on Kafka and KRaft makes CFK add `_c3` beside the USM exporter. Adding it to a running cluster rolls the controllers and brokers. With no license, C3 is a 30-day trial.
9. **An authorizer changes the KRaft restart story.** This demo leaves authorization off. If you turn on the `simple` authorizer with `allow.everyone.if.no.acl.found=true`, add `User:ANONYMOUS` to `superUsers` on Kafka and on KRaft. Controllers need that to re-form quorum after a cold restart.
