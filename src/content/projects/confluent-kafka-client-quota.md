---
title: 'Client Quotas: Same Producers, Three Ceilings'
description: 'Terraform demo of Confluent Cloud client quotas: three identical producers share one cluster, and the brokers hold two of them to 10 MB/s and 1 MB/s while the third runs uncapped.'
pubDate: 2026-11-19
tags: ['Terraform', 'Kafka', 'Confluent Cloud', 'Docker', 'Bash']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/confluent_kafka_client_quota'
image: '/confluent-kafka-client-quota.svg'
---

![Three tenants after a 3-minute race: payments-api held at 9.91 MB/s, nightly-etl held at 1.00 MB/s, new-team uncapped at 97.77 MB/s](/confluent-kafka-client-quota.svg)

## Overview

You start three copies of the same producer. Same topic, same cluster, same 1 KB records, and `--throughput -1`, so the client itself has no speed limit. Three minutes later the scoreboard reads `payments-api` at 9.91 MB/s, `nightly-etl` at 1.00 MB/s, and `new-team` at 97.77 MB/s. Every send succeeded. The tight quota simply waited: its average send latency climbed to about 34 seconds.

Three roommates share one internet line and all run the same speed test. The router caps one device at 1 MB/s and another at 10 MB/s, and leaves the third alone. The uncapped laptop fills the line. The capped ones still finish. The pages just take longer.

That router rule, on a shared Kafka cluster, is a client quota. One service account may produce at most X MB/s and consume at most Y MB/s. This repo is that rule as Terraform, plus a three-minute race that shows the ceilings hold.

## How it works

Terraform is the control plane. The scripts are the load.

- **Identities.** One service account and Kafka API key per tenant, plus a topic admin that can manage only `client-quota-demo-orders`. Each tenant gets `DeveloperWrite` and `DeveloperRead` on that topic, and `DeveloperRead` on consumer groups named `<tenant>-*`.
- **Quotas.** `confluent_kafka_client_quota` sets ingress and egress byte rates on the service account. `payments-api` is 10 MB/s, `nightly-etl` is 1 MB/s, `new-team` has `quota = null`. The cluster-wide `<default>` quota stays off.
- **The race.** `./scripts/demo.sh` runs `kafka-producer-perf-test` in Docker for all three at once: 1 KB records, `acks=all`, no compression, no client-side limit. A summary uses the final third of the default 180 second run.
- **What the broker does.** An over-quota client receives a throttle time, and the connection is briefly muted. Throughput falls. The produce calls still succeed.

```
terraform apply ──► service account + API key per tenant
                ──► confluent_kafka_client_quota (bytes/s on the principal)
        │
        ▼
./scripts/demo.sh
  payments-api   10 MB/s ──┐
  nightly-etl     1 MB/s ──┼──► kafka-producer-perf-test (--throughput -1)
  new-team        none   ──┘
        │
        ▼
brokers: throttle time, connection muted
        │
        ▼
final third: 9.91 MB/s · 1.00 MB/s · 97.77 MB/s (uncapped)
```

Quotas live in the control plane, so `terraform apply` works from a laptop even when the cluster is Private Link or peering. Topic creation and the perf tests are data plane. On a private cluster, set `VM_SSH_TARGET` and those scripts copy themselves onto a VM inside the network and run there.

## What it demonstrates

The published runs are on an Enterprise cluster (Azure, UK South, Private Link), with load from a Standard_B2s VM. Throughput in the scripts is MB/s, meaning 10^6 bytes, which is the unit quotas use.

- **Steady state is close.** In the 180 second race, the final window was 9.91 MB/s on a 10 MB/s quota and 1.00 MB/s on a 1 MB/s quota. The tenant with no quota reached 97.77 MB/s (peak 98.29), which was the VM network.
- **A live edit reaches a running producer.** Change `nightly-etl` in `terraform.tfvars`, run `terraform apply`, and leave `./scripts/produce.sh` running. The new ceiling shows up without a restart, typically within about a minute. A fresh run at 3 MB/s then held 2.9 to 3.05 MB/s.
- **Egress matches.** A consumer with a 3 MB/s egress quota: one burst window, then 2.96 to 2.98 MB/s.
- **The brokers agree.** `./scripts/metrics.sh` reads the Metrics API (`request_bytes`, produce, grouped by `principal_id`). Those numbers arrive a few minutes late and show the same shape.

## Technical details

- **Stack:** Terraform with the Confluent provider, Bash, and the Apache Kafka perf tools in Docker. The Cloud API key comes from the environment. Client configs land in git-ignored `generated/` at mode `0600`.
- **Where it runs:** Option A creates a 1 CKU single-zone Dedicated cluster with a public endpoint. It bills per hour, and provisioning can take 30 minutes or more. Option B looks up an existing Enterprise, Freight, or Dedicated cluster and leaves that cluster alone. A Basic or Standard cluster fails a postcondition. Dedicated is the public option because Enterprise and Freight are private only.
- **The quota object:** Rates are bytes per second, minimum 1 MB/s in and 1 MB/s out (`1 MB = 1,000,000` bytes). A quota is a ceiling. It caps sustained throughput. It does not reserve capacity. Every principal listed on one quota receives the full amount. A principal can sit on one quota per cluster, plus an optional `<default>` for everyone else.
- **RBAC stays on the demo topic.** The topic admin has `DeveloperManage` on that topic only. On a public endpoint Terraform creates the topic (6 partitions, `retention.ms` of one hour). On a private endpoint it skips the topic resource and `scripts/setup-topic.sh` creates it from inside the network.
- **What the runs actually bound:** Steady state landed within a few percent of the quota, for produce and consume. Right after idle, or right after a quota create or change, a client can run far above the cap until the quota is distributed to the brokers. In these runs that window was anywhere from about 7 seconds to more than 90 seconds.

## Challenges & lessons

1. **The ceiling arrives late.** Confluent Cloud slices each quota across the brokers that have been seeing that principal. A client starting from idle can sit at tens of MB/s on a 1 MB/s quota until that happens. The demo runs for 3 minutes and scores only the final third. A 60 second race sometimes settled in about 10 seconds, and a later 90 second race had still not settled for `nightly-etl`.
2. **Over quota looks like slowness.** The 1 MB/s producer's average send latency climbed to about 34 seconds, with peaks around a minute, and `produce-throttle-time-avg` in the hundreds of milliseconds. There is no error to catch and no retry loop to write. Size the cluster for the burst. Use the quota to stop a job monopolising the cluster for minutes or hours.
3. **`<default>` applies to the whole cluster.** The principal `<default>` caps every identity that lacks its own quota: other teams, connectors, anything you forgot. On a cluster you own, that is the guard rail. On a shared cluster it is a surprise. This repo leaves `default_quota = null`. With it set to 2 MB/s, `new-team` would be held there too.
4. **Private clusters split the work in two.** Service accounts, keys, RBAC, and quotas are control plane, so Terraform runs on the laptop. The topic, producers, and consumers need a machine that can resolve the private bootstrap. `show-quotas.sh` and `metrics.sh` stay local because they call public APIs. API-key readiness checks are skipped when the data plane is unreachable, because that check calls the cluster REST endpoint. Delete the topic with `scripts/delete-topic.sh` before `terraform destroy`: destroy still needs the topic-admin key.
5. **The principal is the quota.** An API key, a `client.id`, and a topic name do not carry one. Two apps on one service account share one ceiling and disappear into one principal in the Metrics API. Putting two service accounts on a single quota gives each of them the full rate. OAuth and OIDC apps use an identity pool id (`pool-xxxx`) the same way. `lifecycle { prevent_destroy = true }` is left commented out so the demo can be destroyed. Turn it on when the quota is protecting other tenants.
6. **The tools disagree on the unit.** `kafka-producer-perf-test` prints MiB/s. The scripts multiply by 1.048576 and judge against MB/s, because that is what `ingress_byte_rate` means. A quota change also bursts again while the new limit is distributed: in one run, raising `nightly-etl` from 1 MB/s to 3 MB/s left the producer uncapped for a stretch before 3 MB/s held.
