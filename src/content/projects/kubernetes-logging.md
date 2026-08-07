---
title: 'CFK Platform Logs → Splunk on Kubernetes'
description: 'End-to-end guide for sending Confluent for Kubernetes (CFK) platform logs to Splunk with the Splunk OpenTelemetry Collector — JSON stdout, HEC, and index routing via annotations.'
pubDate: 2026-08-07
tags: ['Kubernetes', 'CFK', 'Splunk', 'OpenTelemetry', 'Kafka', 'Helm']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/kubernetesLogging'
image: '/kubernetes-logging.svg'
---

![CFK pods stdout → Splunk OTel DaemonSet → Splunk HEC index](/kubernetes-logging.svg)

## Overview

You deploy Confluent for Kubernetes, Connect starts humming, and then someone
asks the question every ops team eventually hears: *where do the logs go?*

`kubectl logs` works until it doesn't — pods restart, nodes roll, and the
message you needed last Tuesday is gone. This project is the notes from wiring
**CFK platform logs** into **Splunk** with almost no change to the apps
themselves: containers keep writing to stdout, a **Splunk OpenTelemetry
Collector** DaemonSet tails those files on every node, and HEC ships them into
an index you can actually search.

> Full walkthrough, Helm values template, and a ready-to-apply
> `confluent-platform.yaml` live in the
> [kubernetesLogging](https://github.com/MosheBlumbergX/kubernetesLogging) repo.

## How it works

Logs never leave the Kubernetes path they already use — they just get a second
reader:

```
CFK pods (stdout)
      │
      ▼
kubelet writes container logs on the node
      │
      ▼
Splunk OTel Collector (DaemonSet) ──HEC──► Splunk (index: k8s_logs)
```

A few pieces make it useful instead of noisy:

- **JSON Log4j2** on CFK components so Splunk can extract `severity`,
  `textPayload`, `sourceLocation`, and friends without regex gymnastics.
- **Namespace annotation** `splunk.com/index=k8s_logs` so Confluent traffic
  lands in the right index (pod-level annotations win if you need overrides).
- **Cluster name** in the collector values surfaces as `k8s.cluster.name` —
  handy when more than one cluster feeds the same Splunk.

## Splunk setup (short version)

1. Create an index (e.g. `k8s_logs`) — `_json` sourcetype helps with field
   extraction.
2. Create a **HEC token**; note the endpoint
   (`https://<host>:8088/services/collector`) and token value.
3. Prefer **one HEC token per Kubernetes cluster**.

A free Splunk Cloud trial works fine for demos.

## Install the collector

```bash
helm repo add splunk-otel-collector-chart \
  https://signalfx.github.io/splunk-otel-collector-chart
helm repo update

kubectl create namespace observability
```

Copy `splunkValuesTemplate.yaml` → `splunkValues.yaml` and fill in endpoint,
token, and index:

```yaml
clusterName: k8s-cfk
distribution: aks   # or eks / gke / omit

splunkPlatform:
  endpoint: "https://Splunk-Endpoint:8088/services/collector"
  token: "Token from Splunk"
  index: "Index from Splunk"
  insecureSkipVerify: true  # non-prod / self-signed only

logsCollection:
  containers:
    enabled: true
```

```bash
helm -n observability install splunk-otel \
  -f splunkValues.yaml \
  splunk-otel-collector-chart/splunk-otel-collector
```

That deploys agents on every node; they tail container logs by default.

## Deploy Confluent Platform

```bash
kubectl create namespace confluent \
  && kubectl annotate namespace confluent splunk.com/index="k8s_logs"

helm repo add confluentinc https://packages.confluent.io/helm/charts
helm repo update
helm install confluent-operator confluentinc/confluent-for-kubernetes \
  --namespace confluent

kubectl apply -f confluent-platform.yaml
```

The CR manifests override Log4j2 so Connect (and friends) emit JSON lines like:

```json
{"severity":"INFO","timestamp":"2026-03-16T13:46:11.435Z","textPayload":"…","sourceLocation":{"file":"PluginScanner.java","line":"80","function":"…"},"thread":"main"}
```

## Validation

```bash
kubectl -n observability get pods -l app=splunk-otel-collector
kubectl -n confluent get pods -l "platform.confluent.io/type=connect" -o wide
```

In Splunk:

```spl
index=k8s_logs k8s.cluster.name=k8s-cfk
```

Narrow to Confluent, a pod, or a container with `k8s.namespace.name`,
`k8s.pod.name`, or `k8s.container.name`. Table and timechart queries in the
repo README turn severity into something you can chart.

## Challenges & lessons

1. **Stdout is the contract.** Don't invent a sidecar per app — let kubelet
   write the files and let the collector read them.
2. **JSON before search.** Log4j2 pattern overrides pay off immediately in
   Splunk field extraction; plain text logs fight you forever.
3. **Annotations are the dial.** Namespace `splunk.com/index` for the default;
   pod `include` / `exclude` when one noisy workload should stay out.
4. **HEC errors hide in agent logs.** If Splunk is empty, `grep -i hec` the
   OTel pods before rewriting half the values file.
5. **`insecureSkipVerify` is a demo knob.** Fine for a trial cert; fix the
   server cert before production.
6. **Same pattern scales.** Kafka, Schema Registry, ksqlDB — same collector,
   same annotations, different pods.
