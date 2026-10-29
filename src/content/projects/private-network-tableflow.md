---
title: 'Private Network Tableflow: Iceberg SQL Without a Catalog'
description: 'Private-link Confluent Tableflow on Azure writes Iceberg into a locked-down ADLS account, and a 60-second sync job points Snowflake at the newest metadata file.'
pubDate: 2026-10-29
tags: ['Terraform', 'Confluent', 'Snowflake', 'Iceberg', 'Azure', 'Private Link']
status: 'in-progress'
repo: 'https://github.com/MosheBlumbergX/PrivateNetworkSnowflakeConfluentTableFlowIceberg'
image: '/private-network-tableflow.svg'
---

![Demo VM over Private Link to Tableflow, Iceberg metadata on ADLS, Snowflake refresh](/private-network-tableflow.svg)

## Overview

You already know the public version of this idea. Tableflow turns a Kafka topic into Iceberg, publishes it through an Iceberg REST Catalog, and Snowflake Horizon points at that catalog. That walkthrough is [Tableflow × Snowflake](/projects/snowflake-tableflow-iceberg).

Turn **private networking** on and that catalog is unavailable. Confluent's docs say the built-in Iceberg REST Catalog is not supported on private-networking clusters. On Azure the external catalog sync targets do not fill the gap either: Glue is AWS only, and Unity Catalog and Open Catalog are not supported over private networking.

This demo treats **ADLS as the source of truth**. A VM in your VNet produces Avro `stock_trades` into an Enterprise cluster over a Private Link Attachment. Tableflow writes Iceberg files into ADLS Gen2 through an egress Private Link gateway. Every 60 seconds a small job finds the newest `metadata/*.metadata.json` and tells Snowflake to create or refresh an Iceberg table at that path. Analysts still `SELECT`. The pointer they follow is a metadata file in the container.

The Terraform lives in [PrivateNetworkSnowflakeConfluentTableFlowIceberg](https://github.com/MosheBlumbergX/PrivateNetworkSnowflakeConfluentTableFlowIceberg). The sync pattern comes from Jeremy Ber's [tableflow-manual-sync-to-unity-catalog](https://github.com/jeremyber/tableflow-manual-sync-to-unity-catalog): discover `RUNNING` Tableflow topics, register them in the query engine, stay idempotent. This repo points that pattern at Snowflake and Iceberg, and adds the step that hunts down the latest metadata file.

## How it works

Customer records stay on Azure private paths. The sync's calls to the Confluent API and to Snowflake SQL carry topic names and file paths.

- **Produce:** `producer.py` on the demo VM writes Avro `stock_trades` (default 50 messages a second) through a Private Link Attachment into an Enterprise cluster in `uksouth`. Topic names have to start with `stock_`, because the producer RBAC binding is prefix-scoped. `add_topic.py` can add another `stock_*` topic during a demo; the producer re-reads `topics.txt` every 30 seconds.
- **Materialize:** Tableflow writes Iceberg into your ADLS Gen2 container (hierarchical namespace, bring-your-own storage). The write path is a Confluent egress Private Link gateway with two access points, `blob` and `dfs`. Those private endpoints show up Pending on the storage account until `scripts/approve-egress-endpoint.sh` approves them.
- **Register:** `sync.py` runs from a systemd timer every 60 seconds. It lists Tableflow topics on the Confluent Cloud API, keeps `RUNNING` Iceberg topics, and lists `metadata/*.metadata.json` in ADLS with the VM's managed identity over a blob private endpoint. New topics get `CREATE ICEBERG TABLE ... CATALOG = <object store integration> METADATA_FILE_PATH = ...`. When the snapshot moves, it runs `ALTER ICEBERG TABLE ... REFRESH`. Tables this job created (comment `managed-by=tableflow-sync`) are dropped when Tableflow is gone. Until Snowflake settings exist, the same job logs what it would register and stops. That is discovery-only mode.
- **Query:** Snowflake uses an external volume plus an object-store catalog integration. It reads the same files because the storage firewall (default Deny, selected networks) allows Snowflake's Azure VNet subnets. That is option A, and it works on any Snowflake edition when the account is in the same Azure region. The default query is `TABLEFLOW_PL_DB.CONFLUENT.STOCK_TRADES`.

```
producer.py ──Private Link──► [stock_trades] ──Tableflow egress PL──► ADLS metadata.json
                                                                      │
sync.py (every 60s, blob private endpoint) ◄──────────────────────────┘
    │
    ▼
CREATE / ALTER ICEBERG TABLE … REFRESH
    │
    ▼
Snowflake ──service endpoints──► same ADLS files ──► SELECT
```

Freshness is roughly the Tableflow commit interval plus up to 60 seconds.

## What it demonstrates

- **A warehouse table whose catalog is a file.** Snowflake cannot auto-refresh Iceberg tables from an object-store catalog integration, which is why the timer exists. The table is read-only and externally managed (`allow_writes = false` on the volume).
- **Three private data legs.** Producer to Kafka uses the Private Link Attachment. Tableflow to ADLS uses the egress gateway. Snowflake to ADLS uses service endpoints on the Azure backbone. The storage account stays on selected networks.
- **A topic that shows up in SQL without a Snowflake edit.** `sudo -u demo /opt/tableflow-demo/run add_topic.py stock_orders` creates the topic, enables Tableflow by copying the BYOS storage spec from a topic Terraform already configured, and lets the next sync run register it.

```sql
SELECT symbol, side, COUNT(*) trades, ROUND(AVG(price), 2) avg_price
FROM TABLEFLOW_PL_DB.CONFLUENT.STOCK_TRADES
GROUP BY 1, 2
ORDER BY trades DESC;
```

## Technical details

- **Two Terraform stacks.** `infra/` builds the VNet, Private Link Attachment, ADLS account and firewall, Confluent environment, Enterprise cluster, RBAC, egress gateway, DNS, Tableflow, and the demo VM. `snowflake/` reads that state, adds Snowflake's subnet IDs to the storage firewall, creates the external volume and object-store catalog integration, then the database, schema, warehouse, and a key-pair service user for the sync. Apply `infra` first. Apply `snowflake` twice, with Azure admin consent in between (`snowflake_azure_consent_url`, then `snowflake_azure_consent_completed = true`).
- **The volume URL is the blob endpoint.** `storage_base_url` is `azure://<account>.blob.core.windows.net/<container>/`. Tableflow metadata references `abfss://` paths, and Snowflake resolves them under that base URL.
- **Who can touch the account.** Confluent's multi-tenant app gets `Storage Blob Data Contributor` and `Reader` after an `az ad sp create` retry loop, because cross-tenant Graph calls through the `azuread` provider often 403 and the app can take a minute to appear in a new tenant. After consent, Snowflake's app gets `Storage Blob Data Reader`. The VM lists blobs with a managed identity. Shared keys are disabled.
- **Terraform stays on the control plane.** The cluster REST endpoint is only reachable inside the VNet, so role bindings use the public control plane, the producer API key sets `disable_wait_for_ready`, and topic creation runs on the VM via `az vm run-command`.
- **A few paths stay public, and they carry metadata.** Schema Registry uses its public endpoint (schemas only). The sync's Confluent API and Snowflake SQL calls are control plane. Secrets reach the VM through `az vm run-command`, which is fine for a demo. The Azure tenant has to be yours: Confluent rejects a provider integration that uses Confluent's corporate tenant ID.
- **This apply implements option A.** Public network access stays enabled so selected networks can include Snowflake's subnets, while `default_action` is Deny. The README also describes a Business Critical swap: outbound private endpoints with `USE_PRIVATELINK_ENDPOINT = TRUE`, and public network access disabled on the storage account.

## Challenges & lessons

1. **Storage is the catalog.** On a private-networking cluster the Iceberg REST Catalog is unsupported, and the Azure catalog-sync alternatives are unsupported too. The job that works lists `metadata/*.metadata.json`, keeps the newest file, and passes that path to `CREATE` or `REFRESH`.
2. **The timer is the refresh.** `AUTO_REFRESH` belongs to the IRC design in the sibling repo. Here `tableflow-sync.timer` uses `OnUnitActiveSec=60`. Low-throughput topics commit on a best-effort cadence of about five minutes, so the first `SELECT` can be empty for a few minutes while the producer is already healthy.
3. **Approve both private endpoints.** Tableflow needs `blob` and `dfs` access points. They land as Pending connections on the storage account. DNS records are read after approval, because `private_endpoint_domain` stays empty until the access point is READY.
4. **Match the Snowflake region.** `SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO` returns Snowflake's Azure subnet IDs, and `az storage account network-rule add` attaches them. Those service endpoints line up when the Snowflake account region matches `region` (default `uksouth`). Check with `SELECT CURRENT_REGION()` before you apply. The storage account's Terraform ignores later subnet-ID changes so the snowflake stack can add them out of band.
5. **Consent, then apply again.** The first `snowflake` apply creates the external volume and prints `snowflake_azure_consent_url`. Until a tenant admin accepts it, Snowflake's service principal is missing and the blob reader role cannot be assigned. `snowflake_azure_consent_completed` is the circuit breaker.
6. **Provider integration destroy can 409.** If Confluent still reports the Azure provider integration `ACTIVE`, delete it from state (`terraform state rm confluent_provider_integration_setup.azure`) and destroy again. The README points at the same cleanup note as the original public-catalog repo.
