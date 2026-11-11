---
title: 'Client-Side Encryption: Ciphertext Reaches Kafka'
description: 'Terraform provisions Confluent Cloud CSFLE (PII fields, AWS KMS) and CSPE (whole Avro payload, Azure Key Vault), and a Python client shows a Kafka-authorized consumer without KMS access cannot read the secrets.'
pubDate: 2026-11-11
tags: ['Kafka', 'Terraform', 'Schema Registry', 'AWS KMS', 'Azure Key Vault', 'Python']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/clientSideEncryption'
image: '/client-side-encryption.svg'
---

![Python client encrypts with a DEK, your KMS wraps that DEK, and Kafka stores ciphertext for CSFLE fields or a CSPE payload](/client-side-encryption.svg)

## Overview

You hand an analytics job a Kafka API key and it reads the topic. The record arrives. Email, SSN, and card number stay ciphertext.

A locked briefcase works the same way. The courier can carry it, scan it, and put it on a shelf. Opening it needs a key that stayed in your office. Kafka is the courier. Your cloud KMS is the office.

This repo is that briefcase, twice, provisioned entirely in Terraform. **CSFLE** (client-side field level encryption) locks the fields tagged `PII`, using an AWS KMS key. **CSPE** (client-side payload encryption) locks the entire serialized message, using an Azure Key Vault key. Schema Registry holds the data contract and a wrapped data encryption key (DEK). The key that unwraps it, the KEK, is created with `shared = false`, so Confluent never receives a copy.

A Python producer and consumer prove both paths, including an intruder with the same Kafka and Schema Registry access that still cannot read the secrets.

## How it works

The schema is the policy. The application registers the encryption executors and passes KMS credentials. Which fields, which algorithm, and which KEK come from the contract Terraform registered.

- **CSFLE** is a domain rule, type `ENCRYPT`, on the tag `PII`. It runs on individual fields. `email`, `ssn`, and `card_number` are encrypted. `customer_id`, `full_name`, and `country` stay plaintext.
- **CSPE** is an encoding rule, type `ENCRYPT_PAYLOAD`. It runs after the serializer has turned the record into bytes, so field names and values are hidden together. The same rule type applies to Avro, Protobuf, and JSON Schema. This repo registers Avro.
- **The DEK** is AES-256-GCM. Schema Registry's DEK Registry stores it only in wrapped form. Wrapping and unwrapping call your KMS: AWS KMS for CSFLE, Azure Key Vault for CSPE.
- **Kafka** stores the ciphertext. Brokers, disks, replicas, and a consumer that holds only an API key see the locked record.

```
producer (Avro serializer + encryption executor)
    |
    +-- fetch schema and rules ----► Schema Registry
    |                                  DEK Registry (wrapped DEK)
    +-- wrap / unwrap DEK ----------► Your KMS (shared = false)
    |                                  AWS KMS  |  Azure Key Vault
    v
Kafka (ciphertext only)
    <prefix>-customers    CSFLE    PII fields
    <prefix>-payments     CSPE     whole payload
    |
    +-- KMS credentials --> plaintext
    +-- Kafka ACL only  --> secrets stay locked
```

`clients/common.py` registers `FieldEncryptionExecutor`, `EncryptionExecutor`, `AwsKmsDriver`, and `AzureKmsDriver` once per process. Serializers set `auto.register.schemas` to false and `use.latest.version` to true, so they use the schema and ruleset already in Schema Registry.

## What it demonstrates

`make test` produces five records per deployed use case and checks the raw bytes, the authorized consumer, and an intruder process with cloud credentials stripped.

**CSFLE, topic `<prefix>-customers`.** The authorized consumer assumes an IAM role that can use the KMS key and gets each record back exactly. Raw bytes on the broker contain no plaintext email, SSN, or card number, and they still contain `full_name`. The intruder deserializes the record, because read `on_failure` is `NONE`, and every PII field it sees is ciphertext.

**CSPE, topic `<prefix>-payments`.** The authorized consumer uses an Entra application with Key Vault Crypto User on that one key and round-trips the payment. Raw bytes contain none of `payment_id`, `merchant`, `iban`, or `currency`. The intruder still receives the five messages. Decrypt fails (the demo prints `EOFError`) and no field comes back.

Set `enable_csfle = false` or `enable_cspe = false` when you only have one cloud account.

```bash
make plan && make apply     # writes clients/config.json (mode 600, git-ignored)
make csfle-intruder         # PII is ciphertext, full_name is readable
make cspe-intruder          # payload is unreadable
make test
make destroy
```

Message browsing in the Confluent Cloud console shows the same bytes the intruder sees.

## Technical details

- **Confluent Cloud:** an environment on the Stream Governance Advanced package (encryption rules and the DEK Registry require it), a Basic single-zone cluster, a Terraform manager service account, and a client service account. Topics have three partitions. Resource names start with `var.prefix`.
- **CSFLE contract:** domain rule `encrypt-pii`, kind `TRANSFORM`, mode `WRITEREAD`, tags `["PII"]`, KEK `${prefix}-csfle-kek`, `encrypt.dek.algorithm = AES256_GCM`, `encrypt.dek.expiry.days = 30`. The KEK references an AWS KMS key with automatic rotation. The client IAM role may `kms:Encrypt`, `kms:Decrypt`, `kms:GenerateDataKey`, and `kms:DescribeKey` on that key only. Anyone in the account who can `sts:AssumeRole` into that role can use it.
- **CSPE contract:** encoding rule `encrypt-payload`, type `ENCRYPT_PAYLOAD`, same algorithm, KEK `${prefix}-cspe-kek`. The KEK references an Azure Key Vault RSA-3072 key (`wrapKey`, `unwrapKey`). The vault uses RBAC. The deployer gets Key Vault Crypto Officer so Terraform can create the key. The Kafka client gets Key Vault Crypto User on that key only.
- **Client:** `confluent-kafka[avro,schemaregistry,rules]==2.16.1`. AWS `rule_conf` is `role.arn` plus an optional profile. Azure `rule_conf` is `tenant.id`, `client.id`, and `client.secret`.
- **Permissions:** Kafka access is prefixed ACLs on `${var.prefix}-` (topic READ, WRITE, DESCRIBE, group READ). Schema subjects get `DeveloperRead`. KEKs matching `kek=${prefix}-*` get `DeveloperRead` and `DeveloperWrite`, so the app can create and read DEKs. Unwrap still requires the KMS identity.
- **Demo limits:** one client identity produces and consumes. Terraform state contains the API keys and the Azure client secret. Key Vault purge protection is off, and the AWS KMS deletion window is 7 days, so `make destroy` can finish.

## Challenges & lessons

1. **A Kafka API key does not decrypt.** The client service account can read and write the topics, read the schemas, and use the DEK Registry. Plaintext requires the KMS identity. `shared = false` is what keeps that key out of Confluent.

2. **Basic clusters reject resource roles.** The provider error is `Basic Clusters can not use resource roles`, which is why Kafka access here is ACLs. Schema Registry and KEK permissions stay on RBAC, and that works on a Basic cluster. On Standard or Dedicated, topic access can be `DeveloperRead` and `DeveloperWrite` instead.

3. **Azure RBAC lands after the 60 second wait.** Creating the Key Vault key immediately after the Crypto Officer assignment returns 403, so Terraform sleeps 60 seconds. That is sometimes still too short. `make plan && make apply` again gets past it. Fresh Confluent API keys and role bindings do the same thing: a 401 or 403 right after apply means wait and retry. The config already sleeps 60 seconds before it uses the manager keys.

4. **Ambient credentials unwrap the DEK.** An empty `rule_conf` still picks up `AWS_*` variables, an SSO profile, or `az login` through `DefaultAzureCredential`. The intruder path deletes `AWS_*`, `AZURE_*`, and `ARM_*`, points AWS at an empty credentials file, and for CSPE sends a real tenant and client id with the secret `not-the-secret`. `make test` runs that consumer in a subprocess so the parent process keeps its own credentials. If the authorized consumer gets `AccessDenied`, the AWS base credentials must be allowed to `sts:AssumeRole` into the client role.

5. **`ERROR,NONE` fails closed on write.** `on_failure = "ERROR,NONE"` means the producer errors when it cannot encrypt, so plaintext PII stays off the topic. On read, a CSFLE consumer that cannot decrypt still receives the record, with ciphertext in the tagged fields. `ERROR,ERROR` would fail that reader instead. CSPE uses the same setting, and the payload still fails to deserialize because the bytes never become a record.

6. **`shared = false` blocks managed Confluent services.** Flink and managed connectors cannot unwrap these DEKs. A shared KEK is the switch when those services must process the encrypted fields. In production, split the identities: producers usually need wrap (`Encrypt` / `GenerateDataKey`), and only specific consumers need `Decrypt`. Prefer workload identity (IRSA, pod identity, or Azure managed identity) over the client secret this demo writes into state.

7. **Destroy has two sharp edges.** Deleting the `PII` tag while a schema still references it returns 409, so a 30 second sleep sits between them on destroy. The Key Vault name has to be globally unique and at most 24 characters (`${prefix}-cspe-${hex}`). Purge protection is off so destroy can purge the vault. Turn it on for a real vault, and move Terraform state to a remote encrypted backend before the API keys and Azure secret leave your laptop.
