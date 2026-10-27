---
title: 'Confluent Cloud mTLS: Certificates as Kafka Credentials'
description: 'Terraform and OpenSSL demo that authenticates Confluent Cloud Kafka clients with a private CA over AWS PrivateLink, mapping certificate fields to RBAC through CEL identity pools.'
pubDate: 2026-10-27
tags: ['Kafka', 'Confluent Cloud', 'Terraform', 'mTLS', 'PrivateLink', 'Python']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/confluentCloudMTLS'
image: '/confluent-cloud-mtls.svg'
---

![Private CA keystores sync to an EC2 client, then mTLS on port 9092 over AWS PrivateLink into CEL identity pools on a Confluent Cloud Enterprise cluster](/confluent-cloud-mtls.svg)

## Overview

You sit down to produce to `orders` and the client config has no API key. The
credential is a PKCS12 keystore, `security.protocol=SSL`, and the password that
opens it.

A building badge works the same way. A printer you control stamps a card with
your department. At the door, the reader checks who issued the card, which
department is printed on it, and which rooms that department may enter. A card
with your name on it from a different printer stays shut out. If security
pulls the card, the plastic still looks fine and the reader still says no.

This repo is that door for Kafka. OpenSSL on your laptop is the printer: a
private root, an issuing CA, and one certificate per workload. Terraform
uploads the CA (and a CRL) to Confluent Cloud and writes CEL filters that map
certificate fields onto RBAC. Clients run on an EC2 host inside an AWS VPC,
because the Enterprise cluster only speaks PrivateLink. The certificate is the
credential on the data path, including the call that creates the topics.

## How it works

Four pieces, and the client never names an identity pool:

- **Your CA** (`scripts/pki.sh`) builds a root, an issuing CA, PKCS12
  keystores, a rogue CA for the imposter, and a CRL. CA private keys stay on
  the laptop.
- **Terraform** uploads the PEM chain (issuing CA first, at most four certs)
  as an organization-wide `confluent_certificate_authority`, then creates
  `confluent_certificate_pool` filters and `confluent_role_binding`s. The
  principal is `User:pool-xxxx`.
- **The client host** has no SSH key and no inbound rules. You reach it with
  SSM. `scripts/remote.sh` pushes keystores through a private S3 bucket.
- **The Kafka client** uses plain SSL. Every pool whose CEL filter matches
  applies, and the permissions are the union. `external_identifier` is `CN`,
  which is what audit logs show. Brokers present Let's Encrypt certificates,
  so the public trust store is enough (`certifi` in the Python client).

```
laptop OpenSSL
  root CA -> issuing CA -> PKCS12 keystore
        |  S3 + SSM (CA private keys stay local)
        v
EC2 client (SSM only, Python librdkafka or Java CLI)
  security.protocol=SSL  :9092
        |
        v
VPC endpoint -> PrivateLink -> Enterprise cluster
        |
        v
uploaded CA + CRL -> CEL pools (union) -> RBAC -> orders, fraud-alerts
```

A Route 53 private zone CNAMEs cluster hostnames to the interface endpoint, so
the bootstrap name resolves inside the VPC. Terraform strips the `SASL_SSL://`
prefix from the bootstrap endpoint, because these clients speak SSL.

## What the walkthrough shows

`make demo` runs on the client host and checks each outcome. Identities live
in `pki/identities.txt` and must stay in sync with `local.pools` in
`terraform/mtls.tf`.

- **Platform admin** (`OU=platform`) has `DeveloperManage` on `topic=*` and
  creates `orders` and `fraud-alerts` over the Kafka protocol.
- **Payments** (`OU=payments`) may `DeveloperWrite` `orders`. **Analytics**
  (`OU=analytics`) may `DeveloperRead` `orders` and consumer groups matching
  `analytics-*`.
- **Least privilege:** the analytics cert is authenticated, then
  `TOPIC_AUTHORIZATION_FAILED` on produce and `GROUP_AUTHORIZATION_FAILED` if
  the group is `payments-audit`.
- **SPIFFE SAN:** `fraud-detector` matches
  `SAN.contains("URI:spiffe://acme.demo/ns/risk/")`, reads `orders`, and
  writes `fraud-alerts`.
- **Trusted CA, no pool:** `OU=marketing` hangs, then times out. Confluent
  Cloud does not say why.
- **Imposter:** same subject DN as `orders-producer`, signed by a rogue CA
  that was never uploaded. Rejected.
- **CRL:** `make revoke` lets `legacy-producer` write, revokes it, uploads
  the CRL, and retries until the brokers reject it. `orders-producer` (same
  OU, not revoked) still produces. In testing that flip took about 45
  seconds after the upload. Enterprise clusters enforce CRLs fail-closed.

`make demo-batch` prints the same walkthrough without an interactive Session
Manager session. `make down` destroys the Confluent Cloud and AWS resources.

## Technical details

- **Cluster:** AWS Enterprise cluster, PrivateLink attachment, and a
  connection from the VPC endpoint. Supported elsewhere too (Dedicated on any
  cloud, Azure Enterprise, AWS Freight), but this repo provisions the AWS
  Enterprise path.
- **Cloud API key:** OrganizationAdmin. Certificate authorities are
  org-level, so once this CA is uploaded every mTLS-capable cluster in the
  org trusts it. API key and OAuth auth keep working beside it.
- **Clients:** `clients/mtls_client.py` (`confluent-kafka`) and the Apache
  Kafka CLI (Java 21) via `clients/java-cli.sh`. Config is
  `ssl.keystore.location`, `ssl.keystore.type=PKCS12`, and the keystore
  password. No truststore override beyond the public bundle.
- **PKI:** `clientAuth` extended key usage and `digitalSignature` key usage.
  Certs and CRLs are valid for 30 days (`default_days` / `default_crl_days`
  in `pki/openssl.cnf`). Needs OpenSSL 3. macOS ships LibreSSL.
- **Tags:** Confluent Cloud resources do not take tags, so `owner` and
  `keep_until` go in descriptions. AWS resources get `aws_tags` through
  provider `default_tags`.

## Challenges & lessons

1. **The client has to live in the VPC.** Enterprise clusters have private
   endpoints only. The EC2 instance is SSM-only, and keystores move through
   S3 so CA private keys never leave your machine. If `make up` stalls on
   the host, read `/var/log/cloud-init-output.log`. The Kafka CLI is pulled
   from the Apache CDN, which only keeps current releases. A stale
   `kafka_cli_version` falls back to archive.apache.org, and that download
   is slow.
2. **A missing pool looks like a timeout.** `marketing-app` is signed by the
   trusted CA and still fails authentication, because no CEL filter matches.
   The client is not told why. The same symptom is a PrivateLink or DNS
   miss, so `getent hosts` on the bootstrap name should return VPC addresses
   before you blame the certificate.
3. **Pools stack.** The client config has no pool id. Every matching filter
   applies and the grants are the union. `external_identifier = "CN"` is the
   identity string in audit logs.
4. **Revocation is local, then eventual on the brokers.** `scripts/pki.sh
   revoke` marks the cert and rebuilds `crl-chain.pem`. A targeted Terraform
   apply uploads it. The revoke script polls for up to 10 minutes. Rejection
   shows up as `SslAuthenticationException` / `certificate_revoked`. Refresh
   expired certs and CRLs with `scripts/pki.sh issue` and `scripts/pki.sh
   crl`, then `make up`.
5. **The issuer is the trust boundary.** Copying `orders-producer`'s subject
   DN onto a cert from `Acme Root CA` (the rogue CA) does not authenticate.
   Public CAs are not accepted. A handshake error usually means the cert
   expired, `clientAuth` or `digitalSignature` is missing, or (librdkafka)
   the issuing CA is absent from the uploaded chain.
6. **mTLS covers the Kafka protocol only.** Schema Registry, ksqlDB, Flink,
   and the Kafka REST APIs do not take these certificates, which is why
   topics are created by the platform certificate instead of the Cloud API.
   After the CA exists, a librdkafka client that sets `SASL_SSL` together
   with `ssl.certificate.location` fails. Use one auth method.
7. **This stack bills until you destroy it.** The Enterprise cluster (eCKU
   hours), the PrivateLink endpoint, and a `t3.small` are billed while they
   exist. `make down`, then `make clean` for local certs. If the VPC quota
   is full, set `existing_vpc_id` and `existing_subnet_ids` (DNS hostnames
   on, first subnet with internet access).
