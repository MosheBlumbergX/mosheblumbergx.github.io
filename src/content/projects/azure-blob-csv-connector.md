---
title: 'Azure Blob CSV: Folders Become Kafka Topics'
description: 'Kafka Connect reads CSV blobs from Azure Blob Storage and routes each folder to its own topic, with storage credentials injected at startup from a git-ignored .env.'
pubDate: 2026-10-07
tags: ['Kafka', 'Kafka Connect', 'Azure Blob Storage', 'Docker', 'KRaft']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/kafkaDemos/tree/main/azure-blob-csv-connector'
image: '/azure-blob-csv-connector.svg'
---

![CSV blobs under input/employees and input/new_hires become employee-data and new-hires](/azure-blob-csv-connector.svg)

## Overview

You upload a CSV to a folder in Azure Blob Storage. On the next poll, each row
is a JSON message on a Kafka topic chosen by that folder.

Think of the container as a shared dropbox and each folder as a labeled tray.
`input/employees/` is the employee tray. `input/new_hires/` is the new-hire
tray. `topic.regex.list` is the label: match the blob path, publish to that
topic. The files never land on the Connect worker's disk.

This is the remote version of the
[Spooldir CSV demo](/projects/csv-spooldir-connector). The walkthrough lives in
[azure-blob-csv-connector](https://github.com/MosheBlumbergX/kafkaDemos/tree/main/azure-blob-csv-connector)
inside the [kafkaDemos](https://github.com/MosheBlumbergX/kafkaDemos) repo.

## How it works

`./start-demo.sh` reads a git-ignored `.env`, starts Compose, waits until the
Azure Blob Storage Source plugin is loaded, and PUTs the connector config with
the account name and either the account key or a SAS token filled in. The
checked-in `connector-config.json` keeps a placeholder. Credentials stay in
the environment.

`./upload-csv.sh` pushes a local file with a one-shot Azure CLI container:

```bash
./upload-csv.sh sample_data.csv employees   # -> employee-data
./upload-csv.sh new_hires.csv new_hires     # -> new-hires
```

```
./upload-csv.sh ---> csv-data / input/<folder>/*.csv
                              |
                              v
              Azure Blob Storage Source (GENERIC, CSV)
                              |
          topic.regex.list    +-- input/employees/  ---> [employee-data]
                              +-- input/new_hires/  ---> [new-hires]
                              |
                              v
                    Connect offsets topic (blob already read)
```

| Blob path | Topic |
|-----------|-------|
| `csv-data/input/employees/*.csv` | `employee-data` |
| `csv-data/input/new_hires/*.csv` | `new-hires` |
| anything else, such as `csv-data/input/x.csv` | skipped |

The regex is matched against the full path (`input/employees/file.csv`).
Patterns stay non-overlapping. Every file uses the same CSV settings, and each
file's own header row supplies its field names, so two files can have
different columns.

A row comes through with string values:

```json
{
  "ID": "1",
  "Name": "Alice Smith",
  "Department": "Engineering",
  "Salary": "85000"
}
```

Watch either topic in Kafka UI on [localhost:8080](http://localhost:8080), or
with `kafka-console-consumer` on `employee-data` or `new-hires`.

## What it demonstrates

- **A commercial connector on a trial.** Confluent's Azure Blob Storage Source
  runs on `cp-server-connect` 8.3.2. That image provides the licensing classes.
  `confluent.license` is empty, so the connector runs on the 30-day trial.
- **Folder routing.** One connector, two topics, decided by
  `topic.regex.list`.
- **Secrets kept out of git.** `.env` holds `AZURE_STORAGE_ACCOUNT` plus
  `AZURE_STORAGE_KEY` or `AZURE_STORAGE_SAS_TOKEN`. `start-demo.sh` injects
  them at create time.
- **Offsets instead of moving blobs.** The connector records how far it has
  read in the Connect offsets topic, keyed by connector name. A new file shows
  up on the next poll (`azblob.poll.interval.ms` is 10 seconds).

## Technical details

- **Images:** `confluentinc/cp-kafka:8.3.2` (KRaft, broker and controller),
  `confluentinc/cp-server-connect:8.3.2`, `provectuslabs/kafka-ui`. Hub
  install: `confluentinc/kafka-connect-azure-blob-storage-source:3.1.9`.
- **Mode and format:** `GENERIC`, format class
  `io.confluent.connect.cloud.storage.source.format.CloudStorageCsvFormat`,
  `csv.first.row.as.header=true`. Container name `csv-data`, `topics.dir`
  `input`.
- **Routing string:**
  `employee-data:input/employees/.*\.csv,new-hires:input/new_hires/.*\.csv`.
- **Converters:** string key, JSON value, value schemas disabled.
- **Uploads:** `upload-csv.sh` runs `docker compose run` on the `azure-cli`
  service (Compose profile `tools`) and writes
  `input/<folder>/<name>_<timestamp>.csv`. A SAS token used for upload needs
  Write. The connector itself needs Read and List.
- **Update in place:** the start script PUTs
  `/connectors/azure-blob-csv-connector/config`, so a second run applies
  config changes to the existing connector. `jq` is required.

```bash
curl http://localhost:8083/connectors/azure-blob-csv-connector/status | jq
./stop-demo.sh
```

## Challenges & lessons

1. **This connector needs `cp-server-connect`.** The community
   `cp-kafka-connect` image does not ship the licensing classes the connector
   depends on. With no license key, you get the 30-day trial.
2. **The account name in git is a placeholder.** `connector-config.json`
   stores `"<set from AZURE_STORAGE_ACCOUNT>"`. `start-demo.sh` refuses to
   run until the account and a key or SAS token are in the environment or in
   `.env`, then writes them only into the live config.
3. **The same connector name remembers what it already read.** Deleting and
   recreating `azure-blob-csv-connector` does not republish old blobs. Give it
   a new name, or clear its offsets. New blobs are still picked up on the
   10 second poll.
4. **The regex sees the path, not the file name.** `input/employees/file.csv`
   matches `employee-data`. A file that matches no pattern is skipped. Add a
   topic by adding another `topic:regex` pair and running `./start-demo.sh`
   again.
5. **CSV values arrive as strings.** `Salary` is `"85000"`. The
   [Spooldir demo](/projects/csv-spooldir-connector) sample output shows those
   fields as numbers, because that connector has schema generation turned on.
