---
title: 'Spooldir CSV: Drop a File, Get Kafka JSON'
description: 'A local Docker demo where Kafka Connect Spooldir watches a folder, turns each CSV row into JSON, and publishes it on the employee-data topic.'
pubDate: 2026-10-07
tags: ['Kafka', 'Kafka Connect', 'Docker', 'Spooldir', 'KRaft']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/kafkaDemos/tree/main/csv-spooldir-connector'
image: '/csv-spooldir-connector.svg'
---

![CSV dropped in data/input becomes JSON on employee-data, then the file moves to finished or error](/csv-spooldir-connector.svg)

## Overview

You copy a CSV into a folder. A short wait later, every row is a JSON message
on a Kafka topic, and the file itself has been moved out of the way.

That folder is a mailroom inbox. Kafka Connect's Spooldir CSV Source Connector
is the clerk: new file in, one message per row out, file filed under finished
or error. This demo runs the whole clerk locally in Docker, so you can watch
it with Kafka UI on [localhost:8080](http://localhost:8080).

The walkthrough lives in
[csv-spooldir-connector](https://github.com/MosheBlumbergX/kafkaDemos/tree/main/csv-spooldir-connector)
inside the [kafkaDemos](https://github.com/MosheBlumbergX/kafkaDemos) repo.
The sibling demo reads the same kind of CSV from Azure Blob Storage:
[Azure Blob CSV](/projects/azure-blob-csv-connector).

## How it works

`./start-demo.sh` brings the stack up, waits until Connect answers on port
8083, POSTs `connector-config.json`, and copies `sample_data.csv` into
`data/input/` under a timestamped name. You can do the same steps by hand
with `docker-compose up -d` and a `curl` POST of that config.

```
data/input/*.csv
      |
      v
Spooldir CSV Source (Connect)
      |
      +---> [employee-data] ---> Kafka UI :8080
      |
      +-- ok   ---> data/finished/
      +-- error --> data/error/
```

- **One topic.** `topic` is `employee-data`.
- **Header row.** `csv.first.row.as.header` is true, so `ID`, `Name`,
  `Department`, and `Salary` become the JSON fields.
- **Plain JSON.** Key and value converters are `JsonConverter` with schemas
  disabled.
- **One task.** `tasks.max` is 1. The file pattern is `.*\.csv`.

A row from `sample_data.csv` shows up like this:

```json
{
  "ID": 1,
  "Name": "Alice Smith",
  "Department": "Engineering",
  "Salary": 85000
}
```

## What it demonstrates

- **KRaft, single node.** Kafka 7.7.1 runs as broker and controller. No
  ZooKeeper.
- **Plugin install at start.** The Connect container runs
  `confluent-hub install jcustenborder/kafka-connect-spooldir:2.0.65` before
  it starts the worker.
- **A file lifecycle you can see.** `./data` is bind-mounted to `/data`.
  Input, finished, and error are directories on your disk.
- **Two ways to read the topic.** Kafka UI (Topics, then employee-data), or
  `kafka-console-consumer` inside the `kafka` container.

## Technical details

- **Images:** `confluentinc/cp-kafka:7.7.1`, `confluentinc/cp-kafka-connect:7.7.1`,
  `provectuslabs/kafka-ui`.
- **Connector class:**
  `com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector`.
- **Paths:** `input.path` `/data/input`, `finished.path` `/data/finished`,
  `error.path` `/data/error`.
- **Schema generation** is enabled. The sample output above keeps `ID` and
  `Salary` as numbers.
- **Ports:** broker `9092`, Connect REST `8083`, Kafka UI `8080`.

Check status, or remove the connector:

```bash
curl http://localhost:8083/connectors/csv-spooldir-connector/status | jq
curl -X DELETE http://localhost:8083/connectors/csv-spooldir-connector
```

## Challenges & lessons

1. **Wait for Connect, then POST.** The container installs the Spooldir plugin
   before it listens. `start-demo.sh` polls `http://localhost:8083/` for that
   reason. A POST in the first few seconds lands on nothing.
2. **Processed files leave `data/input/`.** A good file moves under
   `data/finished/`. A bad file moves under `data/error/`. To see another
   batch, drop another CSV. The start script uses a timestamp in the filename
   so a second run does not collide with a file still sitting in input.
3. **Connect's internal topics are replication factor 1.** This is a
   single-broker KRaft node (`process.roles=broker,controller`). Configs,
   offsets, and status use that factor on purpose.
4. **The sample JSON is typed.** With schema generation on, the README's
   expected record has numeric `ID` and `Salary`. The
   [Azure Blob demo](/projects/azure-blob-csv-connector) publishes the same
   columns as strings.
