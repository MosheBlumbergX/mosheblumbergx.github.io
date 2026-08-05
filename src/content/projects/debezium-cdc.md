---
title: 'Debezium CDC Throughput — SQL Server → Kafka'
description: 'Load-testing Debezium SQL Server CDC on the kafka-docker-playground: a wide big_data table, catch-up checks via LSN, and sustained ~20 MB/s insert throughput.'
pubDate: 2026-08-05
tags: ['Kafka', 'Debezium', 'SQL Server', 'CDC', 'Docker', 'Connect']
status: 'completed'
repo: 'https://github.com/vdesabou/kafka-docker-playground/tree/master/connect/connect-debezium-sqlserver-source'
image: '/debezium-cdc.svg'
---

![SQL Server CDC → Debezium → Kafka topic at ~20 MB/s payload](/debezium-cdc.svg)

## Overview

You spin up the [kafka-docker-playground](https://kafka-docker-playground.io/)
Debezium SQL Server example and everything looks fine — a small `customers`
table, a connector, a topic. Then you ask the harder question: *what happens
when the table is huge, and inserts keep coming?*

This project is the notes from answering that. On top of the stock playground
example (which only has `customers`), I added a wide `big_data` table, enabled
CDC, streamed millions of rows into Kafka, and learned how to tell whether the
connector is **caught up**, **lagging**, or simply **idle**.

> **Important:** The upstream playground does **not** include `big_data`. Clone
> the example, then do the manual setup below — or fork the startup script
> locally.

## How it works

Change Data Capture turns every insert into a Kafka event:

```
INSERT big_data ──► SQL Server CDC ──► Debezium connector ──► [server1.testDB.dbo.big_data]
```

A few rules decide whether anything actually streams:

- **CDC must be enabled** on the table (`sp_cdc_enable_table`) — creating the
  table alone is not enough.
- **Restart the connector** after enabling a new capture instance so it picks
  the table up.
- **Load data after CDC + connector** if you want those inserts streamed.
  Rows that existed before are not replayed; for backfill, use an
  [incremental snapshot](https://github.com/vdesabou/kafka-docker-playground/blob/master/connect/connect-debezium-sqlserver-source/debezium-sqlserver-source-incremental-snapshot.sh).
- **Row width matters** — SQL Server CDC change tables stay under the
  **8060-byte in-row limit**. Use `CHAR(4000)`, not 8000.

## Playground setup

Stock example first (interactive search, or the script directly):

```bash
playground run
# search for debezium-sqlserver-source.sh
```

After it succeeds you should have topic `server1.testDB.dbo.customers` and a
running `debezium-sqlserver-source` connector.

References:

- [Playground repo](https://github.com/vdesabou/kafka-docker-playground)
- [Docs site](https://kafka-docker-playground.io/)
- [Example folder](https://github.com/vdesabou/kafka-docker-playground/tree/master/connect/connect-debezium-sqlserver-source)
- [Debezium SQL Server docs](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)

## Add a large `big_data` table

### 1. Create the table

```bash
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -C -No -U sa -P 'Password!' << 'EOF'
USE testDB;
CREATE TABLE big_data (
  id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  payload CHAR(4000) NOT NULL
);
GO
EOF
```

### 2. Enable CDC on it

```bash
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -C -No -U sa -P 'Password!' << 'EOF'
USE testDB;
EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'big_data',
  @role_name = NULL,
  @supports_net_changes = 0;
GO
EOF
```

### 3. Restart the connector

Stock config has no `table.include.list`, so all CDC-enabled tables in `testDB`
are eligible:

```bash
playground connector restart --connector debezium-sqlserver-source
```

### 4. Load data after CDC + connector

**~4 GB** (1,000,000 × 4 KB):

```bash
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -C -No -U sa -P 'Password!' << 'EOF'
USE testDB;
SET NOCOUNT ON;
DECLARE @i INT = 0;
WHILE @i < 100
BEGIN
  INSERT INTO big_data (payload)
  SELECT TOP (10000) REPLICATE('X', 4000)
  FROM sys.all_objects a
  CROSS JOIN sys.all_objects b;
  SET @i = @i + 1;
  PRINT CONCAT('big_data load batch ', @i, '/100');
END
GO
EOF
```

**~40 GB** (10,000,000 × 4 KB) — run in a separate terminal; takes a long time.
Scale the loop to `1000` batches of 10,000.

Kafka topic once streaming: `server1.testDB.dbo.big_data`

```bash
playground topic consume --topic server1.testDB.dbo.big_data --tail
```

## Review: is the connector caught up?

Compare connector offset LSN to SQL Server max LSN. Matching = caught up;
connector behind = lagging under load.

```bash
playground connector offsets get --connector debezium-sqlserver-source
```

```bash
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -C -No -U sa -P 'Password!' << 'EOF'
USE testDB;
SELECT sys.fn_cdc_get_max_lsn() AS db_max_lsn;
SELECT MAX(__$start_lsn) AS max_cdc_lsn FROM cdc.dbo_big_data_CT;
GO
EOF
```

Avoid `COUNT(*)` on large CDC change tables — prefer LSN / topic offset checks.
No Kafka **Bytes In** usually means idle (no new inserts) or already caught up —
not necessarily a failure.

Kafka size is usually **larger than SQL payload** because of the Debezium JSON
envelope. Check with:

```bash
docker exec broker bash -c 'du -sh /var/lib/kafka/data/server1.testDB.dbo.big_data-0'
```

## Sustained ~20 MB/s payload

5,000 rows × 4 KB ≈ 20 MB/s:

```bash
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -C -No -U sa -P 'Password!' << 'EOF'
USE testDB;
SET NOCOUNT ON;
WHILE 1 = 1
BEGIN
  INSERT INTO big_data (payload)
  SELECT TOP (5000) REPLICATE('X', 4000)
  FROM sys.all_objects a
  CROSS JOIN sys.all_objects b;
  WAITFOR DELAY '00:00:01';
END
GO
EOF
```

For a smoother Grafana ribbon at the same average rate, insert 1,250 rows every
250 ms instead. Stop with `Ctrl+C` — disk grows quickly (SQL + CDC + Kafka).

Kafka Bytes In can spike near 20 MiB/s then drop if Debezium cannot keep up.
Pace inserts for a flat chart.

## Useful playground commands

```bash
playground stop
playground re-run

playground connector status
playground connector restart --connector debezium-sqlserver-source
playground connector offsets get --connector debezium-sqlserver-source

playground topic consume --topic server1.testDB.dbo.customers --min-expected-messages 5 --timeout 60
playground topic consume --topic server1.testDB.dbo.big_data --tail
playground topic describe --topic server1.testDB.dbo.big_data
```

## Challenges & lessons

1. **Upstream example ≠ this guide.** Clone the playground, then **add**
   `big_data` yourself (or fork the script).
2. **Enable CDC before expecting Kafka traffic**, and restart the connector after
   enabling a new table.
3. **Row width matters** — stay under 8060 bytes including CDC metadata
   (`CHAR(4000)` is safe here).
4. **Load after CDC** if you want the data streamed; otherwise use incremental
   snapshot for backfill.
5. **Idle connector ≠ broken** — no inserts means no Bytes In.
6. **LSN compare** is the reliable catch-up check; topic `du` shows what already
   landed in Kafka.
7. **Heavy loads can outpace a single Connect task** — expect lag and bursty
   throughput charts.
