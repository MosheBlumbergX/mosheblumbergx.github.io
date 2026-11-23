---
title: 'Build Your Own Iceberg Catalogue: A REST API in One File'
description: 'A read-only Iceberg REST catalogue in about 100 lines of FastAPI that maps table names to metadata files and works with the standard PyIceberg client.'
pubDate: 2026-11-23
tags: ['Apache Iceberg', 'Python', 'FastAPI', 'PyIceberg', 'OAuth2']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/BuildYourOwnIcebergCatalog'
image: '/build-your-own-iceberg-catalog.svg'
---

![Diagram of a PyIceberg reader calling a one-file REST catalogue, which scans a warehouse folder and returns the newest metadata file pointer](/build-your-own-iceberg-catalog.svg)

## Overview

You walk into a library and ask for a book by title. The librarian does not read it to
you. They point at a shelf: *"third aisle, second row."* You fetch the book and read it
yourself.

That is an **Iceberg catalogue**. It does one job: map a table name to the location of
that table's current metadata file. Schema, snapshots and the list of data files all
live in that metadata file, and the client reads them directly. This project builds that
librarian in a single file, `catalog.py`, with no database and no cloud account.

## How it works

- **The producer owns the tables.** `writer.py` stands in for Tableflow, Spark or Flink
  and writes Iceberg data and metadata files into `./warehouse`.
- **The catalogue follows the files.** `catalog.py` scans `warehouse/<namespace>/<table>/metadata/`
  and treats the highest-numbered `*.metadata.json` as the current version.
- **The client knows nothing.** `reader.py` is a stock PyIceberg `RestCatalog`. Only the
  URI and credential would change if you pointed it at Polaris or Tableflow.

```
writer.py ──writes──► warehouse/demo/trades/metadata/00002-….metadata.json
                                   ▲
                                   │ scan: highest number wins
reader.py ──GET /v1/namespaces/demo/tables/trades──► catalog.py (FastAPI)
          ◄── { "metadata-location": "file:///…/00002-….metadata.json" }
```

## What it demonstrates

- **A catalogue is a pointer.** The "load table" response is the whole point: a
  `metadata-location` plus the metadata itself.
- **Append without restarting.** Run `writer.py append`, then `reader.py` again: 1,500
  rows and 2 snapshots. The catalogue never restarted or registered anything.
- **The REST spec in miniature.** Six endpoints are enough for a client to list
  namespaces, list tables and load one, including an OAuth2 client-credentials token flow.
- **It works with curl too.** A bearer token, then plain `GET` requests. No token returns
  `401`, and a `POST` to create a table returns `405` because the catalogue is read-only.

## Technical details

- **Stack:** Python, FastAPI and Uvicorn for the catalogue; PyIceberg and PyArrow for the
  writer and reader.
- **Stateless auth:** the access token is `expiry.HMAC(expiry)`, signed with the client
  secret, so there is no token store. Comparisons use `hmac.compare_digest`.
- **Deliberate omissions:** no create, commit or drop, no views, pagination, caching or
  credential vending. The catalogue rescans the folder on every request, which is fine
  for a demo.
- **Storage:** locations are `file://` here. On cloud storage the pointer would be
  `abfss://`, `s3://` or `gs://`, and the catalogue would list blobs instead of files.

## Challenges & lessons

1. **Writes are the hard part.** A writable catalogue needs atomic compare-and-swap
   commits, and a second writer can corrupt a table. Read-only sidesteps that, and the
   README points to Polaris, Lakekeeper or Nessie when you need more.
2. **Some writers add `.db` to the namespace folder.** The scan strips that suffix so the
   namespace a client sees is `demo`, not `demo.db`.
3. **Python version matters.** PyArrow has no wheels for the newest Python releases, so
   the demo needs Python 3.11 to 3.14.
4. **A real engine needs more than localhost.** Snowflake wants an HTTPS endpoint it can
   reach, OAuth and an external volume, so this demo stops at a local client.
