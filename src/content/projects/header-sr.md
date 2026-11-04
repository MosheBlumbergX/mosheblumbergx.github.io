---
title: 'Schema Registry Headers: A GUID on the Kafka Record'
description: 'A local Docker demo of Confluent Platform 8.4 where Avro records carry a schema GUID in a Kafka header, plus the interop, migration, and client gotchas that come with that wire format.'
pubDate: 2026-11-04
tags: ['Kafka', 'Schema Registry', 'Avro', 'Python', 'Docker', 'Confluent Platform']
status: 'completed'
repo: 'https://github.com/MosheBlumbergX/headerSR'
image: '/header-sr.svg'
---

![Producer writes a schema GUID into __value_schema_id, Kafka stores bare Avro, and the dual deserializer resolves it with GET /schemas/guids](/header-sr.svg)

## Overview

You replicate a topic into another cluster and the consumers start failing. The bytes still begin with `0x00` and a schema id, but id 6 in the new registry is a different schema, or nothing at all.

A coat-check stub works the same way. The number only means something at the counter that printed it. An ISBN is a fingerprint of the book, so any library that holds that book can find it.

Confluent Schema Registry's newer wire format is the ISBN. The classic format stamps a magic byte and a 4-byte numeric id on the front of the Avro payload. That id is a counter in that registry's `_schemas` topic. The header format leaves the payload as bare Avro and puts a schema GUID in the record header `__value_schema_id`. The GUID fingerprints the schema content, including references, rules, and metadata, so the same schema has the same GUID in every registry that stores it.

This repo is that format on a laptop: Kafka in KRaft mode, Schema Registry, and a Python container that runs seven scripts. No cloud account and no credentials.

## How it works

`./run.sh` starts the stack and runs all seven parts. Kafka is on `localhost:9092`. Schema Registry is on `http://localhost:8081`. Each part resets its own topics and subjects, so you can rerun them in any order.

Two ways a record can name its schema:

- **Classic prefix.** The payload is `[0x00][schema id, 4 bytes big-endian][Avro body]`. Python's default serializer is `prefix_schema_id_serializer`.
- **GUID header.** The payload is the Avro body alone. The header `__value_schema_id` is `[0x01][16-byte GUID]`. Keys use `__key_schema_id`. For Protobuf, message indexes follow the GUID inside the header value. Python opts in with `header_schema_id_serializer`.

The deserializer default is `dual_schema_id_deserializer`. It checks the header first, then falls back to the payload prefix. That fallback is what makes a staged migration possible.

```
producer
  header_schema_id_serializer
        |
        v
Kafka record
  header  __value_schema_id = 0x01 + GUID
  payload bare Avro
        |
        v
dual deserializer
  header present --> GET /schemas/guids/{guid}
  else            --> read 0x00 + 4-byte id from the payload
```

Part 5 keeps one consumer open across a cutover on a single topic:

```
prefix history --> upgrade consumer (dual) --> flip producer (header) --> replay both formats
                              |
                              + a prefix-only consumer fails at the cutover offset
```

## What it demonstrates

- **Registry lifecycle.** Register a schema, list subjects and versions, and fetch it by subject, version, numeric id, GUID, or latest. Test compatibility, evolve `User` from v1 (`id`, `name`, `email`) to v2 (a nullable `department`, backward compatible), then soft-delete and hard-delete. The call that is new here is `GET /schemas/guids/{guid}`.
- **Both wire formats, in hex.** One script prints the magic byte and the numeric id in the payload. The next prints `__value_schema_id` and the bare Avro body.
- **The interop matrix.** A prefix producer with a prefix consumer passes. A prefix producer with the dual consumer passes. A header producer with the dual consumer passes. A header producer with a prefix-only consumer fails with `Invalid magic byte`: the legacy consumer treats the first byte of the Avro body as the magic byte.
- **Which side you upgrade first.** Already on Schema Registry with the payload prefix (the path this demo models): upgrade consumers first. The dual deserializer keeps reading prefix records, then you flip producers. Flip producers first and a legacy consumer dies mid-stream at the cutover offset, during the read. Raw Avro that never had Schema Registry is the other starting point: the payload is bare Avro before and after, the header is additive, and producers can move first.
- **A record that carries both formats.** No standard serializer emits both. A proxy, a replicator, a Kafka Streams job, or a custom producer can copy an inbound `__value_schema_id` onto a payload it just re-serialized with a prefix. The dual deserializer trusts the header and passes the entire payload, framing included, to the Avro decoder. The demo record `{id: 99, name: radia perlman, ...}` comes back as `{id: 0, name: '', email: ''}`. No exception. Avro reads the framing as field data and stops when the record looks complete. Java's `DualSchemaIdDeserializer` returns that full payload too. If the prefix says v1 and the header says v2, the header still wins, and v2's `department` appears on a payload written with v1.

## Technical details

- **Stack.** Docker Compose runs `cp-kafka:8.4.0` (KRaft, single broker), `cp-schema-registry:8.4.0` at backward compatibility, and Python 3.12 with `confluent-kafka[avro,schemaregistry]==2.16.1`. Header GUID support landed in Confluent Platform 8.1.1 and in the Python client at 2.10.1. On the JVM the switch is `value.schema.id.serializer=io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer` (and the same property for the key).
- **Portable identity is the win.** A numeric id does not survive a copy into a registry where that counter means something else, so disaster recovery, migration, and multi-region otherwise need id-remapping. A GUID is identical everywhere the schema content is identical. Key bytes stay pure Avro as well, so evolving a key schema leaves the bytes Kafka partitions on unchanged. The README calls that second point narrow, because key schemas rarely evolve.
- **The record gets bigger.** The payload loses 5 bytes. The header adds a 17-byte key, a 17-byte value, and two length varints, about +32 bytes per record. Over 2000 identical `User` records the demo measures 85,988 bytes (prefix) against 149,988 bytes (header) uncompressed, +74%, and 2,363 against 3,241 bytes with zstd, +37%. The header key repeats inside a batch, so compression helps and the overhead remains.
- **The payload is a bare Avro datum.** There is no embedded schema and no sync marker. `avro-tools tojson`, `avro cat`, DuckDB, and Spark refuse it. Fetch the schema by GUID, then use a schemaless reader (`fastavro.schemaless_reader`, or Java's `GenericDatumReader` with a binary decoder). `kafka-avro-console-consumer` understands the header format on its own.
- **Dual-write is not the plan here.** Confluent Platform docs describe a dual-write step with `HeaderAndPayloadSchemaIdSerializer`. That class is absent from the CP 8.4.0 jars this repo inspected, and a message that carries both formats decodes to the wrong record. The path this demo walks does without it: an upgraded consumer already reads prefix messages.

## Challenges & lessons

1. **The header wins, and the prefix stays in the bytes.** Once `__value_schema_id` is present, `dual_schema_id_deserializer` has its schema and feeds every payload byte to Avro, so `0x00 0x00 0x00` becomes id 0 and two empty strings. Strip inbound `__key_schema_id` and `__value_schema_id` before you re-serialize. On a topic you know is prefix-only, set `prefix_schema_id_deserializer` so a stray header is ignored. The same bytes decode differently across a mixed consumer fleet.

2. **Python Avro refuses a header-mode payload of 5 bytes or fewer.** `AvroDeserializer` checks `len(data) <= 5` before it looks at the header and raises `SerializationError`, expecting at least 6 bytes of framing. Prefix mode hides this, because the framing alone is 5 bytes. Header mode makes it ordinary: an `int`, a `boolean`, an enum, an empty record, or a short `string` encodes to 5 bytes or fewer and is unreadable. A value that encodes to 6 bytes (`"abcde"`, a long epoch millis) passes. The guard lives only in the Python Avro deserializer (`_sync/avro.py` and `_async/avro.py`). The Python JSON Schema and Protobuf deserializers have no such check, and the Java client has no minimum length. Keep keys in prefix mode, pick a schema whose body clears 6 bytes, or call `schemaless_reader` yourself after resolving the GUID. `repro/minimal_repro.py` is the standalone reproduction.

3. **Two `__value_schema_id` headers mean two different schemas.** Python walks the header list and honors the first match. Java calls `headers.lastHeader(key)` and honors the last. A proxy that appends a second schema-id header will fan the same record out to different schemas. Strip the inbound header. Do not append another one beside it.

4. **Consumers move first when the payload bytes change.** Prefix records are `[0x00][id][Avro]`. Header records are bare Avro, so a prefix-only consumer breaks on the first new record, at that offset. The dual deserializer covers the old log while producers are still on the prefix. If the topic was raw Avro with no Schema Registry, the payload bytes do not change and producers can go first.

5. **The header has to survive every hop.** A proxy or connector that strips headers drops the only schema id the new format has. Kafka Streams dropped headers before CP 8.3. From 8.3, `dsl.store.format=HEADERS` keeps them. The numeric id still exists on the schema in the registry. The GUID is what this format puts on the wire.
